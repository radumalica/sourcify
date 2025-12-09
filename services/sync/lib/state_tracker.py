"""
State Tracker

Manages the parquet_sync_state table for tracking import progress.
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class StateTracker:
    """Manages sync state in the parquet_sync_state table"""

    def __init__(self, db_connection):
        """
        Initialize the state tracker.

        Args:
            db_connection: psycopg2 database connection
        """
        self.conn = db_connection

    def get_file_state(self, file_path: str) -> Optional[Dict]:
        """
        Get the sync state for a specific file.

        Args:
            file_path: Path to the parquet file

        Returns:
            State dictionary or None if not found
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT * FROM parquet_sync_state WHERE file_path = %s",
                (file_path,)
            )
            result = cursor.fetchone()
            return dict(result) if result else None

    def register_file(self, file_path: str, category: str, manifest_timestamp: datetime):
        """
        Register a new file in the sync state table.

        Args:
            file_path: Path to the parquet file
            category: Category name
            manifest_timestamp: Timestamp from manifest
        """
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO parquet_sync_state
                    (file_path, category, manifest_timestamp, import_status)
                VALUES (%s, %s, %s, 'pending')
                ON CONFLICT (file_path) DO UPDATE
                SET manifest_timestamp = EXCLUDED.manifest_timestamp,
                    updated_at = NOW()
                """,
                (file_path, category, manifest_timestamp)
            )
        self.conn.commit()

    def update_status(
        self,
        file_path: str,
        status: str,
        error_message: Optional[str] = None,
        file_size: Optional[int] = None,
        checksum: Optional[str] = None
    ):
        """
        Update the status of a file.

        Args:
            file_path: Path to the parquet file
            status: New status ('downloading', 'downloaded', 'importing', 'completed', 'failed')
            error_message: Optional error message for failed imports
            file_size: Optional file size in bytes
            checksum: Optional SHA256 checksum
        """
        with self.conn.cursor() as cursor:
            update_fields = ["import_status = %s", "updated_at = NOW()"]
            values = [status]

            if status == 'downloaded':
                update_fields.append("downloaded_at = NOW()")
            elif status == 'completed':
                update_fields.append("imported_at = NOW()")

            if error_message:
                update_fields.append("error_message = %s")
                values.append(error_message)
                update_fields.append("retry_count = retry_count + 1")

            if file_size:
                update_fields.append("file_size_bytes = %s")
                values.append(file_size)

            if checksum:
                update_fields.append("checksum = %s")
                values.append(checksum)

            values.append(file_path)

            query = f"""
                UPDATE parquet_sync_state
                SET {', '.join(update_fields)}
                WHERE file_path = %s
            """

            cursor.execute(query, values)
        self.conn.commit()

    def set_rows_imported(self, file_path: str, rows_imported: int):
        """
        Set the number of rows imported for a file.

        Args:
            file_path: Path to the parquet file
            rows_imported: Number of rows imported
        """
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE parquet_sync_state
                SET rows_imported = %s, updated_at = NOW()
                WHERE file_path = %s
                """,
                (rows_imported, file_path)
            )
        self.conn.commit()

    def get_pending_files(self, category: Optional[str] = None) -> List[Dict]:
        """
        Get all files that need to be synced.

        Args:
            category: Optional category filter

        Returns:
            List of file state dictionaries
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            if category:
                cursor.execute(
                    """
                    SELECT * FROM parquet_sync_state
                    WHERE category = %s AND import_status != 'completed'
                    ORDER BY file_path
                    """,
                    (category,)
                )
            else:
                cursor.execute(
                    """
                    SELECT * FROM parquet_sync_state
                    WHERE import_status != 'completed'
                    ORDER BY category, file_path
                    """
                )
            return [dict(row) for row in cursor.fetchall()]

    def get_files_to_sync(
        self,
        manifest_files: List[Dict],
        manifest_timestamp: datetime
    ) -> List[Dict]:
        """
        Determine which files need to be synced based on manifest.

        Args:
            manifest_files: List of file info from manifest
            manifest_timestamp: Current manifest timestamp

        Returns:
            List of files that need to be synced
        """
        files_to_sync = []

        for file_info in manifest_files:
            file_path = file_info['path']
            state = self.get_file_state(file_path)

            if not state:
                # New file, needs to be synced
                logger.info(f"New file detected: {file_path}")
                self.register_file(
                    file_path,
                    file_info['category'],
                    manifest_timestamp
                )
                files_to_sync.append(file_info)

            elif state['import_status'] != 'completed':
                # File exists but not completed
                logger.info(f"Incomplete file detected: {file_path} (status: {state['import_status']})")
                files_to_sync.append(file_info)

            elif state['manifest_timestamp'] < manifest_timestamp:
                # File updated in manifest
                logger.info(f"Updated file detected: {file_path}")
                self.register_file(
                    file_path,
                    file_info['category'],
                    manifest_timestamp
                )
                files_to_sync.append(file_info)

        return files_to_sync

    def get_sync_statistics(self) -> Dict:
        """
        Get sync statistics.

        Returns:
            Dictionary with statistics
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT
                    category,
                    COUNT(*) as total_files,
                    SUM(CASE WHEN import_status = 'completed' THEN 1 ELSE 0 END) as completed_files,
                    SUM(CASE WHEN import_status = 'failed' THEN 1 ELSE 0 END) as failed_files,
                    SUM(rows_imported) as total_rows_imported,
                    SUM(file_size_bytes) as total_bytes,
                    MAX(imported_at) as last_import
                FROM parquet_sync_state
                GROUP BY category
                ORDER BY category
                """
            )
            stats = [dict(row) for row in cursor.fetchall()]

        return {
            'by_category': stats,
            'total': {
                'files': sum(s['total_files'] for s in stats),
                'completed': sum(s['completed_files'] for s in stats),
                'failed': sum(s['failed_files'] for s in stats),
                'rows': sum(s['total_rows_imported'] or 0 for s in stats),
            }
        }
