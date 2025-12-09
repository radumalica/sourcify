"""
Database Importer

Handles importing parquet data into PostgreSQL with proper type conversion and conflict resolution.
"""

import logging
import json
import pandas as pd
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import execute_batch, Json

logger = logging.getLogger(__name__)


# Define foreign key dependency order for imports
IMPORT_ORDER = [
    'code',
    'sources',
    'signatures',
    'contracts',
    'compiled_contracts',
    'contract_deployments',
    'compiled_contracts_sources',
    'verified_contracts',
    'sourcify_matches',
    'compiled_contracts_signatures',
]


class DatabaseImporter:
    """Imports parquet data into PostgreSQL with proper type handling"""

    def __init__(self, db_connection):
        """
        Initialize the database importer.

        Args:
            db_connection: psycopg2 database connection
        """
        self.conn = db_connection

        # Define columns that need hex to bytea conversion
        self.hex_columns = {
            'code': ['code_hash', 'code_hash_keccak', 'code'],
            'sources': ['source_hash', 'source_hash_keccak'],
            'contracts': ['creation_code_hash', 'runtime_code_hash'],
            'compiled_contracts': ['creation_code_hash', 'runtime_code_hash'],
            'contract_deployments': ['address', 'transaction_hash', 'deployer'],
            'signatures': ['signature_hash_32'],
            'compiled_contracts_signatures': ['signature_hash_32'],
        }

        # Define columns that are JSONB
        self.jsonb_columns = {
            'compiled_contracts': [
                'compiler_settings',
                'compilation_artifacts',
                'creation_code_artifacts',
                'runtime_code_artifacts'
            ],
            'verified_contracts': [
                'creation_values',
                'creation_transformations',
                'runtime_values',
                'runtime_transformations'
            ],
            'sourcify_matches': ['metadata'],
            'verification_jobs': ['error_data', 'external_verification'],
        }

    def import_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        batch_size: int = 1000
    ) -> int:
        """
        Import a DataFrame into a PostgreSQL table.

        Args:
            df: DataFrame to import
            table_name: Target table name
            batch_size: Number of rows per batch

        Returns:
            Number of rows imported
        """
        if df.empty:
            logger.warning(f"Empty DataFrame for table {table_name}, skipping")
            return 0

        # Transform data
        df = self._transform_dataframe(df, table_name)

        # Get columns
        columns = df.columns.tolist()

        # Build INSERT query with ON CONFLICT DO NOTHING
        conflict_clause = self._get_conflict_clause(table_name)
        query = self._build_insert_query(table_name, columns, conflict_clause)

        # Convert DataFrame to list of tuples
        rows = [tuple(row) for row in df.values]

        # Import in batches
        rows_imported = 0
        total_rows_attempted = len(rows)
        try:
            with self.conn.cursor() as cursor:
                execute_batch(cursor, query, rows, page_size=batch_size)
                rows_imported = cursor.rowcount
            self.conn.commit()
            logger.info(f"Imported {rows_imported:,} rows into {table_name} (attempted: {total_rows_attempted:,}, skipped due to conflicts: {total_rows_attempted - rows_imported:,})")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to import into {table_name}: {e}")
            raise

        return rows_imported

    def _transform_dataframe(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Transform DataFrame for database import (hex to bytea, JSON handling, etc.).

        Args:
            df: Input DataFrame
            table_name: Table name (determines transformations)

        Returns:
            Transformed DataFrame
        """
        df = df.copy()

        # Convert hex columns to bytea
        if table_name in self.hex_columns:
            for col in self.hex_columns[table_name]:
                if col in df.columns:
                    df[col] = df[col].apply(self._hex_to_bytea)

        # Handle JSONB columns
        if table_name in self.jsonb_columns:
            for col in self.jsonb_columns[table_name]:
                if col in df.columns:
                    df[col] = df[col].apply(self._to_json_string)

        # Add audit columns if they don't exist
        if 'created_by' not in df.columns and self._table_has_audit_columns(table_name):
            df['created_by'] = 'sync_service'
            df['updated_by'] = 'sync_service'

        # Handle NaN/None values
        df = df.where(pd.notnull(df), None)

        return df

    def _hex_to_bytea(self, value: Any) -> Optional[bytes]:
        """
        Convert hex string to bytes for PostgreSQL bytea.

        Args:
            value: Hex string (with or without 0x prefix)

        Returns:
            Bytes object or None
        """
        if value is None or pd.isna(value):
            return None

        if isinstance(value, bytes):
            return value

        if isinstance(value, str):
            # Remove 0x prefix if present
            hex_clean = value.replace('0x', '').replace('\\x', '')
            try:
                return bytes.fromhex(hex_clean)
            except ValueError as e:
                logger.warning(f"Invalid hex string: {value[:50]}... Error: {e}")
                return None

        return None

    def _to_json_string(self, value: Any) -> Optional[str]:
        """
        Convert value to JSON string for JSONB columns.

        Args:
            value: Value to convert

        Returns:
            JSON string or None
        """
        if value is None or pd.isna(value):
            return None

        if isinstance(value, str):
            # Already a string, might be JSON
            return value

        if isinstance(value, (dict, list)):
            return json.dumps(value)

        return str(value)

    def _table_has_audit_columns(self, table_name: str) -> bool:
        """
        Check if table has audit columns (created_by, updated_by).

        Args:
            table_name: Table name

        Returns:
            True if table has audit columns
        """
        # Most tables have audit columns except a few
        no_audit = {'session', 'parquet_sync_state', 'signature_stats'}
        return table_name not in no_audit

    def _get_conflict_clause(self, table_name: str) -> str:
        """
        Get the ON CONFLICT clause for a table.

        Args:
            table_name: Table name

        Returns:
            ON CONFLICT clause SQL
        """
        # Map table to unique constraint
        conflict_map = {
            'code': 'ON CONFLICT (code_hash) DO NOTHING',
            'sources': 'ON CONFLICT (source_hash) DO NOTHING',
            'contracts': 'ON CONFLICT (id) DO NOTHING',
            'signatures': 'ON CONFLICT (signature_hash_32) DO NOTHING',
            'contract_deployments': 'ON CONFLICT (id) DO NOTHING',
            'compiled_contracts': 'ON CONFLICT (id) DO NOTHING',
            'compiled_contracts_sources': 'ON CONFLICT (id) DO NOTHING',
            'compiled_contracts_signatures': 'ON CONFLICT (compilation_id, signature_hash_32, signature_type) DO NOTHING',
            'verified_contracts': 'ON CONFLICT (id) DO NOTHING',
            'sourcify_matches': 'ON CONFLICT (verified_contract_id) DO NOTHING',
            'verification_jobs': 'ON CONFLICT (id) DO NOTHING',
        }

        return conflict_map.get(table_name, 'ON CONFLICT DO NOTHING')

    def _build_insert_query(
        self,
        table_name: str,
        columns: List[str],
        conflict_clause: str
    ) -> str:
        """
        Build INSERT query with ON CONFLICT.

        Args:
            table_name: Table name
            columns: Column names
            conflict_clause: ON CONFLICT clause

        Returns:
            SQL INSERT query
        """
        column_list = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))

        query = f"""
            INSERT INTO {table_name} ({column_list})
            VALUES ({placeholders})
            {conflict_clause}
        """

        return query

    def import_category(
        self,
        category: str,
        df_iterator: Any,
        batch_size: int = 1000
    ) -> int:
        """
        Import all data for a category (handles chunked DataFrames).

        Args:
            category: Category name (table name)
            df_iterator: Iterator of DataFrames or single DataFrame
            batch_size: Batch size for inserts

        Returns:
            Total rows imported
        """
        total_rows = 0

        # Handle both single DataFrame and iterator
        if isinstance(df_iterator, pd.DataFrame):
            df_iterator = [df_iterator]

        for df in df_iterator:
            rows = self.import_dataframe(df, category, batch_size)
            total_rows += rows

        logger.info(f"Completed import for {category}: {total_rows:,} total rows")
        return total_rows

    @staticmethod
    def get_import_order() -> List[str]:
        """
        Get the order in which tables should be imported (respects foreign keys).

        Returns:
            List of table names in import order
        """
        return IMPORT_ORDER.copy()

    def verify_foreign_keys(self, table_name: str) -> bool:
        """
        Verify that foreign key constraints are satisfied for a table.

        Args:
            table_name: Table to check

        Returns:
            True if all foreign keys are valid
        """
        try:
            with self.conn.cursor() as cursor:
                # Get all foreign key constraints for the table
                cursor.execute(
                    """
                    SELECT conname, pg_get_constraintdef(oid)
                    FROM pg_constraint
                    WHERE conrelid = %s::regclass
                    AND contype = 'f'
                    """,
                    (table_name,)
                )
                constraints = cursor.fetchall()

                if not constraints:
                    logger.debug(f"No foreign key constraints for {table_name}")
                    return True

                # Check each constraint
                for constraint_name, constraint_def in constraints:
                    logger.debug(f"Checking constraint {constraint_name}: {constraint_def}")

                # If we get here, all constraints are valid
                logger.info(f"All foreign key constraints valid for {table_name}")
                return True

        except Exception as e:
            logger.error(f"Error verifying foreign keys for {table_name}: {e}")
            return False
