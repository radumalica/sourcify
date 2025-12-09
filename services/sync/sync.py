#!/usr/bin/env python3
"""
Sourcify Parquet Sync Service

Syncs data from Sourcify's parquet export to a local PostgreSQL database.
"""

import os
import sys
import logging
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

from lib import ManifestManager, StateTracker, ParquetLoader, DatabaseImporter


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/app/logs/sync.log')
    ]
)
logger = logging.getLogger(__name__)


def get_db_connection():
    """
    Create database connection from environment variables.

    Returns:
        psycopg2 connection object
    """
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=int(os.getenv('POSTGRES_PORT', '5432')),
        database=os.getenv('POSTGRES_DB', 'sourcify'),
        user=os.getenv('POSTGRES_USER', 'sourcify'),
        password=os.getenv('POSTGRES_PASSWORD', 'sourcify'),
    )
    conn.set_session(autocommit=False)
    return conn


def sync_category(
    category: str,
    manifest_manager: ManifestManager,
    state_tracker: StateTracker,
    parquet_loader: ParquetLoader,
    database_importer: DatabaseImporter,
    batch_size: int
) -> dict:
    """
    Sync all files for a specific category.

    Args:
        category: Category name (table name)
        manifest_manager: ManifestManager instance
        state_tracker: StateTracker instance
        parquet_loader: ParquetLoader instance
        database_importer: DatabaseImporter instance
        batch_size: Batch size for database inserts

    Returns:
        Dictionary with sync statistics
    """
    logger.info(f"=== Syncing category: {category} ===")

    # Get files for this category from manifest
    category_files = manifest_manager.get_category_files(category)
    manifest_timestamp = manifest_manager.get_manifest_timestamp()

    if not category_files:
        logger.warning(f"No files found for category {category}")
        return {'files': 0, 'rows': 0, 'downloaded': 0, 'skipped': 0, 'errors': 0}

    logger.info(f"Found {len(category_files)} files for {category} in manifest")

    # Determine which files need to be synced
    files_to_sync = state_tracker.get_files_to_sync(category_files, manifest_timestamp)

    if not files_to_sync:
        logger.info(f"All files for {category} are up to date")
        return {'files': 0, 'rows': 0, 'downloaded': 0, 'skipped': len(category_files), 'errors': 0}

    logger.info(f"Need to sync {len(files_to_sync)} files for {category}")

    stats = {
        'files': len(files_to_sync),
        'rows': 0,
        'downloaded': 0,
        'skipped': len(category_files) - len(files_to_sync),
        'errors': 0
    }

    # Process each file
    for file_info in files_to_sync:
        file_path = file_info['path']
        file_url = file_info['url']

        try:
            logger.info(f"Processing file: {file_path}")

            # Update status to downloading
            state_tracker.update_status(file_path, 'downloading')

            # Download file
            checksum, file_size = parquet_loader.download_file(file_url, file_path)
            stats['downloaded'] += 1

            # Update status to downloaded
            state_tracker.update_status(
                file_path,
                'downloaded',
                file_size=file_size,
                checksum=checksum
            )

            # Update status to importing
            state_tracker.update_status(file_path, 'importing')

            # Import data in chunks
            logger.info(f"Importing data from {file_path} into {category}")
            rows_imported = 0

            for df_chunk in parquet_loader.read_parquet_chunked(file_path, batch_size=batch_size):
                chunk_rows = database_importer.import_dataframe(df_chunk, category, batch_size=batch_size)
                rows_imported += chunk_rows

            # Update rows imported and mark as completed
            state_tracker.set_rows_imported(file_path, rows_imported)
            state_tracker.update_status(file_path, 'completed')

            stats['rows'] += rows_imported

            logger.info(f"Successfully imported {rows_imported:,} rows from {file_path}")

        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}", exc_info=True)
            state_tracker.update_status(file_path, 'failed', error_message=str(e))
            stats['errors'] += 1

    logger.info(
        f"Completed {category}: {stats['downloaded']} files downloaded, "
        f"{stats['rows']:,} rows imported, {stats['errors']} errors"
    )

    return stats


def main():
    """Main sync function"""
    # Load environment variables
    load_dotenv()

    logger.info("=" * 80)
    logger.info("Starting Sourcify Parquet Sync")
    logger.info("=" * 80)

    # Get configuration from environment
    manifest_url = os.getenv('MANIFEST_URL', 'https://export.sourcify.dev/manifest.json')
    cache_dir = os.getenv('PARQUET_CACHE_DIR', '/parquet_cache')
    batch_size = int(os.getenv('BATCH_SIZE', '10000'))
    sync_mode = os.getenv('SYNC_MODE', 'incremental')

    logger.info(f"Configuration:")
    logger.info(f"  Manifest URL: {manifest_url}")
    logger.info(f"  Cache directory: {cache_dir}")
    logger.info(f"  Batch size: {batch_size:,}")
    logger.info(f"  Sync mode: {sync_mode}")

    start_time = datetime.now()

    try:
        # Connect to database
        logger.info("Connecting to database...")
        conn = get_db_connection()
        logger.info("Database connection established")

        # Initialize components
        manifest_manager = ManifestManager(manifest_url)
        state_tracker = StateTracker(conn)
        parquet_loader = ParquetLoader(cache_dir)
        database_importer = DatabaseImporter(conn)

        # Fetch manifest
        logger.info("Fetching manifest...")
        manifest_manager.fetch_manifest()

        # Get import order (respects foreign key dependencies)
        import_order = database_importer.get_import_order()
        logger.info(f"Import order: {', '.join(import_order)}")

        # Sync each category in order
        total_stats = {
            'files': 0,
            'rows': 0,
            'downloaded': 0,
            'skipped': 0,
            'errors': 0
        }

        for category in import_order:
            category_stats = sync_category(
                category,
                manifest_manager,
                state_tracker,
                parquet_loader,
                database_importer,
                batch_size
            )

            # Aggregate stats
            for key in total_stats:
                total_stats[key] += category_stats[key]

        # Print final statistics
        logger.info("=" * 80)
        logger.info("Sync completed!")
        logger.info("=" * 80)
        logger.info(f"Total files processed: {total_stats['files']}")
        logger.info(f"Files downloaded: {total_stats['downloaded']}")
        logger.info(f"Files skipped (already synced): {total_stats['skipped']}")
        logger.info(f"Total rows imported: {total_stats['rows']:,}")
        logger.info(f"Errors: {total_stats['errors']}")

        # Print database statistics
        logger.info("\n" + "=" * 80)
        logger.info("Database statistics:")
        logger.info("=" * 80)
        db_stats = state_tracker.get_sync_statistics()

        for category_stat in db_stats['by_category']:
            logger.info(
                f"  {category_stat['category']}: "
                f"{category_stat['completed_files']}/{category_stat['total_files']} files, "
                f"{category_stat['total_rows_imported']:,} rows"
            )

        logger.info("\nOverall:")
        logger.info(f"  Total files: {db_stats['total']['files']}")
        logger.info(f"  Completed: {db_stats['total']['completed']}")
        logger.info(f"  Failed: {db_stats['total']['failed']}")
        logger.info(f"  Total rows: {db_stats['total']['rows']:,}")

        # Calculate duration
        duration = datetime.now() - start_time
        logger.info(f"\nSync duration: {duration}")

        # Close database connection
        conn.close()

        # Exit with appropriate code
        exit_code = 0 if total_stats['errors'] == 0 else 1
        sys.exit(exit_code)

    except Exception as e:
        logger.error(f"Fatal error during sync: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
