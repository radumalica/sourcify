#!/usr/bin/env python3
"""
Sourcify BigQuery Sync Service

Syncs data from Sourcify's public BigQuery dataset to a local PostgreSQL database.
This is much more efficient than the parquet-based sync as it queries only unique data.
"""

import os
import sys
import logging
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# Add parent directory to path to import database_importer from sync service
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sync'))
from lib import DatabaseImporter

from lib import BigQueryLoader, StateTracker


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/app/logs/sync_bigquery.log')
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


def sync_table_from_bigquery(
    local_table_name: str,
    bigquery_table_name: str,
    bigquery_loader: BigQueryLoader,
    database_importer: DatabaseImporter,
    batch_size: int,
    incremental: bool = False
) -> dict:
    """
    Sync a table from BigQuery to PostgreSQL.

    Args:
        local_table_name: Name of the local PostgreSQL table
        bigquery_table_name: Name of the BigQuery table (with public_ prefix)
        bigquery_loader: BigQueryLoader instance
        database_importer: DatabaseImporter instance
        batch_size: Batch size for database inserts
        incremental: If True, only sync new data since last sync

    Returns:
        Dictionary with sync statistics
    """
    logger.info(f"=== Syncing {bigquery_table_name} -> {local_table_name} ===")

    stats = {
        'rows_fetched': 0,
        'rows_imported': 0,
        'rows_skipped': 0,
        'errors': 0
    }

    try:
        # Get last sync timestamp for incremental sync
        last_sync_timestamp = None
        if incremental:
            # TODO: Implement state tracking for incremental syncs
            logger.info("Incremental sync not yet implemented, doing full sync")

        # Get total count
        total_count = bigquery_loader.get_table_count(bigquery_table_name, last_sync_timestamp)
        logger.info(f"Total rows to sync from {bigquery_table_name}: {total_count:,}")

        # Query and import data in chunks
        for df_chunk in bigquery_loader.query_table_chunked(
            bigquery_table_name,
            batch_size=batch_size,
            last_sync_timestamp=last_sync_timestamp
        ):
            stats['rows_fetched'] += len(df_chunk)
            
            # Import to database using local table name
            rows_imported = database_importer.import_dataframe(
                df_chunk,
                local_table_name,
                batch_size=batch_size
            )
            
            stats['rows_imported'] += rows_imported
            stats['rows_skipped'] += (len(df_chunk) - rows_imported)

        logger.info(
            f"Completed {local_table_name}: fetched {stats['rows_fetched']:,} rows, "
            f"imported {stats['rows_imported']:,}, skipped {stats['rows_skipped']:,}"
        )

    except Exception as e:
        logger.error(f"Error syncing {local_table_name}: {e}", exc_info=True)
        stats['errors'] += 1

    return stats


def main():
    """Main sync function"""
    # Load environment variables
    load_dotenv()

    logger.info("=" * 80)
    logger.info("Starting Sourcify BigQuery Sync")
    logger.info("=" * 80)

    # Get configuration from environment
    batch_size = int(os.getenv('BATCH_SIZE', '10000'))
    sync_mode = os.getenv('SYNC_MODE', 'full')  # 'full' or 'incremental'
    gcp_project_id = os.getenv('GCP_PROJECT_ID', None)  # Optional for public datasets

    logger.info(f"Configuration:")
    logger.info(f"  Batch size: {batch_size:,}")
    logger.info(f"  Sync mode: {sync_mode}")
    logger.info(f"  GCP Project ID: {gcp_project_id or 'None (using public access)'}")

    start_time = datetime.now()

    try:
        # Connect to database
        logger.info("Connecting to database...")
        conn = get_db_connection()
        logger.info("Database connection established")

        # Initialize components
        bigquery_loader = BigQueryLoader(project_id=gcp_project_id)
        database_importer = DatabaseImporter(conn)

        # List available tables
        available_tables = bigquery_loader.list_tables()
        logger.info(f"Available BigQuery tables: {', '.join(available_tables)}")

        # Get import order (respects foreign key dependencies)
        import_order = database_importer.get_import_order()
        logger.info(f"Import order: {', '.join(import_order)}")

        # Map local table names to BigQuery table names and filter
        tables_to_sync = []
        for local_table in import_order:
            bq_table = bigquery_loader.table_name_mapping.get(local_table)
            if bq_table and bq_table in available_tables:
                tables_to_sync.append((local_table, bq_table))
        
        logger.info(f"Tables to sync: {', '.join([f'{local}->{bq}' for local, bq in tables_to_sync])}")

        # Sync each table in order
        total_stats = {
            'rows_fetched': 0,
            'rows_imported': 0,
            'rows_skipped': 0,
            'errors': 0
        }

        for local_table, bq_table in tables_to_sync:
            table_stats = sync_table_from_bigquery(
                local_table,
                bq_table,
                bigquery_loader,
                database_importer,
                batch_size,
                incremental=(sync_mode == 'incremental')
            )

            # Aggregate stats
            for key in total_stats:
                total_stats[key] += table_stats[key]

        # Print final statistics
        logger.info("=" * 80)
        logger.info("Sync completed!")
        logger.info("=" * 80)
        logger.info(f"Total rows fetched from BigQuery: {total_stats['rows_fetched']:,}")
        logger.info(f"Total rows imported to PostgreSQL: {total_stats['rows_imported']:,}")
        logger.info(f"Total rows skipped (duplicates): {total_stats['rows_skipped']:,}")
        logger.info(f"Errors: {total_stats['errors']}")

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
