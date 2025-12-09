#!/usr/bin/env python3
"""
Restore Indexes Utility

Manually restore indexes from interrupted sync runs.
Useful if sync was interrupted and you want to restore indexes before starting a new sync.
"""

import os
import sys
import logging
import psycopg2
from dotenv import load_dotenv

from lib import IndexStateManager
from lib.database_importer_optimized import OptimizedDatabaseImporter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def get_db_connection():
    """Create database connection from environment variables"""
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=int(os.getenv('POSTGRES_PORT', '5432')),
        database=os.getenv('POSTGRES_DB', 'sourcify'),
        user=os.getenv('POSTGRES_USER', 'sourcify'),
        password=os.getenv('POSTGRES_PASSWORD', 'sourcify'),
    )
    conn.set_session(autocommit=False)
    return conn


def main():
    """Restore indexes from previous interrupted sync"""
    load_dotenv()

    logger.info("=" * 80)
    logger.info("Index Restoration Utility")
    logger.info("=" * 80)

    # Check for pending restorations
    index_state = IndexStateManager()
    pending_tables = index_state.get_all_pending_tables()

    if not pending_tables:
        logger.info("No pending index restorations found.")
        logger.info("All tables have their indexes intact.")
        return

    logger.info(f"Found {len(pending_tables)} tables with pending index restoration:")
    for table in pending_tables:
        pending = index_state.get_pending_indexes(table)
        logger.info(f"  - {table}: {len(pending)} indexes")

    # Ask for confirmation
    response = input("\nRestore all indexes? (yes/no): ").strip().lower()
    if response not in ['yes', 'y']:
        logger.info("Restoration cancelled.")
        return

    # Connect to database
    logger.info("\nConnecting to database...")
    conn = get_db_connection()

    try:
        importer = OptimizedDatabaseImporter(conn, use_copy=True, manage_indexes=True)

        # Restore each table
        for table in pending_tables:
            logger.info(f"\nRestoring indexes for {table}...")
            pending = index_state.get_pending_indexes(table)

            try:
                importer._restore_indexes_from_state(table, pending)
                index_state.mark_indexes_restored(table)
                logger.info(f"Successfully restored {len(pending)} indexes for {table}")
            except Exception as e:
                logger.error(f"Error restoring indexes for {table}: {e}")
                continue

        logger.info("\n" + "=" * 80)
        logger.info("Index restoration complete!")
        logger.info("=" * 80)

    finally:
        conn.close()


if __name__ == '__main__':
    main()
