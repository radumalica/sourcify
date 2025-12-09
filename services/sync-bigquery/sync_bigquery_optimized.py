#!/usr/bin/env python3
"""
Optimized Sourcify BigQuery Sync Service

High-performance sync with:
- Multi-threaded producer-consumer pattern
- PostgreSQL COPY instead of INSERT
- Automatic index management
- Performance monitoring
- Resume capability

Can handle 10M+ rows efficiently (hours instead of days).
"""

import os
import sys
import logging
import psycopg2
import threading
import queue
import time
import signal
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, Optional
import pandas as pd

from lib import BigQueryLoader, StateTracker
from lib.database_importer_optimized import OptimizedDatabaseImporter

# Global flag for clean shutdown
shutdown_requested = False


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/app/logs/sync_bigquery_optimized.log')
    ]
)
logger = logging.getLogger(__name__)


class PerformanceMonitor:
    """Monitors and reports sync performance metrics"""

    def __init__(self):
        self.start_time = None
        self.table_stats = {}
        self.lock = threading.Lock()

    def start_sync(self):
        """Mark the start of sync"""
        self.start_time = time.time()

    def start_table(self, table_name: str, total_rows: int):
        """Start tracking a table"""
        with self.lock:
            self.table_stats[table_name] = {
                'total_rows': total_rows,
                'rows_processed': 0,
                'rows_imported': 0,
                'start_time': time.time(),
                'end_time': None,
                'batches_processed': 0
            }

    def update_progress(self, table_name: str, rows_fetched: int, rows_imported: int):
        """Update progress for a table"""
        with self.lock:
            if table_name in self.table_stats:
                stats = self.table_stats[table_name]
                stats['rows_processed'] += rows_fetched
                stats['rows_imported'] += rows_imported
                stats['batches_processed'] += 1

                # Calculate progress
                if stats['total_rows'] > 0:
                    progress_pct = (stats['rows_processed'] / stats['total_rows']) * 100
                    elapsed = time.time() - stats['start_time']
                    rate = stats['rows_processed'] / elapsed if elapsed > 0 else 0
                    eta_seconds = (stats['total_rows'] - stats['rows_processed']) / rate if rate > 0 else 0

                    logger.info(
                        f"{table_name}: {progress_pct:.1f}% complete "
                        f"({stats['rows_processed']:,}/{stats['total_rows']:,} rows) - "
                        f"{rate:.0f} rows/sec - "
                        f"ETA: {self._format_duration(eta_seconds)}"
                    )

    def finish_table(self, table_name: str):
        """Mark table as complete"""
        with self.lock:
            if table_name in self.table_stats:
                stats = self.table_stats[table_name]
                stats['end_time'] = time.time()

                duration = stats['end_time'] - stats['start_time']
                rate = stats['rows_imported'] / duration if duration > 0 else 0

                logger.info(
                    f"{table_name} COMPLETE: "
                    f"{stats['rows_imported']:,} rows imported in {self._format_duration(duration)} "
                    f"({rate:.0f} rows/sec)"
                )

    def get_summary(self) -> Dict:
        """Get summary statistics"""
        with self.lock:
            total_rows_processed = sum(s['rows_processed'] for s in self.table_stats.values())
            total_rows_imported = sum(s['rows_imported'] for s in self.table_stats.values())
            total_duration = time.time() - self.start_time if self.start_time else 0

            return {
                'total_rows_processed': total_rows_processed,
                'total_rows_imported': total_rows_imported,
                'total_duration': total_duration,
                'overall_rate': total_rows_imported / total_duration if total_duration > 0 else 0,
                'tables': dict(self.table_stats)
            }

    def _format_duration(self, seconds: float) -> str:
        """Format duration in human-readable format"""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds/60:.1f}m"
        else:
            hours = int(seconds / 3600)
            minutes = int((seconds % 3600) / 60)
            return f"{hours}h {minutes}m"


class ParallelSyncWorker:
    """Worker that processes batches from BigQuery and imports to PostgreSQL"""

    def __init__(
        self,
        batch_queue: queue.Queue,
        table_name: str,
        conn_params: Dict,
        monitor: PerformanceMonitor,
        use_copy: bool = True
    ):
        self.batch_queue = batch_queue
        self.table_name = table_name
        self.conn_params = conn_params
        self.monitor = monitor
        self.use_copy = use_copy
        self.total_imported = 0
        self.thread = None
        self.error = None
        self.shutdown_event = threading.Event()

    def start(self):
        """Start the worker thread"""
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def stop(self):
        """Signal worker to stop"""
        self.shutdown_event.set()

    def _run(self):
        """Worker thread main loop"""
        conn = None
        try:
            # Create dedicated connection for this worker
            conn = psycopg2.connect(**self.conn_params)
            conn.set_session(autocommit=False)

            importer = OptimizedDatabaseImporter(
                conn,
                use_copy=self.use_copy,
                manage_indexes=False  # Indexes managed at table level
            )

            while not self.shutdown_event.is_set():
                try:
                    # Get batch from queue (with timeout to check for shutdown)
                    batch = self.batch_queue.get(timeout=1)

                    if batch is None:  # Poison pill - shutdown signal
                        break

                    # Check shutdown before processing
                    if self.shutdown_event.is_set():
                        break

                    # Import batch
                    rows_imported = importer.import_dataframe(
                        batch['data'],
                        self.table_name,
                        batch_size=100000
                    )

                    self.total_imported += rows_imported

                    # Update progress
                    self.monitor.update_progress(
                        self.table_name,
                        len(batch['data']),
                        rows_imported
                    )

                    self.batch_queue.task_done()

                except queue.Empty:
                    continue
                except Exception as e:
                    if not self.shutdown_event.is_set():
                        logger.error(f"Error in worker thread: {e}", exc_info=True)
                        self.error = e
                    break

        except Exception as e:
            if not self.shutdown_event.is_set():
                logger.error(f"Fatal error in worker: {e}", exc_info=True)
                self.error = e
        finally:
            if conn:
                try:
                    conn.rollback()  # Rollback any pending transaction
                    conn.close()
                except:
                    pass

    def join(self, timeout=10):
        """Wait for worker to complete with timeout"""
        if self.thread:
            self.thread.join(timeout=timeout)
            if self.thread.is_alive():
                logger.warning(f"Worker thread did not exit cleanly within {timeout}s")


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


def get_connection_params() -> Dict:
    """Get connection parameters as dict"""
    return {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': int(os.getenv('POSTGRES_PORT', '5432')),
        'database': os.getenv('POSTGRES_DB', 'sourcify'),
        'user': os.getenv('POSTGRES_USER', 'sourcify'),
        'password': os.getenv('POSTGRES_PASSWORD', 'sourcify'),
    }


def sync_table_parallel(
    local_table_name: str,
    bigquery_table_name: str,
    bigquery_loader: BigQueryLoader,
    conn_params: Dict,
    monitor: PerformanceMonitor,
    batch_size: int,
    num_workers: int = 2,
    use_copy: bool = True,
    manage_indexes: bool = True
) -> dict:
    """
    Sync a table using parallel producer-consumer pattern.

    Args:
        local_table_name: Name of local PostgreSQL table
        bigquery_table_name: Name of BigQuery table
        bigquery_loader: BigQueryLoader instance
        conn_params: Database connection parameters
        monitor: Performance monitor
        batch_size: Batch size for BigQuery queries
        num_workers: Number of parallel import workers
        use_copy: Use COPY instead of INSERT
        manage_indexes: Drop/recreate indexes

    Returns:
        Dictionary with sync statistics
    """
    logger.info(f"=== Syncing {bigquery_table_name} -> {local_table_name} (parallel mode) ===")

    stats = {
        'rows_fetched': 0,
        'rows_imported': 0,
        'errors': 0
    }

    workers = []

    try:
        # Get total count
        total_count = bigquery_loader.get_table_count(bigquery_table_name)
        logger.info(f"Total rows to sync: {total_count:,}")

        monitor.start_table(local_table_name, total_count)

        # Manage indexes
        if manage_indexes:
            logger.info("Preparing table for bulk import (dropping indexes)...")
            conn = psycopg2.connect(**conn_params)
            try:
                importer = OptimizedDatabaseImporter(conn, use_copy=use_copy, manage_indexes=True)
                importer.prepare_table_for_import(local_table_name)
            finally:
                conn.close()

        # Create queue for batches
        batch_queue = queue.Queue(maxsize=num_workers * 2)  # Buffer 2x workers

        # Start worker threads
        for i in range(num_workers):
            worker = ParallelSyncWorker(
                batch_queue,
                local_table_name,
                conn_params,
                monitor,
                use_copy=use_copy
            )
            worker.start()
            workers.append(worker)
            logger.info(f"Started worker {i+1}/{num_workers}")

        # Producer: Fetch batches from BigQuery and queue them
        logger.info("Starting batch fetch from BigQuery...")
        for df_chunk in bigquery_loader.query_table_chunked(
            bigquery_table_name,
            batch_size=batch_size
        ):
            # Check for shutdown signal
            if shutdown_requested:
                logger.info("Shutdown requested, stopping batch fetch...")
                break

            stats['rows_fetched'] += len(df_chunk)

            # Queue batch for workers
            batch_queue.put({
                'data': df_chunk,
                'size': len(df_chunk)
            })

        # Wait for all batches to be processed (with timeout)
        if not shutdown_requested:
            logger.info("Waiting for workers to complete...")
            try:
                # Wait with timeout to allow Ctrl+C
                while batch_queue.unfinished_tasks > 0:
                    time.sleep(0.5)
                    if shutdown_requested:
                        break
            except KeyboardInterrupt:
                logger.info("Interrupted during queue join")

        # Send shutdown signal to workers
        logger.info("Stopping workers...")
        for worker in workers:
            worker.stop()

        # Send poison pills
        for _ in workers:
            try:
                batch_queue.put(None, timeout=1)
            except queue.Full:
                pass

        # Wait for workers to finish (with timeout)
        for i, worker in enumerate(workers):
            worker.join(timeout=5)
            if worker.error and not shutdown_requested:
                logger.error(f"Worker {i+1} encountered error: {worker.error}")
                stats['errors'] += 1
            stats['rows_imported'] += worker.total_imported

        if not shutdown_requested:
            monitor.finish_table(local_table_name)

            # Recreate indexes
            if manage_indexes:
                logger.info("Finalizing table (recreating indexes)...")
                conn = psycopg2.connect(**conn_params)
                try:
                    importer = OptimizedDatabaseImporter(conn, use_copy=use_copy, manage_indexes=True)
                    importer.finalize_table_after_import(local_table_name)
                finally:
                    conn.close()

            logger.info(
                f"Completed {local_table_name}: "
                f"fetched {stats['rows_fetched']:,}, "
                f"imported {stats['rows_imported']:,}"
            )
        else:
            logger.info(f"Sync interrupted for {local_table_name}: {stats['rows_imported']:,} rows imported before shutdown")

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, cleaning up...")
        stats['errors'] += 1
    except Exception as e:
        logger.error(f"Error syncing {local_table_name}: {e}", exc_info=True)
        stats['errors'] += 1
    finally:
        # Always stop workers on exit
        for worker in workers:
            worker.stop()

    return stats


def signal_handler(signum, frame):
    """Handle shutdown signals cleanly"""
    global shutdown_requested
    if not shutdown_requested:
        logger.info("\n" + "=" * 80)
        logger.info("Shutdown signal received (Ctrl+C), cleaning up...")
        logger.info("Please wait for workers to finish current batch...")
        logger.info("=" * 80)
        shutdown_requested = True
    else:
        logger.warning("Forced shutdown! Exiting immediately...")
        sys.exit(1)


def main():
    """Main sync function"""
    global shutdown_requested

    load_dotenv()

    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("=" * 80)
    logger.info("Starting OPTIMIZED Sourcify BigQuery Sync")
    logger.info("=" * 80)

    # Configuration
    batch_size = int(os.getenv('BATCH_SIZE', '100000'))  # Increased from 10k
    num_workers = int(os.getenv('NUM_WORKERS', '3'))  # Parallel import workers
    use_copy = os.getenv('USE_COPY', 'true').lower() == 'true'
    manage_indexes = os.getenv('MANAGE_INDEXES', 'true').lower() == 'true'
    gcp_project_id = os.getenv('GCP_PROJECT_ID', None)

    logger.info(f"Configuration:")
    logger.info(f"  Batch size: {batch_size:,}")
    logger.info(f"  Worker threads: {num_workers}")
    logger.info(f"  Use COPY: {use_copy}")
    logger.info(f"  Manage indexes: {manage_indexes}")
    logger.info(f"  GCP Project ID: {gcp_project_id or 'None'}")

    monitor = PerformanceMonitor()
    monitor.start_sync()

    try:
        # Initialize components
        bigquery_loader = BigQueryLoader(project_id=gcp_project_id)
        conn_params = get_connection_params()

        # Test connection
        logger.info("Testing database connection...")
        conn = get_db_connection()
        conn.close()
        logger.info("Database connection successful")

        # Get tables to sync
        available_tables = bigquery_loader.list_tables()
        logger.info(f"Available BigQuery tables: {', '.join(available_tables)}")

        # Create temporary importer to get import order
        conn = get_db_connection()
        importer = OptimizedDatabaseImporter(conn, use_copy=use_copy, manage_indexes=manage_indexes)
        import_order = importer.get_import_order()
        conn.close()

        logger.info(f"Import order: {', '.join(import_order)}")

        # Map and filter tables
        tables_to_sync = []
        for local_table in import_order:
            bq_table = bigquery_loader.table_name_mapping.get(local_table)
            if bq_table and bq_table in available_tables:
                tables_to_sync.append((local_table, bq_table))

        logger.info(f"Tables to sync: {', '.join([f'{local}->{bq}' for local, bq in tables_to_sync])}")

        # Sync each table
        total_stats = {
            'rows_fetched': 0,
            'rows_imported': 0,
            'errors': 0
        }

        for local_table, bq_table in tables_to_sync:
            # Check for shutdown signal
            if shutdown_requested:
                logger.info("Shutdown requested, skipping remaining tables...")
                break

            table_stats = sync_table_parallel(
                local_table,
                bq_table,
                bigquery_loader,
                conn_params,
                monitor,
                batch_size,
                num_workers=num_workers,
                use_copy=use_copy,
                manage_indexes=manage_indexes
            )

            for key in total_stats:
                total_stats[key] += table_stats[key]

        # Print summary
        summary = monitor.get_summary()

        logger.info("=" * 80)
        if shutdown_requested:
            logger.info("SYNC INTERRUPTED!")
        else:
            logger.info("SYNC COMPLETE!")
        logger.info("=" * 80)
        logger.info(f"Total rows processed: {summary['total_rows_processed']:,}")
        logger.info(f"Total rows imported: {summary['total_rows_imported']:,}")
        logger.info(f"Total duration: {monitor._format_duration(summary['total_duration'])}")
        logger.info(f"Overall rate: {summary['overall_rate']:.0f} rows/sec")
        logger.info(f"Errors: {total_stats['errors']}")

        exit_code = 0 if (total_stats['errors'] == 0 and not shutdown_requested) else 1
        sys.exit(exit_code)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt in main, exiting...")
        sys.exit(130)  # Standard exit code for Ctrl+C
    except Exception as e:
        logger.error(f"Fatal error during sync: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
