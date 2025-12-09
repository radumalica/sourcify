"""
Optimized Database Importer

High-performance importer using PostgreSQL COPY, index management, and parallel processing.
Designed for bulk loading millions of rows efficiently.
"""

import logging
import psycopg2
from psycopg2 import sql
import pandas as pd
from typing import List, Dict, Optional, Set
from io import StringIO
import json
import time

logger = logging.getLogger(__name__)


class OptimizedDatabaseImporter:
    """Optimized importer for PostgreSQL with COPY and index management"""

    def __init__(self, conn, use_copy: bool = True, manage_indexes: bool = True):
        """
        Initialize the optimized database importer.

        Args:
            conn: psycopg2 connection object
            use_copy: Use COPY instead of INSERT (much faster)
            manage_indexes: Automatically drop/recreate indexes during bulk import
        """
        self.conn = conn
        self.use_copy = use_copy
        self.manage_indexes = manage_indexes

        # Track which indexes have been dropped
        self.dropped_indexes: Dict[str, List[Dict]] = {}

        # Define import order based on foreign key dependencies
        self.import_order = [
            'code',
            'sources',
            'contracts',
            'compiled_contracts',
            'compiled_contracts_sources',
            'contract_deployments',
            'verified_contracts',
            'sourcify_matches',
            'signatures',
            'compiled_contracts_signatures',
        ]

        # Apply performance optimizations
        self._optimize_connection()

    def _optimize_connection(self):
        """Apply PostgreSQL performance optimizations for bulk loading"""
        cursor = self.conn.cursor()
        try:
            logger.info("Applying PostgreSQL performance optimizations...")

            # Disable synchronous commit (huge performance gain for bulk inserts)
            cursor.execute("SET synchronous_commit = OFF")

            # Increase work memory for sorting and aggregation
            cursor.execute("SET work_mem = '256MB'")

            # Increase maintenance work memory for index creation
            cursor.execute("SET maintenance_work_mem = '2GB'")

            # Disable JIT compilation (can slow down bulk operations)
            cursor.execute("SET jit = OFF")

            # Note: checkpoint_timeout and max_wal_size require server restart or config file
            # Use postgres-bulk-load.conf for these settings

            self.conn.commit()
            logger.info("Performance optimizations applied")

        except Exception as e:
            logger.warning(f"Could not apply some performance optimizations: {e}")
            self.conn.rollback()
        finally:
            cursor.close()

    def get_import_order(self) -> List[str]:
        """Get the order in which tables should be imported (respects FK dependencies)"""
        return self.import_order

    def prepare_table_for_import(self, table_name: str):
        """
        Prepare a table for bulk import by dropping indexes and disabling triggers.

        Args:
            table_name: Name of the table to prepare
        """
        if not self.manage_indexes:
            return

        logger.info(f"Preparing {table_name} for bulk import...")

        # Get and drop indexes (except primary key and unique constraints)
        indexes = self._get_table_indexes(table_name)
        if indexes:
            self._drop_indexes(table_name, indexes)

        # Disable autovacuum for this table during import
        self._set_table_autovacuum(table_name, enabled=False)

        logger.info(f"Table {table_name} prepared for import")

    def finalize_table_after_import(self, table_name: str):
        """
        Finalize a table after bulk import by recreating indexes and enabling triggers.

        Args:
            table_name: Name of the table to finalize
        """
        if not self.manage_indexes:
            return

        logger.info(f"Finalizing {table_name} after bulk import...")

        # Recreate dropped indexes
        if table_name in self.dropped_indexes:
            self._recreate_indexes(table_name)

        # Re-enable autovacuum
        self._set_table_autovacuum(table_name, enabled=True)

        # Run ANALYZE to update statistics
        self._analyze_table(table_name)

        logger.info(f"Table {table_name} finalized successfully")

    def _get_table_indexes(self, table_name: str) -> List[Dict]:
        """
        Get all indexes for a table (excluding primary keys and unique constraints).

        Args:
            table_name: Name of the table

        Returns:
            List of index definitions
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT
                    i.indexname,
                    i.indexdef
                FROM pg_indexes i
                LEFT JOIN pg_constraint c ON i.indexname = c.conname
                WHERE i.tablename = %s
                AND i.schemaname = 'public'
                AND c.conname IS NULL  -- Exclude constraints (PK, UNIQUE)
                ORDER BY i.indexname
            """, (table_name,))

            indexes = [
                {'name': row[0], 'definition': row[1]}
                for row in cursor.fetchall()
            ]

            logger.info(f"Found {len(indexes)} indexes on {table_name}: {[idx['name'] for idx in indexes]}")
            return indexes

        finally:
            cursor.close()

    def _drop_indexes(self, table_name: str, indexes: List[Dict]):
        """
        Drop indexes for a table.

        Args:
            table_name: Name of the table
            indexes: List of index definitions
        """
        cursor = self.conn.cursor()
        try:
            for idx in indexes:
                logger.info(f"Dropping index {idx['name']}...")
                cursor.execute(sql.SQL("DROP INDEX IF EXISTS {}").format(
                    sql.Identifier(idx['name'])
                ))

            self.conn.commit()
            self.dropped_indexes[table_name] = indexes
            logger.info(f"Dropped {len(indexes)} indexes from {table_name}")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error dropping indexes: {e}")
            raise
        finally:
            cursor.close()

    def _recreate_indexes(self, table_name: str):
        """
        Recreate indexes that were dropped for a table.

        Args:
            table_name: Name of the table
        """
        if table_name not in self.dropped_indexes:
            return

        indexes = self.dropped_indexes[table_name]
        cursor = self.conn.cursor()

        try:
            for idx in indexes:
                logger.info(f"Recreating index {idx['name']}...")
                start_time = time.time()

                cursor.execute(idx['definition'])

                elapsed = time.time() - start_time
                logger.info(f"Index {idx['name']} recreated in {elapsed:.2f}s")

            self.conn.commit()
            del self.dropped_indexes[table_name]
            logger.info(f"Recreated {len(indexes)} indexes for {table_name}")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error recreating indexes: {e}")
            raise
        finally:
            cursor.close()

    def _set_table_autovacuum(self, table_name: str, enabled: bool):
        """
        Enable or disable autovacuum for a table.

        Args:
            table_name: Name of the table
            enabled: Whether to enable or disable autovacuum
        """
        cursor = self.conn.cursor()
        try:
            setting = 'true' if enabled else 'false'
            cursor.execute(
                sql.SQL("ALTER TABLE {} SET (autovacuum_enabled = {})").format(
                    sql.Identifier(table_name),
                    sql.SQL(setting)
                )
            )
            self.conn.commit()
            logger.debug(f"Autovacuum {'enabled' if enabled else 'disabled'} for {table_name}")
        except Exception as e:
            logger.warning(f"Could not set autovacuum for {table_name}: {e}")
            self.conn.rollback()
        finally:
            cursor.close()

    def _analyze_table(self, table_name: str):
        """
        Run ANALYZE on a table to update statistics.

        Args:
            table_name: Name of the table
        """
        cursor = self.conn.cursor()
        try:
            logger.info(f"Analyzing {table_name}...")
            cursor.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(table_name)))
            self.conn.commit()
            logger.info(f"Analysis complete for {table_name}")
        except Exception as e:
            logger.warning(f"Could not analyze {table_name}: {e}")
            self.conn.rollback()
        finally:
            cursor.close()

    def _get_table_columns(self, table_name: str) -> List[str]:
        """
        Get list of column names for a PostgreSQL table, excluding generated columns.

        Args:
            table_name: Name of the table

        Returns:
            List of column names (excluding GENERATED columns)
        """
        cursor = self.conn.cursor()
        try:
            # Exclude GENERATED columns (e.g., signature_hash_4 in signatures table)
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = %s
                AND (is_generated = 'NEVER' OR is_generated IS NULL)
                ORDER BY ordinal_position
            """, (table_name,))

            columns = [row[0] for row in cursor.fetchall()]
            return columns
        finally:
            cursor.close()

    def import_dataframe(self, df: pd.DataFrame, table_name: str, batch_size: int = 100000) -> int:
        """
        Import a pandas DataFrame into a PostgreSQL table using optimized COPY.

        Args:
            df: DataFrame to import
            table_name: Name of the target table
            batch_size: Number of rows to insert per batch (for progress tracking)

        Returns:
            Number of rows successfully imported
        """
        if df.empty:
            logger.warning(f"DataFrame for {table_name} is empty, skipping import")
            return 0

        total_rows = len(df)
        logger.info(f"Importing {total_rows:,} rows into {table_name} using {'COPY' if self.use_copy else 'INSERT'}...")

        # Get actual columns from PostgreSQL table
        pg_columns = self._get_table_columns(table_name)

        # Filter DataFrame to only include columns that exist in PostgreSQL
        df_columns = df.columns.tolist()
        columns_to_import = [col for col in df_columns if col in pg_columns]
        columns_to_skip = [col for col in df_columns if col not in pg_columns]

        if columns_to_skip:
            logger.debug(f"Skipping columns not in PostgreSQL: {', '.join(columns_to_skip)}")

        # Filter and prepare DataFrame
        df = self._prepare_dataframe(df[columns_to_import].copy(), table_name, columns_to_import)

        if df.empty:
            logger.warning(f"No valid rows to import into {table_name} after data preparation")
            return 0

        total_rows = len(df)
        columns = columns_to_import

        # Import using COPY or INSERT
        if self.use_copy:
            return self._import_using_copy(df, table_name, columns)
        else:
            return self._import_using_insert(df, table_name, columns, batch_size)

    def _prepare_dataframe(self, df: pd.DataFrame, table_name: str, columns: List[str]) -> pd.DataFrame:
        """
        Prepare DataFrame for PostgreSQL import (handle data types, NULLs, etc.)

        Args:
            df: DataFrame to prepare
            table_name: Target table name
            columns: List of columns to import

        Returns:
            Prepared DataFrame
        """
        import numpy as np

        # Handle special data types
        for col in columns:
            if df[col].dtype == 'object':
                # Check if column contains dicts (for JSONB columns)
                sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                if isinstance(sample, dict):
                    df[col] = df[col].apply(self._convert_to_json)

        # Convert pandas NaT to None for timestamp columns
        timestamp_columns = ['created_at', 'updated_at']
        for col in timestamp_columns:
            if col in columns:
                df[col] = df[col].replace({pd.NaT: None})

        # Filter out rows with NULL values in NOT NULL columns
        not_null_columns_by_table = {
            'code': ['code_hash', 'code_hash_keccak'],
            'sources': ['source_hash', 'source_hash_keccak', 'content'],
            'contracts': ['creation_code_hash', 'runtime_code_hash'],
            'compiled_contracts': ['compiler', 'version', 'language', 'name', 'fully_qualified_name',
                                  'compiler_settings', 'compilation_artifacts', 'creation_code_hash',
                                  'creation_code_artifacts', 'runtime_code_hash', 'runtime_code_artifacts'],
            'compiled_contracts_sources': ['compilation_id', 'source_hash', 'path'],
            'contract_deployments': ['chain_id', 'address', 'transaction_hash', 'block_number',
                                    'transaction_index', 'deployer', 'contract_id'],
            'verified_contracts': ['deployment_id', 'compilation_id', 'creation_match', 'runtime_match'],
        }

        if table_name in not_null_columns_by_table:
            required_cols = [col for col in not_null_columns_by_table[table_name] if col in columns]
            if required_cols:
                initial_count = len(df)
                df = df.dropna(subset=required_cols)
                dropped_count = initial_count - len(df)
                if dropped_count > 0:
                    logger.warning(
                        f"Dropped {dropped_count} rows from {table_name} due to NULL values in required columns"
                    )

        return df

    def _convert_to_json(self, obj):
        """Convert object to JSON string, handling numpy arrays"""
        if obj is None or pd.isna(obj):
            return None
        if isinstance(obj, dict):
            import numpy as np

            def convert_numpy(o):
                if isinstance(o, np.ndarray):
                    return o.tolist()
                elif isinstance(o, dict):
                    return {k: convert_numpy(v) for k, v in o.items()}
                elif isinstance(o, list):
                    return [convert_numpy(item) for item in o]
                else:
                    return o

            converted = convert_numpy(obj)
            return json.dumps(converted)
        return obj

    def _import_using_copy(self, df: pd.DataFrame, table_name: str, columns: List[str]) -> int:
        """
        Import DataFrame using PostgreSQL COPY (fastest method).
        Uses temporary table to handle conflicts.

        Args:
            df: DataFrame to import
            table_name: Target table name
            columns: List of columns

        Returns:
            Number of rows imported
        """
        temp_table = f"temp_{table_name}_{int(time.time())}"
        cursor = self.conn.cursor()

        try:
            # Create temporary table with same structure
            logger.debug(f"Creating temporary table {temp_table}...")
            cursor.execute(f"""
                CREATE TEMP TABLE {temp_table}
                (LIKE {table_name} INCLUDING DEFAULTS)
                ON COMMIT DROP
            """)

            # Convert special column types for COPY compatibility
            df_copy = df.copy()
            for col in columns:
                if df_copy[col].dtype == 'object':
                    # Check first non-null value to determine type
                    sample = df_copy[col].dropna().iloc[0] if not df_copy[col].dropna().empty else None

                    if isinstance(sample, bytes):
                        # Convert bytes to hex string with \x prefix for PostgreSQL bytea
                        df_copy[col] = df_copy[col].apply(
                            lambda x: '\\x' + x.hex() if isinstance(x, bytes) else (None if pd.isna(x) else x)
                        )
                    elif isinstance(sample, (dict, list)):
                        # Convert dict/list to JSON string for both JSON and JSONB columns
                        # PostgreSQL COPY accepts JSON strings for both types
                        df_copy[col] = df_copy[col].apply(
                            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else (None if pd.isna(x) else x)
                        )
                    elif isinstance(sample, str):
                        # Strings are used for: text, varchar, json/jsonb (already serialized), enums
                        # No special handling needed for COPY - pass through as-is
                        pass
                elif df_copy[col].dtype == 'bool':
                    # Convert Python bool to PostgreSQL format: true/false (lowercase)
                    df_copy[col] = df_copy[col].apply(
                        lambda x: str(x).lower() if not pd.isna(x) else None
                    )

            # Prepare CSV buffer
            buffer = StringIO()
            df_copy.to_csv(
                buffer,
                index=False,
                header=False,
                sep='\t',
                na_rep='\\N',  # PostgreSQL NULL representation
                doublequote=False,
                escapechar='\\'
            )
            buffer.seek(0)

            # COPY data into temporary table
            logger.debug(f"Copying {len(df):,} rows to temporary table...")
            start_time = time.time()

            cursor.copy_from(
                buffer,
                temp_table,
                columns=columns,
                null='\\N'
            )

            copy_time = time.time() - start_time
            logger.debug(f"COPY completed in {copy_time:.2f}s ({len(df)/copy_time:.0f} rows/sec)")

            # Insert from temp table to actual table with conflict handling
            column_list = ', '.join(columns)

            # Determine conflict resolution
            if table_name in ['code', 'sources', 'contracts', 'compiled_contracts',
                              'compiled_contracts_sources', 'contract_deployments']:
                conflict_clause = "ON CONFLICT DO NOTHING"
            else:
                # For mutable data: UPDATE on conflict
                update_cols = [col for col in columns if col not in ['created_at', 'created_by', 'updated_at', 'updated_by']]
                if update_cols:
                    update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
                    conflict_clause = f"ON CONFLICT DO UPDATE SET {update_clause}"
                else:
                    conflict_clause = "ON CONFLICT DO NOTHING"

            logger.debug(f"Inserting from temporary table into {table_name}...")
            start_time = time.time()

            cursor.execute(f"""
                INSERT INTO {table_name} ({column_list})
                SELECT {column_list}
                FROM {temp_table}
                {conflict_clause}
            """)

            rows_imported = cursor.rowcount
            insert_time = time.time() - start_time

            self.conn.commit()

            total_time = copy_time + insert_time
            logger.info(
                f"Imported {rows_imported:,} rows into {table_name} in {total_time:.2f}s "
                f"({rows_imported/total_time:.0f} rows/sec)"
            )

            return rows_imported

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error during COPY import: {e}", exc_info=True)
            raise
        finally:
            cursor.close()

    def _import_using_insert(self, df: pd.DataFrame, table_name: str, columns: List[str], batch_size: int) -> int:
        """
        Fallback: Import using batch INSERT (slower than COPY).

        Args:
            df: DataFrame to import
            table_name: Target table name
            columns: List of columns
            batch_size: Rows per batch

        Returns:
            Number of rows imported
        """
        from psycopg2.extras import execute_values

        total_imported = 0
        total_rows = len(df)

        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i + batch_size]
            batch_data = [tuple(row) for row in batch_df.values]

            cursor = self.conn.cursor()
            try:
                column_list = ', '.join(columns)
                placeholders = ', '.join(['%s'] * len(columns))

                # Conflict handling
                if table_name in ['code', 'sources', 'contracts', 'compiled_contracts',
                                  'compiled_contracts_sources', 'contract_deployments']:
                    conflict_clause = "ON CONFLICT DO NOTHING"
                else:
                    update_cols = [col for col in columns if col not in ['created_at', 'created_by', 'updated_at', 'updated_by']]
                    if update_cols:
                        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
                        conflict_clause = f"ON CONFLICT DO UPDATE SET {update_clause}"
                    else:
                        conflict_clause = "ON CONFLICT DO NOTHING"

                query = f"""
                    INSERT INTO {table_name} ({column_list})
                    VALUES %s
                    {conflict_clause}
                """

                execute_values(
                    cursor,
                    query,
                    batch_data,
                    template=f"({placeholders})",
                    page_size=len(batch_data)
                )

                rows_imported = cursor.rowcount
                total_imported += rows_imported
                self.conn.commit()

                logger.info(f"Batch {i//batch_size + 1}: Imported {rows_imported:,} rows")

            except Exception as e:
                self.conn.rollback()
                logger.error(f"Error importing batch: {e}", exc_info=True)
                continue
            finally:
                cursor.close()

        return total_imported
