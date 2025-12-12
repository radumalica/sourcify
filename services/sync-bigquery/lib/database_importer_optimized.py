"""
Optimized Database Importer

High-performance importer using PostgreSQL COPY, index management, and parallel processing.
Designed for bulk loading millions of rows efficiently.

Supports two COPY modes:
- TEXT format: Compatible but requires careful escaping
- BINARY format: Faster, no escaping needed, handles bytea natively
"""

import logging
import psycopg2
from psycopg2 import sql
import pandas as pd
from typing import List, Dict, Optional, Set, Any, Tuple
from io import StringIO, BytesIO
import json
import time
import csv
import struct
import uuid
from decimal import Decimal
from datetime import datetime, date
from .index_state_manager import IndexStateManager

logger = logging.getLogger(__name__)


class OptimizedDatabaseImporter:
    """Optimized importer for PostgreSQL with COPY and index management"""

    # PostgreSQL binary COPY format signature
    PGCOPY_SIGNATURE = b'PGCOPY\n\xff\r\n\x00'

    def __init__(self, conn, use_copy: bool = True, manage_indexes: bool = True,
                 use_binary: bool = True):
        """
        Initialize the optimized database importer.

        Args:
            conn: psycopg2 connection object
            use_copy: Use COPY instead of INSERT (much faster)
            manage_indexes: Automatically drop/recreate indexes during bulk import
            use_binary: Use binary COPY format (faster, no escaping issues)
        """
        self.conn = conn
        self.use_copy = use_copy
        self.use_binary = use_binary
        self.manage_indexes = manage_indexes

        # Track which indexes have been dropped (in-memory)
        self.dropped_indexes: Dict[str, List[Dict]] = {}

        # Persistent state manager for index restoration across runs
        self.index_state = IndexStateManager()

        # Define import order based on foreign key dependencies
        # Order is critical: parent tables must be imported before child tables
        self.import_order = [
            'code',                          # No FKs
            'sources',                       # No FKs
            'signatures',                    # No FKs (must be before compiled_contracts_signatures)
            'contracts',                     # FK: code (creation_code_hash, runtime_code_hash)
            'compiled_contracts',            # FK: code (creation_code_hash, runtime_code_hash)
            'compiled_contracts_sources',    # FK: compiled_contracts, sources
            'compiled_contracts_signatures', # FK: compiled_contracts, signatures
            'contract_deployments',          # FK: contracts
            'verified_contracts',            # FK: compiled_contracts, contract_deployments
            'sourcify_matches',              # FK: verified_contracts
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
        Also checks for and restores any indexes from previous interrupted runs.

        Args:
            table_name: Name of the table to prepare
        """
        if not self.manage_indexes:
            return

        logger.info(f"Preparing {table_name} for bulk import...")

        # First, check if there are pending indexes from a previous interrupted run
        pending_indexes = self.index_state.get_pending_indexes(table_name)
        if pending_indexes:
            logger.warning(
                f"Found {len(pending_indexes)} indexes from previous interrupted sync. "
                f"Restoring them first..."
            )
            self._restore_indexes_from_state(table_name, pending_indexes)
            self.index_state.mark_indexes_restored(table_name)

        # Get and drop indexes (except primary key and unique constraints)
        indexes = self._get_table_indexes(table_name)
        if indexes:
            self._drop_indexes(table_name, indexes)
            # Save state so we can restore even if interrupted
            self.index_state.mark_indexes_dropped(table_name, indexes)

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

        # Recreate dropped indexes (from in-memory cache)
        if table_name in self.dropped_indexes:
            self._recreate_indexes(table_name)
            # Clear persistent state after successful restoration
            self.index_state.mark_indexes_restored(table_name)

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
        self._restore_indexes_from_state(table_name, indexes)
        del self.dropped_indexes[table_name]

    def _restore_indexes_from_state(self, table_name: str, indexes: List[Dict]):
        """
        Restore indexes from state (used for recovery and normal recreation).

        Args:
            table_name: Name of the table
            indexes: List of index definitions to restore
        """
        cursor = self.conn.cursor()

        try:
            # First, check which indexes actually exist
            cursor.execute("""
                SELECT indexname
                FROM pg_indexes
                WHERE tablename = %s
                AND schemaname = 'public'
            """, (table_name,))
            
            existing_indexes = {row[0] for row in cursor.fetchall()}
            
            for idx in indexes:
                # Skip if index already exists
                if idx['name'] in existing_indexes:
                    logger.info(f"Index {idx['name']} already exists, skipping recreation")
                    continue
                
                logger.info(f"Recreating index {idx['name']}...")
                start_time = time.time()

                cursor.execute(idx['definition'])

                elapsed = time.time() - start_time
                logger.info(f"Index {idx['name']} recreated in {elapsed:.2f}s")

            self.conn.commit()
            logger.info(f"Recreated indexes for {table_name}")

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

        # Import using COPY (binary or text) or INSERT
        if self.use_copy:
            if self.use_binary:
                return self._import_using_binary_copy(df, table_name, columns)
            else:
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
        # Based on official Sourcify schema from dbdiagram
        not_null_columns_by_table = {
            'code': ['code_hash', 'code_hash_keccak'],
            'sources': ['source_hash', 'source_hash_keccak', 'content'],
            # contracts.creation_code_hash can be NULL
            'contracts': ['runtime_code_hash'],
            # compiled_contracts: creation_code_hash and creation_code_artifacts can be NULL
            'compiled_contracts': ['compiler', 'version', 'language', 'name', 'fully_qualified_name',
                                  'compiler_settings', 'compilation_artifacts',
                                  'runtime_code_hash', 'runtime_code_artifacts'],
            'compiled_contracts_sources': ['compilation_id', 'source_hash', 'path'],
            # contract_deployments: only chain_id, address, contract_id are NOT NULL
            'contract_deployments': ['chain_id', 'address', 'contract_id'],
            # verified_contracts: id is bigint (not uuid), deployment_id, compilation_id, creation_match, runtime_match are NOT NULL
            'verified_contracts': ['deployment_id', 'compilation_id', 'creation_match', 'runtime_match'],
            # sourcify_matches: id and verified_contract_id are bigint, metadata is json (not jsonb)
            # creation_match and runtime_match are varchar (not boolean)
            'sourcify_matches': ['verified_contract_id', 'metadata'],
            'signatures': ['signature_hash_32', 'signature'],
            'compiled_contracts_signatures': ['compilation_id', 'signature_hash_32', 'signature_type'],
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
            # Track which columns are bytea (already in hex format, don't escape)
            bytea_columns = set()
            df_copy = df.copy()

            for col in columns:
                if df_copy[col].dtype == 'object':
                    # Check first non-null value to determine type
                    sample = df_copy[col].dropna().iloc[0] if not df_copy[col].dropna().empty else None

                    if isinstance(sample, bytes) or isinstance(sample, memoryview):
                        # Convert bytes to hex string with \x prefix for PostgreSQL COPY format
                        # In COPY text format, \x followed by hex digits is interpreted as bytea
                        # We need to escape the backslash for CSV/COPY: \\x instead of \x
                        # Handle mixed types: ensure all values are either bytes or None
                        def convert_to_hex_or_none(x):
                            if pd.isna(x) or x is None:
                                return None
                            if isinstance(x, bytes):
                                # Use double backslash - one will be consumed by COPY parser
                                return '\\\\x' + x.hex()
                            if isinstance(x, memoryview):
                                return '\\\\x' + bytes(x).hex()
                            # If not bytes/memoryview, log warning and convert to None
                            logger.warning(f"Unexpected type {type(x)} in bytea column {col}, converting to NULL")
                            return None
                        
                        df_copy[col] = df_copy[col].apply(convert_to_hex_or_none)
                        bytea_columns.add(col)  # Mark as bytea to skip escaping
                    elif isinstance(sample, (dict, list)):
                        # Convert dict/list to JSON string for both JSON and JSONB columns
                        # PostgreSQL COPY accepts JSON strings for both types
                        df_copy[col] = df_copy[col].apply(
                            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else (None if pd.isna(x) else x)
                        )
                    elif isinstance(sample, str):
                        # Strings are used for: text, varchar, json/jsonb (already serialized), enums
                        # Ensure all strings are valid UTF-8 by cleaning invalid byte sequences
                        def clean_utf8_string(x):
                            if pd.isna(x):
                                return None
                            # Handle bytes that might be in a string column
                            if isinstance(x, bytes):
                                try:
                                    return x.decode('utf-8', errors='replace')
                                except:
                                    logger.warning(f"Cannot decode bytes in string column {col}, converting to hex representation")
                                    return '\\x' + x.hex()
                            if not isinstance(x, str):
                                # Convert to string
                                return str(x)
                            # Ensure string is valid UTF-8
                            try:
                                # Try to encode to UTF-8 to detect issues
                                x.encode('utf-8')
                                return x
                            except (UnicodeEncodeError, UnicodeDecodeError):
                                # String contains invalid UTF-8 characters
                                # Re-encode with error handling to replace/ignore bad chars
                                try:
                                    return x.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
                                except:
                                    logger.warning(f"Cannot clean string in column {col}, setting to NULL")
                                    return None
                        
                        df_copy[col] = df_copy[col].apply(clean_utf8_string)
                elif df_copy[col].dtype == 'bool':
                    # Convert Python bool to PostgreSQL format: true/false (lowercase)
                    df_copy[col] = df_copy[col].apply(
                        lambda x: str(x).lower() if not pd.isna(x) else None
                    )

            # Escape special characters in string columns for PostgreSQL COPY format
            # PostgreSQL COPY uses backslash escapes: \t, \n, \r, \\
            # Skip bytea columns (tracked in bytea_columns set - they use \x hex format)
            for col in columns:
                if col not in bytea_columns and df_copy[col].dtype == 'object':
                    def escape_for_copy(x):
                        if not isinstance(x, str):
                            return x
                        # Ensure the string is valid UTF-8 before escaping
                        try:
                            x.encode('utf-8')
                        except (UnicodeEncodeError, UnicodeDecodeError):
                            logger.warning(f"Invalid UTF-8 in column {col} during escape, cleaning...")
                            x = x.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
                        # Escape special characters for COPY format
                        # Note: bytea columns are already excluded via bytea_columns set
                        return x.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')

                    df_copy[col] = df_copy[col].apply(escape_for_copy)

            # Prepare CSV buffer for PostgreSQL COPY
            # Use QUOTE_NONE to prevent quoting (critical for bytea NULL handling)
            buffer = StringIO()
            try:
                df_copy.to_csv(
                    buffer,
                    index=False,
                    header=False,
                    sep='\t',
                    na_rep='\\N',  # PostgreSQL NULL representation
                    quoting=csv.QUOTE_NONE,  # Don't quote any values
                    escapechar=None,  # We manually escaped above
                    lineterminator='\n'  # Use Unix line endings
                )
            except Exception as e:
                logger.error(f"Error writing DataFrame to CSV buffer: {e}")
                # Try to identify problematic rows/columns
                for idx, row in df_copy.iterrows():
                    for col in columns:
                        val = row[col]
                        if val is not None and not pd.isna(val):
                            try:
                                str(val).encode('utf-8')
                            except:
                                logger.error(f"Problematic value in row {idx}, column {col}: {type(val)}")
                raise
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
            
            # Count rows in temp table to know how many made it through COPY
            cursor.execute(f"SELECT COUNT(*) FROM {temp_table}")
            rows_in_temp = cursor.fetchone()[0]
            logger.debug(f"COPY completed in {copy_time:.2f}s ({len(df)/copy_time:.0f} rows/sec) - {rows_in_temp:,} rows in temp table")

            # Insert from temp table to actual table with conflict handling
            column_list = ', '.join(columns)

            # Define conflict targets based on primary keys from official Sourcify schema
            conflict_targets = {
                'code': '(code_hash)',                    # PK: code_hash bytea
                'sources': '(source_hash)',              # PK: source_hash bytea
                'signatures': '(signature_hash_32)',     # PK: signature_hash_32 bytea
                'contracts': '(id)',                     # PK: id uuid
                'compiled_contracts': '(id)',            # PK: id uuid
                'compiled_contracts_sources': '(id)',    # PK: id uuid
                'compiled_contracts_signatures': '(id)', # PK: id uuid
                'contract_deployments': '(id)',          # PK: id uuid
                'verified_contracts': '(id)',            # PK: id bigint
                'sourcify_matches': '(id)',              # PK: id bigint
            }

            # Determine conflict resolution
            conflict_target = conflict_targets.get(table_name, '')
            if table_name in ['code', 'sources', 'contracts', 'compiled_contracts',
                              'compiled_contracts_sources', 'contract_deployments',
                              'signatures', 'compiled_contracts_signatures']:
                # Immutable data: skip duplicates
                conflict_clause = f"ON CONFLICT {conflict_target} DO NOTHING" if conflict_target else "ON CONFLICT DO NOTHING"
            else:
                # For mutable data (verified_contracts, sourcify_matches): UPDATE on conflict
                update_cols = [col for col in columns if col not in ['id', 'created_at', 'created_by', 'updated_at', 'updated_by']]
                if update_cols and conflict_target:
                    update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
                    conflict_clause = f"ON CONFLICT {conflict_target} DO UPDATE SET {update_clause}"
                else:
                    conflict_clause = f"ON CONFLICT {conflict_target} DO NOTHING" if conflict_target else "ON CONFLICT DO NOTHING"

            logger.debug(f"Inserting from temporary table into {table_name}...")
            start_time = time.time()

            # Disable FK constraint triggers for faster bulk insert
            # This is safe because we import in dependency order (parent tables first)
            cursor.execute(f"ALTER TABLE {table_name} DISABLE TRIGGER ALL")

            # Use RETURNING to count actual rows inserted
            cursor.execute(f"""
                INSERT INTO {table_name} ({column_list})
                SELECT {column_list}
                FROM {temp_table}
                {conflict_clause}
                RETURNING 1
            """)

            # Count the returned rows
            rows_imported = len(cursor.fetchall())

            # Re-enable FK constraint triggers
            cursor.execute(f"ALTER TABLE {table_name} ENABLE TRIGGER ALL")

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

    def _import_using_binary_copy(self, df: pd.DataFrame, table_name: str, columns: List[str]) -> int:
        """
        Import using PostgreSQL binary COPY format - fastest method, no escaping needed.

        Binary format advantages:
        - No text encoding/escaping issues (bytea with null bytes works perfectly)
        - Faster parsing on PostgreSQL side
        - More compact representation

        Args:
            df: DataFrame to import (should have original Python types, not string-converted)
            table_name: Target table name
            columns: List of columns

        Returns:
            Number of rows imported
        """
        cursor = self.conn.cursor()
        temp_table = f"temp_{table_name}_{abs(hash(time.time())) % 10000000000}"

        try:
            # Create temporary table with same structure
            logger.debug(f"Creating temporary table {temp_table}...")
            cursor.execute(f"""
                CREATE TEMP TABLE {temp_table}
                (LIKE {table_name} INCLUDING DEFAULTS)
                ON COMMIT DROP
            """)

            # Build binary buffer
            logger.debug(f"Building binary buffer for {len(df):,} rows...")
            start_time = time.time()

            buffer = BytesIO()

            # Write PGCOPY header
            buffer.write(self.PGCOPY_SIGNATURE)
            buffer.write(struct.pack('>I', 0))  # flags (no OIDs)
            buffer.write(struct.pack('>I', 0))  # header extension length

            num_columns = len(columns)

            # Convert DataFrame rows to binary format
            for _, row in df.iterrows():
                # Write number of fields in this row
                buffer.write(struct.pack('>H', num_columns))

                for col in columns:
                    value = row[col]
                    self._write_binary_value(buffer, value)

            # Write trailer
            buffer.write(struct.pack('>h', -1))

            buffer_size = buffer.tell()
            buffer.seek(0)

            build_time = time.time() - start_time
            logger.debug(f"Binary buffer built in {build_time:.2f}s ({buffer_size / 1024 / 1024:.1f} MB)")

            # COPY binary data into temporary table
            logger.debug(f"Copying {len(df):,} rows to temporary table (binary)...")
            start_time = time.time()

            cursor.copy_expert(
                f"COPY {temp_table} ({', '.join(columns)}) FROM STDIN WITH BINARY",
                buffer
            )

            copy_time = time.time() - start_time
            logger.debug(f"Binary COPY completed in {copy_time:.2f}s ({len(df)/copy_time:.0f} rows/sec)")

            # Insert from temp table to actual table with conflict handling
            column_list = ', '.join(columns)

            # Define conflict targets based on primary keys from official Sourcify schema
            # For tables with natural keys (code, sources, signatures): use the natural key
            # For tables with surrogate keys (id): use the id since BigQuery includes it
            conflict_targets = {
                'code': '(code_hash)',                    # PK: code_hash bytea
                'sources': '(source_hash)',              # PK: source_hash bytea
                'signatures': '(signature_hash_32)',     # PK: signature_hash_32 bytea
                'contracts': '(id)',                     # PK: id uuid
                'compiled_contracts': '(id)',            # PK: id uuid
                'compiled_contracts_sources': '(id)',    # PK: id uuid
                'compiled_contracts_signatures': '(id)', # PK: id uuid
                'contract_deployments': '(id)',          # PK: id uuid
                'verified_contracts': '(id)',            # PK: id bigint
                'sourcify_matches': '(id)',              # PK: id bigint
            }

            conflict_target = conflict_targets.get(table_name, '')
            if table_name in ['code', 'sources', 'contracts', 'compiled_contracts',
                              'compiled_contracts_sources', 'contract_deployments',
                              'signatures', 'compiled_contracts_signatures']:
                conflict_clause = f"ON CONFLICT {conflict_target} DO NOTHING" if conflict_target else "ON CONFLICT DO NOTHING"
            else:
                update_cols = [col for col in columns if col not in ['id', 'created_at', 'created_by', 'updated_at', 'updated_by']]
                if update_cols and conflict_target:
                    update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
                    conflict_clause = f"ON CONFLICT {conflict_target} DO UPDATE SET {update_clause}"
                else:
                    conflict_clause = f"ON CONFLICT {conflict_target} DO NOTHING" if conflict_target else "ON CONFLICT DO NOTHING"

            logger.debug(f"Inserting from temporary table into {table_name}...")
            start_time = time.time()

            # Disable FK constraint triggers for faster bulk insert
            # This is safe because we import in dependency order (parent tables first)
            cursor.execute(f"ALTER TABLE {table_name} DISABLE TRIGGER ALL")

            cursor.execute(f"""
                INSERT INTO {table_name} ({column_list})
                SELECT {column_list}
                FROM {temp_table}
                {conflict_clause}
            """)

            # Re-enable FK constraint triggers
            cursor.execute(f"ALTER TABLE {table_name} ENABLE TRIGGER ALL")

            rows_imported = cursor.rowcount
            insert_time = time.time() - start_time

            self.conn.commit()

            total_time = build_time + copy_time + insert_time
            logger.info(
                f"Imported {rows_imported:,} rows into {table_name} in {total_time:.2f}s "
                f"({rows_imported/total_time:.0f} rows/sec) [binary COPY]"
            )

            return rows_imported

        except Exception as e:
            self.conn.rollback()
            logger.warning(f"Binary COPY failed: {e}. Falling back to chunked text COPY...")
            # Fall back to text COPY in smaller chunks to avoid building huge buffers
            try:
                cursor.close()
            except:
                pass

            try:
                total_imported = 0
                total_rows = len(df)
                # Reasonable chunk size to limit memory usage; tuneable
                chunk_size = 100000
                num_chunks = (total_rows + chunk_size - 1) // chunk_size
                
                logger.info(f"Attempting chunked text COPY fallback: {total_rows:,} rows in {num_chunks} chunks of {chunk_size:,}")
                
                fallback_start_time = time.time()
                for chunk_idx, start in enumerate(range(0, total_rows, chunk_size), 1):
                    end = min(start + chunk_size, total_rows)
                    chunk = df.iloc[start:end]
                    chunk_size_actual = len(chunk)
                    
                    logger.info(f"[Chunk {chunk_idx}/{num_chunks}] Processing rows {start:,}..{end:,} ({chunk_size_actual:,} rows)")
                    
                    chunk_start = time.time()
                    imported = self._import_using_copy(chunk, table_name, columns)
                    chunk_time = time.time() - chunk_start
                    
                    total_imported += imported
                    progress_pct = (end / total_rows) * 100
                    
                    logger.info(
                        f"[Chunk {chunk_idx}/{num_chunks}] Imported {imported:,} rows in {chunk_time:.2f}s "
                        f"({imported/chunk_time:.0f} rows/s) | "
                        f"Total: {total_imported:,}/{total_rows:,} ({progress_pct:.1f}%)"
                    )
                
                total_time = time.time() - fallback_start_time
                logger.info(
                    f"Text COPY fallback completed: {total_imported:,} rows imported in {total_time:.2f}s "
                    f"({total_imported/total_time:.0f} rows/s avg)"
                )
                return total_imported
            except Exception as e2:
                logger.error(f"Text COPY fallback (chunked) failed: {e2}", exc_info=True)
                raise
        finally:
            try:
                cursor.close()
            except:
                pass

    def _write_binary_value(self, buffer: BytesIO, value: Any) -> None:
        """
        Write a single value in PostgreSQL binary COPY format.

        Format: 4-byte length (big-endian) followed by raw bytes
        NULL is represented as length -1

        Args:
            buffer: BytesIO buffer to write to
            value: Value to encode
        """
        # Handle NULL
        if value is None or (isinstance(value, float) and pd.isna(value)):
            buffer.write(struct.pack('>i', -1))
            return

        # Handle bytes (bytea) - write directly
        if isinstance(value, bytes):
            buffer.write(struct.pack('>i', len(value)))
            buffer.write(value)
            return

        # Handle memoryview (BigQuery often returns this)
        if isinstance(value, memoryview):
            data = bytes(value)
            buffer.write(struct.pack('>i', len(data)))
            buffer.write(data)
            return

        # Handle string (text, varchar, json)
        if isinstance(value, str):
            encoded = value.encode('utf-8')
            buffer.write(struct.pack('>i', len(encoded)))
            buffer.write(encoded)
            return

        # Handle boolean
        if isinstance(value, bool):
            buffer.write(struct.pack('>i', 1))
            buffer.write(b'\x01' if value else b'\x00')
            return

        # Handle integers
        if isinstance(value, int):
            # PostgreSQL bigint is 8 bytes
            buffer.write(struct.pack('>i', 8))
            buffer.write(struct.pack('>q', value))
            return

        # Handle float
        if isinstance(value, float):
            # PostgreSQL double precision is 8 bytes
            buffer.write(struct.pack('>i', 8))
            buffer.write(struct.pack('>d', value))
            return

        # Handle Decimal (numeric)
        # PostgreSQL binary numeric format is complex, so we convert to int if possible
        # This works for block_number, transaction_index which are whole numbers
        if isinstance(value, Decimal):
            # Check if it's a whole number that fits in int64
            if value == value.to_integral_value() and -2**63 <= value <= 2**63-1:
                # Send as bigint - PostgreSQL will cast to numeric
                int_val = int(value)
                buffer.write(struct.pack('>i', 8))
                buffer.write(struct.pack('>q', int_val))
            else:
                # For large or fractional decimals, we need proper numeric encoding
                # PostgreSQL numeric binary format: ndigits(2), weight(2), sign(2), dscale(2), digits(2*ndigits)
                # This is complex - for now, raise an error to fall back to text COPY
                raise ValueError(f"Decimal value {value} requires text COPY format (binary numeric encoding not implemented)")
            return

        # Handle UUID
        if isinstance(value, uuid.UUID):
            # PostgreSQL UUID is 16 bytes
            buffer.write(struct.pack('>i', 16))
            buffer.write(value.bytes)
            return

        # Handle datetime/timestamp
        if isinstance(value, datetime):
            # PostgreSQL timestamp: microseconds since 2000-01-01
            # Epoch for PostgreSQL is 2000-01-01 00:00:00 UTC
            pg_epoch = datetime(2000, 1, 1)
            if value.tzinfo is not None:
                # Convert to UTC
                import pytz
                value = value.astimezone(pytz.UTC).replace(tzinfo=None)
            delta = value - pg_epoch
            microseconds = int(delta.total_seconds() * 1_000_000)
            buffer.write(struct.pack('>i', 8))
            buffer.write(struct.pack('>q', microseconds))
            return

        # Handle date
        if isinstance(value, date):
            # PostgreSQL date: days since 2000-01-01
            pg_epoch = date(2000, 1, 1)
            days = (value - pg_epoch).days
            buffer.write(struct.pack('>i', 4))
            buffer.write(struct.pack('>i', days))
            return

        # Handle dict/list (JSON/JSONB) - serialize to string
        if isinstance(value, (dict, list)):
            encoded = json.dumps(value).encode('utf-8')
            buffer.write(struct.pack('>i', len(encoded)))
            buffer.write(encoded)
            return

        # Handle numpy types
        try:
            import numpy as np
            if isinstance(value, np.integer):
                buffer.write(struct.pack('>i', 8))
                buffer.write(struct.pack('>q', int(value)))
                return
            if isinstance(value, np.floating):
                buffer.write(struct.pack('>i', 8))
                buffer.write(struct.pack('>d', float(value)))
                return
            if isinstance(value, np.bool_):
                buffer.write(struct.pack('>i', 1))
                buffer.write(b'\x01' if value else b'\x00')
                return
        except ImportError:
            pass

        # Fallback: convert to string
        logger.warning(f"Unknown type {type(value)} for value, converting to string")
        encoded = str(value).encode('utf-8')
        buffer.write(struct.pack('>i', len(encoded)))
        buffer.write(encoded)

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

                # Define conflict targets based on primary keys from official Sourcify schema
                conflict_targets = {
                    'code': '(code_hash)',                    # PK: code_hash bytea
                    'sources': '(source_hash)',              # PK: source_hash bytea
                    'signatures': '(signature_hash_32)',     # PK: signature_hash_32 bytea
                    'contracts': '(id)',                     # PK: id uuid
                    'compiled_contracts': '(id)',            # PK: id uuid
                    'compiled_contracts_sources': '(id)',    # PK: id uuid
                    'compiled_contracts_signatures': '(id)', # PK: id uuid
                    'contract_deployments': '(id)',          # PK: id uuid
                    'verified_contracts': '(id)',            # PK: id bigint
                    'sourcify_matches': '(id)',              # PK: id bigint
                }

                # Conflict handling
                conflict_target = conflict_targets.get(table_name, '')
                if table_name in ['code', 'sources', 'contracts', 'compiled_contracts',
                                  'compiled_contracts_sources', 'contract_deployments',
                                  'signatures', 'compiled_contracts_signatures']:
                    conflict_clause = f"ON CONFLICT {conflict_target} DO NOTHING" if conflict_target else "ON CONFLICT DO NOTHING"
                else:
                    update_cols = [col for col in columns if col not in ['id', 'created_at', 'created_by', 'updated_at', 'updated_by']]
                    if update_cols and conflict_target:
                        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
                        conflict_clause = f"ON CONFLICT {conflict_target} DO UPDATE SET {update_clause}"
                    else:
                        conflict_clause = f"ON CONFLICT {conflict_target} DO NOTHING" if conflict_target else "ON CONFLICT DO NOTHING"

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
