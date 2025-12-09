"""
Database Importer

Handles importing data from pandas DataFrames into PostgreSQL database.
"""

import logging
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from typing import List

logger = logging.getLogger(__name__)


class DatabaseImporter:
    """Imports data into PostgreSQL database"""

    def __init__(self, conn):
        """
        Initialize the database importer.
        
        Args:
            conn: psycopg2 connection object
        """
        self.conn = conn
        
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

    def get_import_order(self) -> List[str]:
        """Get the order in which tables should be imported (respects FK dependencies)"""
        return self.import_order

    def _get_table_columns(self, table_name: str) -> List[str]:
        """
        Get list of column names for a PostgreSQL table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column names
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            
            columns = [row[0] for row in cursor.fetchall()]
            return columns
        finally:
            cursor.close()

    def import_dataframe(self, df: pd.DataFrame, table_name: str, batch_size: int = 10000) -> int:
        """
        Import a pandas DataFrame into a PostgreSQL table.
        
        Args:
            df: DataFrame to import
            table_name: Name of the target table
            batch_size: Number of rows to insert per batch
            
        Returns:
            Number of rows successfully imported
        """
        if df.empty:
            logger.warning(f"DataFrame for {table_name} is empty, skipping import")
            return 0

        total_imported = 0
        total_rows = len(df)
        
        # Get actual columns from PostgreSQL table
        pg_columns = self._get_table_columns(table_name)
        
        # Filter DataFrame to only include columns that exist in PostgreSQL
        df_columns = df.columns.tolist()
        columns_to_import = [col for col in df_columns if col in pg_columns]
        columns_to_skip = [col for col in df_columns if col not in pg_columns]
        
        if columns_to_skip:
            logger.info(f"Skipping columns not in PostgreSQL table {table_name}: {', '.join(columns_to_skip)}")
        
        # Filter DataFrame
        df = df[columns_to_import].copy()
        columns = columns_to_import
        
        # Handle special data types for PostgreSQL
        import json
        import numpy as np
        
        def convert_to_json(obj):
            """Convert object to JSON, handling numpy arrays"""
            if obj is None or pd.isna(obj):
                return obj
            if isinstance(obj, dict):
                # Convert numpy arrays to lists recursively
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
        
        for col in columns:
            if df[col].dtype == 'object':
                # Check if column contains dicts (for JSONB columns)
                sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                if isinstance(sample, dict):
                    # Convert dicts to JSON strings for JSONB columns
                    df[col] = df[col].apply(convert_to_json)
                elif isinstance(sample, bytes):
                    # Bytes are already in correct format for bytea columns
                    pass
            elif df[col].dtype == 'int64':
                # BigQuery INT64 -> PostgreSQL bigint/numeric (already compatible)
                pass
            elif df[col].dtype == 'float64':
                # BigQuery FLOAT64 -> PostgreSQL numeric (already compatible)
                pass
            elif df[col].dtype == 'bool':
                # BigQuery BOOL -> PostgreSQL boolean (already compatible)
                pass
        
        # Handle UUID columns (convert strings to UUID if needed)
        uuid_columns = ['id', 'compilation_id', 'deployment_id', 'contract_id']
        for col in uuid_columns:
            if col in columns and df[col].dtype == 'object':
                # UUIDs from BigQuery come as strings, which PostgreSQL can handle
                pass
        
        # Process in batches
        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i + batch_size]
            batch_data = [tuple(row) for row in batch_df.values]
            
            try:
                imported = self._insert_batch(table_name, columns, batch_data)
                total_imported += imported
                
                skipped = len(batch_data) - imported
                logger.info(
                    f"Imported {imported} rows into {table_name} "
                    f"(attempted: {len(batch_data)}, skipped due to conflicts: {skipped})"
                )
                
            except Exception as e:
                logger.error(f"Error importing batch into {table_name}: {e}", exc_info=True)
                # Continue with next batch
                continue

        return total_imported

    def _insert_batch(self, table_name: str, columns: List[str], data: List[tuple]) -> int:
        """
        Insert a batch of data into a table.
        
        Args:
            table_name: Name of the target table
            columns: List of column names
            data: List of tuples containing row data
            
        Returns:
            Number of rows inserted
        """
        if not data:
            return 0

        # Build column list
        column_list = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Determine conflict resolution strategy based on table
        if table_name in ['code', 'sources', 'contracts', 'compiled_contracts', 
                          'compiled_contracts_sources', 'contract_deployments']:
            # For immutable data: DO NOTHING on conflict
            conflict_clause = "ON CONFLICT DO NOTHING"
        else:
            # For mutable data: UPDATE timestamps
            update_cols = [col for col in columns if col not in ['created_at', 'created_by']]
            if 'updated_at' in columns and 'updated_by' in columns:
                # Let triggers handle updated_at and updated_by
                update_cols = [col for col in update_cols if col not in ['updated_at', 'updated_by']]
            
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
        
        cursor = self.conn.cursor()
        try:
            # Use execute_values for efficient batch insert
            execute_values(
                cursor,
                query,
                data,
                template=f"({placeholders})",
                page_size=len(data)
            )
            
            rows_affected = cursor.rowcount
            self.conn.commit()
            
            return rows_affected
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting into {table_name}: {e}")
            raise
        finally:
            cursor.close()
