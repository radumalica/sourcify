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
        
        # Convert DataFrame to list of tuples
        columns = df.columns.tolist()
        
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
