"""
BigQuery Loader

Handles querying and loading data from Sourcify's public BigQuery dataset.
"""

import logging
from typing import Iterator, Optional, Dict, Any
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig
import pandas as pd

logger = logging.getLogger(__name__)


class BigQueryLoader:
    """Loads data from Sourcify's BigQuery dataset"""

    def __init__(self, project_id: str = None):
        """
        Initialize the BigQuery loader.
        
        Args:
            project_id: Google Cloud project ID (optional for public datasets)
        """
        # Initialize BigQuery client with credentials
        # Credentials should be set via GOOGLE_APPLICATION_CREDENTIALS env var
        if not project_id:
            raise ValueError("GCP_PROJECT_ID must be set in environment variables")
        
        self.client = bigquery.Client(project=project_id)
        
        # Sourcify dataset from Analytics Hub
        # Dataset is linked to your project after subscription
        self.dataset_project = project_id  # Use your project ID
        self.dataset_id = "sourcify_dataset"  # Dataset name after Analytics Hub subscription
        
        # Mapping from local table names to BigQuery table names (with public_ prefix)
        self.table_name_mapping = {
            'code': 'public_code',
            'sources': 'public_sources',
            'contracts': 'public_contracts',
            'signatures': 'public_signatures',
            'compiled_contracts': 'public_compiled_contracts',
            'contract_deployments': 'public_contract_deployments',
            'compiled_contracts_sources': 'public_compiled_contracts_sources',
            'verified_contracts': 'public_verified_contracts',
            'sourcify_matches': 'public_sourcify_matches',
            'compiled_contracts_signatures': 'public_compiled_contracts_signatures',
        }
        
        logger.info(f"Initialized BigQuery client for dataset: {self.dataset_project}.{self.dataset_id}")

    def _get_order_by_column(self, table_name: str) -> str:
        """
        Get the column to use for ORDER BY based on table name.
        Uses primary key or unique identifier for consistent pagination.
        
        Args:
            table_name: Name of the BigQuery table
            
        Returns:
            Column name to use for ORDER BY
        """
        # Map table names to their primary key / unique identifier
        # Note: Avoid using BYTES columns for ORDER BY as they're slow without indexes
        order_by_map = {
            'public_code': 'created_at',  # Changed from code_hash for better performance
            'public_sources': 'created_at',  # Changed from source_hash for better performance
            'public_contracts': 'id',
            'public_compiled_contracts': 'id',
            'public_compiled_contracts_sources': 'id',
            'public_contract_deployments': 'id',
            'public_verified_contracts': 'id',
            'public_sourcify_matches': 'id',
            'public_signatures': 'id',
            'public_compiled_contracts_signatures': 'id',
        }
        
        return order_by_map.get(table_name, 'created_at')

    def query_table_chunked(
        self,
        table_name: str,
        batch_size: int = 10000,
        last_sync_timestamp: Optional[str] = None,
        order_by: str = None
    ) -> Iterator[pd.DataFrame]:
        """
        Query a table from BigQuery in chunks using LIMIT/OFFSET pagination.
        
        Args:
            table_name: Name of the table to query
            batch_size: Number of rows per batch
            last_sync_timestamp: Only fetch rows created/updated after this timestamp
            order_by: Column to order by (for consistent pagination)
            
        Yields:
            DataFrame chunks
        """
        full_table_id = f"{self.dataset_project}.{self.dataset_id}.{table_name}"
        
        # For initial sync, get all data
        # For incremental sync, filter by timestamp
        where_clause = ""
        if last_sync_timestamp:
            where_clause = f"WHERE created_at > TIMESTAMP('{last_sync_timestamp}')"
        
        logger.info(f"Querying BigQuery table: {table_name}")
        
        # Use cursor-based pagination (WHERE > last_value) instead of OFFSET
        # This is MUCH faster for large datasets
        order_by_column = self._get_order_by_column(table_name)
        total_rows = 0
        last_value = None
        
        while True:
            # Build WHERE clause for cursor-based pagination
            pagination_where = ""
            if last_value is not None:
                # For bytea columns, we need to handle them specially
                if order_by_column in ['code_hash', 'source_hash']:
                    # Convert bytes to hex string for comparison
                    hex_value = last_value.hex()
                    pagination_where = f"AND {order_by_column} > FROM_HEX('{hex_value}')"
                else:
                    # For other types (id, created_at), use direct comparison
                    pagination_where = f"AND {order_by_column} > '{last_value}'"
            
            # Combine with existing where clause
            combined_where = where_clause
            if pagination_where:
                if combined_where:
                    combined_where += f" {pagination_where}"
                else:
                    combined_where = f"WHERE {pagination_where[4:]}"  # Remove "AND "
            
            query = f"""
                SELECT *
                FROM `{full_table_id}`
                {combined_where}
                ORDER BY {order_by_column}
                LIMIT {batch_size}
            """
            
            logger.debug(f"Query: {query}")
            
            # Execute query
            query_job = self.client.query(query)
            df = query_job.to_dataframe()
            
            if df.empty:
                break
            
            total_rows += len(df)
            logger.info(f"Loaded batch of {len(df):,} rows from {table_name} (total so far: {total_rows:,})")
            
            # Remember last value for next iteration
            last_value = df[order_by_column].iloc[-1]
            
            yield df
            
            # If we got fewer rows than batch_size, we've reached the end
            if len(df) < batch_size:
                break
        
        logger.info(f"Completed reading {total_rows:,} rows from {table_name}")

    def get_table_count(self, table_name: str, last_sync_timestamp: Optional[str] = None) -> int:
        """
        Get the total row count for a table.
        
        Args:
            table_name: Name of the table
            last_sync_timestamp: Only count rows created/updated after this timestamp
            
        Returns:
            Total row count
        """
        full_table_id = f"{self.dataset_project}.{self.dataset_id}.{table_name}"
        
        where_clause = ""
        if last_sync_timestamp:
            where_clause = f"WHERE created_at > TIMESTAMP('{last_sync_timestamp}')"
        
        query = f"""
            SELECT COUNT(*) as total
            FROM `{full_table_id}`
            {where_clause}
        """
        
        query_job = self.client.query(query)
        result = list(query_job.result())[0]
        
        return result.total

    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Get the schema for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with schema information
        """
        full_table_id = f"{self.dataset_project}.{self.dataset_id}.{table_name}"
        table = self.client.get_table(full_table_id)
        
        schema_info = {
            'num_rows': table.num_rows,
            'num_bytes': table.num_bytes,
            'created': table.created,
            'modified': table.modified,
            'schema': [
                {
                    'name': field.name,
                    'type': field.field_type,
                    'mode': field.mode
                }
                for field in table.schema
            ]
        }
        
        return schema_info

    def list_tables(self) -> list:
        """
        List all tables in the Sourcify dataset.
        
        Returns:
            List of table names
        """
        dataset_ref = f"{self.dataset_project}.{self.dataset_id}"
        tables = self.client.list_tables(dataset_ref)
        
        table_names = [table.table_id for table in tables]
        logger.info(f"Found {len(table_names)} tables in dataset: {', '.join(table_names)}")
        
        return table_names
