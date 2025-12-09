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
        # For public datasets, we don't need authentication
        self.client = bigquery.Client(project=project_id) if project_id else bigquery.Client()
        
        # Sourcify's public BigQuery dataset
        # Tables have public_ prefix (e.g., public_code, public_verified_contracts)
        self.dataset_project = "bigquery-public-data"
        self.dataset_id = "sourcify"
        
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

    def query_table_chunked(
        self,
        table_name: str,
        batch_size: int = 10000,
        last_sync_timestamp: Optional[str] = None,
        order_by: str = None
    ) -> Iterator[pd.DataFrame]:
        """
        Query a table from BigQuery in chunks.
        
        Args:
            table_name: Name of the table to query
            batch_size: Number of rows per batch
            last_sync_timestamp: Only fetch rows created/updated after this timestamp
            order_by: Column to order by (for consistent pagination)
            
        Yields:
            DataFrame chunks
        """
        # Build the query
        full_table_id = f"{self.dataset_project}.{self.dataset_id}.{table_name}"
        
        # For initial sync, get all data
        # For incremental sync, filter by timestamp
        where_clause = ""
        if last_sync_timestamp:
            where_clause = f"WHERE created_at > TIMESTAMP('{last_sync_timestamp}')"
        
        # Order by clause for consistent pagination
        order_clause = f"ORDER BY {order_by}" if order_by else ""
        
        query = f"""
            SELECT *
            FROM `{full_table_id}`
            {where_clause}
            {order_clause}
        """
        
        logger.info(f"Querying BigQuery table: {table_name}")
        logger.debug(f"Query: {query}")
        
        # Execute query with pagination
        query_job = self.client.query(query)
        
        # Fetch results in batches
        total_rows = 0
        for batch in query_job.result(page_size=batch_size).to_dataframe_iterable():
            total_rows += len(batch)
            logger.info(f"Loaded batch of {len(batch):,} rows from {table_name} (total so far: {total_rows:,})")
            yield batch
        
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
