"""
Sourcify BigQuery Sync Library

Provides components for syncing data from Sourcify's BigQuery dataset to PostgreSQL.
"""

from .bigquery_loader import BigQueryLoader
from .database_importer_optimized import OptimizedDatabaseImporter
from .state_tracker import StateTracker
from .index_state_manager import IndexStateManager

# Alias for backwards compatibility
DatabaseImporter = OptimizedDatabaseImporter

__all__ = ['BigQueryLoader', 'OptimizedDatabaseImporter', 'DatabaseImporter', 'StateTracker', 'IndexStateManager']
