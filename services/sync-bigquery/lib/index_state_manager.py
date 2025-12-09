"""
Index State Manager

Tracks dropped indexes across sync runs to ensure they're always restored,
even if the sync is interrupted.
"""

import json
import os
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class IndexStateManager:
    """Manages persistent state of dropped indexes"""

    def __init__(self, state_file: str = '/app/logs/index_state.json'):
        """
        Initialize the index state manager.

        Args:
            state_file: Path to state file for tracking dropped indexes
        """
        self.state_file = state_file
        self.state = self._load_state()

    def _load_state(self) -> Dict:
        """Load state from file"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load index state file: {e}")
                return {}
        return {}

    def _save_state(self):
        """Save state to file"""
        try:
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save index state file: {e}")

    def mark_indexes_dropped(self, table_name: str, indexes: List[Dict]):
        """
        Mark indexes as dropped for a table.

        Args:
            table_name: Name of the table
            indexes: List of index definitions that were dropped
        """
        self.state[table_name] = {
            'indexes': indexes,
            'dropped': True,
            'timestamp': str(os.times())
        }
        self._save_state()
        logger.info(f"Marked {len(indexes)} indexes as dropped for {table_name}")

    def mark_indexes_restored(self, table_name: str):
        """
        Mark indexes as restored for a table.

        Args:
            table_name: Name of the table
        """
        if table_name in self.state:
            del self.state[table_name]
            self._save_state()
            logger.info(f"Marked indexes as restored for {table_name}")

    def get_pending_indexes(self, table_name: str) -> Optional[List[Dict]]:
        """
        Get indexes that need to be restored for a table.

        Args:
            table_name: Name of the table

        Returns:
            List of index definitions to restore, or None if none pending
        """
        if table_name in self.state and self.state[table_name].get('dropped'):
            return self.state[table_name]['indexes']
        return None

    def get_all_pending_tables(self) -> List[str]:
        """
        Get all tables with pending index restoration.

        Returns:
            List of table names
        """
        return [
            table for table, data in self.state.items()
            if data.get('dropped')
        ]

    def has_pending_indexes(self, table_name: str) -> bool:
        """
        Check if a table has pending index restoration.

        Args:
            table_name: Name of the table

        Returns:
            True if indexes need to be restored
        """
        return table_name in self.state and self.state[table_name].get('dropped', False)

    def clear_all(self):
        """Clear all pending index state"""
        self.state = {}
        self._save_state()
        logger.info("Cleared all pending index state")
