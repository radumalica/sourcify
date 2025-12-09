"""
Manifest Manager

Handles fetching and parsing the Sourcify export manifest.json file.
"""

import logging
import requests
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urljoin

logger = logging.getLogger(__name__)


class ManifestManager:
    """Manages manifest fetching and parsing from Sourcify export"""

    def __init__(self, manifest_url: str, base_url: Optional[str] = None):
        """
        Initialize the manifest manager.

        Args:
            manifest_url: URL to the manifest.json file
            base_url: Base URL for parquet files (defaults to manifest URL directory)
        """
        self.manifest_url = manifest_url
        self.base_url = base_url or manifest_url.rsplit('/', 1)[0] + '/'
        self.manifest_data = None

    def fetch_manifest(self, timeout: int = 30) -> Dict:
        """
        Fetch the manifest from the remote URL.

        Args:
            timeout: Request timeout in seconds

        Returns:
            Parsed manifest dictionary

        Raises:
            requests.RequestException: If fetching fails
        """
        logger.info(f"Fetching manifest from {self.manifest_url}")

        try:
            response = requests.get(self.manifest_url, timeout=timeout)
            response.raise_for_status()
            self.manifest_data = response.json()

            logger.info(
                f"Manifest fetched successfully. "
                f"Timestamp: {self.manifest_data.get('dateStr', 'unknown')}"
            )

            return self.manifest_data

        except requests.RequestException as e:
            logger.error(f"Failed to fetch manifest: {e}")
            raise

    def get_manifest_timestamp(self) -> datetime:
        """
        Get the manifest timestamp as a datetime object.

        Returns:
            Manifest timestamp
        """
        if not self.manifest_data:
            raise ValueError("Manifest not loaded. Call fetch_manifest() first.")

        # Try to parse the ISO timestamp
        date_str = self.manifest_data.get('dateStr')
        if date_str:
            # Remove microseconds precision beyond 6 digits and Z timezone
            date_str = date_str.replace('Z', '+00:00')
            if '.' in date_str:
                parts = date_str.split('.')
                if '+' in parts[1]:
                    micros, tz = parts[1].split('+')
                    micros = micros[:6]  # Truncate to 6 digits
                    date_str = f"{parts[0]}.{micros}+{tz}"

            return datetime.fromisoformat(date_str)

        # Fallback to Unix timestamp
        timestamp = self.manifest_data.get('timestamp')
        if timestamp:
            return datetime.fromtimestamp(timestamp / 1000)  # Convert from milliseconds

        raise ValueError("No valid timestamp found in manifest")

    def get_files_by_category(self) -> Dict[str, List[Dict]]:
        """
        Get files organized by category.

        Returns:
            Dictionary mapping category name to list of file info dicts
        """
        if not self.manifest_data:
            raise ValueError("Manifest not loaded. Call fetch_manifest() first.")

        files_data = self.manifest_data.get('files', {})
        result = {}

        for category, file_list in files_data.items():
            file_info_list = []
            for file_path in file_list:
                file_info_list.append({
                    'path': file_path,
                    'url': urljoin(self.base_url, file_path),
                    'category': category,
                })
            result[category] = file_info_list

        logger.info(
            f"Manifest contains {len(result)} categories: "
            f"{', '.join(f'{cat}({len(files)})' for cat, files in result.items())}"
        )

        return result

    def get_all_files(self) -> List[Dict]:
        """
        Get all files as a flat list.

        Returns:
            List of file info dictionaries
        """
        files_by_category = self.get_files_by_category()
        all_files = []
        for file_list in files_by_category.values():
            all_files.extend(file_list)
        return all_files

    def get_category_files(self, category: str) -> List[Dict]:
        """
        Get files for a specific category.

        Args:
            category: Category name (e.g., 'code', 'sources', 'contracts')

        Returns:
            List of file info dictionaries for that category
        """
        files_by_category = self.get_files_by_category()
        return files_by_category.get(category, [])
