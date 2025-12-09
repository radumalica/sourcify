"""
Parquet Loader

Handles downloading and reading parquet files with memory management.
"""

import logging
import os
import hashlib
import time
import requests
import pandas as pd
import pyarrow.parquet as pq
from typing import Iterator, Optional
import gc

logger = logging.getLogger(__name__)


class ParquetLoader:
    """Loads parquet files with download and memory management"""

    def __init__(self, cache_dir: str):
        """
        Initialize the parquet loader.

        Args:
            cache_dir: Directory to cache downloaded parquet files
        """
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

    def download_file(
        self,
        url: str,
        file_path: str,
        max_retries: int = 3,
        timeout: int = 300
    ) -> tuple[str, int]:
        """
        Download a parquet file with retry logic.

        Args:
            url: URL to download from
            file_path: Relative path for the file (used for local cache)
            max_retries: Maximum number of retry attempts
            timeout: Request timeout in seconds

        Returns:
            Tuple of (checksum, file_size)

        Raises:
            requests.RequestException: If download fails after retries
        """
        # Determine local file path
        local_path = os.path.join(self.cache_dir, file_path)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        # Check if file already exists and is valid
        if os.path.exists(local_path):
            logger.info(f"File already exists in cache: {local_path}")
            file_size = os.path.getsize(local_path)
            checksum = self._calculate_checksum(local_path)
            return checksum, file_size

        # Download the file
        for attempt in range(max_retries):
            try:
                logger.info(f"Downloading {url} (attempt {attempt + 1}/{max_retries})")

                response = requests.get(url, stream=True, timeout=timeout)
                response.raise_for_status()

                # Stream download to file
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

                # Calculate checksum
                checksum = self._calculate_checksum(local_path)
                file_size = os.path.getsize(local_path)

                logger.info(
                    f"Downloaded successfully: {file_path} "
                    f"(size: {file_size:,} bytes, checksum: {checksum[:16]}...)"
                )

                return checksum, file_size

            except Exception as e:
                logger.warning(f"Download attempt {attempt + 1} failed: {e}")

                # Remove partial file
                if os.path.exists(local_path):
                    os.remove(local_path)

                if attempt == max_retries - 1:
                    logger.error(f"Failed to download {url} after {max_retries} attempts")
                    raise

                # Exponential backoff
                wait_time = 2 ** attempt
                logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)

    def _calculate_checksum(self, file_path: str) -> str:
        """
        Calculate SHA256 checksum of a file.

        Args:
            file_path: Path to the file

        Returns:
            SHA256 hex digest
        """
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha256.update(chunk)
        return sha256.hexdigest()

    def get_local_path(self, file_path: str) -> str:
        """
        Get the local cache path for a file.

        Args:
            file_path: Relative file path

        Returns:
            Full local path
        """
        return os.path.join(self.cache_dir, file_path)

    def read_parquet_chunked(
        self,
        file_path: str,
        batch_size: int = 10000
    ) -> Iterator[pd.DataFrame]:
        """
        Read a parquet file in chunks to manage memory.

        Args:
            file_path: Relative path to the parquet file
            batch_size: Number of rows per batch

        Yields:
            DataFrame chunks
        """
        local_path = self.get_local_path(file_path)

        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Parquet file not found: {local_path}")

        logger.info(f"Reading parquet file: {local_path} with batch_size={batch_size:,}")

        parquet_file = pq.ParquetFile(local_path)
        total_rows = 0

        for batch in parquet_file.iter_batches(batch_size=batch_size):
            df = batch.to_pandas()
            total_rows += len(df)

            logger.info(f"Loaded batch of {len(df):,} rows (total so far: {total_rows:,})")

            yield df

            # Periodic garbage collection
            if total_rows % 100000 == 0:
                gc.collect()

        logger.info(f"Completed reading {total_rows:,} rows from {file_path}")

    def read_parquet_full(self, file_path: str) -> pd.DataFrame:
        """
        Read entire parquet file into memory.

        Args:
            file_path: Relative path to the parquet file

        Returns:
            Complete DataFrame

        Warning:
            Use with caution for large files. Prefer read_parquet_chunked().
        """
        local_path = self.get_local_path(file_path)

        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Parquet file not found: {local_path}")

        logger.info(f"Reading full parquet file: {local_path}")
        df = pd.read_parquet(local_path)
        logger.info(f"Loaded {len(df):,} rows from {file_path}")

        return df

    def get_row_count(self, file_path: str) -> int:
        """
        Get the number of rows in a parquet file without loading all data.

        Args:
            file_path: Relative path to the parquet file

        Returns:
            Number of rows
        """
        local_path = self.get_local_path(file_path)

        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Parquet file not found: {local_path}")

        parquet_file = pq.ParquetFile(local_path)
        return parquet_file.metadata.num_rows

    def delete_file(self, file_path: str) -> bool:
        """
        Delete a specific parquet file from cache.

        Args:
            file_path: Relative path to the parquet file

        Returns:
            True if deleted successfully, False otherwise
        """
        local_path = self.get_local_path(file_path)

        if not os.path.exists(local_path):
            logger.debug(f"File not found, already deleted: {local_path}")
            return True

        try:
            file_size = os.path.getsize(local_path)
            os.remove(local_path)
            logger.info(f"Deleted cached file: {file_path} (freed {file_size:,} bytes)")
            return True
        except Exception as e:
            logger.warning(f"Failed to delete {local_path}: {e}")
            return False

    def cleanup_cache(self, keep_completed: bool = True, max_age_days: Optional[int] = None):
        """
        Clean up old files from the cache.

        Args:
            keep_completed: If True, keep files that were successfully imported
            max_age_days: Remove files older than this many days
        """
        logger.info("Cleaning up parquet cache...")

        # This would require coordination with StateTracker
        # For now, we'll just log that cleanup is not implemented
        logger.warning("Cache cleanup not yet implemented")
