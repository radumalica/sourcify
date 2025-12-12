#!/usr/bin/env python3
"""
Sourcify Parquet Sync Service

Downloads and imports parquet files from Sourcify's public export manifest.
Much faster than BigQuery queries for initial bulk loads.
"""

import os
import sys
import logging
import psycopg2
import requests
import time
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional
from dotenv import load_dotenv
import pandas as pd
import pyarrow.parquet as pq

from lib import OptimizedDatabaseImporter, IndexStateManager
from validate_transformations import validate_creation_transformations, validate_runtime_transformations

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/app/logs/sync_parquet.log')
    ]
)
logger = logging.getLogger(__name__)


class ParquetSyncTracker:
    """Tracks which parquet files have been imported"""
    
    def __init__(self, conn):
        self.conn = conn
    
    def reset_stuck_imports(self, timeout_minutes: int = 10):
        """Reset imports that are stuck in downloading/importing/failed status"""
        cursor = self.conn.cursor()
        try:
            # Reset failed imports immediately (no timeout)
            cursor.execute("""
                UPDATE parquet_sync_state
                SET import_status = 'pending',
                    error_message = 'Retry after previous failure'
                WHERE import_status = 'failed'
                RETURNING file_path, category, import_status
            """)
            
            failed_files = cursor.fetchall()
            
            # Reset stuck downloading/importing (with timeout)
            cursor.execute("""
                UPDATE parquet_sync_state
                SET import_status = 'pending',
                    error_message = 'Reset from stuck status after timeout'
                WHERE import_status IN ('downloading', 'importing')
                AND updated_at < NOW() - INTERVAL '%s minutes'
                RETURNING file_path, category, import_status
            """ % timeout_minutes, ())
            
            stuck_files = cursor.fetchall()
            
            reset_files = failed_files + stuck_files
            self.conn.commit()
            
            if reset_files:
                logger.warning(f"Reset {len(reset_files)} imports for retry:")
                for file_path, category, status in reset_files:
                    logger.warning(f"  - {file_path} (was {status})")
            
            return len(reset_files)
        finally:
            cursor.close()
    
    def mark_file_downloading(self, file_path: str, category: str, manifest_timestamp: int, file_size: int = None):
        """Mark a file as being downloaded"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO parquet_sync_state 
                (file_path, category, manifest_timestamp, file_size_bytes, import_status, downloaded_at)
                VALUES (%s, %s, to_timestamp(%s/1000.0), %s, 'downloading', NOW())
                ON CONFLICT (file_path) 
                DO UPDATE SET 
                    downloaded_at = NOW(),
                    import_status = 'downloading',
                    manifest_timestamp = EXCLUDED.manifest_timestamp
            """, (file_path, category, manifest_timestamp, file_size))
            self.conn.commit()
        finally:
            cursor.close()
    
    def mark_file_importing(self, file_path: str):
        """Mark a file as being imported"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                UPDATE parquet_sync_state
                SET import_status = 'importing'
                WHERE file_path = %s
            """, (file_path,))
            self.conn.commit()
        finally:
            cursor.close()
    
    def mark_file_imported(self, file_path: str, rows_imported: int):
        """Mark a file as successfully imported"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                UPDATE parquet_sync_state
                SET import_status = 'completed',
                    imported_at = NOW(),
                    rows_imported = %s,
                    error_message = NULL,
                    retry_count = 0
                WHERE file_path = %s
            """, (rows_imported, file_path))
            self.conn.commit()
        finally:
            cursor.close()
    
    def mark_file_failed(self, file_path: str, error_message: str):
        """Mark a file as failed"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                UPDATE parquet_sync_state
                SET import_status = 'failed',
                    error_message = %s,
                    retry_count = retry_count + 1
                WHERE file_path = %s
            """, (error_message, file_path))
            self.conn.commit()
        finally:
            cursor.close()
    
    def is_file_imported(self, file_path: str, manifest_timestamp: int) -> bool:
        """Check if a file has already been imported"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT import_status
                FROM parquet_sync_state
                WHERE file_path = %s
            """, (file_path,))
            
            result = cursor.fetchone()
            if not result:
                return False
            
            status = result[0]
            
            # File is imported if status is completed
            # We don't check manifest timestamp because parquet files are immutable
            # Once a file is successfully imported, we don't need to re-import it
            return status == 'completed'
        finally:
            cursor.close()
    
    def get_table_row_count(self, table_name: str) -> int:
        """Get the approximate number of rows in a table (fast estimate)"""
        cursor = self.conn.cursor()
        try:
            # Use PostgreSQL's statistics for a fast estimate instead of COUNT(*)
            # This is much faster for large tables
            cursor.execute("""
                SELECT reltuples::bigint 
                FROM pg_class 
                WHERE relname = %s
            """, (table_name,))
            result = cursor.fetchone()
            return result[0] if result and result[0] else 0
        finally:
            cursor.close()
    
    def is_category_imported(self, category: str, manifest_timestamp: int) -> bool:
        """Check if a category has been fully imported for this manifest"""
        cursor = self.conn.cursor()
        try:
            # Check if all files for this category are marked as completed
            cursor.execute("""
                SELECT COUNT(*) as total,
                       COUNT(CASE WHEN import_status = 'completed' THEN 1 END) as completed
                FROM parquet_sync_state
                WHERE category = %s
            """, (category,))
            
            result = cursor.fetchone()
            if not result or result[0] == 0:
                return False
            
            total, completed = result
            return total == completed and total > 0
        finally:
            cursor.close()
    
    def get_category_stats(self, category: str) -> Dict:
        """Get import statistics for a category"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN import_status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN import_status = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(rows_imported) as total_rows
                FROM parquet_sync_state
                WHERE category = %s
            """, (category,))
            
            result = cursor.fetchone()
            return {
                'total': result[0] or 0,
                'completed': result[1] or 0,
                'failed': result[2] or 0,
                'total_rows': result[3] or 0
            }
        finally:
            cursor.close()


def get_db_connection():
    """Create database connection from environment variables"""
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=int(os.getenv('POSTGRES_PORT', '5432')),
        database=os.getenv('POSTGRES_DB', 'sourcify'),
        user=os.getenv('POSTGRES_USER', 'sourcify'),
        password=os.getenv('POSTGRES_PASSWORD', 'sourcify'),
    )
    conn.set_session(autocommit=False)
    return conn


def download_parquet_file(url: str, local_path: str) -> int:
    """
    Download a parquet file from URL
    
    Returns:
        File size in bytes
    """
    logger.info(f"Downloading {url}...")
    
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    downloaded = 0
    
    with open(local_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
    
    logger.info(f"Downloaded {downloaded:,} bytes")
    return downloaded


def import_parquet_file(file_path: str, table_name: str, conn, use_copy: bool = True) -> int:
    """
    Import a parquet file into PostgreSQL
    
    Returns:
        Number of rows imported
    """
    logger.info(f"Reading parquet file: {file_path}")

    # Read parquet file using pyarrow for better control over nullable types
    # The default pandas bool dtype cannot represent NULL and converts NULL to False
    parquet_table = pq.read_table(file_path)

    # Identify boolean columns that need special handling
    boolean_columns = []
    for i, field in enumerate(parquet_table.schema):
        if pq.types.is_boolean(field.type):
            boolean_columns.append(field.name)

    if boolean_columns:
        logger.info(f"Identified boolean columns that require NULL preservation: {boolean_columns}")

    # Convert to pandas DataFrame with standard dtypes (to preserve existing bytea handling)
    # Boolean columns will be converted to object dtype manually to preserve NULLs
    df = parquet_table.to_pandas()

    rows_count = len(df)

    logger.info(f"Parquet file contains {rows_count:,} rows")

    if rows_count == 0:
        logger.warning("Parquet file is empty, skipping import")
        return 0

    # Log dataframe info
    logger.info(f"DataFrame columns: {list(df.columns)}")
    logger.info(f"DataFrame dtypes: {dict(df.dtypes)}")

    # Convert dtypes for compatibility and NULL preservation
    for col in df.columns:
        dtype_str = str(df[col].dtype)

        # Convert string[python] to object
        if dtype_str.startswith('string'):
            logger.info(f"Converting column '{col}' from {dtype_str} to object dtype")
            df[col] = df[col].astype('object')

        # Convert bool to object to preserve NULL values
        # IMPORTANT: This must be done BEFORE any data processing
        elif df[col].dtype == 'bool' or col in boolean_columns:
            # For boolean columns, check if there are any NaN/NA values in the pyarrow table
            # If yes, we need to convert to object dtype to preserve them
            logger.info(f"Converting column '{col}' from bool to object dtype to preserve NULL values")
            # Get the original pyarrow column to check for nulls
            pa_col = parquet_table.column(col)
            if pa_col.null_count > 0:
                logger.info(f"  Column '{col}' has {pa_col.null_count} NULL values that will be preserved")
                # Convert using pyarrow data to preserve NULLs
                df[col] = pa_col.to_pandas(deduplicate_objects=False)
                # Now convert to object dtype
                df[col] = df[col].astype('object')
            else:
                # No NULLs, safe to use standard conversion
                df[col] = df[col].astype('object')

        # Replace pd.NA with None for all columns
        df[col] = df[col].replace({pd.NA: None})
    
    # Special handling for bytea columns that might be base64 encoded or raw bytes
    # BigQuery stores these as BYTES, but Parquet/pandas might convert them to base64 strings
    import base64
    bytea_columns_by_table = {
        'contracts': ['creation_code_hash', 'runtime_code_hash'],
        'compiled_contracts': ['creation_code_hash', 'runtime_code_hash'],
        'code': ['code_hash', 'code_hash_keccak'],
        'sources': ['source_hash', 'source_hash_keccak'],
        'signatures': ['signature_hash_32'],
        'compiled_contracts_signatures': ['signature_hash_32'],
    }
    
    if table_name in bytea_columns_by_table:
        for col in bytea_columns_by_table[table_name]:
            if col in df.columns:
                # Check what type we're getting
                sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                logger.info(f"Column '{col}': sample type={type(sample)}, value preview={repr(sample)[:100] if sample else None}")
                
                def ensure_bytes(x):
                    if pd.isna(x) or x is None:
                        return None
                    if isinstance(x, bytes):
                        return x  # Already bytes - use directly
                    if isinstance(x, memoryview):
                        return bytes(x)  # Convert memoryview to bytes
                    if isinstance(x, str):
                        # Could be base64 encoded - try to decode
                        try:
                            return base64.b64decode(x)
                        except Exception as e:
                            logger.warning(f"Failed to decode base64 in column '{col}': {e}")
                            # Maybe it's hex encoded?
                            if x.startswith('\\x') or x.startswith('0x'):
                                try:
                                    return bytes.fromhex(x.replace('\\x', '').replace('0x', ''))
                                except:
                                    pass
                            return None
                    logger.warning(f"Unexpected type {type(x)} in bytea column '{col}'")
                    return None
                
                df[col] = df[col].apply(ensure_bytes)
                # Verify
                sample_after = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                logger.info(f"Column '{col}' after conversion: type={type(sample_after)}, len={len(sample_after) if sample_after else 0}")

    # Clean up ALL object and string columns to ensure valid UTF-8 encoding
    # BUT: Skip columns that contain bytea (binary) data OR boolean data
    # This is critical because parquet files from BigQuery may contain invalid UTF-8 in text columns
    logger.info(f"Validating UTF-8 encoding in text columns...")

    # Identify bytea and boolean columns by checking their content
    bytea_columns = set()
    boolean_columns = set()
    text_columns = []

    for col in df.columns:
        # Check both object and string dtypes
        if df[col].dtype == 'object' or str(df[col].dtype).startswith('string'):
            # Check first few non-null values to determine column type
            samples = df[col].dropna().head(10)
            if len(samples) > 0 and df[col].dtype == 'object':
                # Check for bytea columns
                bytes_count = sum(isinstance(x, bytes) or isinstance(x, memoryview) for x in samples)
                if bytes_count > len(samples) / 2:  # If more than half are bytes, it's a bytea column
                    bytea_columns.add(col)
                    logger.info(f"Column '{col}' identified as bytea (binary), skipping UTF-8 cleaning")
                    continue

                # Check for boolean columns (converted from bool to object to preserve NULL)
                bool_count = sum(isinstance(x, bool) for x in samples)
                if bool_count > len(samples) / 2:  # If more than half are booleans, it's a boolean column
                    boolean_columns.add(col)
                    logger.info(f"Column '{col}' identified as boolean, skipping UTF-8 cleaning")
                    continue

            text_columns.append(col)
    
    # Clean ALL text columns (object and string dtypes, but not bytea)
    for col in text_columns:
        # For text columns, ensure valid UTF-8
        def ensure_valid_utf8(x):
            if pd.isna(x) or x is None:
                return None
            if isinstance(x, bytes):
                # Bytes in a string column - decode with error handling
                try:
                    decoded = x.decode('utf-8', errors='replace')
                    if decoded != x.decode('utf-8', errors='ignore'):
                        logger.warning(f"Replaced invalid UTF-8 bytes in column '{col}'")
                    return decoded
                except Exception as e:
                    logger.error(f"Cannot decode bytes in text column '{col}': {e}, converting to NULL")
                    return None
            if not isinstance(x, str):
                # Convert to string
                try:
                    return str(x)
                except Exception as e:
                    logger.error(f"Cannot convert value to string in column '{col}': {e}")
                    return None
            # Validate and clean UTF-8 for strings
            try:
                x.encode('utf-8')
                return x
            except (UnicodeEncodeError, UnicodeDecodeError) as e:
                try:
                    # Replace invalid UTF-8 sequences
                    cleaned = x.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
                    logger.warning(f"Replaced invalid UTF-8 in column '{col}': {e}")
                    return cleaned
                except Exception as e2:
                    logger.error(f"Cannot clean string in column '{col}': {e2}, converting to NULL")
                    return None
        
        df[col] = df[col].apply(ensure_valid_utf8)
        logger.info(f"Cleaned text column '{col}'")
        
        # Verify cleaning worked - check a sample
        sample_val = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
        if sample_val:
            try:
                sample_val.encode('utf-8')
                logger.debug(f"Column '{col}' UTF-8 validation passed")
            except Exception as e:
                logger.error(f"Column '{col}' still has invalid UTF-8 after cleaning: {e}")
                raise ValueError(f"Failed to clean UTF-8 in column '{col}'")
    
    # Also ensure bytea columns are properly handled
    for col in bytea_columns:
        def ensure_bytes_or_none(x):
            if pd.isna(x) or x is None:
                return None
            if isinstance(x, bytes):
                return x
            if isinstance(x, memoryview):
                return bytes(x)
            # Unexpected type in bytea column
            logger.error(f"Unexpected type {type(x)} in bytea column {col}, converting to NULL")
            return None

        df[col] = df[col].apply(ensure_bytes_or_none)

    # Normalize JSON columns to strings for tables with JSON validation constraints
    # This is needed because Parquet may store JSON as either strings OR native Python objects
    if table_name in ['verified_contracts', 'sourcify_matches']:
        json_columns = ['creation_values', 'creation_transformations', 'runtime_values', 'runtime_transformations']
        for col in json_columns:
            if col in df.columns:
                logger.info(f"Normalizing JSON column '{col}' to ensure consistent string format...")

                def normalize_json(x):
                    if pd.isna(x) or x is None:
                        return None
                    if isinstance(x, str):
                        # Already a string - keep as-is
                        return x
                    if isinstance(x, (dict, list)):
                        # Convert Python object to JSON string
                        return json.dumps(x)
                    logger.warning(f"Unexpected type {type(x).__name__} in JSON column '{col}', converting to JSON string")
                    return json.dumps(x)

                df[col] = df[col].apply(normalize_json)
                logger.info(f"Normalized column '{col}'")

    # Validate and fix JSON columns for verified_contracts table
    if table_name == 'verified_contracts':
        logger.info("Validating JSON columns for verified_contracts...")
        validation_errors = []
        fix_count = 0

        for idx, row in df.iterrows():
            # Validate creation_transformations
            if 'creation_transformations' in df.columns and pd.notna(row['creation_transformations']):
                creation_trans_str = row['creation_transformations']
                try:
                    creation_trans = json.loads(creation_trans_str)
                except json.JSONDecodeError as e:
                    error_msg = f"Row {idx}: Invalid JSON in creation_transformations: {e}\nData: {creation_trans_str[:200]}"
                    logger.error(error_msg)
                    validation_errors.append(error_msg)
                    df.at[idx, 'creation_transformations'] = None
                    continue

                # Fix: Convert float offsets to integers (pandas/parquet sometimes converts ints to floats)
                if isinstance(creation_trans, list):
                    for trans in creation_trans:
                        if isinstance(trans, dict) and 'offset' in trans:
                            if isinstance(trans['offset'], float):
                                trans['offset'] = int(trans['offset'])
                                fix_count += 1

                # Validate structure
                is_valid, error = validate_creation_transformations(creation_trans)
                if not is_valid:
                    error_msg = f"Row {idx}: creation_transformations validation failed: {error}\nData: {json.dumps(creation_trans, indent=2)}"
                    logger.error(error_msg)
                    validation_errors.append(error_msg)
                    df.at[idx, 'creation_transformations'] = None
                else:
                    # Write back the (possibly fixed) JSON
                    df.at[idx, 'creation_transformations'] = json.dumps(creation_trans)

            # Validate runtime_transformations
            if 'runtime_transformations' in df.columns and pd.notna(row['runtime_transformations']):
                runtime_trans_str = row['runtime_transformations']
                try:
                    runtime_trans = json.loads(runtime_trans_str)
                except json.JSONDecodeError as e:
                    error_msg = f"Row {idx}: Invalid JSON in runtime_transformations: {e}\nData: {runtime_trans_str[:200]}"
                    logger.error(error_msg)
                    validation_errors.append(error_msg)
                    df.at[idx, 'runtime_transformations'] = None
                    continue

                # Fix: Convert float offsets to integers (pandas/parquet sometimes converts ints to floats)
                if isinstance(runtime_trans, list):
                    for trans in runtime_trans:
                        if isinstance(trans, dict) and 'offset' in trans:
                            if isinstance(trans['offset'], float):
                                trans['offset'] = int(trans['offset'])
                                fix_count += 1

                # Validate structure
                is_valid, error = validate_runtime_transformations(runtime_trans)
                if not is_valid:
                    error_msg = f"Row {idx}: runtime_transformations validation failed: {error}\nData: {json.dumps(runtime_trans, indent=2)}"
                    logger.error(error_msg)
                    validation_errors.append(error_msg)
                    df.at[idx, 'runtime_transformations'] = None
                else:
                    # Write back the (possibly fixed) JSON
                    df.at[idx, 'runtime_transformations'] = json.dumps(runtime_trans)

        if fix_count > 0:
            logger.info(f"Fixed {fix_count} transformation objects (converted float offsets to integers)")

        if validation_errors:
            logger.warning(f"Found {len(validation_errors)} validation errors. Invalid data has been set to NULL to allow import to continue.")
            # Log to a separate file for detailed analysis
            with open(f'/app/logs/validation_errors_{table_name}.log', 'a') as f:
                f.write(f"\n=== Validation errors at {datetime.now(timezone.utc)} ===\n")
                for error in validation_errors:
                    f.write(f"{error}\n\n")

    # Import using optimized database importer
    importer = OptimizedDatabaseImporter(conn, use_copy=use_copy, manage_indexes=False)
    rows_imported = importer.import_dataframe(df, table_name, batch_size=100000)
    
    logger.info(f"Imported {rows_imported:,} rows into {table_name}")
    
    return rows_imported


def sync_category_from_parquet(
    category: str,
    files: List[str],
    manifest_timestamp: int,
    base_url: str,
    conn,
    tracker: ParquetSyncTracker,
    download_dir: str = '/tmp/parquet',
    use_copy: bool = True,
    manage_indexes: bool = True
) -> Dict:
    """
    Sync a category (table) by downloading and importing parquet files
    
    Args:
        category: Table name (e.g., 'code', 'sources')
        files: List of parquet file paths from manifest
        manifest_timestamp: Timestamp from manifest
        base_url: Base URL for downloading files
        conn: Database connection
        tracker: Parquet sync tracker
        download_dir: Directory for temporary parquet files
        use_copy: Use PostgreSQL COPY for imports
        manage_indexes: Drop/recreate indexes during import
        
    Returns:
        Dictionary with sync statistics
    """
    logger.info(f"=== Syncing category: {category} ===")
    logger.info(f"Total files in category: {len(files)}")
    
    # Check if table already has data
    existing_rows = tracker.get_table_row_count(category)
    if existing_rows > 0:
        logger.info(f"Table '{category}' currently contains {existing_rows:,} rows")
    
    # Create download directory
    os.makedirs(download_dir, exist_ok=True)
    
    stats = {
        'total_files': len(files),
        'files_downloaded': 0,
        'files_imported': 0,
        'files_skipped': 0,
        'files_failed': 0,
        'total_rows': 0,
        'errors': []
    }
    
    # Check how many files are already imported
    files_to_import = []
    for file_path in files:
        if tracker.is_file_imported(file_path, manifest_timestamp):
            stats['files_skipped'] += 1
            logger.debug(f"Skipping already imported file: {file_path}")
        else:
            files_to_import.append(file_path)
    
    logger.info(f"Files to import: {len(files_to_import)} (skipped {stats['files_skipped']} already imported)")
    
    if not files_to_import:
        logger.info(f"All files for {category} are already imported, skipping")
        return stats
    
    # Manage indexes - drop before import
    if manage_indexes:
        logger.info(f"Preparing {category} for bulk import (dropping indexes)...")
        importer = OptimizedDatabaseImporter(conn, use_copy=use_copy, manage_indexes=True)
        importer.prepare_table_for_import(category)
    
    # Import each file
    for idx, file_path in enumerate(files_to_import, 1):
        file_name = os.path.basename(file_path)
        local_path = os.path.join(download_dir, file_name)
        file_url = f"{base_url}/{file_path}"
        
        try:
            logger.info(f"[{idx}/{len(files_to_import)}] Processing {file_path}")
            
            # Mark as downloading
            tracker.mark_file_downloading(file_path, category, manifest_timestamp)
            
            # Download file
            start_download = time.time()
            file_size = download_parquet_file(file_url, local_path)
            download_time = time.time() - start_download
            logger.info(f"Download completed in {download_time:.2f}s ({file_size/1024/1024/download_time:.2f} MB/s)")
            
            stats['files_downloaded'] += 1
            
            # Import file
            tracker.mark_file_importing(file_path)
            start_import = time.time()
            rows_imported = import_parquet_file(local_path, category, conn, use_copy=use_copy)
            import_time = time.time() - start_import
            logger.info(f"Import completed in {import_time:.2f}s ({rows_imported/import_time:.0f} rows/s)")
            
            # Mark as imported
            tracker.mark_file_imported(file_path, rows_imported)
            stats['files_imported'] += 1
            stats['total_rows'] += rows_imported
            
            # Delete local file to save space
            os.remove(local_path)
            logger.debug(f"Deleted local file: {local_path}")
            
            # Progress update
            progress_pct = (idx / len(files_to_import)) * 100
            logger.info(f"Progress: {progress_pct:.1f}% ({idx}/{len(files_to_import)} files, {stats['total_rows']:,} rows imported)")
            
        except Exception as e:
            error_msg = f"Error processing {file_path}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            tracker.mark_file_failed(file_path, str(e))
            stats['files_failed'] += 1
            stats['errors'].append(error_msg)
            
            # Clean up local file if it exists
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except:
                    pass
    
    # Manage indexes - recreate after import
    if manage_indexes and stats['files_imported'] > 0:
        logger.info(f"Finalizing {category} (recreating indexes)...")
        importer = OptimizedDatabaseImporter(conn, use_copy=use_copy, manage_indexes=True)
        importer.finalize_table_after_import(category)
    
    logger.info(
        f"Completed {category}: "
        f"{stats['files_imported']} files imported, "
        f"{stats['files_skipped']} skipped, "
        f"{stats['files_failed']} failed, "
        f"{stats['total_rows']:,} total rows"
    )
    
    return stats


def main():
    """Main sync function"""
    load_dotenv()
    
    logger.info("=" * 80)
    logger.info("Starting Sourcify Parquet Sync")
    logger.info("=" * 80)
    
    # Configuration
    manifest_url = os.getenv('MANIFEST_URL', 'https://export.sourcify.dev/manifest.json')
    base_url = os.getenv('PARQUET_BASE_URL', 'https://export.sourcify.dev')
    download_dir = os.getenv('DOWNLOAD_DIR', '/app/parquet')
    use_copy = os.getenv('USE_COPY', 'true').lower() == 'true'
    manage_indexes = os.getenv('MANAGE_INDEXES', 'true').lower() == 'true'
    
    # Table import order (respects foreign key dependencies)
    import_order = [
        'code',
        'sources',
        'signatures',
        'contracts',
        'compiled_contracts',
        'compiled_contracts_sources',
        'compiled_contracts_signatures',
        'contract_deployments',
        'verified_contracts',
        'sourcify_matches',
    ]
    
    logger.info(f"Configuration:")
    logger.info(f"  Manifest URL: {manifest_url}")
    logger.info(f"  Base URL: {base_url}")
    logger.info(f"  Download directory: {download_dir}")
    logger.info(f"  Use COPY: {use_copy}")
    logger.info(f"  Manage indexes: {manage_indexes}")
    logger.info(f"  Import order: {', '.join(import_order)}")
    
    start_time = datetime.now()
    
    try:
        # Connect to database
        logger.info("Connecting to database...")
        conn = get_db_connection()
        tracker = ParquetSyncTracker(conn)
        logger.info("Database connection established")
        
        # Fetch manifest
        logger.info(f"Fetching manifest from {manifest_url}...")
        response = requests.get(manifest_url)
        response.raise_for_status()
        manifest = response.json()
        
        manifest_timestamp = manifest['timestamp']
        manifest_date = manifest['dateStr']
        files_by_category = manifest['files']
        
        logger.info(f"Manifest timestamp: {manifest_date} ({manifest_timestamp})")
        logger.info(f"Available categories: {', '.join(files_by_category.keys())}")
        
        # Reset any stuck imports from previous runs
        logger.info("")
        logger.info("Checking for stuck imports...")
        stuck_count = tracker.reset_stuck_imports(timeout_minutes=10)
        if stuck_count == 0:
            logger.info("No stuck imports found")
        logger.info("")
        
        # Show existing data summary
        logger.info("=== Current database state ===")
        for category in import_order:
            try:
                row_count = tracker.get_table_row_count(category)
                cat_stats = tracker.get_category_stats(category)
                if row_count > 0 or cat_stats['completed'] > 0:
                    logger.info(f"  {category}: {row_count:,} rows | Tracking: {cat_stats['completed']}/{cat_stats['total']} files completed")
            except Exception as e:
                logger.debug(f"  {category}: Unable to check ({e})")
        logger.info("")
        
        # Sync each category in order
        total_stats = {
            'total_files': 0,
            'files_downloaded': 0,
            'files_imported': 0,
            'files_skipped': 0,
            'files_failed': 0,
            'total_rows': 0,
            'errors': []
        }
        
        for category in import_order:
            if category not in files_by_category:
                logger.warning(f"Category '{category}' not found in manifest, skipping")
                continue
            
            files = files_by_category[category]
            
            category_stats = sync_category_from_parquet(
                category,
                files,
                manifest_timestamp,
                base_url,
                conn,
                tracker,
                download_dir,
                use_copy,
                manage_indexes
            )
            
            # Aggregate stats
            for key in ['total_files', 'files_downloaded', 'files_imported', 'files_skipped', 'files_failed', 'total_rows']:
                total_stats[key] += category_stats[key]
            total_stats['errors'].extend(category_stats['errors'])
        
        # Print final statistics
        logger.info("=" * 80)
        logger.info("Sync completed!")
        logger.info("=" * 80)
        logger.info(f"Total files: {total_stats['total_files']}")
        logger.info(f"Files downloaded: {total_stats['files_downloaded']}")
        logger.info(f"Files imported: {total_stats['files_imported']}")
        logger.info(f"Files skipped (already imported): {total_stats['files_skipped']}")
        logger.info(f"Files failed: {total_stats['files_failed']}")
        logger.info(f"Total rows imported: {total_stats['total_rows']:,}")
        
        if total_stats['errors']:
            logger.error(f"\nErrors ({len(total_stats['errors'])}):")
            for error in total_stats['errors'][:10]:  # Show first 10 errors
                logger.error(f"  - {error}")
            if len(total_stats['errors']) > 10:
                logger.error(f"  ... and {len(total_stats['errors']) - 10} more")
        
        # Calculate duration
        duration = datetime.now() - start_time
        logger.info(f"\nSync duration: {duration}")
        
        # Close database connection
        conn.close()
        
        # Exit with appropriate code
        exit_code = 0 if total_stats['files_failed'] == 0 else 1
        sys.exit(exit_code)
        
    except Exception as e:
        logger.error(f"Fatal error during sync: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
