# Sourcify Parquet Sync

Fast and efficient sync using Sourcify's public parquet export files.

## Overview

This sync method downloads pre-exported parquet files from `https://export.sourcify.dev/` and imports them directly into PostgreSQL. This is **much faster** than querying BigQuery, especially for initial bulk loads.

### Advantages over BigQuery Sync

- **10-100x faster** for initial bulk loads (no BigQuery query overhead)
- **No BigQuery costs** (uses public HTTP downloads)
- **Resumable** - tracks which files have been imported
- **Incremental** - only imports new/updated files
- **Bandwidth efficient** - downloads compressed parquet files

### Performance

For the complete Sourcify dataset (~10M rows in code table):
- **BigQuery sync**: 12+ hours
- **Parquet sync**: 1-2 hours (depending on network bandwidth)

## Quick Start

### 1. Run the Parquet Sync

```bash
# Run the parquet sync service
docker compose run --rm sync-parquet

# Or run directly with Python
cd services/sync-bigquery
python sync_parquet.py
```

### 2. Monitor Progress

The sync will:
1. Download the manifest from `https://export.sourcify.dev/manifest.json`
2. Check which files are already imported (tracked in `parquet_sync_state` table)
3. Download and import only new/missing files
4. Delete local files after import to save disk space
5. Track progress in the database

## Configuration

Configure via environment variables in `.env` or `docker-compose.yml`:

```bash
# Manifest URL (default: https://export.sourcify.dev/manifest.json)
MANIFEST_URL=https://export.sourcify.dev/manifest.json

# Base URL for downloading parquet files
PARQUET_BASE_URL=https://export.sourcify.dev

# Temporary directory for downloaded files (deleted after import)
DOWNLOAD_DIR=/tmp/parquet

# Use PostgreSQL COPY instead of INSERT (recommended)
USE_COPY=true

# Automatically drop/recreate indexes during import (HUGE speedup)
MANAGE_INDEXES=true

# Database connection
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=sourcify
POSTGRES_USER=sourcify
POSTGRES_PASSWORD=sourcify
```

## How It Works

### 1. Manifest Structure

The manifest contains:
```json
{
  "timestamp": 1765274786032,
  "dateStr": "2025-12-09T10:06:26.032362Z",
  "files": {
    "code": [
      "code/code_0_100000.parquet",
      "code/code_100000_200000.parquet",
      ...
    ],
    "sources": [...],
    ...
  }
}
```

### 2. Import Order

Tables are imported in dependency order to respect foreign keys:
1. `code` (no dependencies)
2. `sources` (no dependencies)
3. `signatures` (no dependencies)
4. `contracts` (depends on code)
5. `compiled_contracts` (depends on code)
6. `compiled_contracts_sources` (depends on compiled_contracts, sources)
7. `compiled_contracts_signatures` (depends on compiled_contracts, signatures)
8. `contract_deployments` (depends on contracts)
9. `verified_contracts` (depends on compiled_contracts, contract_deployments)
10. `sourcify_matches` (depends on verified_contracts)

### 3. Tracking

The `parquet_sync_state` table tracks each file:
- `file_path`: Path from manifest (e.g., "code/code_0_100000.parquet")
- `category`: Table name (e.g., "code")
- `manifest_timestamp`: Timestamp from manifest
- `import_status`: 'pending', 'downloading', 'importing', 'completed', 'failed'
- `rows_imported`: Number of rows imported from this file
- `imported_at`: When the import completed

### 4. Incremental Updates

On subsequent runs:
- Fetches latest manifest
- Compares manifest timestamp with tracked files
- Only imports files that are new or have been updated
- Skips files already imported with same or newer timestamp

## Use Cases

### Initial Bulk Load

Perfect for setting up a new Sourcify database replica:

```bash
# First run - imports everything
docker compose run --rm sync-parquet
```

Expected time: 1-2 hours for complete dataset

### Incremental Updates

Use with scheduler for periodic updates:

```bash
# Run daily to sync new data
0 2 * * * docker compose run --rm sync-parquet
```

Only new/updated files will be downloaded and imported.

### Resuming Failed Sync

If a sync is interrupted:
- Already imported files are tracked in database
- Restart the sync - it will resume where it left off
- Failed files can be retried (tracked with `retry_count`)

## Troubleshooting

### Disk Space

Parquet files are deleted after import, but you need temporary space:
- Average file size: 10-50 MB
- Peak disk usage: ~100-200 MB (one file at a time)
- Configure `DOWNLOAD_DIR` to use a location with sufficient space

### Network Issues

If downloads fail:
- Files are marked as 'failed' in `parquet_sync_state`
- Rerun the sync to retry failed files
- Check `retry_count` column to see how many times a file has failed

### Import Errors

Check logs for details:
```bash
docker compose logs sync-parquet

# Or view log file
cat /app/logs/sync_parquet.log
```

Common issues:
- Foreign key violations (import order issue)
- Duplicate key violations (file already imported)
- Data type mismatches

### Reset Tracking

To re-import everything from scratch:

```sql
-- Clear all tracking data
TRUNCATE parquet_sync_state;

-- Or clear specific category
DELETE FROM parquet_sync_state WHERE category = 'code';
```

## Comparison: Parquet vs BigQuery Sync

| Feature | Parquet Sync | BigQuery Sync |
|---------|--------------|---------------|
| **Speed** | ⚡️ Very Fast (1-2h) | 🐌 Slow (12+ h) |
| **Cost** | ✅ Free (HTTP) | 💰 BigQuery costs |
| **Resumable** | ✅ Yes | ❌ No |
| **Incremental** | ✅ Yes | ⚠️ Limited |
| **Tracking** | ✅ File-level | ⚠️ Table-level |
| **Network** | 📦 Efficient | 📡 Chatty |
| **Dependencies** | HTTP only | GCP credentials |

## Recommendation

**Use Parquet Sync for:**
- Initial bulk loads
- Periodic full refreshes
- Environments without GCP access
- Bandwidth-constrained environments

**Use BigQuery Sync for:**
- Real-time queries of specific data
- Custom filtering/transformation
- When you already have BigQuery infrastructure

**Best Approach:**
Use Parquet Sync for initial load and periodic full refreshes, then use incremental updates from the scheduler service.
