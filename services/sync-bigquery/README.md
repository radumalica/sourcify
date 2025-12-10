# Sourcify Data Sync Services

This directory contains services for syncing data from Sourcify's public datasets.

## Quick Start

### For Initial Bulk Load (Recommended)

Use the **Parquet Sync** - fastest method (1-2 hours for complete dataset):

```bash
docker compose run --rm sync-parquet
```

### For Incremental Updates

Use the **BigQuery Sync Scheduler** - runs automatically on schedule:

```bash
docker compose up -d sync-bigquery-scheduler
```

## Available Sync Methods

### 1. Parquet Sync (⚡️ Fastest)

Downloads pre-exported parquet files from `https://export.sourcify.dev/`

**Pros:**
- 10-100x faster than BigQuery for bulk loads
- No GCP credentials needed
- Resumable - tracks imported files
- Free (uses public HTTP)

**Use for:**
- Initial database setup
- Periodic full refreshes
- Any bulk data imports

**Documentation:** [README_PARQUET.md](README_PARQUET.md)

**Run:**
```bash
docker compose run --rm sync-parquet
```

### 2. BigQuery Sync - Optimized (⚡ Fast)

Queries Sourcify's public BigQuery dataset with optimizations

**Pros:**
- 10-50x faster than original BigQuery sync
- Can filter/transform data
- Works with existing BigQuery infrastructure

**Use for:**
- When you need custom queries
- Incremental syncs
- When parquet files aren't available

**Documentation:** [README_OPTIMIZED.md](README_OPTIMIZED.md)

**Run:**
```bash
docker compose run --rm sync-bigquery-optimized
```

### 3. BigQuery Sync Scheduler (🔄 Automated)

Runs BigQuery sync on a schedule for continuous updates

**Configuration:**
```bash
# In .env file
SYNC_SCHEDULE="0 2 * * *"  # Daily at 2 AM
SYNC_MODE="incremental"     # Only sync new data
RUN_ON_STARTUP="false"      # Don't run immediately
```

**Run:**
```bash
docker compose up -d sync-bigquery-scheduler
```

## Comparison

| Method | Speed | Cost | Resumable | Best For |
|--------|-------|------|-----------|----------|
| **Parquet** | ⚡️⚡️⚡️ Very Fast | ✅ Free | ✅ Yes | Initial loads |
| **BigQuery Optimized** | ⚡️⚡️ Fast | 💰 BigQuery costs | ❌ No | Custom queries |
| **BigQuery Scheduler** | ⚡️⚡️ Fast | 💰 BigQuery costs | ✅ Yes | Incremental updates |

## Recommended Workflow

1. **Initial Setup**: Use Parquet Sync for fast bulk load
   ```bash
   docker compose run --rm sync-parquet
   ```

2. **Ongoing Updates**: Use BigQuery Scheduler for incremental updates
   ```bash
   docker compose up -d sync-bigquery-scheduler
   ```

3. **Periodic Full Refresh**: Run Parquet Sync monthly/quarterly
   ```bash
   docker compose run --rm sync-parquet
   ```

## Configuration

All sync services use these common environment variables:

```bash
# Database connection
POSTGRES_HOST=db
POSTGRES_PORT=5432
POSTGRES_DB=sourcify
POSTGRES_USER=sourcify
POSTGRES_PASSWORD=sourcify

# Performance tuning
USE_COPY=true              # Use PostgreSQL COPY (faster)
MANAGE_INDEXES=true        # Drop/recreate indexes during import

# BigQuery specific
GCP_PROJECT_ID=your-project-id
BATCH_SIZE=100000
NUM_WORKERS=3

# Parquet specific
MANIFEST_URL=https://export.sourcify.dev/manifest.json
PARQUET_BASE_URL=https://export.sourcify.dev
```

## Monitoring

### Logs

```bash
# View logs
docker compose logs sync-parquet
docker compose logs sync-bigquery-scheduler

# Or check log files
tail -f services/sync-bigquery/logs/sync_parquet.log
tail -f services/sync-bigquery/logs/sync_bigquery_optimized.log
```

### Database Tracking

Check sync progress in the database:

```sql
-- Parquet sync progress
SELECT 
    category,
    COUNT(*) as total_files,
    SUM(CASE WHEN import_status = 'completed' THEN 1 ELSE 0 END) as completed,
    SUM(rows_imported) as total_rows
FROM parquet_sync_state
GROUP BY category;

-- View failed files
SELECT file_path, error_message, retry_count
FROM parquet_sync_state
WHERE import_status = 'failed';
```

## Troubleshooting

### Disk Space Issues

Parquet sync needs temporary disk space:
- Configure `DOWNLOAD_DIR` in `.env`
- Ensure ~1GB free space
- Files are deleted after import

### Network Issues

If downloads fail:
- Check internet connectivity
- Verify `MANIFEST_URL` is accessible
- Retry failed files: rerun sync

### Import Errors

Check logs for details:
```bash
docker compose logs sync-parquet --tail=100
```

Common issues:
- Foreign key violations (check import order)
- Duplicate keys (file already imported)
- Data type mismatches

### Reset and Start Fresh

```sql
-- Clear parquet tracking
TRUNCATE parquet_sync_state;

-- Or clear specific table
DELETE FROM parquet_sync_state WHERE category = 'code';
```

## Performance Tips

### For Faster Imports

1. **Use Parquet Sync** instead of BigQuery for bulk loads
2. **Enable index management**: `MANAGE_INDEXES=true`
3. **Increase PostgreSQL memory** (uncomment in docker-compose.yml):
   ```yaml
   POSTGRES_SHARED_BUFFERS: 2GB
   POSTGRES_WORK_MEM: 256MB
   POSTGRES_MAINTENANCE_WORK_MEM: 2GB
   ```
4. **Use SSD storage** for PostgreSQL data volume
5. **Good network bandwidth** for downloading parquet files

### For Incremental Updates

1. Use **BigQuery Scheduler** with `SYNC_MODE=incremental`
2. Run during off-peak hours
3. Monitor `parquet_sync_state` for new files

## Support

- **Parquet Sync Issues**: See [README_PARQUET.md](README_PARQUET.md)
- **BigQuery Sync Issues**: See [README_OPTIMIZED.md](README_OPTIMIZED.md)
- **General Questions**: Check the main Sourcify documentation
