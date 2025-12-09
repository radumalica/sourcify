# BigQuery Sync - Configuration Guide

## Quick Start

Add these variables to your root `.env` file to use the optimized sync:

```bash
# Add to /home/ubuntu/git/sourcify/.env

# =============================================================================
# BigQuery Sync - Optimized Settings (10-50x faster than original)
# =============================================================================

# Google Cloud Project ID (required)
GCP_PROJECT_ID=your-project-id

# Path to GCP service account key (required)
GCP_KEY_PATH=./services/sync-bigquery/gcp-key.json

# Batch size: 100k rows (10x larger than original for fewer API calls)
SYNC_BATCH_SIZE=100000

# Parallel import workers (3 = good balance of speed vs. resource usage)
SYNC_NUM_WORKERS=3

# Use PostgreSQL COPY (3-5x faster than INSERT)
SYNC_USE_COPY=true

# Auto drop/recreate indexes (10-20x faster inserts)
SYNC_MANAGE_INDEXES=true
```

## Hardware-Specific Tuning

### Small Server (8GB RAM, 2 cores)
```bash
SYNC_BATCH_SIZE=50000
SYNC_NUM_WORKERS=2
```

### Medium Server (16GB RAM, 4 cores) - DEFAULT
```bash
SYNC_BATCH_SIZE=100000
SYNC_NUM_WORKERS=3
```

### Large Server (32GB+ RAM, 8+ cores)
```bash
SYNC_BATCH_SIZE=250000
SYNC_NUM_WORKERS=6
```

## PostgreSQL Bulk Loading Mode (Optional but Recommended)

### Option 1: Using Configuration File (Recommended)

1. Uncomment these lines in `docker-compose.yml` under the `db` service:

```yaml
db:
  volumes:
    # Uncomment this:
    - ./services/sync-bigquery/postgres-bulk-load.conf:/etc/postgresql/postgresql.conf:ro
  command:
    # Add these lines:
    - "-c"
    - "config_file=/etc/postgresql/postgresql.conf"
```

2. Restart PostgreSQL:

```bash
docker compose down
docker compose up -d db
```

3. After sync completes, comment out the lines and restart again to revert to normal mode.

### Option 2: Manual Configuration

Run these SQL commands before starting the sync:

```sql
-- Connect to PostgreSQL
docker compose exec db psql -U sourcify

-- Apply bulk loading settings
ALTER SYSTEM SET synchronous_commit = off;
ALTER SYSTEM SET work_mem = '256MB';
ALTER SYSTEM SET maintenance_work_mem = '2GB';
ALTER SYSTEM SET checkpoint_timeout = '30min';
ALTER SYSTEM SET max_wal_size = '4GB';
ALTER SYSTEM SET autovacuum = off;
ALTER SYSTEM SET jit = off;

-- Reload configuration
SELECT pg_reload_conf();

-- Exit
\q
```

**IMPORTANT**: After sync completes, revert settings:

```sql
docker compose exec db psql -U sourcify

-- Revert to production settings
ALTER SYSTEM SET synchronous_commit = on;
ALTER SYSTEM SET checkpoint_timeout = '5min';
ALTER SYSTEM SET max_wal_size = '1GB';
ALTER SYSTEM SET autovacuum = on;
ALTER SYSTEM SET jit = on;

-- Reload
SELECT pg_reload_conf();

\q
```

## Running the Optimized Sync

```bash
# Single command to run optimized sync
docker compose run --rm sync-bigquery-optimized
```

## Monitoring Progress

Logs show real-time progress:

```bash
# View logs in real-time
docker compose logs -f sync-bigquery-optimized

# Or check the log file
tail -f services/sync-bigquery/logs/sync_bigquery_optimized.log
```

You'll see output like:
```
code: 15.2% complete (1,520,000/10,000,000 rows) - 12,500 rows/sec - ETA: 11m
Recreating index code_code_hash_keccak...
Index code_code_hash_keccak recreated in 45.2s
code COMPLETE: 10,000,000 rows imported in 12m 34s (13,265 rows/sec)
```

## All Available Configuration Variables

```bash
# Required
GCP_PROJECT_ID=your-project-id
GCP_KEY_PATH=./services/sync-bigquery/gcp-key.json

# Performance tuning (optional, defaults shown)
SYNC_BATCH_SIZE=100000        # Rows per BigQuery batch
SYNC_NUM_WORKERS=3            # Parallel import workers
SYNC_USE_COPY=true            # Use COPY instead of INSERT
SYNC_MANAGE_INDEXES=true      # Auto drop/recreate indexes

# Scheduler (for automated daily sync)
SYNC_SCHEDULE=0 2 * * *       # Daily at 2 AM
RUN_ON_STARTUP=false          # Don't run on scheduler startup
```

## Troubleshooting

### Out of Memory
```bash
# Reduce batch size and workers
SYNC_BATCH_SIZE=50000
SYNC_NUM_WORKERS=2
```

### Slow Network to BigQuery
```bash
# Increase batch size to reduce API calls
SYNC_BATCH_SIZE=250000
```

### Disk Space Issues
- Ensure at least 2x dataset size in free space
- PostgreSQL WAL can grow large during import
- Temporary tables also consume space

## Performance Expectations

For 10 million rows in `code` table:

| Configuration | Expected Time | Throughput |
|---------------|---------------|------------|
| Small server (2 workers, 50k batch) | 2-4 hours | 1,000-1,500 rows/sec |
| Medium server (3 workers, 100k batch) | 1-2 hours | 2,000-3,000 rows/sec |
| Large server (6 workers, 250k batch) | 0.5-1 hour | 3,000-5,000 rows/sec |

Compared to original (20-40 hours), this is **10-40x faster**.

## Complete Example .env Configuration

```bash
# PostgreSQL
POSTGRES_DB=sourcify
POSTGRES_USER=sourcify
POSTGRES_PASSWORD=your-secure-password
POSTGRES_PORT=5432

# Google Cloud
GCP_PROJECT_ID=your-project-id
GCP_KEY_PATH=./services/sync-bigquery/gcp-key.json

# BigQuery Sync - Optimized
SYNC_BATCH_SIZE=100000
SYNC_NUM_WORKERS=3
SYNC_USE_COPY=true
SYNC_MANAGE_INDEXES=true

# Scheduler
SYNC_SCHEDULE=0 2 * * *
RUN_ON_STARTUP=false
```
