&nbsp;

<p align="center">
  &nbsp;
  <a href="https://sourcify.dev"><img src="https://raw.githubusercontent.com/sourcifyeth/assets/master/logo-assets-png/sourcify-eth-card.png" alt="sourcify logo" role="presentation" width=300></a>
</p>

[![codecov](https://codecov.io/gh/argotorg/sourcify/branch/staging/graph/badge.svg?token=eN6XDAwWfV)](https://codecov.io/gh/argotorg/sourcify)
[![Matrix Chat](https://img.shields.io/badge/Matrix%20-chat-brightgreen?style=plastic&logo=matrix)](https://matrix.to/#/#ethereum_source-verify:gitter.im)
[![Discord](https://img.shields.io/badge/Discord%20-chat-brightgreen?style=plastic&logo=discord)](https://discord.com/invite/6aqd9cfZ9s)
[![X Follow](https://img.shields.io/twitter/follow/SourcifyEth?style=plastic&logo=x)](https://X.com/SourcifyEth)

Sourcify ([sourcify.dev](https://sourcify.dev)) is a source-code verification service for Ethereum smart contracts supporting Solidity and Vyper. Sourcify is fully commited to

- Open-source (MIT License)
- Open-data (see [Downloading the repository](https://docs.sourcify.dev/docs/repository/))
- Open-standards (see the [Verifier Alliance](https://github.com/verifier-alliance))

in smart-contract verification instead of siloed, propriety services. We foster these values across the ecosystem and work actively to push the status-quo in this direction.

Different than other verification services, Sourcify leverages the [Solidity metadata](https://docs.sourcify.dev/docs/metadata/) and file and its integrity hash to ["fully verify"](https://docs.sourcify.dev/docs/full-vs-partial-match/) the Solidity contracts (see [the playground](https://playground.sourcify.dev)).

Sourcify mainly consists of:

- [sourcify-server](/services/server) - an HTTP server to run source-code verifications and store the verified contracts for the supported chains through an [API](https://docs.sourcify.dev/docs/api/)
- [sourcify-database](/services/database) - a PostgreSQL database to store the verified contracts and their metadata, and a repository for the database schema and migrations.
- [sourcify-monitor](/services/monitor) - a standalone service that listens to various EVM chains for new contract creations and automatically submits them to a Sourcify API for verification if published on IPFS.
- Packages:
  - [@ethereum-sourcify/lib-sourcify](/packages/lib-sourcify/): The core library for Sourcify. It contains the logic to verify contracts.
  - [@ethereum-sourcify/bytecode-utils](/packages/bytecode-utils/): A library to extract and parse the CBOR encoded metadata from the bytecode.
  - [@ethereum-sourcify/compilers](/packages/compilers/): A wrapper around Solidity and Vyper compilers to download the right version and invoke the compilation with a common interface.
  - [@ethereum-sourcify/compilers-types](/packages/compilers-types/): TypeScript types for the compilers.
- [Sourcify UI](https://github.com/sourcifyeth/ui) - a web UI to interact with the server, lookup, and verify contracts
- [repo.sourcify.dev](https://github.com/sourcifyeth/repo.sourcify.dev) - a web UI to browse and display verified contract information.

_ℹ️ [This monorepo](https://github.com/argotorg/sourcify) contains the main modules. The [sourcifyeth Github organization](https://github.com/sourcifyeth) contains all other auxiliary services and components._

## Documentation

For more details refer to [docs.sourcify.dev](https://docs.sourcify.dev/docs/intro/)

## How we work

Sourcify aims to be fully open and transparent. You can see what we are working day-to-day on on our [Public Issue Board](https://github.com/orgs/ethereum/projects/46) as well our [Quarterly Milestones](https://github.com/orgs/ethereum/projects/46/views/3) for our longer term plans.

## Adding a new chain

If you'd like to add a new chain support to Sourcify please follow the [chain support instructions](https://docs.sourcify.dev/docs/chain-support/) in docs.

## Running Sourcify with Docker Compose

Sourcify provides a comprehensive Docker Compose setup that includes all services: database, server, monitor, 4byte signature service, and parquet sync.

### Storage Requirements

All data is stored in Docker named volumes under `/var/lib/docker/volumes/`:
- **postgres_data**: Database storage (~50-100GB for full dataset)
- **parquet_cache**: Downloaded parquet files (~100-200GB)

If you have mounted `/var/lib/docker` to a large partition (e.g., 1.5TB), all data will automatically be stored there. The parquet cache can be cleaned after successful sync if space is needed.

### Quick Start

1. **Configure environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration (API keys, etc.)
   ```

2. **Start all services:**
   ```bash
   docker compose up -d
   ```

   This will automatically:
   - Start PostgreSQL database
   - **Run database migrations** (first time and on updates)
   - Start all services:
     - **db**: PostgreSQL database with pg_cron extension
     - **server**: Sourcify verification server (port 5555)
     - **4byte**: Signature lookup service (port 4444)
     - **monitor**: Chain monitoring service
     - **sync-scheduler**: Daily automatic sync from official Sourcify export

3. **Check service status:**
   ```bash
   docker compose ps
   docker compose logs -f server

   # Verify migrations ran successfully
   docker compose logs migrations
   ```

### Initial Data Sync

To populate your local database with existing verified contracts from the official Sourcify export:

```bash
# Run the initial sync (downloads ~900 parquet files)
docker compose run --rm sync
```

This will:
- Download parquet files from https://export.sourcify.dev/manifest.json
- Import data into PostgreSQL tables in the correct foreign key order
- Track sync state to enable incremental updates
- Use `INSERT ... ON CONFLICT DO NOTHING` to preserve local discoveries

**Note:** The initial sync can take several hours depending on your internet connection and hardware.

### Incremental Syncs

After the initial sync, the `sync-scheduler` service runs daily (by default at 2 AM) to fetch new data:

```bash
# View sync scheduler logs
docker compose logs -f sync-scheduler

# Manually trigger a sync
docker compose run --rm sync
```

### Database Migrations

Database migrations are handled automatically:
- **First startup**: Migrations run when you first start the services with `docker compose up -d`
- **Subsequent startups**: Migrations run on every startup but are idempotent (dbmate detects already-applied migrations)
- **Updates**: When you pull new code with updated migrations, they will automatically apply on next startup

The migrations service is a dependency for server, 4byte, and monitor services, so they won't start until migrations complete successfully.

To manually check or run migrations:
```bash
# View migration status
docker compose run --rm migrations npm run migrate:status

# Manually run migrations
docker compose up migrations

# View migration logs
docker compose logs migrations
```

### Service Architecture

```
┌─────────────┐
│  PostgreSQL │  ← Shared database
└──────┬──────┘
       │
   ┌───┴────────────────────────────┐
   │                                │
┌──▼──────────┐            ┌────▼──────────┐
│   Server    │◄───────────┤   Monitor     │
│  (port 5555)│  HTTP POST │ (chain events)│
└─────────────┘            └───────────────┘
       │
   ┌───┴──────────────────┐
   │                      │
┌──▼────────┐     ┌──────▼──────────┐
│  4byte    │     │  Sync Service   │
│(port 4444)│     │ (parquet import)│
└───────────┘     └─────────────────┘
```

### Configuration

Key environment variables in `.env`:

```bash
# Database
POSTGRES_DB=sourcify
POSTGRES_USER=sourcify
POSTGRES_PASSWORD=sourcify

# Service Ports
SERVER_PORT=5555
FOURBYTE_PORT=4444

# Monitor API Keys (required for chain monitoring)
ALCHEMY_API_KEY=your_key_here
INFURA_API_KEY=your_key_here

# Sync Configuration
SYNC_SCHEDULE=0 2 * * *  # Daily at 2 AM (cron format)
SYNC_BATCH_SIZE=10000    # Rows per database batch
```

### Monitoring

**Check sync status:**
```bash
# View sync statistics in database
docker compose exec db psql -U sourcify -d sourcify -c \
  "SELECT category, COUNT(*), SUM(rows_imported) FROM parquet_sync_state GROUP BY category;"
```

**View logs:**
```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f server
docker compose logs -f sync-scheduler
```

**Access services:**
- Server API: http://localhost:5555
- 4byte API: http://localhost:4444
- Database: localhost:5432

### Troubleshooting

**Issue: Sync fails with foreign key constraint errors**
- Solution: Ensure migrations are up to date: `docker compose up migrations`

**Issue: Monitor not detecting contracts**
- Solution: Check API keys in `.env` and monitor logs: `docker compose logs monitor`

**Issue: Out of disk space**
- Solution: The parquet cache is stored in a Docker named volume at `/var/lib/docker/volumes/sourcify_parquet_cache/_data/`. To clean it:
  ```bash
  # Stop services using the volume
  docker compose stop sync sync-scheduler

  # Remove the volume (WARNING: will need to re-download on next sync)
  docker volume rm sourcify_parquet_cache

  # Recreate volume
  docker compose up -d sync-scheduler
  ```
- The parquet cache requires ~100-200GB for the full dataset
- Database storage is separate and stored in the `postgres_data` volume

**Issue: Sync taking too long**
- Solution: The initial sync downloads ~900 files. Subsequent syncs are incremental and much faster.

**Issue: Need to check disk usage**
- Check Docker volume sizes:
  ```bash
  docker system df -v
  # or specifically:
  docker volume ls
  du -sh /var/lib/docker/volumes/sourcify_parquet_cache
  du -sh /var/lib/docker/volumes/sourcify_postgres_data
  ```

### Development

For development, you can run individual services:

```bash
# Start only database and server
docker compose up db server

# Run sync manually
docker compose run --rm sync

# Rebuild after code changes
docker compose build server
docker compose up -d server
```

_Sourcify is an [Argot Collective](https://argot.org) project_
