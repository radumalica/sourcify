-- migrate:up
CREATE TABLE parquet_sync_state (
    id BIGSERIAL PRIMARY KEY,
    file_path VARCHAR NOT NULL UNIQUE,
    category VARCHAR NOT NULL,
    manifest_timestamp TIMESTAMPTZ NOT NULL,
    downloaded_at TIMESTAMPTZ,
    imported_at TIMESTAMPTZ,
    rows_imported BIGINT DEFAULT 0,
    file_size_bytes BIGINT,
    checksum VARCHAR,
    import_status VARCHAR NOT NULL DEFAULT 'pending',
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX parquet_sync_state_category_idx ON parquet_sync_state(category);
CREATE INDEX parquet_sync_state_status_idx ON parquet_sync_state(import_status);
CREATE INDEX parquet_sync_state_manifest_timestamp_idx ON parquet_sync_state(manifest_timestamp);

-- Trigger for updated_at
CREATE TRIGGER update_parquet_sync_state_updated_at
    BEFORE UPDATE ON parquet_sync_state
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_updated_at();

-- migrate:down
DROP TRIGGER IF EXISTS update_parquet_sync_state_updated_at ON parquet_sync_state;
DROP TABLE IF EXISTS parquet_sync_state;
