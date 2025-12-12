-- migrate:up

-- Add index on verified_contracts(deployment_id) for efficient lookups by deployment
-- This is critical for contract verification queries that join from contract_deployments
-- to verified_contracts via deployment_id
-- Without this index, queries scan the entire verified_contracts_pseudo_pkey index
-- which is defined as (compilation_id, deployment_id) and inefficient for deployment_id lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_verified_contracts_deployment_id
ON verified_contracts(deployment_id);

-- migrate:down

-- Remove the index
DROP INDEX IF EXISTS idx_verified_contracts_deployment_id;
