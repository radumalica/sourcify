-- migrate:up

-- Add index on compiled_contracts_signatures(signature_hash_32) for efficient lookups
-- This is critical for signature search queries that check if a signature has verified contracts
-- Without this index, the EXISTS subquery in searchSignaturesByPattern does a table scan
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_compiled_contracts_signatures_hash_32
ON compiled_contracts_signatures(signature_hash_32);

-- migrate:down

-- Remove the index
DROP INDEX IF EXISTS idx_compiled_contracts_signatures_hash_32;
