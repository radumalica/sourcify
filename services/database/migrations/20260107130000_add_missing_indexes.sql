-- migrate:up

-- This migration adds indexes that are defined in the Verifier Alliance schema
-- but were missing from the live database. These indexes are critical for:
-- 1. Foreign key lookups and JOINs
-- 2. Query performance on frequently accessed columns
-- 3. Efficient cascade operations

-- ==============================================================================
-- CODE TABLE INDEXES
-- ==============================================================================

-- Index on code_hash_keccak for lookups by keccak hash (common in Ethereum)
-- This column is frequently used for matching against on-chain bytecode hashes
CREATE INDEX IF NOT EXISTS code_code_hash_keccak
ON code(code_hash_keccak);

-- Partial index on first 75 bytes of code for efficient factory contract detection
-- Factory contracts often have identical bytecode prefixes
CREATE INDEX IF NOT EXISTS idx_code_code_first_75
ON code(SUBSTRING(code FROM 1 FOR 75));

-- ==============================================================================
-- COMPILED_CONTRACTS TABLE INDEXES
-- ==============================================================================

-- Index on creation_code_hash for FK lookups and joins with code table
-- Critical for verification queries that need to fetch creation code
CREATE INDEX IF NOT EXISTS compiled_contracts_creation_code_hash
ON compiled_contracts(creation_code_hash);

-- Index on runtime_code_hash for FK lookups and joins with code table
-- Critical for verification queries that need to fetch runtime code
CREATE INDEX IF NOT EXISTS compiled_contracts_runtime_code_hash
ON compiled_contracts(runtime_code_hash);

-- ==============================================================================
-- COMPILED_CONTRACTS_SIGNATURES TABLE INDEXES
-- ==============================================================================

-- Composite index on (signature_type, signature_hash_32) for efficient signature filtering
-- Used when searching for specific types of signatures (functions, events, errors)
-- Note: signature_hash_32 alone already has idx_compiled_contracts_signatures_hash_32
CREATE INDEX IF NOT EXISTS compiled_contracts_signatures_type_signature_idx
ON compiled_contracts_signatures(signature_type, signature_hash_32);

-- ==============================================================================
-- COMPILED_CONTRACTS_SOURCES TABLE INDEXES
-- ==============================================================================

-- Index on compilation_id for FK lookups and reverse joins
-- Critical for queries that fetch all sources for a given compilation
CREATE INDEX IF NOT EXISTS compiled_contracts_sources_compilation_id
ON compiled_contracts_sources(compilation_id);

-- Index on source_hash for FK lookups and joins with sources table
-- Used when checking if a source file already exists
CREATE INDEX IF NOT EXISTS compiled_contracts_sources_source_hash
ON compiled_contracts_sources(source_hash);

-- ==============================================================================
-- CONTRACTS TABLE INDEXES
-- ==============================================================================

-- Index on creation_code_hash for FK lookups and matching against compiled contracts
-- Essential for finding contracts by their creation code
CREATE INDEX IF NOT EXISTS contracts_creation_code_hash
ON contracts(creation_code_hash);

-- Composite index on (creation_code_hash, runtime_code_hash) for exact contract matching
-- Used heavily in verification to find existing contract entries
CREATE INDEX IF NOT EXISTS contracts_creation_code_hash_runtime_code_hash
ON contracts(creation_code_hash, runtime_code_hash);

-- Index on runtime_code_hash for FK lookups and matching against compiled contracts
-- Essential for finding contracts by their runtime code
CREATE INDEX IF NOT EXISTS contracts_runtime_code_hash
ON contracts(runtime_code_hash);

-- ==============================================================================
-- VERIFIED_CONTRACTS TABLE INDEXES
-- ==============================================================================

-- Index on compilation_id for FK lookups and finding all verifications of a compilation
-- Critical for queries like "show all chains where this contract is deployed"
CREATE INDEX IF NOT EXISTS verified_contracts_compilation_id
ON verified_contracts(compilation_id);

-- Note: verified_contracts_deployment_id already exists as idx_verified_contracts_deployment_id

-- ==============================================================================
-- SOURCIFY_MATCHES TABLE INDEXES
-- ==============================================================================

-- Index on verified_contract_id for FK lookups and joins
-- Used to fetch Sourcify-specific match metadata for verified contracts
-- Note: This index is defined in migration 20250722133557_sourcify.sql but missing from live DB
CREATE INDEX IF NOT EXISTS sourcify_matches_verified_contract_id_idx
ON sourcify_matches(verified_contract_id);

-- migrate:down

-- Remove all indexes added in this migration
DROP INDEX IF EXISTS code_code_hash_keccak;
DROP INDEX IF EXISTS idx_code_code_first_75;
DROP INDEX IF EXISTS compiled_contracts_creation_code_hash;
DROP INDEX IF EXISTS compiled_contracts_runtime_code_hash;
DROP INDEX IF EXISTS compiled_contracts_signatures_type_signature_idx;
DROP INDEX IF EXISTS compiled_contracts_sources_compilation_id;
DROP INDEX IF EXISTS compiled_contracts_sources_source_hash;
DROP INDEX IF EXISTS contracts_creation_code_hash;
DROP INDEX IF EXISTS contracts_creation_code_hash_runtime_code_hash;
DROP INDEX IF EXISTS contracts_runtime_code_hash;
DROP INDEX IF EXISTS verified_contracts_compilation_id;
DROP INDEX IF EXISTS sourcify_matches_verified_contract_id_idx;
