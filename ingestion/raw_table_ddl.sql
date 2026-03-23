-- =========================================================================== --
--  Raw / Bronze Layer — Delta Table DDL Definitions
--
--  These DDL statements define the three bronze-layer tables used by the
--  ingestion pipeline. All tables use Delta format for ACID guarantees,
--  schema enforcement, and MERGE-based idempotent writes.
--
--  Target: Databricks / Delta Lake
--
--  Usage: Executed by ensure_tables_exist() in shared_utils.py at the start
--         of every pipeline run. All statements are idempotent (IF NOT EXISTS).
-- =========================================================================== --

-- --------------------------------------------------------------------------- --
--  Schemas                                                                    --
-- --------------------------------------------------------------------------- --

CREATE SCHEMA IF NOT EXISTS workspace.bronze_landing;
CREATE SCHEMA IF NOT EXISTS workspace.bronze;
CREATE SCHEMA IF NOT EXISTS workspace.default;

-- --------------------------------------------------------------------------- --
--  1. Landing Table  (exact mirror of API response + ingestion metadata)
--     Purpose: audit trail — no validation, no deduplication.
--     Note: All columns except ingestion_timestamp are nullable because
--           this is a pre-validation mirror — fields may arrive null/missing.
-- --------------------------------------------------------------------------- --

CREATE TABLE IF NOT EXISTS workspace.bronze_landing.transactions (
    id                   INT           COMMENT 'Supabase internal primary key (nullable)',
    transaction_id       STRING        COMMENT 'Application-level unique ID (TXN-NNNN)',
    account_id           STRING        COMMENT 'Account identifier (ACC-NNNN)',
    transaction_date     STRING        COMMENT 'ISO 8601 UTC timestamp from source',
    amount               STRING        COMMENT 'Transaction amount (stored as string in landing)',
    currency             STRING        COMMENT 'ISO 4217 currency code',
    transaction_type     STRING        COMMENT 'debit or credit',
    merchant_name        STRING        COMMENT 'Merchant or counterparty name',
    merchant_category    STRING        COMMENT 'Merchant category',
    status               STRING        COMMENT 'completed | pending | failed | reversed',
    country_code         STRING        COMMENT 'ISO 3166-1 alpha-2 country code',
    ingestion_timestamp  STRING        NOT NULL  COMMENT 'UTC timestamp when this batch was loaded'
)
USING DELTA
COMMENT 'Raw landing table — exact copy of API/CSV source with ingestion timestamp. Append-only audit trail.';


-- --------------------------------------------------------------------------- --
--  2. Raw / Bronze Table  (validated, deduplicated records)
--     Purpose: clean bronze data ready for downstream transformations.
-- --------------------------------------------------------------------------- --

CREATE TABLE IF NOT EXISTS workspace.bronze.transactions (
    id                   INT           COMMENT 'Supabase internal primary key (nullable)',
    transaction_id       STRING        NOT NULL  COMMENT 'Application-level unique ID (TXN-NNNN format, pattern: ^TXN-[A-Z0-9]+$)',
    account_id           STRING        NOT NULL  COMMENT 'Account identifier (ACC-NNNN format, pattern: ^ACC-\\d{4}$)',
    transaction_date     STRING        NOT NULL  COMMENT 'ISO 8601 UTC timestamp (strict: YYYY-MM-DDTHH:MM:SSZ)',
    amount               DOUBLE        NOT NULL  COMMENT 'Transaction amount — strictly positive (> 0)',
    currency             STRING        NOT NULL  COMMENT 'ISO 4217 code: USD | EUR | GBP | CHF | JPY | AUD | CAD',
    transaction_type     STRING        NOT NULL  COMMENT 'debit | credit (lowercase, case-sensitive)',
    merchant_name        STRING        NOT NULL  COMMENT 'Merchant name — must contain at least one non-whitespace char',
    merchant_category    STRING        NOT NULL  COMMENT 'One of 12 allowed categories (case-sensitive)',
    status               STRING        NOT NULL  COMMENT 'completed | pending | failed | reversed (lowercase)',
    country_code         STRING        NOT NULL  COMMENT 'ISO 3166-1 alpha-2 assigned code',
    natural_key_hash     STRING        NOT NULL  COMMENT 'SHA-256 hash of natural key fields (all except transaction_id/id)',
    ingestion_timestamp  STRING        NOT NULL  COMMENT 'UTC timestamp when this record was ingested'
)
USING DELTA
COMMENT 'Validated bronze transactions. Deduplicated via MERGE on natural_key_hash. Schema-validated before insert.';


-- --------------------------------------------------------------------------- --
--  3. Quarantine / Dead-Letter Table  (invalid and duplicate records)
--     Purpose: capture defective records with error reasons for investigation.
-- --------------------------------------------------------------------------- --

CREATE TABLE IF NOT EXISTS workspace.bronze.quarantine_transactions (
    id                   INT           COMMENT 'Supabase internal primary key (nullable)',
    transaction_id       STRING        COMMENT 'Application-level ID (may be missing or malformed)',
    account_id           STRING        COMMENT 'Account ID (may be missing or malformed)',
    transaction_date     STRING        COMMENT 'Timestamp from source (may be invalid)',
    amount               STRING        COMMENT 'Amount as string to preserve original bad values',
    currency             STRING        COMMENT 'Currency code (may be invalid)',
    transaction_type     STRING        COMMENT 'Transaction type (may be invalid)',
    merchant_name        STRING        COMMENT 'Merchant name (may be blank/whitespace-only)',
    merchant_category    STRING        COMMENT 'Merchant category (may be invalid)',
    status               STRING        COMMENT 'Status (may be invalid)',
    country_code         STRING        COMMENT 'Country code (may be invalid)',
    error_reason         STRING        NOT NULL  COMMENT 'Semicolon-separated list of validation errors',
    ingestion_timestamp  STRING        NOT NULL  COMMENT 'UTC timestamp when this record was quarantined'
)
USING DELTA
COMMENT 'Quarantine table for records that failed validation or were detected as duplicates. Append-only.';


-- --------------------------------------------------------------------------- --
--  4. Watermark Table  (incremental ingestion state)
--     Purpose: persist high-water mark for incremental runs (Task 3).
-- --------------------------------------------------------------------------- --

CREATE TABLE IF NOT EXISTS workspace.default.ingestion_watermark (
    max_transaction_date STRING        NOT NULL  COMMENT 'Highest transaction_date successfully ingested (ISO 8601)',
    last_run_timestamp   STRING        NOT NULL  COMMENT 'UTC timestamp of the last pipeline run',
    records_ingested     INT           NOT NULL  COMMENT 'Number of new records ingested in the last run'
)
USING DELTA
COMMENT 'Single-row watermark table for incremental ingestion. Overwritten each run.';
