-- --------------------------------------------------------------------------- --
--  Silver: SCD Type 2 transactions
--
--  Uses the reusable scd_type2 macro to derive SCD2 columns:
--    • valid_from  — ingestion_timestamp of this version
--    • valid_to    — ingestion_timestamp of the next version (NULL if current)
--    • is_current  — TRUE for the latest version
--    • version_number — sequential version counter per transaction_id
--
--  The macro also handles:
--    • Exact duplicate removal (same PK + same ingestion_timestamp)
--    • Consecutive-hash dedup (skips versions where nothing actually changed)
--
--  Output: silver.transactions
-- --------------------------------------------------------------------------- --

{{ config(
    materialized='table',
    alias='transactions'
) }}

WITH source AS (
    SELECT
        transaction_id,
        account_id,
        -- Bronze stores transaction_date as an ISO 8601 string (e.g. "2025-03-23T12:00:00Z").
        -- The ingestion layer normalises the Supabase "+00:00" suffix to "Z" but keeps it as
        -- a plain string to preserve the raw value. Here we parse that string into proper
        -- TIMESTAMP and DATE types so downstream models can use native date/time operations.
        CAST(
            TO_TIMESTAMP(transaction_date, "yyyy-MM-dd'T'HH:mm:ss'Z'")
            AS TIMESTAMP
        )                                    AS transaction_timestamp,
        CAST(
            TO_DATE(transaction_date, "yyyy-MM-dd'T'HH:mm:ss'Z'")
            AS DATE
        )                                    AS transaction_date,
        amount,
        currency,
        transaction_type,
        merchant_name,
        merchant_category,
        status,
        country_code,
        natural_key_hash,
        ingestion_timestamp
    FROM {{ source('bronze', 'transactions') }}
),

{{ scd_type2(
    source_cte  = 'source',
    primary_key = 'transaction_id',
    order_by    = 'ingestion_timestamp',
    hash_column = 'natural_key_hash'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['transaction_id', 'natural_key_hash']) }} AS transaction_sk,
    transaction_id,
    account_id,
    transaction_timestamp,
    transaction_date,
    amount,
    currency,
    transaction_type,
    merchant_name,
    merchant_category,
    status,
    country_code,
    natural_key_hash,
    valid_from,
    valid_to,
    is_current,
    version_number
FROM scd2_versioned
