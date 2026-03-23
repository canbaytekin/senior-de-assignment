-- --------------------------------------------------------------------------- --
--  Dimension: Merchant
--  Grain: one row per unique merchant_name
--  Note: merchant_category is a transaction-level attribute and lives on
--        fact_transaction rather than here.
-- --------------------------------------------------------------------------- --

WITH merchants AS (
    SELECT DISTINCT
        merchant_name
    FROM {{ ref('silver_transactions') }}
    WHERE is_current = TRUE
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['merchant_name']) }} AS merchant_sk,
    merchant_name
FROM merchants
