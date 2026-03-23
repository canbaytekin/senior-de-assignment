-- --------------------------------------------------------------------------- --
--  Dimension: Account
--  Grain: one row per account_id (from current transactions)
-- --------------------------------------------------------------------------- --

SELECT DISTINCT
    account_id
FROM {{ ref('silver_transactions') }}
WHERE is_current = TRUE
