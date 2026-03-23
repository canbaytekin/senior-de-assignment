-- --------------------------------------------------------------------------- --
--  Fact: Transaction
--  Grain: one row per current transaction (is_current = TRUE from silver SCD2)
--  FKs: account_id → dim_account, merchant_sk → dim_merchant,
--        transaction_date → dim_date, currency_sk → dim_currency
--  Degenerate dims: currency, country_code
--
--  amount_original : raw amount in the transaction's original currency
--  amount          : amount converted to USD via dim_currency.usd_exchange_rate
--                    — use this column for all monetary aggregations
-- --------------------------------------------------------------------------- --

SELECT
    t.transaction_sk,
    t.transaction_id,
    t.account_id,
    m.merchant_sk,
    c.currency_sk,
    t.transaction_date,
    t.amount                             AS amount_original,
    ROUND(t.amount * c.usd_exchange_rate, 2) AS amount,
    t.currency,
    t.transaction_type,
    t.merchant_category,
    t.status,
    t.country_code
FROM {{ ref('silver_transactions') }} t
LEFT JOIN {{ ref('dim_merchant') }} m
    ON t.merchant_name = m.merchant_name
LEFT JOIN {{ ref('dim_currency') }} c
    ON t.currency = c.currency_code
WHERE t.is_current = TRUE
