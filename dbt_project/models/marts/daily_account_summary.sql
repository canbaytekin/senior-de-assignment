-- --------------------------------------------------------------------------- --
--  Mart: Daily Account Summary
--
--  Reads from the gold star schema (fact_transaction + dim_merchant).
--  Filters to completed transactions, aggregates per (account_id, date).
--  One row per (account_id, transaction_date).
--
--  All monetary amounts (total_debit_amount, total_credit_amount, net_amount)
--  are in USD. fact_transaction.amount is already USD-converted via
--  dim_currency.usd_exchange_rate — no further conversion is needed here.
-- --------------------------------------------------------------------------- --

WITH completed AS (
    SELECT
        f.account_id,
        f.transaction_date,
        f.amount,
        f.currency,
        f.transaction_type,
        m.merchant_name,
        f.merchant_category
    FROM {{ ref('fact_transaction') }} f
    LEFT JOIN {{ ref('dim_merchant') }} m
        ON f.merchant_sk = m.merchant_sk
    WHERE LOWER(f.status) = 'completed'
),

daily_agg AS (
    SELECT
        account_id,
        transaction_date,

        COALESCE(SUM(CASE WHEN transaction_type = 'debit'  THEN amount END), 0)
            AS total_debit_amount,
        COALESCE(SUM(CASE WHEN transaction_type = 'credit' THEN amount END), 0)
            AS total_credit_amount,

        COALESCE(SUM(CASE WHEN transaction_type = 'credit' THEN amount END), 0)
          - COALESCE(SUM(CASE WHEN transaction_type = 'debit'  THEN amount END), 0)
            AS net_amount,

        COUNT(*) AS transaction_count,
        COUNT(DISTINCT merchant_name) AS distinct_merchants,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(currency))) AS currencies,
        CURRENT_TIMESTAMP() AS updated_at

    FROM completed
    GROUP BY account_id, transaction_date
),

category_spend AS (
    SELECT
        account_id,
        transaction_date,
        merchant_category,
        SUM(amount) AS category_total
    FROM completed
    GROUP BY account_id, transaction_date, merchant_category
),

ranked_categories AS (
    SELECT
        account_id,
        transaction_date,
        merchant_category,
        ROW_NUMBER() OVER (
            PARTITION BY account_id, transaction_date
            ORDER BY category_total DESC, merchant_category ASC
        ) AS rn
    FROM category_spend
)

SELECT
    d.account_id,
    d.transaction_date,
    d.total_debit_amount,
    d.total_credit_amount,
    d.net_amount,
    d.transaction_count,
    d.distinct_merchants,
    rc.merchant_category AS top_category,
    d.currencies,
    d.updated_at
FROM daily_agg d
LEFT JOIN ranked_categories rc
    ON  d.account_id       = rc.account_id
    AND d.transaction_date = rc.transaction_date
    AND rc.rn = 1
ORDER BY d.account_id, d.transaction_date