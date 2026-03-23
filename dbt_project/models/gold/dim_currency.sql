-- --------------------------------------------------------------------------- --
--  Dimension: Currency
--  Grain: one row per ISO 4217 currency code
--
--  Static reference table — usd_exchange_rate is the multiplier to convert
--  1 unit of the given currency into USD  (e.g. 1 EUR × 1.0850 = 1.085 USD).
--
--  Supported currencies: USD, EUR, CAD, GBP, CHF, JPY, AUD
--  Rates are approximate reference rates; update usd_exchange_rate when
--  refreshing for production use.
-- --------------------------------------------------------------------------- --

{{ config(
    materialized='table',
    alias='currency'
) }}

WITH currency_rates AS (
    SELECT *
    FROM (
        VALUES
            ('USD', 'US Dollar',          CAST(1.000000 AS DECIMAL(12, 6))),
            ('EUR', 'Euro',               CAST(1.085000 AS DECIMAL(12, 6))),
            ('CAD', 'Canadian Dollar',    CAST(0.735000 AS DECIMAL(12, 6))),
            ('GBP', 'British Pound',      CAST(1.270000 AS DECIMAL(12, 6))),
            ('CHF', 'Swiss Franc',        CAST(1.120000 AS DECIMAL(12, 6))),
            ('JPY', 'Japanese Yen',       CAST(0.006700 AS DECIMAL(12, 6))),
            ('AUD', 'Australian Dollar',  CAST(0.635000 AS DECIMAL(12, 6)))
    ) AS t(currency_code, currency_name, usd_exchange_rate)
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['currency_code']) }} AS currency_sk,
    currency_code,
    currency_name,
    usd_exchange_rate
FROM currency_rates
