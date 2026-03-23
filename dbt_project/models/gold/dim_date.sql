-- --------------------------------------------------------------------------- --
--  Dimension: Date
--  Grain: one row per calendar date (derived from transaction data range)
-- --------------------------------------------------------------------------- --

WITH date_spine AS (
    SELECT DISTINCT transaction_date AS date_day
    FROM {{ ref('silver_transactions') }}
)

SELECT
    date_day,
    YEAR(date_day)                       AS year,
    QUARTER(date_day)                    AS quarter,
    MONTH(date_day)                      AS month,
    DAY(date_day)                        AS day,
    DAYOFWEEK(date_day)                  AS day_of_week,
    DATE_FORMAT(date_day, 'EEEE')        AS day_name,
    DATE_FORMAT(date_day, 'MMMM')        AS month_name,
    WEEKOFYEAR(date_day)                 AS week_of_year
FROM date_spine
