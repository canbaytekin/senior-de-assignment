{% macro scd_type2(source_cte, primary_key, order_by, hash_column) %}
{#-
    Reusable SCD Type 2 macro.

    Generates three CTEs on top of a user-defined source CTE and outputs
    a final CTE called `scd2_versioned` that the calling model can SELECT from.

    Parameters
    ----------
    source_cte   : str   – Name of the CTE that contains the cleaned source rows.
    primary_key  : str   – Business key column (e.g. 'transaction_id').
    order_by     : str   – Column that determines version ordering (e.g. 'ingestion_timestamp').
    hash_column  : str   – Change-detection hash column (e.g. 'natural_key_hash').
                           Consecutive rows with the same hash for a given PK are
                           treated as duplicates and collapsed.

    Produced columns (appended to the source columns)
    --------------------------------------------------
    version_number : INT       – Sequential version counter per PK (1-based).
    valid_from     : TIMESTAMP – When this version became effective.
    valid_to       : TIMESTAMP – When the next version replaced it (NULL if current).
    is_current     : BOOLEAN   – TRUE for the latest version.

    Usage
    -----
    WITH source AS (
        SELECT … FROM {{ source('bronze', 'my_table') }}
    ),

    {{ scd_type2(
        source_cte  = 'source',
        primary_key = 'my_pk',
        order_by    = 'ingestion_timestamp',
        hash_column = 'natural_key_hash'
    ) }}

    SELECT
        {{ dbt_utils.generate_surrogate_key(['my_pk', 'natural_key_hash']) }} AS my_sk,
        my_pk,
        …,
        valid_from,
        valid_to,
        is_current,
        version_number
    FROM scd2_versioned
-#}

{#-- Step 1: Remove exact duplicates (same PK + same ordering value) --#}
_scd2_exact_dedup AS (
    SELECT * FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY {{ primary_key }}, {{ order_by }}
                ORDER BY {{ order_by }}
            ) AS _scd2_rn
        FROM {{ source_cte }}
    )
    WHERE _scd2_rn = 1
),

{#-- Step 2: Detect consecutive versions with identical hash (no real change) --#}
_scd2_with_prev_hash AS (
    SELECT
        *,
        LAG({{ hash_column }}) OVER (
            PARTITION BY {{ primary_key }}
            ORDER BY {{ order_by }}
        ) AS _scd2_prev_hash
    FROM _scd2_exact_dedup
),

_scd2_changed AS (
    SELECT *
    FROM _scd2_with_prev_hash
    WHERE _scd2_prev_hash IS NULL
       OR _scd2_prev_hash != {{ hash_column }}
),

{#-- Step 3: Compute SCD Type 2 fields --#}
scd2_versioned AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY {{ primary_key }}
            ORDER BY {{ order_by }}
        ) AS version_number,
        CAST({{ order_by }} AS TIMESTAMP) AS valid_from,
        LEAD(CAST({{ order_by }} AS TIMESTAMP)) OVER (
            PARTITION BY {{ primary_key }}
            ORDER BY {{ order_by }}
        ) AS valid_to,
        CASE
            WHEN LEAD(CAST({{ order_by }} AS TIMESTAMP)) OVER (
                PARTITION BY {{ primary_key }}
                ORDER BY {{ order_by }}
            ) IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current
    FROM _scd2_changed
)
{% endmacro %}
