# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Test Script — Inject Synthetic April 2024 Data for Incremental Load Testing
# MAGIC
# MAGIC This notebook is called **inline** from `task3_incremental.py` via `%run`
# MAGIC between Step 3 (landing append) and Step 4 (classification). It follows the
# MAGIC assignment hint:
# MAGIC
# MAGIC > *"After your first full load, manually insert 2-3 records with April 2024
# MAGIC > dates into your raw table, then run again to verify only those are processed."*
# MAGIC
# MAGIC **What it does:**
# MAGIC Inserts 3 synthetic rows directly into the **landing table** with April 2024
# MAGIC dates. These rows are then picked up by the pipeline's own classification step
# MAGIC (Step 4) and routed to raw or quarantine alongside any API-fetched records.
# MAGIC This notebook does **not** run any classification or merge logic itself.
# MAGIC
# MAGIC **Test rows:**
# MAGIC - Row A — fully valid, new transaction → expected in **RAW**
# MAGIC - Row B — intentionally invalid (bad currency + negative amount) → expected in **QUARANTINE**
# MAGIC - Row C — same transaction_id as an existing raw record but with a changed
# MAGIC   amount (different natural-key hash) → expected in **RAW** as a new row
# MAGIC
# MAGIC **This script is idempotent:** it checks whether test rows already exist in
# MAGIC landing before inserting, so repeated runs do not accumulate duplicates.
# MAGIC
# MAGIC **Usage:** This notebook is already wired into `task3_incremental.py` via
# MAGIC `%run ./test_incremental` (between Step 3 and Step 4). To disable it,
# MAGIC comment out that `%run` line. The pipeline will pick up the injected rows
# MAGIC and route them to raw / quarantine as part of its normal flow.

# COMMAND ----------

# MAGIC %run ./shared_utils

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  Guard — skip insertion if synthetic test rows already exist in landing       #
# --------------------------------------------------------------------------- #
TEST_TXN_IDS = ("TXN-9901", "TXN-9902")

existing_test_rows = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM {LANDING_TABLE}
    WHERE transaction_id IN ('{TEST_TXN_IDS[0]}', '{TEST_TXN_IDS[1]}')
""").collect()[0]["cnt"]

if existing_test_rows > 0:
    log.info("TEST | Synthetic test rows already present in landing (%d found). "
             "Skipping insertion to maintain idempotency.", existing_test_rows)
else:
    synthetic_ts = utc_now_iso()

    # — Row A: fully valid new record (April 2024) —
    synthetic_valid = (
        None,                                   # id
        "TXN-9901",                             # transaction_id
        "ACC-1001",                             # account_id (valid range)
        "2024-04-05T10:30:00Z",                 # transaction_date (April 2024)
        "250.00",                               # amount (string in landing)
        "USD",                                  # currency
        "debit",                                # transaction_type
        "SyntheticMerchant",                    # merchant_name
        "retail",                               # merchant_category
        "completed",                            # status
        "US",                                   # country_code
        synthetic_ts,                           # ingestion_timestamp
    )

    # — Row B: invalid record (bad currency + negative amount → quarantine) —
    synthetic_invalid = (
        None,
        "TXN-9902",
        "ACC-1002",                             # account_id (valid range)
        "2024-04-10T14:00:00Z",                 # transaction_date (April 2024)
        "-999.99",                              # negative amount → validation fail
        "INVALID_CUR",                          # bad currency   → validation fail
        "debit",
        "BadMerchant",
        "retail",
        "completed",
        "US",
        synthetic_ts,
    )

    # — Row C: same transaction_id as existing TXN-0160 but with changed amount —
    # Original TXN-0160 has amount=142.50; we use 5000.00 to produce a different
    # natural_key_hash. The MERGE matches on hash, so this arrives as a new row.
    synthetic_changed = (
        None,
        "TXN-0160",                             # same transaction_id as existing
        "ACC-1005",                             # same account_id
        "2024-04-15T09:00:00Z",                 # April 2024 date
        "5000.00",                              # changed amount → different hash
        "USD",                                  # currency
        "credit",                               # transaction_type
        "IKEA",                                 # merchant_name
        "home_and_garden",                      # merchant_category
        "completed",                            # status
        "NL",                                   # country_code
        synthetic_ts,
    )

    synthetic_rows = [synthetic_valid, synthetic_invalid, synthetic_changed]

    df_synthetic = spark.createDataFrame(synthetic_rows, schema=LANDING_SCHEMA)
    write_delta_table(df_synthetic, LANDING_TABLE, mode="append")
    log.info("TEST | Injected %d synthetic rows into '%s' (ingestion_timestamp=%s)",
          len(synthetic_rows), LANDING_TABLE, synthetic_ts)
    log.info("  Row A (TXN-9901): valid, April 2024  -- expected in RAW")
    log.info("  Row B (TXN-9902): invalid, April 2024 -- expected in QUARANTINE")
    log.info("  Row C (TXN-0160): changed amount (5000.00), April 2024 -- expected in RAW (new hash)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Expected Outcome
# MAGIC
# MAGIC After Step 4+ of the pipeline completes, the synthetic rows should be routed as follows:
# MAGIC
# MAGIC | Row | transaction_id | Result | Reason |
# MAGIC |-----|---------------|--------|--------|
# MAGIC | A | TXN-9901 | **RAW** | Valid record with April 2024 date |
# MAGIC | B | TXN-9902 | **QUARANTINE** | Negative amount + invalid currency |
# MAGIC | C | TXN-0160 | **RAW** | Changed amount (5000.00) produces a new natural-key hash |
# MAGIC
# MAGIC The idempotency guard ensures repeated runs do not re-insert these rows.
