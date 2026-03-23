# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 3 — Incremental Ingestion with Watermark Logic
# MAGIC
# MAGIC This notebook extends Task 1 to run **incrementally**. On each execution it:
# MAGIC 1. Reads the persisted **high-water mark** (max `transaction_date` from the previous run).
# MAGIC 2. Fetches only records **newer** than that watermark from the API (using `gte.` filter).
# MAGIC 3. **Appends** all fetched records to the **landing table** (exact source mirror).
# MAGIC 4. Determines which landing rows are unprocessed by comparing `ingestion_timestamp`
# MAGIC    against the max `ingestion_timestamp` across `raw_transactions` and `quarantine_transactions`.
# MAGIC 5. Applies validation & deduplication on the unprocessed rows.
# MAGIC 6. MERGEs clean records into the raw Delta table and appends quarantine rows.
# MAGIC
# MAGIC **Architecture:** API → `landing_transactions` (append) → filter unprocessed → validate/classify → `raw_transactions` + `quarantine_transactions`
# MAGIC
# MAGIC **Watermark store:** A single-row Delta table (`default.ingestion_watermark`).
# MAGIC This is simple, durable, and queryable — no external dependency needed.
# MAGIC
# MAGIC **Late-arriving data:** A configurable lookback window (default 2 days) is subtracted
# MAGIC from the watermark before querying the API. This ensures records that arrive with a
# MAGIC `transaction_date` slightly behind the watermark are still captured. The MERGE
# MAGIC operation prevents true duplicates from being inserted.

# COMMAND ----------

# MAGIC %run ./shared_utils

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  CONFIGURATION — loaded from shared_utils; override here if needed           #
# --------------------------------------------------------------------------- #
# All API settings and table names are defined in shared_utils.
# LOOKBACK_DAYS is also defined there (default 2).

# Ensure all schemas and Delta tables exist (idempotent — reads raw_table_ddl.sql).
ensure_tables_exist()

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  STEP 1 — Read or initialise the watermark                                   #
# --------------------------------------------------------------------------- #

# get_watermark (shared_utils) — reads the max_transaction_date from the
# single-row watermark Delta table. Returns None on the very first run.
current_watermark = get_watermark(WATERMARK_TABLE)

if current_watermark is None:
    log.info("STEP 1 | First run detected -- no watermark found. Will ingest ALL records.")
    fetch_from = None
else:
    # Apply lookback window for late-arriving data
    wm_dt = datetime.strptime(current_watermark.replace("+00:00", "Z"), "%Y-%m-%dT%H:%M:%SZ")
    lookback_dt = wm_dt - timedelta(days=LOOKBACK_DAYS)
    fetch_from = lookback_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("STEP 1 | Watermark: %s", current_watermark)
    log.info("STEP 1 | Lookback: %d days -- fetching from %s", LOOKBACK_DAYS, fetch_from)

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  STEP 2 — Fetch records from API (with date filter)                          #
# --------------------------------------------------------------------------- #

# fetch_records (shared_utils) — paginates the API; when from_date is set it
# adds a gte. filter so only records newer than the lookback date are returned.
raw_records = fetch_records(
    API_BASE_URL, API_ENDPOINT, API_KEY, PAGE_SIZE,
    ORDER_COLUMN, ORDER_DIR, from_date=fetch_from
)

if not raw_records:
    log.info("STEP 2 | No new records since last run. Pipeline complete.")
    # save_watermark (shared_utils) — persist unchanged watermark with 0
    # records so monitoring can see the run executed but found nothing new.
    save_watermark(
        WATERMARK_TABLE,
        current_watermark or "1970-01-01T00:00:00Z",
        utc_now_iso(),
        0
    )
    dbutils.notebook.exit("No new records")

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  STEP 3 — Append fetched records to the landing table                        #
# --------------------------------------------------------------------------- #

# utc_now_iso (shared_utils) — consistent UTC timestamp for the entire run.
ingestion_ts = utc_now_iso()

if raw_records:
    # build_landing_dataframe (shared_utils) — maps API dicts to LANDING_SCHEMA.
    df_landing = build_landing_dataframe(raw_records, ingestion_ts)

    # write_delta_table (shared_utils) — mode="append" preserves previous
    # landing rows so the table acts as a full audit trail of every API fetch.
    write_delta_table(df_landing, LANDING_TABLE, mode="append")
    log.info("STEP 3 | Appended %d rows to landing table '%s'", df_landing.count(), LANDING_TABLE)

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  ⚠️  TEST INCREMENTAL LOAD — Uncomment the line below to inject synthetic    #
#  test rows (April 2024 dates) into the landing table before classification.  #
#  This simulates a "second run" scenario. See test_incremental.py for details.#
# --------------------------------------------------------------------------- #
# %run ./test_incremental

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  STEP 4 — Determine unprocessed landing rows & classify                      #
# --------------------------------------------------------------------------- #

# get_max_processed_ingestion_ts (shared_utils) — returns MAX(ingestion_timestamp)
# across both raw and quarantine tables, so we only process landing rows newer
# than that cutoff and avoid re-processing already-classified records.
max_processed_ts = get_max_processed_ingestion_ts(RAW_TABLE, QUARANTINE_TABLE)

# ORDER BY ingestion_timestamp ASC, transaction_date ASC ensures deterministic
# duplicate handling: when the same record was loaded multiple times (with
# different ingestion_timestamps), the oldest ingestion is encountered first
# by classify_records and kept in deduped_records, while newer copies are
# routed to quarantine. Without this ordering the "winner" would be arbitrary.
if max_processed_ts:
    log.info("STEP 4 | Max processed ingestion_timestamp: %s", max_processed_ts)
    landing_df = spark.sql(f"""
        SELECT * FROM {LANDING_TABLE}
        WHERE ingestion_timestamp > '{max_processed_ts}'
        ORDER BY ingestion_timestamp ASC, transaction_date ASC
    """)
else:
    log.info("STEP 4 | No previously processed rows -- processing entire landing table.")
    landing_df = spark.sql(f"""
        SELECT * FROM {LANDING_TABLE}
        ORDER BY ingestion_timestamp ASC, transaction_date ASC
    """)

landing_count = landing_df.count()
log.info("STEP 4 | Landing rows to process: %d", landing_count)

if landing_count > 0:
    # landing_records_to_dicts (shared_utils) — Spark rows → dicts with
    # amount cast to float, ready for validate_record() in classify_records.
    landing_records = landing_records_to_dicts(landing_df)
else:
    landing_records = []

# Build set of natural-key hashes already in the raw table to prevent re-inserting
# collect_existing_keys (shared_utils) — queries the raw table and returns a set.
existing_keys = collect_existing_keys(RAW_TABLE) if landing_records else set()

if landing_records:
    # classify_records (shared_utils) — validates, deduplicates, and splits
    # records. existing_keys prevents re-inserting rows already in the raw
    # table (cross-batch dedup on top of the in-batch dedup).
    deduped_records, quarantine_records, dup_count = classify_records(
        landing_records, ingestion_ts, existing_keys=existing_keys
    )
    log.info("STEP 4 | New clean records to append: %d", len(deduped_records))
    log.info("STEP 4 | Duplicates detected (batch + existing): %d", dup_count)
else:
    deduped_records = []
    quarantine_records = []
    dup_count = 0

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  STEP 5 — Append new records to the raw Delta table via MERGE                #
# --------------------------------------------------------------------------- #

if deduped_records:
    # merge_into_raw (shared_utils) — builds the DataFrame and MERGEs on
    # natural_key_hash to avoid duplicates at the Delta level.
    merged_count = merge_into_raw(deduped_records, RAW_TABLE)
    log.info("STEP 5 | Merged %d new rows into '%s'", merged_count, RAW_TABLE)
else:
    log.info("STEP 5 | No new records to append.")

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  STEP 6 — Append quarantine records                                          #
# --------------------------------------------------------------------------- #

if quarantine_records:
    # append_quarantine (shared_utils) — builds the DataFrame and appends
    # to the quarantine table preserving history from earlier runs.
    q_count = append_quarantine(quarantine_records, QUARANTINE_TABLE)
    log.info("STEP 6 | Appended %d quarantine rows", q_count)
else:
    log.info("STEP 6 | No quarantine records this run.")

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  STEP 7 — Update the watermark                                              #
# --------------------------------------------------------------------------- #

# compute_new_watermark_max (shared_utils) — picks the highest
# transaction_date from this batch, but keeps the old watermark if it was
# already higher (guards against late-arriving data shifting the mark back).
# Handles empty deduped_records gracefully (returns current or sentinel).
new_max = compute_new_watermark_max(deduped_records, current_watermark)

# save_watermark (shared_utils) — overwrites the single-row watermark table
# so the next run resumes from the correct point.
save_watermark(WATERMARK_TABLE, new_max, ingestion_ts, len(deduped_records))
log.info("STEP 7 | Watermark updated to: %s", new_max)

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  STEP 8 — Summary report                                                     #
# --------------------------------------------------------------------------- #

log.info("=" * 60)
log.info("  INCREMENTAL INGESTION SUMMARY")
log.info("=" * 60)
log.info("  Previous watermark          : %s", current_watermark or '(none -- first run)')
log.info("  Fetch-from (with lookback)  : %s", fetch_from or '(all records)')
log.info("  Records fetched from API    : %d", len(raw_records) if raw_records else 0)
log.info("  Landing rows appended       : %d", df_landing.count() if raw_records else 0)
log.info("  Landing rows to process     : %d", landing_count)
log.info("  Max processed ingestion_ts  : %s", max_processed_ts or '(none)')
log.info("  Failed validation           : %d", len(quarantine_records) - dup_count)
log.info("  Duplicates (batch+existing) : %d", dup_count)
log.info("  New rows appended to raw    : %d", len(deduped_records))
log.info("  New watermark               : %s", new_max)
log.info("  Ingestion timestamp (UTC)   : %s", ingestion_ts)
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Watermark Strategy, Landing Layer & Late-Arriving Data
# MAGIC
# MAGIC **Landing layer:** All records fetched from the API are first appended to
# MAGIC `bronze_landing.transactions` — an exact mirror of the source with only an
# MAGIC `ingestion_timestamp` added. No rows are filtered or modified. This provides
# MAGIC a full audit trail and allows reprocessing from source if needed.
# MAGIC
# MAGIC **Unprocessed-row detection:** After writing to the landing table, the pipeline
# MAGIC computes `MAX(ingestion_timestamp)` across the union of `raw_transactions` and
# MAGIC `quarantine_transactions`. Only landing rows with `ingestion_timestamp >` that
# MAGIC value are fed into validation/classification. This ensures every landing row
# MAGIC is processed exactly once into raw or quarantine.
# MAGIC
# MAGIC **Watermark store:** A single-row Delta table (`default.ingestion_watermark`) keyed by
# MAGIC `max_transaction_date`. This leverages Delta's ACID guarantees — concurrent writers
# MAGIC can't corrupt the watermark, and it's trivially queryable for monitoring.
# MAGIC
# MAGIC **Mechanism:** After each successful run the pipeline records the maximum
# MAGIC `transaction_date` from the ingested batch. On the next run it reads this value,
# MAGIC subtracts a configurable lookback window (default 2 days), and passes the adjusted
# MAGIC timestamp as a `gte.` filter to the API. This means the API returns a small overlap
# MAGIC of already-seen records, but the MERGE into the raw Delta table (matched on
# MAGIC `transaction_id`) ensures no true duplicates are written.
# MAGIC
# MAGIC **Edge cases:**
# MAGIC - **First run (no watermark):** The pipeline detects the missing table/row and ingests
# MAGIC   everything — equivalent to Task 1.
# MAGIC - **No new records:** The API returns an empty page; the pipeline logs "0 rows processed"
# MAGIC   and keeps the watermark unchanged.
# MAGIC - **Late-arriving data:** The 2-day lookback window re-fetches a small sliding window of
# MAGIC   records that the source system may have back-dated. For this 3-month banking dataset a
# MAGIC   2-day window is conservative enough to catch late postings while keeping API traffic low.
# MAGIC   In production the window size would be tuned based on observed source-system latency.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick look at the watermark table

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {WATERMARK_TABLE}"))