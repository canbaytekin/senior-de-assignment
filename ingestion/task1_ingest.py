# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 1 — Raw Data Ingestion with Data Quality Handling
# MAGIC
# MAGIC This notebook fetches all transactions from a REST API (Supabase),
# MAGIC validates each record against the schema, quarantines invalid/duplicate rows,
# MAGIC and persists clean records into a Delta **raw/bronze** table.
# MAGIC
# MAGIC **How to use:** Set the widgets / parameters in Cmd 2, then _Run All_.
# MAGIC
# MAGIC Shared functions (validation, API helpers, schemas) are loaded from `shared_utils`.

# COMMAND ----------

# MAGIC %run ./shared_utils

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  CONFIGURATION — loaded from shared_utils; override here if needed           #
# --------------------------------------------------------------------------- #
# All API settings (API_BASE_URL, API_ENDPOINT, API_KEY, PAGE_SIZE,
# ORDER_COLUMN, ORDER_DIR) and table names (LANDING_TABLE, RAW_TABLE,
# QUARANTINE_TABLE, WATERMARK_TABLE, CSV_FALLBACK_PATH) are defined
# in shared_utils and imported via %run above.

# Ensure all schemas and Delta tables exist (idempotent — reads raw_table_ddl.sql).
ensure_tables_exist()

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  STEP 1 — Fetch all records from API (with CSV fallback)                     #
# --------------------------------------------------------------------------- #

try:
    # fetch_records (shared_utils) — paginates through the Supabase REST API
    # with exponential back-off, returning all transaction dicts in one list.
    raw_records = fetch_records(
        API_BASE_URL, API_ENDPOINT, API_KEY, PAGE_SIZE, ORDER_COLUMN, ORDER_DIR
    )
    if not raw_records:
        raise RuntimeError("API returned 0 records")
except Exception as e:
    if CSV_FALLBACK_PATH:
        log.warning("API fetch failed (%s); falling back to CSV: %s", e, CSV_FALLBACK_PATH)
        # load_from_csv (shared_utils) — reads a CSV into Spark and converts
        # rows to dicts, used as a fallback when the API is unreachable.
        raw_records = load_from_csv(CSV_FALLBACK_PATH)
    else:
        raise

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  STEP 2 — Persist landing table (exact copy of source + ingestion timestamp) #
# --------------------------------------------------------------------------- #

# utc_now_iso (shared_utils) — single consistent UTC timestamp for all
# records written in this run, ensuring they share one ingestion batch ID.
ingestion_ts = utc_now_iso()

# build_landing_dataframe (shared_utils) — converts raw API dicts into a
# Spark DataFrame matching LANDING_SCHEMA (all strings, no validation).
df_landing = build_landing_dataframe(raw_records, ingestion_ts)

# Append to landing table (created by ensure_tables_exist via DDL).
write_delta_table(df_landing, LANDING_TABLE, mode="append")
log.info("STEP 2 | Landing table '%s' appended -- %d rows", LANDING_TABLE, df_landing.count())

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  STEP 3 — Read from landing, validate, deduplicate, and classify records     #
# --------------------------------------------------------------------------- #

landing_df = spark.sql(f"SELECT * FROM {LANDING_TABLE}")

# landing_records_to_dicts (shared_utils) — converts Spark rows back to plain
# dicts and casts amount to float so validate_record() can check it numerically.
landing_records = landing_records_to_dicts(landing_df)

# Build set of natural-key hashes already in the raw table to prevent
# re-inserting rows that were loaded in a previous run.
# collect_existing_keys (shared_utils) — queries the raw table for all
# natural_key_hash values and returns them as a set.
existing_keys = collect_existing_keys(RAW_TABLE)

# classify_records (shared_utils) — validates every record via validate_record(),
# detects duplicates using SHA-256 natural-key hashes, and splits results into
# (clean deduped records, quarantine records, duplicate count).
# existing_keys ensures cross-run deduplication.
deduped_records, quarantine_records, dup_count = classify_records(
    landing_records, ingestion_ts, existing_keys=existing_keys
)

log.info("STEP 3 | Classification -- clean=%d, quarantined=%d, duplicates=%d",
         len(deduped_records), len(quarantine_records), dup_count)

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  STEP 4 — Persist raw (bronze) table as Delta                                #
# --------------------------------------------------------------------------- #

if deduped_records:
    # merge_into_raw (shared_utils) — builds the DataFrame and MERGEs on
    # natural_key_hash to avoid duplicates at the Delta level.
    merged_count = merge_into_raw(deduped_records, RAW_TABLE)
    log.info("STEP 4 | Merged %d new rows into '%s'", merged_count, RAW_TABLE)
else:
    log.info("STEP 4 | No new clean records to write to raw table.")

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  STEP 5 — Persist quarantine table as Delta                                  #
# --------------------------------------------------------------------------- #

if quarantine_records:
    # append_quarantine (shared_utils) — builds the quarantine DataFrame and
    # appends to the quarantine table preserving history.
    q_count = append_quarantine(quarantine_records, QUARANTINE_TABLE)
    log.info("STEP 5 | Appended %d quarantine rows to '%s'", q_count, QUARANTINE_TABLE)
else:
    log.info("STEP 5 | No quarantine records this run.")

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  STEP 6 — Update the watermark                                              #
# --------------------------------------------------------------------------- #

# compute_new_watermark_max (shared_utils) — returns the highest
# transaction_date among clean records, or a sentinel epoch if none exist.
# Reads existing watermark so it never moves backward.
current_watermark = get_watermark(WATERMARK_TABLE)
new_max = compute_new_watermark_max(deduped_records, current_watermark)

# save_watermark (shared_utils) — overwrites the single-row watermark Delta
# table so the next incremental run (Task 3) knows where to resume from.
save_watermark(WATERMARK_TABLE, new_max, ingestion_ts, len(deduped_records))
log.info("STEP 6 | Watermark updated to: %s", new_max)

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  STEP 7 — Summary report                                                     #
# --------------------------------------------------------------------------- #

log.info("=" * 60)
log.info("  INGESTION SUMMARY")
log.info("=" * 60)
log.info("  Total fetched from API      : %d", len(raw_records))
log.info("  Landing table rows          : %d", df_landing.count())
log.info("  Failed validation           : %d", len(quarantine_records) - dup_count)
log.info("  Duplicates detected         : %d", dup_count)
log.info("  Total quarantined           : %d", len(quarantine_records))
log.info("  Clean rows in raw table     : %d", len(deduped_records))
log.info("  Ingestion timestamp (UTC)   : %s", ingestion_ts)
log.info("  Landing table               : %s", LANDING_TABLE)
log.info("  Raw table                   : %s", RAW_TABLE)
log.info("  Quarantine table            : %s", QUARANTINE_TABLE)
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick look at the tables

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {RAW_TABLE} LIMIT 10"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {QUARANTINE_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation approach & duplicate-handling notes
# MAGIC
# MAGIC **Validation:**
# MAGIC Every record is checked against the full schema (Sections 3.2 / 3.3)
# MAGIC via `validate_record()` in `shared_utils`:
# MAGIC - Required field presence
# MAGIC - Format checks (`TXN-NNNN`, `ACC-NNNN`, strict ISO 8601 with `T` separator)
# MAGIC - `amount > 0` (strictly positive)
# MAGIC - Case-sensitive enum membership for `currency`, `transaction_type`, `status`, `merchant_category`
# MAGIC - `merchant_name` must have ≥ 1 non-whitespace character
# MAGIC - `country_code` validated against the full ISO 3166-1 alpha-2 assigned list (not just format)
# MAGIC - `transaction_date` parsed to confirm it's a real calendar date
# MAGIC
# MAGIC **Duplicate handling:**
# MAGIC Duplicates are detected by building a natural key from all fields **except** `transaction_id` (and the
# MAGIC Supabase-generated `id`). The **first** occurrence is kept; subsequent matches are routed to quarantine
# MAGIC with `error_reason = duplicate_of:<original_txn_id>`. This approach is chosen over an `is_duplicate`
# MAGIC flag to keep the raw table clean and free of known-bad data.