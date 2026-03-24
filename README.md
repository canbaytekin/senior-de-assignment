# Senior DE Assignment

## Overview

This project ingests payment transactions from a Supabase REST API into a Databricks Delta Lake under the bronze layer, applies data quality validation, and builds a layered dbt model (silver → gold → marts).

The pipeline has two ingestion modes:
- **Task 1** (`task1_ingest.py`) — full load: fetches all records, validates, deduplicates, and writes to bronze Delta tables.
- **Task 3** (`task3_incremental.py`) — incremental load: reads a watermark, fetches only new records, and MERGEs them into existing tables.

dbt then transforms the clean bronze data into silver, gold (dims + facts), and a daily marts aggregation.

---

## Why This Tech Stack?


### Databricks + Delta Lake

I went with Databricks as the compute and storage layer because it gives me ACID transactions on top of a data lake — something you really need when you're doing incremental MERGEs and want idempotent writes. Delta Lake's support for time travel also means every ingestion run is auditable: if something goes wrong, I can roll back to a previous table version without rebuilding from scratch. Unity Catalog adds governance on top, which keeps the medallion layers (bronze_landing, bronze, silver, gold, marts) cleanly separated without resorting to naming hacks.

Databricks bundles compute + storage + catalog + scheduling in one managed environment, which keeps infrastructure overhead low for a project like this.

Databricks allows you to run Python code, SQL code, shell scripts, and load other notebooks to use functions from them for tidy development. This makes it a good candidate for data platform projects.

### dbt (Data Build Tool)

dbt was a natural fit for the transformation layer. The main advantage is that it lets me define the entire bronze → silver → gold → marts pipeline as modular SQL models with a built-in DAG — dbt figures out the execution order so I don't have to manage dependencies manually. Beyond orchestration, dbt's built-in testing framework (`not_null`, `unique`, `relationships`, `accepted_values`) gives me a lightweight data contract at every layer: if a foreign key breaks between `fact_transaction` and `dim_account`, the pipeline fails loudly instead of silently producing bad aggregations. It also has useful libraries like elementary and dbt-expectations for handling cases that the basic functionality of dbt's built-in functions can't handle.

The alternative would have been raw SQL scripts or PySpark notebooks for transformations, but those require manual dependency management and don't come with a declarative testing framework out of the box. dbt also makes it easy to write reusable macros (like the SCD Type 2 macro used in the silver layer) that would otherwise be repetitive boilerplate.

### Python + PySpark for Ingestion

The ingestion layer (Task 1 and Task 3) runs as Databricks notebooks written in Python. Python is the practical choice here — it has solid HTTP libraries for calling the Supabase REST API, and PySpark DataFrames are the native way to interact with Delta tables on Databricks. The ingestion logic (pagination, retry with exponential backoff, validation, deduplication) is procedural and fits Python's strengths. Writing the same logic in pure SQL would be awkward at best.


### JSON Schema for Validation Rules

Rather than scattering validation rules across Python code, I defined the transaction schema in a standard JSON Schema file (`transactions_schema.json`). This serves as both documentation and a single source of truth for what a valid record looks like. The Python validation function mirrors these rules, but having the schema in a declarative format means it's easy to review, share, and update without digging through code.

---

## Pipeline Data Flow

Both ingestion tasks follow the same core pattern: **API → Landing Table → Read Back as Dicts → Validate & Classify → Raw / Quarantine Tables**. The landing table (`bronze_landing.transactions`) is never modified or filtered — it is an **append-only audit trail** that serves as the source of truth for every record ever fetched from the API.

### Why the Landing → Read-Back → Classify Pattern?

A natural question is: *why not validate the API records directly and skip the landing table?* The round-trip (dicts → Spark DataFrame → Delta table → read back → dicts) exists for a specific reason:

1. **Audit & Reproducibility** — The landing table is an exact, unmodified mirror of what the API returned (plus an `ingestion_timestamp`). If a validation rule changes or a bug is found in the classification logic, we can **reprocess from landing** without re-fetching from the API. This is critical in production where the source API may not be idempotent or may have rate limits.

2. **Source of Truth for Compliance** — `bronze_landing.transactions` is the single source of truth for audit checks. Any record that ever entered the pipeline can be traced back to its original form in the landing table, including records that were quarantined or deduplicated. This supports data lineage, reconciliation (landing count vs. raw + quarantine counts), and forensic investigation of data quality issues.

3. **Decoupling Fetch from Processing** — By persisting to landing first, the pipeline is resilient to mid-run failures. If the classification step crashes, the data is already safely stored in landing and can be reprocessed without re-fetching. In Task 3 (incremental), this also enables **exactly-once processing**: unprocessed landing rows are identified by comparing `ingestion_timestamp` against what's already in raw + quarantine.

4. **Landing as the Reprocessing Checkpoint** — In Task 3, the pipeline reads only landing rows with `ingestion_timestamp > max(ingestion_timestamp)` across raw and quarantine tables. This means if a run partially fails (e.g. raw MERGE succeeds but quarantine append fails), the next run will pick up exactly where it left off — the landing table is the checkpoint.

### Task 1 — Full Load (`task1_ingest.py`)

Full load: fetches all records from the API, validates, deduplicates, and writes to bronze Delta tables. Used for the initial population of the data lake.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 0 — Ensure Tables Exist                                              │
│  Execute raw_table_ddl.sql (CREATE IF NOT EXISTS for all schemas & tables)  │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 1 — Fetch All Records from API                                       │
│  Paginate through Supabase REST API → list of dicts (raw_records)           │
│  If API fails → fall back to CSV file                                       │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 2 — Persist to Landing Table                                         │
│  Add ingestion_timestamp → build Spark DataFrame (LANDING_SCHEMA,           │
│  all STRING columns) → APPEND to bronze_landing.transactions                │
│  ⚠ No validation — exact mirror of the API response                        │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 3 — Read Landing → Validate → Classify                               │
│                                                                             │
│  3a. SELECT * FROM bronze_landing.transactions → Spark DataFrame            │
│                                                                             │
│  3b. landing_records_to_dicts() → convert DataFrame back to list of dicts   │
│      (cast amount from STRING → float for numeric validation)               │
│                                                                             │
│  3c. collect_existing_keys() → get all natural_key_hash values already      │
│      in bronze.transactions (for cross-run dedup)                           │
│                                                                             │
│  3d. classify_records() runs for each record:                               │
│      ├─ validate_record() — 11-point check (format, type, range, enum)     │
│      │   ├─ PASS → compute natural_key_hash (SHA-256 of 9 business fields) │
│      │   │   ├─ Hash NOT seen → deduped_records (clean)                    │
│      │   │   └─ Hash already seen → quarantine (duplicate_of:TXN-XXXX)     │
│      │   └─ FAIL → quarantine_records (with error_reason listing all       │
│      │             failures, e.g. "invalid_amount:-5; invalid_currency:XYZ")│
│      └─ Output: (deduped_records, quarantine_records, dup_count)           │
└──────────────┬─────────────────────────────┬───────────────────────────────┘
               │                             │
               ▼                             ▼
┌──────────────────────────────┐  ┌──────────────────────────────────────────┐
│  STEP 4 — Raw Table          │  │  STEP 5 — Quarantine Table               │
│  MERGE into                  │  │  APPEND to                               │
│  bronze.transactions         │  │  bronze.quarantine_transactions           │
│  (matched on                 │  │  (all columns as STRING to preserve      │
│   natural_key_hash —         │  │   malformed values for investigation)    │
│   last-resort dedup guard)   │  │                                          │
└──────────────┬───────────────┘  └──────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 6 — Update Watermark                                                 │
│  Save max(transaction_date) to default.ingestion_watermark                  │
│  (so Task 3 knows where to resume from)                                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Task 3 — Incremental Load (`task3_incremental.py`)

Incremental load: reads the watermark, fetches only new/late-arriving records, and MERGEs them into existing tables. Designed for repeated scheduled runs after the initial full load.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 0 — Ensure Tables Exist                                              │
│  Execute raw_table_ddl.sql (idempotent — safe to run on every execution)    │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 1 — Read Watermark                                                   │
│  get_watermark() → max_transaction_date from default.ingestion_watermark    │
│  ├─ First run (no watermark) → fetch_from = None (fetch ALL records)       │
│  └─ Subsequent run → fetch_from = watermark - LOOKBACK_DAYS (default 30d)   │
│     (lookback window catches late-arriving records)                          │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 2 — Fetch Records from API (with date filter)                        │
│  Paginate API with gte.{fetch_from} filter → only new + late-arriving       │
│  records are returned. If API fails → fall back to CSV.                     │
│  If no records → save empty watermark & exit early.                         │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 3 — Append to Landing Table                                          │
│  Add ingestion_timestamp → build DataFrame → APPEND to                      │
│  bronze_landing.transactions (preserves full audit history)                  │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 4 — Filter Unprocessed Landing Rows → Validate → Classify            │
│                                                                             │
│  4a. get_max_processed_ingestion_ts() → MAX(ingestion_timestamp) across     │
│      raw + quarantine tables = "everything already processed"               │
│                                                                             │
│  4b. SELECT * FROM landing WHERE ingestion_timestamp > max_processed_ts     │
│      ORDER BY ingestion_timestamp ASC, transaction_date ASC                 │
│      (deterministic ordering: oldest ingestion wins when duplicates exist)  │
│                                                                             │
│  4c. landing_records_to_dicts() → Spark rows back to plain dicts            │
│      (cast amount STRING → float for numeric validation)                    │
│                                                                             │
│  4d. collect_existing_keys() → existing natural_key_hash set from raw table │
│                                                                             │
│  4e. classify_records() → same 11-point validation + 3-tier dedup as Task 1│
│      Output: (deduped_records, quarantine_records, dup_count)               │
└──────────────┬─────────────────────────────┬───────────────────────────────┘
               │                             │
               ▼                             ▼
┌──────────────────────────────┐  ┌──────────────────────────────────────────┐
│  STEP 5 — Raw Table          │  │  STEP 6 — Quarantine Table               │
│  MERGE into                  │  │  APPEND to                               │
│  bronze.transactions         │  │  bronze.quarantine_transactions           │
│  (natural_key_hash match     │  │                                          │
│   prevents duplicates even   │  │                                          │
│   from lookback overlap)     │  │                                          │
└──────────────┬───────────────┘  └──────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 7 — Update Watermark                                                 │
│  new_max = max(transaction_date) from this batch (never moves backward)     │
│  Save to default.ingestion_watermark for next run                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Key difference from Task 1:** Task 3 does not read the entire landing table. It filters to only **unprocessed rows** (`ingestion_timestamp > max_processed_ts`), which is what makes it incremental. The landing table's `ingestion_timestamp` acts as the processing checkpoint — rows that have already been classified into raw or quarantine are never re-processed.

### Reconciliation via Landing Table

Because `bronze_landing.transactions` is append-only and never modified, you can always verify data completeness:

```sql
-- Total records ever ingested
SELECT COUNT(*) FROM bronze_landing.transactions;

-- Should equal raw + quarantine (every record goes to one or the other)
SELECT
  (SELECT COUNT(*) FROM bronze.transactions) +
  (SELECT COUNT(*) FROM bronze.quarantine_transactions)
AS processed_total;
```

If these two counts diverge, it means some landing rows haven't been processed yet — which is exactly what Task 3's `ingestion_timestamp` filter is designed to catch on the next run.

---

## Data Quality Handling

Data quality is enforced at multiple stages of the pipeline — from the moment records are fetched from the API all the way through to the final marts layer. The goal is to catch problems early, quarantine bad data so it doesn't pollute downstream tables, and validate expectations at every layer boundary.

### Ingestion-Time Validation (Python)

Every record fetched from the Supabase API goes through an 11-point validation check in `shared_utils.py` before it's allowed into the bronze raw table. These checks cover:

- **Required fields** — all 10 business fields must be present and non-null.
- **Format validation** — `transaction_id` must match `TXN-[A-Z0-9]+`, `account_id` must match `ACC-\d{4}`, `transaction_date` must be valid ISO 8601 UTC.
- **Type and range checks** — `amount` must be numeric and strictly positive (not zero), `currency` must be one of 7 allowed ISO 4217 codes, `transaction_type` must be `debit` or `credit`, `status` must be one of `completed/pending/failed/reversed`.
- **Semantic validation** — `country_code` is checked against the full set of ~250 officially assigned ISO 3166-1 alpha-2 codes (so "UK" and "EN" are rejected even though they match the regex pattern), `merchant_name` must contain at least one non-whitespace character, and `merchant_category` must be one of 12 predefined categories.

Records that fail any check are routed to a **quarantine table** (`bronze.quarantine_transactions`) with their `error_reason` field listing all failures (semicolon-separated). The quarantine table stores every column as STRING so that malformed values (like a negative amount or an invalid currency code) are preserved for investigation rather than lost to a type-cast error.

### Deduplication

Duplicate detection uses a SHA-256 hash of 9 business fields (`account_id`, `transaction_date`, `amount`, `currency`, `transaction_type`, `merchant_name`, `merchant_category`, `status`, `country_code`) as a natural key. This hash is computed once and stored alongside each record.

**Why a hash instead of a tuple of raw field values?** The alternative would be to build a 9-element tuple from the natural-key fields and use that directly for comparison. A SHA-256 hash was chosen for four concrete reasons:

1. **Memory** — a single fixed 64-character hex string vs. a 9-element tuple of variable-length strings; at high row counts this materially reduces the overhead of the in-memory dedup set.
2. **Lookup speed** — Python `set`/`dict` operations hash the key internally; hashing one short string is faster than hashing a 9-element tuple on every lookup.
3. **Less Spark I/O** — downstream queries (e.g. Task 3's existing-key collection) read one column instead of nine, reducing the data shuffled from the Delta table to the driver.
4. **Simpler MERGE** — the Delta MERGE statement can match on a single indexed column (`natural_key_hash`) rather than a 9-column composite condition, which is both more readable and more optimizer-friendly.

1. **In-batch dedup** — during a full load, a Python dictionary tracks hashes within the current batch. If the same hash appears twice, the second occurrence goes to quarantine with `duplicate_of:{winner_id}`.
2. **Cross-run dedup** — before writing, the pipeline queries the raw table for all existing hashes. Any incoming record whose hash already exists is quarantined instead of re-inserted.
3. **Delta MERGE guard** — the final write uses a MERGE statement keyed on `natural_key_hash`, providing an atomic last line of defense against duplicates even under concurrent writes.

### Late-Arriving Data Handling

The incremental pipeline (Task 3) uses a watermark-based strategy with a configurable lookback window (30 days). Instead of fetching only records newer than the watermark, it fetches from `watermark - 30 days` to catch records that arrived late in the source system. The MERGE and dedup logic ensure that already-processed records aren't duplicated — only genuinely new records make it into the raw table. The watermark itself never moves backward: if a batch of late-arriving records has dates older than the current watermark, the watermark stays where it is.

### Silver Layer — SCD Type 2 and Schema Enforcement

The dbt silver model applies a reusable SCD Type 2 macro I wrote ([`dbt_project/macros/scd_type2.sql`](dbt_project/macros/scd_type2.sql)) that:

- Deduplicates exact replicas (same `transaction_id` + same `ingestion_timestamp`).
- Detects consecutive versions with identical content (same `natural_key_hash`) and collapses them — so re-ingesting an unchanged record doesn't create a new version.
- Assigns `version_number`, `valid_from`, `valid_to`, and `is_current` fields, enabling point-in-time analysis of any transaction.

The silver layer also casts `transaction_date` from string to proper TIMESTAMP and DATE types, catching any records that somehow passed validation with an unparseable date. dbt schema tests at this layer enforce `not_null` on all business columns, `unique` on the surrogate key (`transaction_sk`), and guarantee that every current-version record has `valid_to IS 9999-12-31` and `is_current IS 1`.

This macro is directly transferable to platform work: any new data domain can use the same `scd_type2` macro for slowly changing attributes, and slot into the same `dbt build` pipeline with tests enforced out of the box — enabling self-service analytics without ad-hoc data wrangling.

### Gold Layer — Kimball Star Schema and Referential Integrity

The gold layer is intentionally modelled as a **Kimball-style star schema** derived from the single `transactions` source table. The goal here is twofold: to demonstrate dimensional modelling fundamentals, and to produce a reusable, self-describing schema that data analysts, analytics engineers, and data engineers can build on without needing to understand the raw bronze structure.

The design decisions follow standard Kimball practices:

- **Conformed dimensions** (`dim_account`, `dim_merchant`, `dim_currency`, `dim_date`) are extracted from the fact data so they can be shared across future fact tables — e.g. a `fact_payment_event` added later can reuse the same `dim_account` and `dim_date` without duplication.
- **Surrogate keys** (`merchant_sk`, `currency_sk`) decouple the fact table from natural keys that could change in the source system, making the schema resilient to upstream changes.
- **`dim_date`** is intentionally minimal: only distinct dates that appear in transactions. I used a shortcut here to finish the assignment more quickly. In a real-world scenario, I would create a full date dimension, which would allow analysts to write time-series queries with no gaps — a common requirement for dashboarding and cohort analysis.
- **`dim_currency`** is intentionally minimal: it contains only the 7 currencies present in the example dataset, with exchange rates hard-coded in the model. The decision not to aggregate transaction amounts across currencies in the mart table is deliberate — cross-currency aggregation without a reliable exchange rate snapshot produces misleading totals. In a production solution, I would replace the hard-coded values by sourcing exchange rates from https://data.ecb.europa.eu/ (or another source) via its public REST API, upsert the latest rates into `dim_currency` with SCD Type 2 history, and always keep the exchange rate column current without manual intervention.
- The **fact table** (`fact_transaction`) holds only foreign keys, additive measures (`amount`, `exchange_rate`), and a few degenerate dimensions (`transaction_type`, `status`), keeping it narrow and scan-efficient for aggregation queries.

The gold star schema (dimensions + fact table) is tested with dbt `relationships` tests that act as foreign key checks:

- `fact_transaction.account_id` must exist in `dim_account`
- `fact_transaction.merchant_sk` must exist in `dim_merchant`
- `fact_transaction.currency_sk` must exist in `dim_currency`
- `fact_transaction.transaction_date` must exist in `dim_date`

Additional tests enforce uniqueness on dimension keys, accepted value ranges on currency exchange rates (must be > 0), and enum validation on `merchant_category` (12 allowed values). If any of these checks fail, `dbt build` stops and the pipeline doesn't produce broken downstream tables.

### Marts Layer — Aggregation Validation

The daily account summary mart aggregates only `completed` transactions. dbt tests at this layer enforce:

- Composite uniqueness on `(account_id, transaction_date)` — no duplicate daily rows.
- `transaction_count >= 1` — every row represents at least one real transaction.
- Foreign key to `dim_account` — no orphaned account IDs.
- `top_category` must be one of the 12 valid merchant categories.

### Summary: Quality at Every Layer

| Layer | What's Checked | Failure Route |
|-------|---------------|---------------|
| Bronze Landing | Nothing — raw source mirror | N/A (append-only audit trail) |
| Bronze Raw | 11-point validation + 3-tier deduplication | Quarantine table (with error reasons) |
| Silver | Type casting, SCD2 versioning, not-null constraints | dbt build fails |
| Gold | Foreign key relationships, uniqueness, enum values, positive exchange rates | dbt build fails |
| Marts | Composite uniqueness, minimum counts, FK integrity, enum values | dbt build fails |

---

## Prerequisites

- Python 3.13
- A Databricks workspace with a running SQL Warehouse
- [dbt-databricks](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup)

---

## Setup

### 1. Connect the GitHub Repo to Databricks

Databricks can sync notebooks directly from a Git repository so you don't have to upload files manually. Follow these steps:

1. **Open your Databricks workspace** — Log in at your Databricks URL (e.g. `https://<your-workspace>.cloud.databricks.com`).

2. **Navigate to Repos** — In the left sidebar, click **Workspace**, then click **Repos** (under your username). 

3. **Add the repo** — Click the **Add Repo** button (top-right). In the dialog:
   - **Git repository URL:** paste `https://github.com/canbaytekin/senior-de-assignment.git`
   - **Git provider:** select **GitHub**
   - **Repository name:** leave the default (`senior-de-assignment`) or rename it
   - Click **Create Repo**

   Databricks will clone the repository into your Repos folder.

4. **Create a `.env` file** inside the `ingestion/` directory with the following values (shared here since this is an assignment):

    ```dotenv
    API_BASE_URL=https://fgbjekjqnbmtkmeewexb.supabase.co/rest/v1
    API_ENDPOINT=/transactions
    SUPABASE_API_KEY=sb_publishable_W2MbiakvFFthMHtlrzSkQw_URTiUI6G
    ```

The ingestion notebooks (`task1_ingest.py`, `task3_incremental.py`) load these via `shared_utils.py` using `python-dotenv`. 

5. **Navigate to the ingestion notebooks** — Expand **Repos → senior-de-assignment → ingestion** in the sidebar. You should see:
   - `shared_utils.py`
   - `task1_ingest.py`
   - `task3_incremental.py`
   - `test_incremental.py`

6. **Attach a cluster** — Open any notebook (e.g. `task1_ingest.py`). At the top of the notebook, click the cluster dropdown and select a running cluster (or create one).

---

## Running the Ingestion (Databricks)

The ingestion notebooks live in the `ingestion/` folder of the repo you just connected. Open them directly from **Repos → senior-de-assignment → ingestion**.

### Running the Notebooks

> **Note:** Both notebooks include a `%pip install requests python-dotenv` cell at the top that automatically installs the required packages before execution.

> **Warning you can ignore:** When running Task 1 or Task 3 notebooks, Databricks may show the following warning — simply close it and ignore it. The required libraries are already loaded and the environment works correctly:
>
> *"Environment configurations are not persisted in source-format notebooks. Enable Include in exports in the environment panel to preserve your environment."*

**Full load (Task 1)** — Open `task1_ingest.py` in Databricks and click **Run All**.

This will create:
- `workspace.bronze_landing.transactions` — raw mirror of the API response
- `workspace.bronze.transactions` — validated, deduplicated records
- `workspace.bronze.quarantine_transactions` — invalid / duplicate records

**Incremental load (Task 3)** — run on subsequent executions:

Open `task3_incremental.py` in Databricks and click **Run All**.

This will:
1. Read the watermark from `workspace.default.ingestion_watermark`
2. Fetch only new records from the API (with a 30-day lookback for late arrivals)
3. MERGE clean records into the raw table and append quarantine rows
4. Update the watermark

To simulate incremental runs and verify that the watermark advances, `test_incremental.py` is executed between STEP 3 and STEP 4 in the `task3_incremental.py` notebook.

---

## Running dbt Models

After the bronze tables are populated, follow these steps to build all downstream layers.

### 1. Clone the repo and create a virtual environment on your local machine

```bash
git clone https://github.com/canbaytekin/senior-de-assignment.git
cd senior-de-assignment
```

Open the folder in VSCode, then run the following commands line by line in the VSCode terminal:

```bash
python3.13 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

> **Note:** `requirements.txt` is for the **local dbt connection** only (running `dbt build` / `dbt test` from your machine against Databricks). The ingestion notebooks run on Databricks, where PySpark, Delta Lake, and other runtime libraries are pre-installed — they are not included in `requirements.txt` to avoid version conflicts with the cluster.

### 2. Configure dbt profile

> **Security note:** `profiles.yml` contains a Databricks personal access token and is **.gitignore'd**. Never commit credentials to version control — use environment variables or a local file that stays out of the repo.

Create the file `dbt_project/profiles.yml` with the following content:

```yaml
senior_de_assignment:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: workspace          # Unity Catalog name
      schema: default
      host: <your-databricks-host>
      http_path: <your-sql-warehouse-http-path>
      token: <your-personal-access-token>
      threads: 1
```

Replace the placeholder values with your actual Databricks workspace credentials.

### 3. Install dbt packages

```bash
cd dbt_project
dbt deps
```

---

### 4. Run dbt to build all downstream layers

Run the commands below in the VSCode terminal.

Make sure you are in the `dbt_project` directory. If not, change to it with the first command below.

```bash
cd dbt_project
dbt build
```

Or if you run it a second time with a full refresh:

```bash
cd dbt_project
dbt build --full-refresh
```

The models produce the following tables in the `workspace` catalog:

| Layer  | Table                              | Description                          |
|--------|------------------------------------|--------------------------------------|
| Silver | `workspace.silver.silver_transactions` | Cleaned, typed transactions       |
| Gold   | `workspace.gold.dim_account`       | Account dimension                    |
| Gold   | `workspace.gold.dim_merchant`      | Merchant dimension                   |
| Gold   | `workspace.gold.dim_date`          | Date dimension                       |
| Gold   | `workspace.gold.fact_transaction`  | Transaction fact table               |
| Marts  | `workspace.marts.daily_account_summary` | Daily aggregates per account    |

---

## Sample Outputs

Pre-generated output files are not included because I chose the Databricks + dbt solution.


---

## What I Would Improve Given More Time

### Metadata-Driven Ingestion Framework

The biggest improvement I'd make is replacing the current hard-coded ingestion scripts with a **metadata-driven framework**. Instead of writing a separate notebook for every new data source, I'd design a central metadata table (e.g. `workspace.default.ingestion_metadata`) that holds everything the pipeline needs to know about each source:

| Column | Purpose |
|--------|---------|
| `source_name` | Logical name of the data source (e.g. `supabase_transactions`) |
| `source_type` | How to connect — `rest_api`, `jdbc`, `sftp`, `s3`, etc. |
| `connection_secret_name` | Name of the Key Vault secret that stores connection details |
| `endpoint` / `table_name` | API endpoint path or source database table name |
| `columns_to_source` | Comma-separated list (or JSON array) of columns to extract |
| `primary_key_columns` | Columns that form the natural key for dedup/MERGE |
| `load_type` | `full_load`, `incremental`, `one_time`, `snapshot`, etc. |
| `notebook_path` | Which Databricks notebook to execute for this source |
| `target_schema` | Destination schema in the lakehouse |
| `target_table` | Destination table name |
| `is_active` | Flag to enable/disable a source without deleting the row |
| `schedule_cron` | Optional cron expression for Databricks Workflow scheduling |

**Connection details in Key Vault** — The `connection_secret_name` column maps to an Azure Key Vault secret (or Databricks secret scope) that stores all sensitive connection information: API base URL, endpoint, token, username, password, JDBC connection string, etc. The ingestion notebook would retrieve these at runtime using `dbutils.secrets.get(scope, key)`, so no credentials ever live in code or config files.

**CSV as the source of truth** — The metadata table's source of truth would be a CSV file committed to the repo (e.g. `config/ingestion_metadata.csv`). When a data engineer needs to onboard a new source, they simply add a row to this CSV with all the necessary details — source type, columns, PK, load type, notebook path, and the Key Vault secret name. A scheduled Databricks workflow job would regularly read this CSV and upsert the rows into the metadata table, keeping it in sync without manual SQL inserts.

**Load-type dispatch** — Based on the `load_type` column, the orchestrator notebook would call the appropriate ingestion function:

- `full_load` — truncate-and-replace (or MERGE with full dataset)
- `incremental` — watermark-based fetch + MERGE (what Task 3 does today)
- `one_time` — run once, mark the row as completed
- `snapshot` — periodic full extract stored as a versioned partition

Each load type would have its own reusable function, but the `notebook_path` column also allows source-specific overrides for edge cases that can't be generalised into a common pattern.

### Medallion Architecture Restructuring

I'd restructure the medallion layers to better reflect their intended purpose:

- **Bronze** — Handle SCD Type 2 versioning here, directly on top of raw data. The bronze layer's job is to be a faithful, versioned history of everything that came from the source. Tracking change history belongs at this level because it's about preserving what the source system told us and when.

- **Silver** — Use this layer for **cross-source unification and standardisation**. In a real production environment, you rarely have a single source. You might have transactions coming from Supabase, account data from a CRM API, merchant details from an internal database, and currency rates from an external FX feed. The silver layer would reconcile and align these different sources — standardising column names, resolving entity matches, applying business logic to harmonise formats (e.g. normalising date formats, currency codes, merchant identifiers) — so that everything downstream speaks the same language.

- **Gold** — Build the Kimball star schema (or snowflake schema if needed) on top of the clean, unified silver data. Dimensions and facts would be derived from already-standardised entities, making the gold layer purely about analytical modelling rather than data cleaning.

This separation gives each layer a single, clear responsibility: bronze preserves history, silver unifies and standardises, gold models for analytics.

### Repository Separation — dbt and Databricks Notebooks

Currently, the dbt project and the Databricks ingestion notebooks live in a single repository. In a production setting, I'd split them into **two separate repositories**:

- **Ingestion repo** — Contains the Databricks notebooks (`task1_ingest.py`, `task3_incremental.py`, `shared_utils.py`), the metadata configuration, and any orchestration logic (e.g. Databricks Workflows definitions). This repo would be synced to Databricks via Repos and owned by the team responsible for data ingestion and bronze-layer operations.

- **dbt repo** — Contains the dbt project (`models/`, `macros/`, `dbt_project.yml`, `packages.yml`, tests, and seeds). This repo would be managed independently with its own CI/CD pipeline — running `dbt build` and `dbt test` on pull requests via a CI job against a staging Databricks SQL Warehouse before merging to main.

**Why separate?** The two codebases have different development lifecycles, deployment targets, and ownership boundaries. Ingestion notebooks are executed on Databricks clusters and change when source systems change (new API endpoints, schema drift, new data sources). dbt models change when business logic or analytical requirements evolve (new dimensions, updated mart aggregations, test additions). Coupling them in one repo means a change to an ingestion notebook triggers CI checks for dbt models (and vice versa), slowing down both teams. Separate repos allow each side to version, review, test, and deploy independently — with clear interfaces defined at the bronze/silver table boundary.

### dbt Run Monitoring with Elementary

I'd add observability over dbt runs by integrating the [Elementary](https://www.elementary-data.com/) dbt package — but only as a **data-collection layer**, not as an end-to-end monitoring solution. After each `dbt build`, Elementary captures rich run metadata: model execution times, test results, row counts, schema changes, and freshness checks. I'd use this raw metadata to build **custom monitoring tables** in the warehouse rather than relying on Elementary's own dashboards or alerting features.

**Why build our own on top of Elementary?**

- **Flexibility** — Elementary's built-in monitoring UI and alerting are opinionated. Custom tables let us define our own SLAs, aggregate metrics the way our team needs them, and plug the data into whatever dashboarding or alerting tool we already use (e.g. Grafana, Datadog, or a simple Slack webhook).
- **Reduced vendor lock-in** — If Elementary becomes unmaintained, changes its licence, or introduces breaking changes, we only lose the data-collection layer — a relatively thin dependency. Our monitoring logic, dashboards, and alerts remain intact because they sit on top of our own tables, not Elementary's UI.
- **Unified observability** — By landing dbt run metadata into standard Delta tables alongside ingestion pipeline metrics (e.g. watermark progress, row counts from bronze), we can build a single monitoring layer that covers the entire data platform — not just the dbt portion.

In practice, the workflow would be: Elementary collects the run artifacts → a post-run dbt macro or scheduled job materialises the relevant metrics into a `monitoring.dbt_run_history` table → downstream dashboards and alerts consume that table directly.
