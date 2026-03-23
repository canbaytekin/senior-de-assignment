# Senior DE Assignment

## Overview

This project ingests payment transactions from a Supabase REST API into a Databricks Delta Lake, applies data quality validation, and builds a layered dbt model (silver → gold → marts).

The pipeline has two ingestion modes:
- **Task 1** (`task1_ingest.py`) — full load: fetches all records, validates, deduplicates, and writes to bronze Delta tables.
- **Task 3** (`task3_incremental.py`) — incremental load: reads a watermark, fetches only new records, and MERGEs them into existing tables.

dbt then transforms the clean bronze data into silver, gold (dims + facts), and a daily marts aggregation.

---

## Why This Tech Stack?

Choosing the right tools matters as much as writing the code. Here's the reasoning behind each major technology decision in this project:

### Databricks + Delta Lake

I went with Databricks as the compute and storage layer because it gives me ACID transactions on top of a data lake — something you really need when you're doing incremental MERGEs and want idempotent writes. Delta Lake's support for time travel also means every ingestion run is auditable: if something goes wrong, I can roll back to a previous table version without rebuilding from scratch. Unity Catalog adds governance on top (catalog → schema → table namespacing), which keeps the medallion layers (bronze_landing, bronze, silver, gold, marts) cleanly separated without resorting to naming hacks.

I considered plain Parquet on S3 + Athena, but that stack doesn't give you MERGE semantics natively — you'd need something like Apache Hudi or Iceberg and a separate query engine. Databricks bundles compute + storage + catalog + scheduling in one managed environment, which keeps infrastructure overhead low for a project like this.

### dbt (Data Build Tool)

dbt was a natural fit for the transformation layer. The main advantage is that it lets me define the entire bronze → silver → gold → marts pipeline as modular SQL models with a built-in DAG — dbt figures out the execution order so I don't have to manage dependencies manually. Beyond orchestration, dbt's built-in testing framework (`not_null`, `unique`, `relationships`, `accepted_values`) gives me a lightweight data contract at every layer: if a foreign key breaks between `fact_transaction` and `dim_account`, the pipeline fails loudly instead of silently producing bad aggregations.

The alternative would have been raw SQL scripts or PySpark notebooks for transformations, but those require manual dependency management and don't come with a declarative testing framework out of the box. dbt also makes it easy to write reusable macros (like the SCD Type 2 macro used in the silver layer) that would otherwise be repetitive boilerplate.

### Python + PySpark for Ingestion

The ingestion layer (Task 1 and Task 3) runs as Databricks notebooks written in Python. Python is the practical choice here — it has solid HTTP libraries for calling the Supabase REST API, and PySpark DataFrames are the native way to interact with Delta tables on Databricks. The ingestion logic (pagination, retry with exponential backoff, validation, deduplication) is procedural and fits Python's strengths. Writing the same logic in pure SQL would be awkward at best.

### Supabase as the Data Source

Supabase was the given API for this assignment, but it's worth noting that its auto-generated REST endpoints with built-in filtering (`gte`, `lte`, offset/limit pagination) made incremental loading straightforward — I can pass a date range directly in the query parameters instead of fetching everything and filtering client-side.

### JSON Schema for Validation Rules

Rather than scattering validation rules across Python code, I defined the transaction schema in a standard JSON Schema file (`transactions_schema.json`). This serves as both documentation and a single source of truth for what a valid record looks like. The Python validation function mirrors these rules, but having the schema in a declarative format means it's easy to review, share, and update without digging through code.

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

### Deduplication (Three-Tier Strategy)

Duplicate detection uses a SHA-256 hash of 9 business fields (everything except `transaction_id`) as a natural key. This hash is computed once and stored alongside each record:

1. **In-batch dedup** — during a full load, a Python dictionary tracks hashes within the current batch. If the same hash appears twice, the second occurrence goes to quarantine with `duplicate_of:{winner_id}`.
2. **Cross-run dedup** — before writing, the pipeline queries the raw table for all existing hashes. Any incoming record whose hash already exists is quarantined instead of re-inserted.
3. **Delta MERGE guard** — the final write uses a MERGE statement keyed on `natural_key_hash`, providing an atomic last line of defense against duplicates even under concurrent writes.

### Late-Arriving Data Handling

The incremental pipeline (Task 3) uses a watermark-based strategy with a configurable lookback window (default: 2 days). Instead of fetching only records newer than the watermark, it fetches from `watermark - 2 days` to catch records that arrived late in the source system. The MERGE and dedup logic ensure that already-processed records aren't duplicated — only genuinely new records make it into the raw table. The watermark itself never moves backward: if a batch of late-arriving records has dates older than the current watermark, the watermark stays where it is.

### Silver Layer — SCD Type 2 and Schema Enforcement

The dbt silver model applies a reusable SCD Type 2 macro that:

- Deduplicates exact replicas (same `transaction_id` + same `ingestion_timestamp`).
- Detects consecutive versions with identical content (same `natural_key_hash`) and collapses them — so re-ingesting an unchanged record doesn't create a new version.
- Assigns `version_number`, `valid_from`, `valid_to`, and `is_current` fields, enabling point-in-time analysis of any transaction.

The silver layer also casts `transaction_date` from string to proper TIMESTAMP and DATE types, catching any records that somehow passed validation with an unparseable date. dbt schema tests at this layer enforce `not_null` on all business columns, `unique` on the surrogate key (`transaction_sk`), and guarantee that every current-version record has `valid_to IS NULL`.

### Gold Layer — Referential Integrity

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

### 1. Clone the repo and create a virtual environment

```bash
git clone <repo-url>
cd senior-de-assignment

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

## Running the Ingestion (Databricks)

The ingestion notebooks are designed to run on Databricks. Upload the three files under `ingestion/` to a Databricks workspace folder (keeping them in the same directory so `%run ./shared_utils` resolves correctly):

```
shared_utils.py
task1_ingest.py
task3_incremental.py
```

### Running the Notebooks

> **Note:** Both notebooks include a `%pip install requests python-dotenv` cell at the top that automatically installs the required packages before execution.

**Full load (Task 1)** — Open `task1_ingest.py` in Databricks and click **Run All**.

This will create:
- `workspace.bronze_landing.transactions` — raw mirror of the API response
- `workspace.bronze.transactions` — validated, deduplicated records
- `workspace.bronze.quarantine_transactions` — invalid / duplicate records

**Incremental load (Task 3)** — run on subsequent executions:

Open `task3_incremental.py` in Databricks and click **Run All**.

This will:
1. Read the watermark from `workspace.default.ingestion_watermark`
2. Fetch only new records from the API (with a 2-day lookback for late arrivals)
3. MERGE clean records into the raw table and append quarantine rows
4. Update the watermark

To simulate two incremental runs and verify the watermark advances, you can run Task 3 twice. The output watermark files from the original runs are saved in `outputs/watermark_run1.json` and `outputs/watermark_run2.json` for reference.

---

## Running dbt Models

After the bronze tables are populated, run dbt to build all downstream layers:

```bash
cd dbt_project
dbt build
```

Or run layers individually:

```bash
dbt run --select silver      # bronze → silver (cleaned transactions)
dbt run --select gold        # silver → dims + fact table
dbt run --select marts       # gold → daily_account_summary
```

Run tests only:

```bash
dbt test
```

Run a specific model:

```bash
dbt run --select daily_account_summary
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

Pre-generated output files are included in the `outputs/` directory for reference:

| File | Description |
|------|-------------|
| `outputs/daily_summary_output.csv` | Result of `daily_account_summary` mart |
| `outputs/quarantine_sample.csv` | Sample of quarantined (invalid) records |
| `outputs/watermark_run1.json` | Watermark state after first incremental run |
| `outputs/watermark_run2.json` | Watermark state after second incremental run |

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
