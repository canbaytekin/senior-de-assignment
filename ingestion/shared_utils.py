# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///


# COMMAND ----------

# MAGIC %md
# MAGIC # Shared Utilities — Validation, API Helpers & Schemas
# MAGIC
# MAGIC This notebook contains **constants, validation functions, API helpers, and Spark schemas**
# MAGIC shared by both **Task 1** (full ingestion) and **Task 3** (incremental ingestion).
# MAGIC
# MAGIC **Usage:** Both task notebooks call `%run ./shared_utils` to load these definitions
# MAGIC without executing any data-mutating logic.

# COMMAND ----------

# MAGIC %pip install requests python-dotenv

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  IMPORTS                                                                     #
# --------------------------------------------------------------------------- #
import logging
import os
import requests
import time
import re
import hashlib
from datetime import datetime, timezone, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from delta.tables import DeltaTable
from dotenv import load_dotenv

try:
    _THIS_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _THIS_DIR = None  # __file__ is not defined in Databricks notebooks

# In Databricks Repos, resolve the notebook directory from dbutils context
if not _THIS_DIR:
    try:
        _nb_path = str(
            dbutils.notebook.entry_point
            .getDbutils().notebook().getContext()
            .notebookPath().get()
        )
        # notebookPath() returns the workspace-relative path, e.g.
        # /Repos/user@example.com/repo-name/ingestion/shared_utils
        _candidate = "/Workspace" + os.path.dirname(_nb_path)
        if os.path.isdir(_candidate):
            _THIS_DIR = _candidate
    except Exception:
        pass

# Fallback: search well-known locations for raw_table_ddl.sql
if not _THIS_DIR or not os.path.isfile(os.path.join(_THIS_DIR, "raw_table_ddl.sql")):
    _search_dirs = [os.getcwd()]
    try:
        _search_dirs.append(os.path.dirname(os.path.abspath(__file__)))
    except NameError:
        pass
    # Common Databricks Repos mount-points
    _search_dirs += [
        "/Workspace/Repos/baytekin93can@gmail.com/senior-de-assignment/ingestion",
        os.path.join(os.getcwd(), "ingestion"),
    ]
    for _d in _search_dirs:
        if _d and os.path.isfile(os.path.join(_d, "raw_table_ddl.sql")):
            _THIS_DIR = _d
            break

if _THIS_DIR:
    load_dotenv(os.path.join(_THIS_DIR, ".env"))

spark = SparkSession.builder.getOrCreate()

# --------------------------------------------------------------------------- #
#  LOGGING                                                                     #
# --------------------------------------------------------------------------- #
log = logging.getLogger("ingestion_pipeline")
if not log.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ"
    ))
    _handler.formatter.converter = time.gmtime
    log.addHandler(_handler)
    log.setLevel(logging.INFO)

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  PIPELINE CONFIGURATION — shared across Task 1 & Task 3                      #
# --------------------------------------------------------------------------- #

API_BASE_URL     = os.environ.get("API_BASE_URL", "https://fgbjekjqnbmtkmeewexb.supabase.co/rest/v1")
API_ENDPOINT     = os.environ.get("API_ENDPOINT", "/transactions")
API_KEY          = os.environ.get("SUPABASE_API_KEY", "sb_publishable_W2MbiakvFFthMHtlrzSkQw_URTiUI6G")
PAGE_SIZE        = 100
ORDER_COLUMN     = "transaction_date"
ORDER_DIR        = "asc"

LANDING_TABLE    = "workspace.bronze_landing.transactions"
RAW_TABLE        = "workspace.bronze.transactions"
QUARANTINE_TABLE = "workspace.bronze.quarantine_transactions"
WATERMARK_TABLE  = "workspace.default.ingestion_watermark"

CSV_FALLBACK_PATH = None     # e.g. "/dbfs/FileStore/transactions.csv"
LOOKBACK_DAYS     = 2

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  REFERENCE CONSTANTS — enum validation sets                                  #
# --------------------------------------------------------------------------- #

VALID_CURRENCIES       = {"USD", "EUR", "GBP", "CHF", "JPY", "AUD", "CAD"}
VALID_TXN_TYPES        = {"debit", "credit"}
VALID_STATUSES         = {"completed", "pending", "failed", "reversed"}
VALID_MERCHANT_CATS    = {
    "e-commerce", "travel", "food_and_beverage", "groceries",
    "electronics", "retail", "entertainment", "health",
    "transportation", "home_and_garden", "payroll", "transfer",
}

# ISO 3166-1 alpha-2 assigned codes (full list)
VALID_COUNTRY_CODES = {
    "AF","AX","AL","DZ","AS","AD","AO","AI","AQ","AG","AR","AM","AW","AU","AT",
    "AZ","BS","BH","BD","BB","BY","BE","BZ","BJ","BM","BT","BO","BQ","BA","BW",
    "BV","BR","IO","BN","BG","BF","BI","CV","KH","CM","CA","KY","CF","TD","CL",
    "CN","CX","CC","CO","KM","CG","CD","CK","CR","CI","HR","CU","CW","CY","CZ",
    "DK","DJ","DM","DO","EC","EG","SV","GQ","ER","EE","SZ","ET","FK","FO","FJ",
    "FI","FR","GF","PF","TF","GA","GM","GE","DE","GH","GI","GR","GL","GD","GP",
    "GU","GT","GG","GN","GW","GY","HT","HM","VA","HN","HK","HU","IS","IN","ID",
    "IR","IQ","IE","IM","IL","IT","JM","JP","JE","JO","KZ","KE","KI","KP","KR",
    "KW","KG","LA","LV","LB","LS","LR","LY","LI","LT","LU","MO","MG","MW","MY",
    "MV","ML","MT","MH","MQ","MR","MU","YT","MX","FM","MD","MC","MN","ME","MS",
    "MA","MZ","MM","NA","NR","NP","NL","NC","NZ","NI","NE","NG","NU","NF","MK",
    "MP","NO","OM","PK","PW","PS","PA","PG","PY","PE","PH","PN","PL","PT","PR",
    "QA","RE","RO","RU","RW","BL","SH","KN","LC","MF","PM","VC","WS","SM","ST",
    "SA","SN","RS","SC","SL","SG","SX","SK","SI","SB","SO","ZA","GS","SS","ES",
    "LK","SD","SR","SJ","SE","CH","SY","TW","TJ","TZ","TH","TL","TG","TK","TO",
    "TT","TN","TR","TM","TC","TV","UG","UA","AE","GB","US","UM","UY","UZ","VU",
    "VE","VN","VG","VI","WF","EH","YE","ZM","ZW",
}

# Natural key fields used for duplicate detection
NATURAL_KEY_FIELDS = [
    "account_id", "transaction_date", "amount", "currency",
    "transaction_type", "merchant_name", "merchant_category",
    "status", "country_code",
]


def utc_now_iso():
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def compute_natural_key_hash(rec):
    """
    Return a SHA-256 hex digest built from the natural-key fields of *rec*.

    Why a hash instead of a raw tuple?
    1. Memory  — one fixed 64-char string vs. a 9-element tuple of variable-
       length strings; at high row counts this materially reduces set overhead.
    2. Lookup speed — Python set/dict operations hash the key internally;
       hashing a single short string is faster than hashing a 9-element tuple.
    3. Less Spark I/O — downstream queries (e.g. Task 3 existing-key collection)
       read one column instead of nine, reducing data shuffled to the driver.
    4. Simpler MERGE — Delta MERGE can match on a single indexed column rather
       than a 9-column composite condition.
    """
    key_str = "|".join(str(rec.get(f, "")) for f in NATURAL_KEY_FIELDS)
    return hashlib.sha256(key_str.encode("utf-8")).hexdigest()

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  SPARK SCHEMAS                                                               #
# --------------------------------------------------------------------------- #

# Only RAW_SCHEMA includes natural_key_hash. It is intentionally absent from
# QUARANTINE_SCHEMA and LANDING_SCHEMA because:
#   - Landing is a pre-validation mirror of the API response; computing a hash
#     before validation would be premature and misleading.
#   - Quarantine holds invalid records whose natural-key fields may be missing,
#     corrupt, or wrongly typed, so a reliable hash cannot be guaranteed.
#   - The hash is only needed for MERGE-based dedup in the raw table (Task 3)
#     and in-memory duplicate detection during classify_records().

RAW_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("transaction_date", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("merchant_name", StringType(), False),
    StructField("merchant_category", StringType(), False),
    StructField("status", StringType(), False),
    StructField("country_code", StringType(), False),
    StructField("natural_key_hash", StringType(), False),
    StructField("ingestion_timestamp", StringType(), False),
])

QUARANTINE_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("amount", StringType(), True),       # string to preserve bad values
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("status", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("error_reason", StringType(), False),
    StructField("ingestion_timestamp", StringType(), False),
])

LANDING_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("status", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("ingestion_timestamp", StringType(), False),
])

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  API HELPERS                                                                 #
# --------------------------------------------------------------------------- #

def _request_with_retry(url, headers, params=None, max_retries=5, timeout=30):
    """GET with exponential back-off for 429 / 5xx / timeout."""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 401:
                raise RuntimeError("HTTP 401 — check API_KEY / Authorization header")
            if resp.status_code == 400:
                log.warning("HTTP 400 — %s", resp.text)
                return []   # bad request for this page; return empty
            if resp.status_code in (429,) or resp.status_code >= 500:
                wait = min(2 ** attempt, 60)
                log.warning("HTTP %d — retrying in %ds (attempt %d/%d)", resp.status_code, wait, attempt+1, max_retries)
                time.sleep(wait)
                continue
            resp.raise_for_status()
        except requests.exceptions.Timeout:
            wait = min(2 ** attempt, 60)
            log.warning("Timeout — retrying in %ds (attempt %d/%d)", wait, attempt+1, max_retries)
            time.sleep(wait)
        except requests.exceptions.ConnectionError:
            wait = min(2 ** attempt, 60)
            log.warning("Connection error — retrying in %ds (attempt %d/%d)", wait, attempt+1, max_retries)
            time.sleep(wait)
    raise RuntimeError(f"Failed after {max_retries} retries for {url}")


def fetch_records(base_url, endpoint, api_key, page_size,
                  order_col, order_dir, from_date=None):
    """
    Paginate through the API endpoint and return a list of dicts.
    If from_date is given, only fetch records with transaction_date >= from_date.
    When from_date is None the entire dataset is fetched (full load).
    """
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
    }
    url = f"{base_url}{endpoint}"
    offset = 0
    all_records = []

    while True:
        params = {
            "limit": page_size,
            "offset": offset,
            "order": f"{order_col}.{order_dir}",
        }
        if from_date:
            params["transaction_date"] = f"gte.{from_date}"

        page = _request_with_retry(url, headers, params=params)
        if not page:
            break
        all_records.extend(page)
        log.info("Fetched %d records (offset=%d, total so far=%d)", len(page), offset, len(all_records))
        if len(page) < page_size:
            break
        offset += page_size

    log.info("Total records fetched: %d", len(all_records))
    return all_records


def load_from_csv(path):
    """Fallback: load records from a CSV file on DBFS/local."""
    df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    records = [row.asDict() for row in df.collect()]
    log.info("Total records loaded from CSV: %d", len(records))
    return records

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  VALIDATION FUNCTIONS                                                        #
# --------------------------------------------------------------------------- #

def _parse_iso8601(dt_string):
    """
    Validate strict ISO 8601 with 'T' separator.
    Accepts both trailing Z and +00:00 (Supabase returns +00:00).
    Returns a datetime if valid, else None.
    """
    if not isinstance(dt_string, str):
        return None
    # Normalise Supabase +00:00 → Z for consistent storage
    normalised = dt_string.replace("+00:00", "Z")
    # Must contain a 'T' separator
    if "T" not in normalised:
        return None
    # Strict ISO 8601: only %Y-%m-%dT%H:%M:%SZ — no fractional seconds
    try:
        return datetime.strptime(normalised, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None


def validate_record(rec):
    """
    Return a list of error strings. Empty list ⇒ record is valid.
    """
    errors = []

    # ---- Required field presence ----
    required_fields = [
        "transaction_id", "account_id", "transaction_date",
        "amount", "currency", "transaction_type",
        "merchant_name", "merchant_category", "status", "country_code",
    ]
    for f in required_fields:
        if f not in rec or rec[f] is None:
            errors.append(f"missing_field:{f}")

    # Short-circuit if critical fields are missing
    if errors:
        return errors

    # ---- transaction_id format (matches JSON schema: ^TXN-[A-Z0-9]+$) ----
    tid = str(rec["transaction_id"])
    if not re.match(r"^TXN-[A-Z0-9]+$", tid):
        errors.append(f"invalid_transaction_id:{tid}")

    # ---- account_id format ----
    aid = str(rec["account_id"])
    if not re.match(r"^ACC-\d{4}$", aid):
        errors.append(f"invalid_account_id:{aid}")

    # ---- transaction_date ----
    dt_raw = str(rec["transaction_date"])
    parsed = _parse_iso8601(dt_raw)
    if parsed is None:
        errors.append(f"invalid_transaction_date:{dt_raw}")

    # ---- amount: must be numeric and > 0 ----
    try:
        amt = float(rec["amount"])
        if amt <= 0:
            errors.append(f"non_positive_amount:{rec['amount']}")
    except (ValueError, TypeError):
        errors.append(f"invalid_amount:{rec['amount']}")

    # ---- currency ----
    if rec["currency"] not in VALID_CURRENCIES:
        errors.append(f"invalid_currency:{rec['currency']}")

    # ---- transaction_type (case-sensitive) ----
    if rec["transaction_type"] not in VALID_TXN_TYPES:
        errors.append(f"invalid_transaction_type:{rec['transaction_type']}")

    # ---- merchant_name: non-empty, non-whitespace ----
    mn = rec.get("merchant_name", "")
    if not isinstance(mn, str) or mn.strip() == "":
        errors.append("blank_merchant_name")

    # ---- merchant_category ----
    if rec["merchant_category"] not in VALID_MERCHANT_CATS:
        errors.append(f"invalid_merchant_category:{rec['merchant_category']}")

    # ---- status (case-sensitive) ----
    if rec["status"] not in VALID_STATUSES:
        errors.append(f"invalid_status:{rec['status']}")

    # ---- country_code: must be assigned ISO 3166-1 alpha-2 ----
    cc = str(rec.get("country_code", ""))
    if cc not in VALID_COUNTRY_CODES:
        errors.append(f"invalid_country_code:{cc}")

    return errors

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  DATAFRAME BUILDERS                                                          #
# --------------------------------------------------------------------------- #

def write_delta_table(df, table_name, mode="overwrite"):
    """Write a Spark DataFrame to a Delta table with standard options."""
    writer = df.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(table_name)


def compute_new_watermark_max(deduped_records, current_watermark=None):
    """Return the new high-water mark from deduped records."""
    if deduped_records:
        new_max = max(r["transaction_date"] for r in deduped_records)
        if current_watermark and current_watermark > new_max:
            new_max = current_watermark
        return new_max
    return current_watermark or "1970-01-01T00:00:00Z"


def _build_dataframe(records, schema, row_mapper):
    """Generic helper: convert record dicts to a Spark DataFrame via row_mapper."""
    if not records:
        return spark.createDataFrame([], schema=schema)
    rows = [row_mapper(r) for r in records]
    return spark.createDataFrame(rows, schema=schema)


def _landing_row(r):
    return (
        int(r.get("id", 0)) if r.get("id") is not None else None,
        str(r.get("transaction_id", "")),
        str(r.get("account_id", "")),
        str(r.get("transaction_date", "")),
        str(r.get("amount", "")),
        str(r.get("currency", "")),
        str(r.get("transaction_type", "")),
        str(r.get("merchant_name", "")),
        str(r.get("merchant_category", "")),
        str(r.get("status", "")),
        str(r.get("country_code", "")),
        r["ingestion_timestamp"] if "ingestion_timestamp" in r else "",
    )


def _raw_row(r):
    return (
        int(r.get("id", 0)) if r.get("id") is not None else None,
        str(r["transaction_id"]),
        str(r["account_id"]),
        str(r["transaction_date"]),
        float(r["amount"]),
        str(r["currency"]),
        str(r["transaction_type"]),
        str(r["merchant_name"]),
        str(r["merchant_category"]),
        str(r["status"]),
        str(r["country_code"]),
        str(r["natural_key_hash"]),
        str(r["ingestion_timestamp"]),
    )


def _quarantine_row(q):
    return (
        int(q.get("id", 0)) if q.get("id") is not None else None,
        str(q.get("transaction_id", "")),
        str(q.get("account_id", "")),
        str(q.get("transaction_date", "")),
        str(q.get("amount", "")),
        str(q.get("currency", "")),
        str(q.get("transaction_type", "")),
        str(q.get("merchant_name", "")),
        str(q.get("merchant_category", "")),
        str(q.get("status", "")),
        str(q.get("country_code", "")),
        str(q["error_reason"]),
        str(q["ingestion_timestamp"]),
    )


def build_landing_dataframe(records, ingestion_ts):
    """Convert raw API records to a Spark DataFrame for the landing table (no validation)."""
    # Inject ingestion_timestamp before mapping
    for r in records:
        r["ingestion_timestamp"] = ingestion_ts
    return _build_dataframe(records, LANDING_SCHEMA, _landing_row)


def get_max_processed_ingestion_ts(raw_table, quarantine_table):
    """
    Return the maximum ingestion_timestamp across the raw and quarantine tables.
    Returns None if neither table exists or both are empty.
    """
    parts = []
    for tbl in (raw_table, quarantine_table):
        try:
            if spark.catalog.tableExists(tbl):
                parts.append(f"SELECT MAX(ingestion_timestamp) AS max_ts FROM {tbl}")
        except Exception:
            pass
    if not parts:
        return None
    union_sql = " UNION ALL ".join(parts)
    row = spark.sql(f"SELECT MAX(max_ts) AS max_ts FROM ({union_sql})").collect()
    if row and row[0][0]:
        return row[0][0]
    return None


def landing_records_to_dicts(landing_df):
    """Convert a landing Spark DataFrame back to a list of dicts (for classify_records)."""
    records = []
    for row in landing_df.collect():
        d = row.asDict()
        # Restore amount to numeric for validation
        try:
            d["amount"] = float(d["amount"])
        except (ValueError, TypeError):
            pass  # keep as string — validation will catch it
        records.append(d)
    return records


def build_raw_dataframe(deduped_records):
    """Convert a list of validated record dicts to a Spark DataFrame."""
    return _build_dataframe(deduped_records, RAW_SCHEMA, _raw_row)


def build_quarantine_dataframe(quarantine_records):
    """Convert a list of quarantine record dicts to a Spark DataFrame."""
    return _build_dataframe(quarantine_records, QUARANTINE_SCHEMA, _quarantine_row)


def collect_existing_keys(raw_table):
    """Return a set of natural_key_hash values already in the raw table."""
    existing_keys = set()
    if spark.catalog.tableExists(raw_table):
        for row in spark.sql(f"SELECT natural_key_hash FROM {raw_table}").collect():
            existing_keys.add(row[0])
    return existing_keys


def merge_into_raw(deduped_records, raw_table):
    """Build a DataFrame from deduped records and MERGE into the raw Delta table."""
    df = build_raw_dataframe(deduped_records)
    delta_tbl = DeltaTable.forName(spark, raw_table)
    (
        delta_tbl.alias("tgt")
        .merge(df.alias("src"), "tgt.natural_key_hash = src.natural_key_hash")
        .whenNotMatchedInsertAll()
        .execute()
    )
    return df.count()


def append_quarantine(quarantine_records, quarantine_table):
    """Build a DataFrame from quarantine records and append to the quarantine table."""
    df = build_quarantine_dataframe(quarantine_records)
    write_delta_table(df, quarantine_table, mode="append")
    return df.count()


def classify_records(raw_records, ingestion_ts, existing_keys=None):
    """
    Validate and deduplicate records.
    Returns (deduped_records, quarantine_records, dup_count).

    existing_keys: optional set of natural-key tuples already in the raw table
                   (used by Task 3 incremental to avoid cross-batch duplicates).
    """
    if existing_keys is None:
        existing_keys = set()

    valid_records = []
    quarantine_records = []

    for rec in raw_records:
        errs = validate_record(rec)
        if errs:
            quarantine_records.append({
                **rec,
                "error_reason": "; ".join(errs),
                "ingestion_timestamp": ingestion_ts,
            })
        else:
            valid_records.append(rec)

    # Duplicate detection — uses SHA-256 hash of natural-key fields rather than
    # storing full 9-element tuples; see compute_natural_key_hash() for rationale.
    seen = {}
    deduped_records = []
    dup_count = 0

    for rec in valid_records:
        h = compute_natural_key_hash(rec)
        if h in existing_keys or h in seen:
            dup_count += 1
            quarantine_records.append({
                **rec,
                "error_reason": f"duplicate_of:{seen.get(h, 'existing_record')}",
                "ingestion_timestamp": ingestion_ts,
            })
        else:
            seen[h] = rec["transaction_id"]
            deduped_records.append({
                **rec,
                "transaction_date": str(rec["transaction_date"]).replace("+00:00", "Z"),
                "natural_key_hash": h,
                "ingestion_timestamp": ingestion_ts,
            })

    return deduped_records, quarantine_records, dup_count

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  WATERMARK HELPERS                                                           #
# --------------------------------------------------------------------------- #

WATERMARK_SCHEMA = StructType([
    StructField("max_transaction_date", StringType(), False),
    StructField("last_run_timestamp", StringType(), False),
    StructField("records_ingested", IntegerType(), False),
])


def get_watermark(table_name):
    """
    Read the high-water mark from the watermark Delta table.
    Returns None on first run (table doesn't exist or is empty).
    """
    try:
        if spark.catalog.tableExists(table_name):
            rows = spark.sql(
                f"SELECT max_transaction_date FROM {table_name} LIMIT 1"
            ).collect()
            if rows and rows[0][0]:
                return rows[0][0]  # ISO 8601 string
    except Exception as e:
        log.info("Watermark table not found or empty (%s); treating as first run.", e)
    return None


def save_watermark(table_name, max_date, run_ts, records_ingested):
    """
    Overwrite the single-row watermark Delta table with the latest values.
    """
    new_row = spark.createDataFrame([
        (max_date, run_ts, records_ingested)
    ], schema=WATERMARK_SCHEMA)
    write_delta_table(new_row, table_name, mode="overwrite")

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  TABLE INITIALISATION — execute DDL from raw_table_ddl.sql                   #
# --------------------------------------------------------------------------- #

DDL_PATH = os.path.join(_THIS_DIR, "raw_table_ddl.sql") if _THIS_DIR else "raw_table_ddl.sql"


def ensure_tables_exist(ddl_path=DDL_PATH):
    """
    Read raw_table_ddl.sql and execute each CREATE statement so that all
    schemas and tables exist before the pipeline writes any data.

    Every statement in the file is idempotent (IF NOT EXISTS / IF NOT EXISTS),
    so this is safe to call on every run.
    """
    with open(ddl_path, "r") as f:
        ddl_content = f.read()

    for statement in ddl_content.split(";"):
        stmt = statement.strip()
        # Strip leading SQL comment lines so the CREATE check isn't
        # blocked by -- comment blocks that precede the DDL keyword.
        stmt_body = "\n".join(
            ln for ln in stmt.splitlines()
            if not ln.lstrip().startswith("--")
        ).strip()
        if stmt_body and stmt_body.upper().startswith("CREATE"):
            spark.sql(stmt)

    log.info("ensure_tables_exist | DDL executed — all schemas and tables ensured.")

# COMMAND ----------


