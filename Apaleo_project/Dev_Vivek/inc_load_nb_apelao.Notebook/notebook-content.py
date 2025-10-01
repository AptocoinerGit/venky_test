# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "77d118b3-0f3e-42ed-86b2-56ea046c8cc2",
# META       "default_lakehouse_name": "dev_test_lh_v",
# META       "default_lakehouse_workspace_id": "1130aaf5-97e6-4499-8c32-46b2d32eb718",
# META       "known_lakehouses": [
# META         {
# META           "id": "77d118b3-0f3e-42ed-86b2-56ea046c8cc2"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ------------------------------------------------------------
# Property Performance • Append-Only Incremental Loader (No MERGE, No DELETE)
# Target table: property_performance   (Delta, partitioned)
# Audit  table: property_performance_audit
# ------------------------------------------------------------

from datetime import date, datetime, timedelta
import os, time, json, uuid
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# -------------------- Config --------------------
API_BASE = "https://api.apaleo.com"
TOKEN_URL = "https://identity.apaleo.com/connect/token"

# Prefer env vars (or paste your credentials while testing)
CLIENT_ID = os.getenv("APALEO_CLIENT_ID") or "FFHG-SP-ALLES"
CLIENT_SECRET = os.getenv("APALEO_CLIENT_SECRET") or "vjHKFWlh30LfeKonuChEuxbW3V70vU"

DEFAULT_BACKFILL_DAYS = 365
DEFAULT_TIMEOUT = (5, 60)
RETRY_POLICY = Retry(
    total=5,
    backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"]
)

RUN_ID = uuid.uuid4().hex

# ---------------- Token + HTTP ------------------
SESSION = requests.Session()
SESSION.mount("https://", HTTPAdapter(max_retries=RETRY_POLICY))

_token = None
_token_expiry = 0

def _fetch_access_token():
    resp = SESSION.post(
        TOKEN_URL,
        data={"grant_type": "client_credentials"},
        auth=(CLIENT_ID, CLIENT_SECRET),
        timeout=DEFAULT_TIMEOUT
    )
    if resp.status_code != 200:
        raise Exception(f"Token request failed: {resp.status_code} {resp.text}")
    data = resp.json()
    return data["access_token"], int(data.get("expires_in", 3600))

def get_access_token():
    global _token, _token_expiry
    now = int(time.time())
    if _token and now < _token_expiry - 60:
        return _token
    token, expires_in = _fetch_access_token()
    _token = token
    _token_expiry = now + expires_in
    return _token

def get_json(url):
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    resp = SESSION.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
    if resp.status_code == 401:
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        resp = SESSION.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    return resp.json(), resp.status_code

# --------------- Helpers ------------------------
def flatten_df(df):
    while True:
        struct_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
        array_cols  = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType)]
        if not struct_cols and not array_cols:
            break
        for c in struct_cols:
            df = df.select("*", *[F.col(f"{c}.{x}").alias(f"{c}_{x}") for x in df.select(f"{c}.*").columns]).drop(c)
        for c in array_cols:
            df = df.withColumn(c, F.explode_outer(c))
    return df

def to_date_str(d: date) -> str:
    return d.strftime("%Y-%m-%d")

def log_audit(property_id, day_str, rows_fetched, rows_written, status, http_status, error_message, t0, t1):
    df = spark.createDataFrame([{
        "run_id": RUN_ID,
        "started_at": datetime.fromtimestamp(t0),
        "finished_at": datetime.fromtimestamp(t1),
        "property_id": property_id,
        "business_date": datetime.strptime(day_str, "%Y-%m-%d").date(),
        "rows_fetched": int(rows_fetched),
        "rows_written": int(rows_written),
        "status": status,
        "http_status": int(http_status) if http_status is not None else None,
        "error_message": (error_message or "")[:2000]
    }])
    df.write.mode("append").saveAsTable("property_performance_audit")

# --------------- Create tables if missing -------
spark.sql("""
CREATE TABLE IF NOT EXISTS property_performance
(
  property_id    STRING,
  business_date  DATE,
  InsertDate     TIMESTAMP,
  raw_payload    STRING
)
USING delta
PARTITIONED BY (business_date)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS property_performance_audit
(
  run_id         STRING,
  started_at     TIMESTAMP,
  finished_at    TIMESTAMP,
  property_id    STRING,
  business_date  DATE,
  rows_fetched   INT,
  rows_written   INT,
  status         STRING,
  http_status    INT,
  error_message  STRING
)
USING delta
""")

# --------------- Properties (pagination) --------
def get_all_properties():
    props = []
    page = 1
    size = 50
    while True:
        url = f"{API_BASE}/inventory/v1/properties?pageNumber={page}&pageSize={size}"
        data, _ = get_json(url)
        items = data.get("properties", [])
        props.extend(items)
        if len(items) < size:
            break
        page += 1
        if page > 2000:
            break
    return [p["id"] for p in props if "id" in p]

property_ids = get_all_properties()
if not property_ids:
    raise Exception("No properties found from Apaleo.")
print(f"Properties: {property_ids}")

# --------------- Last loaded date (anchor) ------
try:
    mx = (spark.table("property_performance")
                .groupBy("property_id")
                .agg(F.max("business_date").alias("max_date"))
                .collect())
    last_done = { r["property_id"]: r["max_date"] for r in mx if r["max_date"] is not None }
except Exception:
    last_done = {}

print("Last processed dates:", last_done)

# --------------- Fetch one day ------------------
def fetch_property_performance(property_id, day_str):
    url = (f"{API_BASE}/reports/v1/reports/property-performance"
           f"?propertyId={property_id}&from={day_str}&to={day_str}&pageSize=500")
    # Use the low-level call to capture http status for audit
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    resp = SESSION.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
    if resp.status_code == 401:
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        resp = SESSION.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
    if resp.status_code != 200:
        return None, resp.status_code, resp.text
    try:
        return resp.json(), resp.status_code, None
    except Exception as e:
        return None, resp.status_code, str(e)

# --------------- Append-only main loop ----------
today = date.today()
overall = {"written": 0, "skipped_exists": 0, "no_data": 0, "failed": 0}

for prop in property_ids:
    # Start from next day after last loaded; else backfill window
    start_day = (last_done[prop] + timedelta(days=1)) if prop in last_done else (today - timedelta(days=DEFAULT_BACKFILL_DAYS))
    end_day = today
    if start_day > end_day:
        continue

    # Pull existing keys for THIS property and date range to avoid per-day queries
    # This uses partition pruning on business_date and a filter on property_id
    existing_keys = (
        spark.table("property_performance")
             .select("property_id", "business_date")
             .where((F.col("property_id") == prop) &
                    (F.col("business_date") >= F.to_date(F.lit(to_date_str(start_day)))) &
                    (F.col("business_date") <= F.to_date(F.lit(to_date_str(end_day)))))
             .distinct()
             .collect()
    )
    existing_dates = { r["business_date"] for r in existing_keys }  # set of datetime.date

    day = start_day
    while day <= end_day:
        day_str = to_date_str(day)

        # If already present, skip (append-only guarantee)
        if day in existing_dates:
            overall["skipped_exists"] += 1
            # Optional: log as skipped to audit (commented to keep audit lighter)
            # log_audit(prop, day_str, 0, 0, "skipped_exists", 200, None, time.time(), time.time())
            day += timedelta(days=1)
            continue

        t0 = time.time()
        raw_json, http_status, err = fetch_property_performance(prop, day_str)

        if err is not None:
            overall["failed"] += 1
            log_audit(prop, day_str, 0, 0, "failed", http_status, err, t0, time.time())
            print(f"[FAILED] {prop} {day_str} -> {http_status} {err}")
            day += timedelta(days=1)
            continue

        # No data? Don’t write anything, just log
        if not raw_json or (isinstance(raw_json, dict) and len(raw_json.keys()) == 0):
            overall["no_data"] += 1
            log_audit(prop, day_str, 0, 0, "no_data", http_status, None, t0, time.time())
            print(f"[NO DATA] {prop} {day_str}")
            day += timedelta(days=1)
            continue

        # Build DataFrame + keep raw JSON
        raw_str = json.dumps(raw_json)
        df_raw = spark.read.json(spark.sparkContext.parallelize([raw_str]))
        df_flat = flatten_df(df_raw)

        df_final = (df_flat
            .withColumn("property_id", F.lit(prop))
            .withColumn("business_date", F.to_date(F.lit(day_str)))
            .withColumn("InsertDate", F.current_timestamp())
            .withColumn("raw_payload", F.lit(raw_str))
        )

        count_in = df_final.count()
        if count_in == 0:
            overall["no_data"] += 1
            log_audit(prop, day_str, 0, 0, "no_data", http_status, "Flatten produced 0 rows", t0, time.time())
            print(f"[NO DATA] {prop} {day_str} (0 rows after flatten)")
            day += timedelta(days=1)
            continue

        # Append ONLY (never modify existing rows)
        df_final.write.mode("append").saveAsTable("property_performance")
        overall["written"] += count_in
        log_audit(prop, day_str, count_in, count_in, "success", http_status, None, t0, time.time())
        print(f"[SAVED] {prop} {day_str} rows={count_in}")

        day += timedelta(days=1)

print("\n=== RUN SUMMARY ===")
print(overall)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
