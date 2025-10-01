# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4b376c56-22a1-4856-96ef-1ee6bf6f927d",
# META       "default_lakehouse_name": "Dev_Bronze_Lakehouse",
# META       "default_lakehouse_workspace_id": "1130aaf5-97e6-4499-8c32-46b2d32eb718",
# META       "known_lakehouses": [
# META         {
# META           "id": "4b376c56-22a1-4856-96ef-1ee6bf6f927d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ----------------------------------------------------
# 1) Imports & Spark session
# ----------------------------------------------------
import time
import json
import requests
from datetime import datetime, timedelta, date

from pyspark.sql import SparkSession
from pyspark.sql import Row, functions as F, types as T

spark = SparkSession.builder.getOrCreate()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------
# 2) Config - EDIT as needed
# ----------------------------------------------------
# Move secrets to Fabric parameters / Key Vault in production
client_id = "FFHG-SP-ALLES"            # <-- replace with secret ref
client_secret = "vjHKFWlh30LfeKonuChEuxbW3V70vU"  # <-- replace with secret ref
token_url = "https://identity.apaleo.com/connect/token"

# Properties to fetch
property_ids = ["BER", "MUC", "LND", "PAR", "VIE"]

# Target table (v2)
TARGET_TABLE = "property_performance_V2"

# When table is empty, start loading from this date
SEED_START_DATE = date(2025, 9, 19)

# End date (today). If you prefer UTC days, use datetime.utcnow().date()
END_DATE = date.today()

# Request/retry tuning
DEFAULT_PAGE_SIZE = 500
MAX_RETRIES = 5
BACKOFF_BASE_SEC = 1.0


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------
# 3) Auth & HTTP helpers
# ----------------------------------------------------
def get_access_token() -> str:
    """Get a fresh access token via client_credentials."""
    resp = requests.post(
        token_url,
        data={"grant_type": "client_credentials"},
        auth=(client_id, client_secret),
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Token request failed ({resp.status_code}): {resp.text}")
    return resp.json()["access_token"]


class ApiClient:
    """GET with token refresh on 401 and exponential backoff on 429/5xx."""
    def __init__(self):
        self.token = get_access_token()

    def _headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    def get(self, url, params=None):
        last_error = None
        refreshed = False

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=60)

                if resp.status_code == 200:
                    return resp

                if resp.status_code == 401 and not refreshed:
                    # refresh token once and retry immediately
                    self.token = get_access_token()
                    refreshed = True
                    continue

                if resp.status_code in (429, 500, 502, 503, 504):
                    last_error = f"{resp.status_code}: {resp.text[:300]}"
                    time.sleep(BACKOFF_BASE_SEC * (2 ** (attempt - 1)))
                    continue

                # other non-2xx
                resp.raise_for_status()

            except requests.RequestException as ex:
                last_error = str(ex)
                time.sleep(BACKOFF_BASE_SEC * (2 ** (attempt - 1)))
                continue

        raise RuntimeError(f"GET failed after {MAX_RETRIES} attempts. Last error: {last_error}")


api = ApiClient()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------
# 4) Incremental helpers
# ----------------------------------------------------
def table_exists(table_name: str) -> bool:
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        return False

def get_last_loaded_date(table_name: str):
    """Return max(BusinessDate) as a Python date, or None if missing/empty."""
    if not table_exists(table_name):
        return None
    df = spark.read.table(table_name)
    if "BusinessDate" not in df.columns:
        return None
    row = df.select(F.max("BusinessDate").alias("max_dt")).collect()[0]
    return row["max_dt"] if row and row["max_dt"] is not None else None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------
# 5) Compute incremental window
# ----------------------------------------------------
last_loaded = get_last_loaded_date(TARGET_TABLE)
start_date = SEED_START_DATE if last_loaded is None else (last_loaded + timedelta(days=1))

if start_date > END_DATE:
    print(f"No new dates to load. Last loaded = {last_loaded}, end = {END_DATE}.")
    raise SystemExit(0)

print(f"Incremental window: {start_date} → {END_DATE} (inclusive)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------
# 6) Fetch API data into raw dataframe
# ----------------------------------------------------
rows = []
current = start_date
while current <= END_DATE:
    date_str = current.strftime("%Y-%m-%d")

    for pid in property_ids:
        url = "https://api.apaleo.com/reports/v1/reports/property-performance"
        params = {
            "propertyId": pid,
            "from": date_str,
            "to": date_str,
            "pageSize": DEFAULT_PAGE_SIZE,
        }
        try:
            resp = api.get(url, params=params)
            data = resp.json()
            rows.append(Row(PropertyId=pid, BusinessDate=date_str, data=json.dumps(data)))
            print(f"OK  {pid} {date_str}")
        except Exception as ex:
            # Log and continue; optionally write to a control table
            print(f"ERR {pid} {date_str} → {ex}")

    current += timedelta(days=1)

if not rows:
    print("No rows fetched from API (all calls failed?). Aborting.")
    raise SystemExit(1)

df_raw = spark.createDataFrame(rows)
# display(df_raw)  # optional


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------
# 7) Parse & Flatten JSON
# ----------------------------------------------------
# Infer schema from one sample
sample_json = (
    df_raw.select("data")
          .filter(F.col("data").isNotNull())
          .limit(1)
          .collect()[0][0]
)
json_schema = spark.read.json(spark.sparkContext.parallelize([sample_json])).schema

# Parse to struct and bring fields top-level
df_parsed = df_raw.withColumn("parsed", F.from_json(F.col("data"), json_schema))
df_struct = df_parsed.select("PropertyId", "BusinessDate", F.col("parsed.*"))

# Recursive flattener for StructType (arrays are left as-is)
def flatten_struct(df_in, sep="_"):
    flat_cols = []
    has_struct = False

    for field in df_in.schema.fields:
        if isinstance(field.dataType, T.StructType):
            has_struct = True
            for child in field.dataType.fields:
                flat_cols.append(F.col(f"`{field.name}`.`{child.name}`").alias(f"{field.name}{sep}{child.name}"))
        else:
            flat_cols.append(F.col(field.name))

    out = df_in.select(*flat_cols)
    if any(isinstance(f.dataType, T.StructType) for f in out.schema.fields):
        return flatten_struct(out, sep=sep)
    return out

df_flat = flatten_struct(df_struct)

# Standardize types
df_final = (
    df_flat
    .withColumn("BusinessDate", F.to_date("BusinessDate"))
    .withColumn("PropertyId", F.col("PropertyId").cast("string"))
)

df_final.printSchema()
df_final.show(5, truncate=False)
# display(df_final)  # optional


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------
# 8) Create target table if missing (empty schema)
# ----------------------------------------------------
if not table_exists(TARGET_TABLE):
    (df_final.limit(0)
        .write
        .format("delta")
        .mode("overwrite")
        # .partitionBy("BusinessDate")  # enable if you want partitioning
        .saveAsTable(TARGET_TABLE))
    print(f"Created empty table: {TARGET_TABLE}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------
# 9) Idempotent write (MERGE)
# ----------------------------------------------------
df_final.createOrReplaceTempView("staging_property_performance_v2")

spark.sql(f"""
MERGE INTO {TARGET_TABLE} AS T
USING staging_property_performance_v2 AS S
ON T.PropertyId = S.PropertyId AND T.BusinessDate = S.BusinessDate
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print("MERGE completed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------
# 10) Verify
# ----------------------------------------------------
spark.read.table(TARGET_TABLE)\
    .where(F.col("BusinessDate") >= F.lit(start_date))\
    .orderBy("BusinessDate", "PropertyId")\
    .show(50, truncate=False)

# display(spark.read.table(TARGET_TABLE))  # optional


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- truncate table  Dev_Bronze_Lakehouse.property_performance_v2 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
