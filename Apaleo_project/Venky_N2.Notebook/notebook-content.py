# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "dfb61056-7dab-43a8-9f69-c233cff12310",
# META       "default_lakehouse_name": "Test_LH",
# META       "default_lakehouse_workspace_id": "1130aaf5-97e6-4499-8c32-46b2d32eb718",
# META       "known_lakehouses": [
# META         {
# META           "id": "dfb61056-7dab-43a8-9f69-c233cff12310"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Cell 1: Imports & Spark session
from datetime import date, timedelta
import time, json, requests
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode_outer, col, current_timestamp, to_date
from pyspark.sql.types import StructType, ArrayType

# initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("apaleo_etl")

# Spark session (adjust appName as needed)
spark = SparkSession.builder.appName("Apaleo_PropertyPerformance_ETL").getOrCreate()

# Important: dynamic partition overwrite mode for safe partition overwrites
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

print("Spark initialized:", spark.sparkContext.appName)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: Configuration (replace with secrets in prod)
CLIENT_ID = "FFHG-SP-ALLES"
CLIENT_SECRET = "vjHKFWlh30LfeKonuChEuxbW3V70vU"
TOKEN_URL = "https://identity.apaleo.com/connect/token"

PROPERTY_IDS = ["BER", "MUC", "LND", "PAR", "VIE"]

# Table names
RAW_TABLE = "property_performance_raw"   # keeps raw_json (audit)
FLAT_TABLE = "property_performance_flat" # target flattened table

# Other config
PAGE_SIZE = 500
BOOTSTRAP_DAYS = 30   # number of days for initial bootstrap


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3: Get access token
def get_access_token():
    resp = requests.post(
        TOKEN_URL,
        data={"grant_type": "client_credentials"},
        auth=(CLIENT_ID, CLIENT_SECRET),
        timeout=30
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError("No access_token received")
    return token

# quick test (DO NOT run this often; token is short-lived)
access_token = get_access_token()
print("Got token length:", (access_token))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4: Fetch with retries (exponential backoff)
def fetch_with_retries(url, headers=None, max_retries=4, backoff_base=1.5, timeout=60):
    attempt = 0
    while attempt <= max_retries:
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            # success
            if r.status_code == 200:
                return r
            # unauthorized -> return immediate so caller can refresh token
            if r.status_code == 401:
                return r
            # retryable statuses
            if r.status_code in (429, 500, 502, 503, 504):
                logger.warning(f"Transient status {r.status_code} from {url}. attempt {attempt}")
            else:
                return r
        except requests.RequestException as e:
            logger.warning(f"Requests exception: {e} (attempt {attempt})")
        attempt += 1
        sleep_time = backoff_base ** attempt
        time.sleep(sleep_time)
    raise RuntimeError(f"Failed to GET {url} after {max_retries} retries")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5: Flatten function (recursive)
def flatten_df(df):
    """
    Recursively flatten StructType fields and explode ArrayType fields.
    Call this AFTER creating df from raw JSON.
    """
    while True:
        struct_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
        array_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType)]
        if not struct_cols and not array_cols:
            break

        # expand structs
        for col_name in struct_cols:
            try:
                expanded = [col(f"{col_name}.{c}").alias(f"{col_name}_{c}")
                            for c in df.select(f"{col_name}.*").columns]
                df = df.select("*", *expanded).drop(col_name)
            except Exception as e:
                logger.warning(f"Failed to expand struct {col_name}: {e}")
                df = df.drop(col_name)

        # explode arrays (outer)
        for col_name in array_cols:
            try:
                df = df.withColumn(col_name, explode_outer(col_name))
            except Exception as e:
                logger.warning(f"Failed to explode array {col_name}: {e}")
                df = df.drop(col_name)
    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6: Quick test for single property & single day (run interactively)
prop = PROPERTY_IDS[0]
test_day = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")  # yesterday

access_token = get_access_token()
url = f"https://api.apaleo.com/reports/v1/reports/property-performance?propertyId={prop}&from={test_day}&to={test_day}&pageSize={PAGE_SIZE}"
r = fetch_with_retries(url, headers={"Authorization": f"Bearer {access_token}"})

print("HTTP status:", r.status_code)
if r.status_code == 200:
    payload = r.json()
    print("Top-level keys:", list(payload.keys())[:20])
    # load into dataframe
    json_str = json.dumps(payload)
    df_raw = spark.read.json(spark.sparkContext.parallelize([json_str]))
    print("Raw DF schema (nested):")
    df_raw.printSchema()
    print("Raw DF sample rows:")
    display(df_raw.limit(5).toPandas())   # in notebook, show a small sample
    # Now flatten
    df_flat = flatten_df(df_raw)
    print("Flattened DF schema:")
    df_flat.printSchema()
    print("Flattened DF sample rows:")
    display(df_flat.limit(5).toPandas())
else:
    print("Error payload:", r.text)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 7: Create minimal RAW table (keeps raw JSON) and FLAT table placeholder if needed
# Create raw table (audit) if not exists
if not spark.catalog.tableExists(RAW_TABLE):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
            property_id STRING,
            business_date DATE,
            raw_json STRING,
            InsertDate TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (business_date, property_id)
    """)
    print("Created RAW table:", RAW_TABLE)
else:
    print("RAW table exists:", RAW_TABLE)

# If flat table doesn't exist, we won't create a fixed schema (we'll create it on first bootstrap write)
if spark.catalog.tableExists(FLAT_TABLE):
    print("Flat table exists:", FLAT_TABLE)
else:
    print("Flat table does not exist yet. It will be created on bootstrap write.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 8: function to process payload and write to both raw and flat tables
def process_and_write_single(property_id, day_str, payload, raw_table=RAW_TABLE, flat_table=FLAT_TABLE):
    """
    - payload: JSON object (parsed)
    - Writes raw JSON to RAW_TABLE (append) for audit
    - Flattens and writes flattened columns to FLAT_TABLE using partition overwrite for that property/date
    """
    json_str = json.dumps(payload)

    # 1) write to RAW table (append) for audit
    df_raw = spark.createDataFrame([(property_id, day_str, json_str)], schema="property_id string, business_date string, raw_json string")
    # convert business_date to date type
    df_raw = df_raw.withColumn("business_date", to_date(col("business_date"))) \
                   .withColumn("InsertDate", current_timestamp())
    # append raw
    df_raw.write.format("delta").mode("append").partitionBy("business_date", "property_id").saveAsTable(raw_table)
    logger.info(f"Wrote RAW for {property_id}/{day_str}")

    # 2) prepare flattened DF
    df = spark.read.json(spark.sparkContext.parallelize([json_str]))
    df_flat = flatten_df(df)

    # add keys / audit
    df_flat = df_flat.withColumn("property_id", lit(property_id)) \
                     .withColumn("business_date", to_date(lit(day_str))) \
                     .withColumn("InsertDate", current_timestamp())

    # 3) write flattened DF to flat_table:
    # If flat_table does not exist yet -> create by writing with partitionBy once (mode overwrite)
    if not spark.catalog.tableExists(flat_table):
        logger.info(f"Flat table {flat_table} not found: creating with inferred schema (first bootstrap write).")
        df_flat.write.format("delta") \
             .mode("overwrite") \
             .option("overwriteSchema", "true") \
             .partitionBy("business_date", "property_id") \
             .saveAsTable(flat_table)
        logger.info(f"Flat table {flat_table} created.")
    else:
        # overwrite only the partition for that date+property (idempotent)
        replace_condition = f"business_date = '{day_str}' AND property_id = '{property_id}'"
        df_flat.write.format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", replace_condition) \
            .option("overwriteSchema", "true") \
            .saveAsTable(flat_table)
        logger.info(f"Flat partition overwritten: {property_id}/{day_str}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 9: Bootstrap (run once)
def run_bootstrap(property_list=PROPERTY_IDS, days_back=BOOTSTRAP_DAYS):
    access_token = get_access_token()
    today = date.today()
    from_date = today - timedelta(days=days_back)
    for prop in property_list:
        logger.info(f"Bootstrapping property: {prop}")
        day = from_date
        while day <= today:
            day_str = day.strftime("%Y-%m-%d")
            url = f"https://api.apaleo.com/reports/v1/reports/property-performance?propertyId={prop}&from={day_str}&to={day_str}&pageSize={PAGE_SIZE}"
            r = fetch_with_retries(url, headers={"Authorization": f"Bearer {access_token}"})
            if r.status_code == 401:
                access_token = get_access_token()
                r = fetch_with_retries(url, headers={"Authorization": f"Bearer {access_token}"})
            if r.status_code != 200:
                logger.warning(f"Skipping {prop} {day_str}: {r.status_code} {getattr(r,'text', '')}")
                day += timedelta(days=1)
                continue
            payload = r.json()
            if not payload:
                logger.info(f"No data for {prop} {day_str}")
                day += timedelta(days=1)
                continue
            try:
                process_and_write_single(prop, day_str, payload)
            except Exception as e:
                logger.error(f"Failed to process {prop} {day_str}: {e}")
            day += timedelta(days=1)
    logger.info("Bootstrap finished.")

# Run bootstrap for quick test: (set days_back=1 for first test)
# run_bootstrap(property_list=["BER"], days_back=1)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 10: Incremental run (to be scheduled daily)
def get_max_business_date(table_name):
    try:
        m = spark.table(table_name).agg({"business_date": "max"}).collect()[0][0]
        if m is None:
            return None
        if isinstance(m, str):
            return date.fromisoformat(m)
        return m
    except Exception:
        return None

def run_incremental(property_list=PROPERTY_IDS):
    if not spark.catalog.tableExists(FLAT_TABLE):
        logger.error("Flat table not found. Run bootstrap first.")
        return

    access_token = get_access_token()
    today = date.today()
    max_date = get_max_business_date(FLAT_TABLE)
    if max_date is None:
        # safe fallback: run bootstrap instead (or set from_date manually)
        logger.error("No max_date found in FLAT_TABLE. Run bootstrap first.")
        return

    from_date = max_date + timedelta(days=1)
    if from_date > today:
        logger.info("No new dates to process.")
        return

    for prop in property_list:
        logger.info(f"Incremental property: {prop}")
        day = from_date
        while day <= today:
            day_str = day.strftime("%Y-%m-%d")
            url = f"https://api.apaleo.com/reports/v1/reports/property-performance?propertyId={prop}&from={day_str}&to={day_str}&pageSize={PAGE_SIZE}"
            r = fetch_with_retries(url, headers={"Authorization": f"Bearer {access_token}"})
            if r.status_code == 401:
                access_token = get_access_token()
                r = fetch_with_retries(url, headers={"Authorization": f"Bearer {access_token}"})
            if r.status_code != 200:
                logger.warning(f"Skipping {prop} {day_str}: {r.status_code} {getattr(r,'text','')}")
                day += timedelta(days=1)
                continue
            payload = r.json()
            if not payload:
                logger.info(f"No data for {prop} {day_str}")
                day += timedelta(days=1)
                continue
            try:
                process_and_write_single(prop, day_str, payload)
            except Exception as e:
                logger.error(f"Failed to process {prop} {day_str}: {e}")
            day += timedelta(days=1)
    logger.info("Incremental finished.")

# To run incremental manually (for testing)
# run_incremental(property_list=["BER"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 11: Verification and quick checks

# 1) show partitions
print("Partitions (sample):")
display(spark.sql(f"SHOW PARTITIONS {FLAT_TABLE}").limit(50).toPandas())

# 2) sample rows for a date
sample_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
print(f"Sample rows for {sample_date}:")
display(spark.sql(f"SELECT * FROM {FLAT_TABLE} WHERE business_date = DATE '{sample_date}' LIMIT 20").toPandas())

# 3) check row counts per partition for duplicates (basic check)
display(spark.sql(f"""
SELECT property_id, business_date, COUNT(*) as cnt
FROM {FLAT_TABLE}
GROUP BY property_id, business_date
ORDER BY business_date DESC
LIMIT 50
""").toPandas())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
