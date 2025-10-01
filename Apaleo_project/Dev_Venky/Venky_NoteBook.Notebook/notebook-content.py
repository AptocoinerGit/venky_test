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

from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode_outer, col, max as spark_max, sha2, concat_ws, current_timestamp
from pyspark.sql.types import StructType, ArrayType
import requests, json

spark = SparkSession.builder.getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

client_id = "FFHG-SP-ALLES"
client_secret = "vjHKFWlh30LfeKonuChEuxbW3V70vU"
token_url = "https://identity.apaleo.com/connect/token"

def get_access_token():
    response = requests.post(
        token_url,
        data={"grant_type": "client_credentials"},
        auth=(client_id, client_secret)
    )
    if response.status_code != 200:
        raise Exception(f"Failed to get token: {response.text}")
    print("New access token issued.")
    return response.json()["access_token"]

# Generate initial token
access_token = get_access_token()
print(f"access_token {access_token}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

url = "https://api.apaleo.com/inventory/v1/properties?pageNumber=1&pageSize=50"
headers = {"Authorization": f"Bearer {access_token}"}

response = requests.get(url, headers=headers)
if response.status_code != 200:
    raise Exception(f"Failed to fetch properties: {response.text}")

props_json = response.json()
property_ids = [p["id"] for p in props_json.get("properties", [])]
print(f"Property IDs: {property_ids}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# 3. Flatten helper (schema-agnostic)
# -------------------
def flatten_df(df):
    while True:
        struct_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
        array_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType)]

        if not struct_cols and not array_cols:
            break

        for col_name in struct_cols:
            expanded = [col(f"{col_name}.{c}").alias(f"{col_name}_{c}") for c in df.select(f"{col_name}.*").columns]
            df = df.select("*", *expanded).drop(col_name)

        for col_name in array_cols:
            df = df.withColumn(col_name, explode_outer(col_name))

    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Incremental load logic (manual dates)
# -------------------
from_date = date(2025, 9, 20)   # <-- Manual start date
to_date = date.today()     # <-- Manual end date
table_name = "PerformanceTable111"

# Read existing max dates per property to avoid loading older data
try:
    max_dates_df = spark.table(table_name) \
                        .groupBy("property_id") \
                        .agg(spark_max("business_date").alias("max_date")) \
                        .collect()
    property_last_dates = {row["property_id"]: row["max_date"] for row in max_dates_df if row["max_date"] is not None}
except Exception:
    property_last_dates = {}

print("Last processed dates:", property_last_dates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. Loop property × day
# -------------------
for property_id in property_ids:
    last_date = property_last_dates.get(property_id)
    actual_from_date = max(from_date, date.fromisoformat(last_date) + timedelta(days=1)) if last_date else from_date

    day = actual_from_date
    while day <= to_date:
        day_str = day.strftime("%Y-%m-%d")

        url = f"https://api.apaleo.com/reports/v1/reports/property-performance?propertyId={property_id}&from={day_str}&to={day_str}&pageSize=500"
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(url, headers=headers)


        if response.status_code == 401:
            print(f"Token expired while fetching {property_id} on {day_str}, retrying...")
            access_token = get_access_token()
            headers = {"Authorization": f"Bearer {access_token}"}
            response = requests.get(url, headers=headers)


        if response.status_code != 200:
            print(f"Failed for {property_id} on {day_str}: {response.text}")
            day += timedelta(days=1)
            continue

        raw_json = response.json()

        # Flatten JSON dynamically
        df = spark.read.json(spark.sparkContext.parallelize([json.dumps(raw_json)]))
        df = flatten_df(df)

        # Add business keys
        df = df.withColumn("property_id", lit(property_id)) \
               .withColumn("business_date", lit(day_str)) \
               .withColumn("InsertDate", current_timestamp())

      # Generate row hash ONLY from stable columns
# -------------------
# Exclude volatile columns like InsertDate
        hash_cols = [c for c in df.columns if c not in ["InsertDate", "row_hash"]]

        df = df.withColumn("row_hash", sha2(concat_ws("||", *[col(c) for c in hash_cols]), 256))   

        # Filter only new rows
        try:
            existing_hashes_df = spark.table(table_name).select("row_hash")
            df_to_append = df.join(existing_hashes_df, on="row_hash", how="left_anti")
        except Exception:
            # Table may not exist yet
            df_to_append = df

        # Append to Silver
        if df_to_append.count() > 0:
            df_to_append.write.mode("append").saveAsTable(table_name)
            print(f"Saved {property_id} → {day_str} ({df_to_append.count()} new rows)")
        else:
            print(f"No new transactions for {property_id} → {day_str}")

        day += timedelta(days=1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
