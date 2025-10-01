# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2b38ac85-fd87-4eb9-9968-260ea66ffe84",
# META       "default_lakehouse_name": "Apaleo_RT_LH",
# META       "default_lakehouse_workspace_id": "1130aaf5-97e6-4499-8c32-46b2d32eb718",
# META       "known_lakehouses": [
# META         {
# META           "id": "2b38ac85-fd87-4eb9-9968-260ea66ffe84"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#-------------------Importing Packages

from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode_outer, col, max as spark_max,current_timestamp
from pyspark.sql.types import StructType, ArrayType
import requests, json, os
from pyspark.sql import DataFrame

spark = SparkSession.builder.getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------- Apaleo credentials  
#-----------------Accessing API with ClientId and ClientSecret

client_id = "FFHG-SP-ALLES"
client_secret = "vjHKFWlh30LfeKonuChEuxbW3V70vU"
token_url = "https://identity.apaleo.com/connect/token"

#----------------------- Get access token & Reading Access token 
token_response = requests.post(
    token_url,
    data={"grant_type": "client_credentials"},
    auth=(client_id, client_secret)
)
access_token = token_response.json()["access_token"]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(access_token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def flatten_df(df: DataFrame) -> DataFrame:
    """Recursively flattens StructType and ArrayType columns"""
    if df is None or df.rdd.isEmpty():
        return df

    while True:
        struct_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
        array_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType)]
        if not struct_cols and not array_cols:
            break

        # Flatten struct columns
        for col_name in struct_cols:
            expanded = [col(f"{col_name}.{c}").alias(f"{col_name}_{c}") for c in df.select(f"{col_name}.*").columns]
            df = df.select("*", *expanded).drop(col_name)

        # Explode array columns
        for col_name in array_cols:
            df = df.withColumn(col_name, explode_outer(col(col_name)))

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_property_ids(access_token):
    property_ids = []
    page = 1
    while True:
        url = f"https://api.apaleo.com/inventory/v1/properties?pageNumber={page}&pageSize=50"
        headers = {"Authorization": f"Bearer {access_token}"}
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            print(f"Failed to fetch properties on page {page}: {resp.text}")
            break
        data = resp.json().get("properties", [])
        if not data:
            break
        property_ids.extend([p["id"] for p in data])
        if len(data) < 50:
            break
        page += 1
    return property_ids

property_ids = get_property_ids(access_token)
print(f"Property IDs: {property_ids}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    max_dates_df = spark.table(table_name) \
                        .groupBy("property_id") \
                        .agg({"business_date": "max"}) \
                        .collect()
    property_last_dates = {row["property_id"]: row["max(business_date)"] 
                           for row in max_dates_df if row["max(business_date)"] is not None}
except Exception:
    property_last_dates = {}

print("Last processed dates:", property_last_dates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------- Config ----------
table_name = "property_performance"
start_date = date(2025, 9, 1)
today = date.today()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------- Incremental loading ----------

for property_id in property_ids:
    last_date = property_last_dates.get(property_id)
    
    if last_date:
        from_date = date.fromisoformat(last_date) + timedelta(days=1)
    else:
        from_date = start_date   

    to_date = today
    print(f"Property {property_id}: Loading from {from_date} to {to_date}")

    day = from_date
    while day <= to_date:
        day_str = day.strftime("%Y-%m-%d")

        url = f"https://api.apaleo.com/reports/v1/reports/property-performance?propertyId={property_id}&from={day_str}&to={day_str}&pageSize=500"
        headers = {"Authorization": f"Bearer {access_token}"}

        success = False

        for attempt in range(1, 3):  # max 2 attempts
            response = requests.get(url, headers=headers)

            if response.status_code == 401:
                print(f"Token expired while fetching {property_id} on {day_str}, refreshing token...")
                access_token = get_access_token()
                headers = {"Authorization": f"Bearer {access_token}"}
                print(f"Retrying attempt {attempt} for {property_id} on {day_str}...")
                continue  # retry with refreshed token

            if response.status_code == 200:
                success = True
                break  # exit retry loop on success

            print(f"Attempt {attempt} failed for {property_id} on {day_str}: {response.status_code}")

        if not success:
            print(f"Skipping {property_id} on {day_str} after 2 failed attempts")
            day += timedelta(days=1)
            continue

        raw_json = response.json()
        df = spark.read.json(spark.sparkContext.parallelize([json.dumps(raw_json)]))
        df = flatten_df(df)
        df = df.withColumn("property_id", lit(property_id)) \
               .withColumn("business_date", lit(day_str)) \
               .withColumn("InsertDate", current_timestamp())

        df = df.dropDuplicates(["property_id", "business_date"])
        df.write.mode("append").saveAsTable(table_name)

        print(f"Saved {property_id} → {day_str}")
        day += timedelta(days=1)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
