# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0f7481a9-b5d9-4295-8468-5f47bed1ff01",
# META       "default_lakehouse_name": "DataLH",
# META       "default_lakehouse_workspace_id": "1130aaf5-97e6-4499-8c32-46b2d32eb718",
# META       "known_lakehouses": [
# META         {
# META           "id": "0f7481a9-b5d9-4295-8468-5f47bed1ff01"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Initialize Spark Session and Import Essential Libraries**

# CELL ********************

from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode_outer, col, max as spark_max
from pyspark.sql.types import StructType, ArrayType
import requests, json, os,time

spark = SparkSession.builder.getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Get Secure Access Token for Apaleo API**

# CELL ********************

client_id = "FFHG-SP-ALLES"
client_secret = "vjHKFWlh30LfeKonuChEuxbW3V70vU"
token_url = "https://identity.apaleo.com/connect/token"

def fetch_token():
    response = requests.post(
        token_url,
        data={"grant_type": "client_credentials"},
        auth=(client_id, client_secret)
    )
    if response.status_code != 200:
        raise Exception(f"Token request failed: {response.text}")
    return response.json()


token_data = fetch_token()
access_token = token_data["access_token"]
expiry_time = time.time() + token_data["expires_in"]

def get_valid_token():
    global access_token, expiry_time, token_data
    if time.time() >= expiry_time:
        print("Token expired. Fetching new token...")
        token_data = fetch_token()
        access_token = token_data["access_token"]
        expiry_time = time.time() + token_data["expires_in"] - 60
    return access_token

access_token = get_valid_token()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Fetch the Latest Property IDs from Apaleo API**

# CELL ********************

url = "https://api.apaleo.com/inventory/v1/properties?pageNumber=1&pageSize=50"
headers = {"Authorization": f"Bearer {access_token}"}

response = requests.get(url, headers=headers)

if response.status_code != 200:
    print(f"Failed to fetch properties: {response.text}")
else:
    props_json = response.json()
    print(f"Properties fetched from API")

PropertyId = [p["id"] for p in props_json.get("properties", [])]
print(f"Property IDs: {PropertyId}")                                                                                                     


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Flatten Nested JSON Data in Spark DataFrame**

# CELL ********************

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

# MARKDOWN ********************

# **Determine Data Load Start Date Using Incremental
# Watermarking**

# CELL ********************

today = date.today()      

try:
    max_dates_df = spark.table("newsilverapaleo_property_performance") \
                        .groupBy("PropertyId") \
                        .agg(spark_max("BusinessDate").alias("max_date")) \
                        .collect()


    property_last_dates = {row["PropertyId"]: row["max_date"] for row in max_dates_df if row["max_date"] is not None}

except Exception:
    property_last_dates = {}

print("Last processed dates:", property_last_dates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Iterate Over Properties and Dates to Fetch and Store Daily
# Performance Data**

# CELL ********************

for PropertyId in PropertyId:
    last_date = property_last_dates.get(PropertyId)

    if last_date:
        from_date = date.fromisoformat(last_date) + timedelta(days=1)
    else:
        from_date = today - timedelta(days=365)  

    to_date = today
    print(f"Property {PropertyId}: Loading from {from_date} to {to_date}")

    day = from_date
    while day <= to_date:
        day_str = day.strftime("%Y-%m-%d")

        url = f"https://api.apaleo.com/reports/v1/reports/property-performance?propertyId={PropertyId}&from={day_str}&to={day_str}&pageSize=500"
       
        headers = {"Authorization": f"Bearer {get_valid_token()}"}
        response = requests.get(url, headers=headers)


        if response.status_code != 200:
            print(f"Failed for {PropertyId} on {day_str}: {response.text}")
            day += timedelta(days=1)
            continue

        raw_json = response.json()

        df = spark.read.json(spark.sparkContext.parallelize([json.dumps(raw_json)]))
        df = flatten_df(df)
        df = df.withColumn("PropertyId", lit(PropertyId)) \
               .withColumn("BusinessDate", lit(day_str))

        df = df.dropDuplicates(["PropertyId", "BusinessDate"])

        df.write.mode("append").saveAsTable("newsilverapaleo_property_performance")

        print(f"Saved {PropertyId} → {day_str}")
        day += timedelta(days=1)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
