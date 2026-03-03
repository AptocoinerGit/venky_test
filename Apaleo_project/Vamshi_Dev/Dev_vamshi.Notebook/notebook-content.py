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

# ****FINAL CODE INCREMENTAL LOAD AND WITH DYNAMIC DATE****

# CELL ********************

from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode_outer, col
from pyspark.sql.types import StructType, ArrayType
import requests, json, os

spark = SparkSession.builder.getOrCreate()

# -------------------
# 1. Apaleo credentials
# -------------------
client_id = "FFHG-SP-ALLES"
client_secret = "vjHKFWlh30LfeKonuChEuxbW3V70vU"
token_url = "https://identity.apaleo.com/connect/token"

token_response = requests.post(
    token_url,
    data={"grant_type": "client_credentials"},
    auth=(client_id, client_secret)
)
access_token = token_response.json()["access_token"]

# -------------------
# 2. Properties
# -------------------
property_ids = ["BER", "MUC", "LND", "PAR"]

# -------------------
# 3. Helper: flatten
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

# -------------------
# 4. Incremental date logic
# -------------------
today = date.today()

try:
    # Try to read max date from existing silver table
    max_date = spark.table("newsilverapaleo_property_performance") \
                    .agg({"business_date": "max"}).collect()[0][0]

    if max_date is None:
        from_date = today - timedelta(days=3)  # fallback start range
    else:
        from_date = (date.fromisoformat(max_date) + timedelta(days=1))

except Exception:
    # Table doesn't exist → full load
    from_date = today - timedelta(days=3)

to_date = today

print(f" Incremental Load from {from_date} to {to_date}")

# -------------------
# 5. Loop property × day
# -------------------
for property_id in property_ids:
    print(f"Fetching data for Property: {property_id}")

    day = from_date
    while day <= to_date:
        day_str = day.strftime("%Y-%m-%d")

        url = f"https://api.apaleo.com/reports/v1/reports/property-performance?propertyId={property_id}&from={day_str}&to={day_str}&pageSize=500"
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f" Failed for {property_id} on {day_str}: {response.text}")
            day += timedelta(days=1)
            continue

        raw_json = response.json()

       
        # Flatten
        df = spark.read.json(spark.sparkContext.parallelize([json.dumps(raw_json)]))
        df = flatten_df(df)
        df = df.withColumn("property_id", lit(property_id)) \
               .withColumn("business_date", lit(day_str))

        # Append to Silver table
        df.write.mode("append").saveAsTable("newsilverapaleo_property_performance")

        print(f" Saved {property_id} → {day_str}")
        day += timedelta(days=1)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **This code loads data incrementally and testing is done.**

# CELL ********************

from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode_outer, col, max as spark_max
from pyspark.sql.types import StructType, ArrayType
import requests, json, os

spark = SparkSession.builder.getOrCreate()

# -------------------
# 1. Apaleo credentials
# -------------------
client_id = "FFHG-SP-ALLES"
client_secret = "vjHKFWlh30LfeKonuChEuxbW3V70vU"
token_url = "https://identity.apaleo.com/connect/token"

token_response = requests.post(
    token_url,
    data={"grant_type": "client_credentials"},
    auth=(client_id, client_secret)
)
access_token = token_response.json()["access_token"]

# -------------------
# 2. Properties
# -------------------
property_ids = ["BER", "MUC", "LND", "PAR"] #, "VIE"

# -------------------
# 3. Helper: flatten
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

# -------------------
# 4. Incremental load logic (per property)
# -------------------
today = date.today()   # date(2025,9,21)           

# Try to read existing max dates per property_id
try:
    max_dates_df = spark.table("newsilverapaleo_property_performance") \
                        .groupBy("property_id") \
                        .agg(spark_max("business_date").alias("max_date")) \
                        .collect()

    # Convert to dict: {property_id: max_date}
    property_last_dates = {row["property_id"]: row["max_date"] for row in max_dates_df if row["max_date"] is not None}

except Exception:
    property_last_dates = {}

print("Last processed dates:", property_last_dates)
##
# -------------------
# 5. Loop property × day
# -------------------
for property_id in property_ids:
    last_date = property_last_dates.get(property_id)

    if last_date:
        from_date = date.fromisoformat(last_date) + timedelta(days=1)
    else:
        from_date = today - timedelta(days=5)   # fallback if new property

    to_date = today
    print(f"Property {property_id}: Loading from {from_date} to {to_date}")

    day = from_date
    while day <= to_date:
        day_str = day.strftime("%Y-%m-%d")

        url = f"https://api.apaleo.com/reports/v1/reports/property-performance?propertyId={property_id}&from={day_str}&to={day_str}&pageSize=500"
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Failed for {property_id} on {day_str}: {response.text}")
            day += timedelta(days=1)
            continue

        raw_json = response.json()

        # Flatten JSON → DF
        df = spark.read.json(spark.sparkContext.parallelize([json.dumps(raw_json)]))
        df = flatten_df(df)
        df = df.withColumn("property_id", lit(property_id)) \
               .withColumn("business_date", lit(day_str))

        # Deduplicate just in case
        df = df.dropDuplicates(["property_id", "business_date"])

        # Append to Silver
        df.write.mode("append").saveAsTable("newsilverapaleo_property_performance")

        print(f"Saved {property_id} → {day_str}")
        day += timedelta(days=1)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Final Code From LakeHouse Property_table Getting Dynamic Property ids**

# CELL ********************

from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode_outer, col, max as spark_max
from pyspark.sql.types import StructType, ArrayType
import requests, json, os

spark = SparkSession.builder.getOrCreate()

# -------------------
# 1. Apaleo credentials
# -------------------
client_id = "FFHG-SP-ALLES"
client_secret = "vjHKFWlh30LfeKonuChEuxbW3V70vU"
token_url = "https://identity.apaleo.com/connect/token"

token_response = requests.post(
    token_url,
    data={"grant_type": "client_credentials"},
    auth=(client_id, client_secret)
)
access_token = token_response.json()["access_token"]

# -------------------
# 2. Properties
# -------------------


# Apaleo Properties API endpoint
url = "https://api.apaleo.com/inventory/v1/properties?pageNumber=1&pageSize=50"
headers = {"Authorization": f"Bearer {access_token}"}

response = requests.get(url, headers=headers)

if response.status_code != 200:
    print(f"Failed to fetch properties: {response.text}")
else:
    props_json = response.json()
    print(f"Properties fetched from API")

property_ids = [p["id"] for p in props_json.get("properties", [])]
print(f"Property IDs: {property_ids}")                                                                                                     

# -------------------
# 3. Helper: flatten
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

# -------------------
# 4. Incremental load logic (per property)
# -------------------
today = date.today()   # date(2025,9,21)           

# Try to read existing max dates per property_id
try:
    max_dates_df = spark.table("newsilverapaleo_property_performance") \
                        .groupBy("property_id") \
                        .agg(spark_max("business_date").alias("max_date")) \
                        .collect()

    # Convert to dict: {property_id: max_date}
    property_last_dates = {row["property_id"]: row["max_date"] for row in max_dates_df if row["max_date"] is not None}

except Exception:
    property_last_dates = {}

print("Last processed dates:", property_last_dates)
##
# -------------------
# 5. Loop property × day
# -------------------
for property_id in property_ids:
    last_date = property_last_dates.get(property_id)

    if last_date:
        from_date = date.fromisoformat(last_date) + timedelta(days=1)
    else:
        from_date = today - timedelta(days=365)   # fallback if new property

    to_date = today
    print(f"Property {property_id}: Loading from {from_date} to {to_date}")

    day = from_date
    while day <= to_date:
        day_str = day.strftime("%Y-%m-%d")

        url = f"https://api.apaleo.com/reports/v1/reports/property-performance?propertyId={property_id}&from={day_str}&to={day_str}&pageSize=500"
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Failed for {property_id} on {day_str}: {response.text}")
            day += timedelta(days=1)
            continue

        raw_json = response.json()

        # Flatten JSON → DF
        df = spark.read.json(spark.sparkContext.parallelize([json.dumps(raw_json)]))
        df = flatten_df(df)
        df = df.withColumn("property_id", lit(property_id)) \
               .withColumn("business_date", lit(day_str))

        # Deduplicate just in case
        df = df.dropDuplicates(["property_id", "business_date"])

        # Append to Silver
        df.write.mode("append").saveAsTable("newsilverapaleo_property_performance")

        print(f"Saved {property_id} → {day_str}")
        day += timedelta(days=1)


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

from pyspark.sql import SparkSession

# Spark session
spark = SparkSession.builder.getOrCreate()

# Read properties table from Lakehouse
properties_df = spark.read.table("silver_properties")   # replace with your actual table name

# Collect IDs into a Python list
property_ids = [row["id"] for row in properties_df.select("id").collect()]

print("🏨 Found properties from Lakehouse:",property_ids )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# updated consolidated auto


# CELL ********************

# MAGIC %%sql
# MAGIC TRUNCATE TABLE newsilverapaleo_property_performance

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode_outer, col, max as spark_max
from pyspark.sql.types import StructType, ArrayType
import requests, json, os

spark = SparkSession.builder.getOrCreate()

# -------------------
# 1. Apaleo credentials
# -------------------
client_id = "FFHG-SP-ALLES"
client_secret = "vjHKFWlh30LfeKonuChEuxbW3V70vU"
token_url = "https://identity.apaleo.com/connect/token"

# -------------------
# 2. Helper: Token management
# -------------------
def get_access_token():
    """Fetch new access token from Apaleo"""
    response = requests.post(
        token_url,
        data={"grant_type": "client_credentials"},
        auth=(client_id, client_secret)
    )
    response.raise_for_status()
    return response.json()["access_token"]

def call_api_with_retry(url, headers):
    """Call API, refresh token if expired"""
    response = requests.get(url, headers=headers)

    if response.status_code == 401:  # token expired
        print("⚠️ Token expired. Refreshing...")
        new_token = get_access_token()
        headers["Authorization"] = f"Bearer {new_token}"
        response = requests.get(url, headers=headers)

    return response, headers



# -------------------
# 3. Fetch property IDs dynamically
# -------------------
props_url = "https://api.apaleo.com/inventory/v1/properties?pageNumber=1&pageSize=50"
headers = {"Authorization": f"Bearer {access_token}"}

response, headers = call_api_with_retry(props_url, headers)
if response.status_code != 200:
    raise Exception(f"Failed to fetch properties: {response.text}")

props_json = response.json()
property_ids = [p["id"] for p in props_json.get("properties", [])]
print(f"Property IDs: {property_ids}")

# -------------------
# 4. Helper: flatten JSON
# -------------------
def flatten_df(df):
    """Recursively flatten structs and arrays"""
    while True:
        struct_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
        array_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType)]

        if not struct_cols and not array_cols:
            break

        for col_name in struct_cols:
            expanded = [col(f"{col_name}.{c}").alias(f"{col_name}_{c}") 
                        for c in df.select(f"{col_name}.*").columns]
            df = df.select("*", *expanded).drop(col_name)

        for col_name in array_cols:
            df = df.withColumn(col_name, explode_outer(col_name))

    return df

# -------------------
# 5. Incremental load logic
# -------------------
today = date.today()

try:
    max_dates_df = spark.table("newsilverapaleo_property_performance") \
                        .groupBy("property_id") \
                        .agg(spark_max("business_date").alias("max_date")) \
                        .collect()
    property_last_dates = {row["property_id"]: row["max_date"] 
                           for row in max_dates_df if row["max_date"] is not None}
except Exception:
    property_last_dates = {}

print("Last processed dates:", property_last_dates)

# -------------------
# 6. Loop property × day
# -------------------
for property_id in property_ids:
    last_date = property_last_dates.get(property_id)
    
    if last_date:
        from_date = date.fromisoformat(last_date) + timedelta(days=1)
    else:
        from_date = today - timedelta(days=365)   # fallback if new property

    to_date = today
    print(f"Property {property_id}: Loading from {from_date} to {to_date}")

    day = from_date
    while day <= to_date:
        day_str = day.strftime("%Y-%m-%d")

        api_url = f"https://api.apaleo.com/reports/v1/reports/property-performance?propertyId={property_id}&from={day_str}&to={day_str}&pageSize=500"
        headers = {"Authorization": f"Bearer {access_token}"}

        response, headers = call_api_with_retry(api_url, headers)
        if response.status_code != 200:
            print(f"❌ Failed for {property_id} on {day_str}: {response.text}")
            day += timedelta(days=1)
         


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# consolidated v

# CELL ********************

from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode_outer, col, max as spark_max
from pyspark.sql.types import StructType, ArrayType
import requests, json, os,time


spark = SparkSession.builder.getOrCreate()

# -------------------
# 1. Apaleo credentials
# -------------------
client_id = "FFHG-SP-ALLES"
client_secret = "vjHKFWlh30LfeKonuChEuxbW3V70vU"
token_url = "https://identity.apaleo.com/connect/token"

# Function to fetch a new token
def fetch_token():
    response = requests.post(
        token_url,
        data={"grant_type": "client_credentials"},
        auth=(client_id, client_secret)
    )
    if response.status_code != 200:
        raise Exception(f"Token request failed: {response.text}")
    return response.json()

# When requesting token
token_data = fetch_token()
access_token = token_data["access_token"]
expiry_time = time.time() + token_data["expires_in"] - 60  # refresh 1 min before expiry

def get_valid_token():
    global access_token, expiry_time, token_data
    if time.time() >= expiry_time:
        print("Token expired. Fetching new token...")
        token_data = fetch_token()
        access_token = token_data["access_token"]
        expiry_time = time.time() + token_data["expires_in"] - 60
    return access_token


access_token = get_valid_token()
# -------------------
# 2. Properties
# -------------------


# Apaleo Properties API endpoint
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

# -------------------
# 3. Helper: flatten
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

# -------------------
# 4. Incremental load logic (per property)
# -------------------
today = date.today()   # date(2025,9,21)           

# Try to read existing max dates per property_id
try:
    max_dates_df = spark.table("newsilverapaleo_property_performance") \
                        .groupBy("PropertyId") \
                        .agg(spark_max("BusinessDate").alias("max_date")) \
                        .collect()

    # Convert to dict: {property_id: max_date}
    property_last_dates = {row["PropertyId"]: row["max_date"] for row in max_dates_df if row["max_date"] is not None}

except Exception:
    property_last_dates = {}

print("Last processed dates:", property_last_dates)
##
# -------------------
# 5. Loop property × day
# -------------------
for PropertyId in PropertyId:
    last_date = property_last_dates.get(PropertyId)
    if last_date:
        from_date = date.fromisoformat(last_date) + timedelta(days=1)
    else:
        from_date = today - timedelta(days=365)   # fallback if new property
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
        # Flatten JSON → DF
        df = spark.read.json(spark.sparkContext.parallelize([json.dumps(raw_json)]))
        df = flatten_df(df)
        df = df.withColumn("PropertyId", lit(PropertyId)) \
               .withColumn("BusinessDate", lit(day_str))
        # Deduplicate just in case
        df = df.dropDuplicates(["PropertyId", "BusinessDate"])
        # Append to Silver
        df.write.mode("append").saveAsTable("newsilverapaleo_property_performance")
        print(f"Saved {PropertyId} → {day_str}")
        day += timedelta(days=1)

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

# CELL ********************

df = spark.table("newsilverapaleo_property_performance") \
          .withColumnRenamed("property_id", "PropertyId") \
          .withColumnRenamed("business_date", "BusinessDate")

df.write.mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable("newsilverapaleo_property_performance")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
