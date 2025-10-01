# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bf243886-c544-49a3-be29-3518ea12fc80",
# META       "default_lakehouse_name": "GetDataAPI",
# META       "default_lakehouse_workspace_id": "1130aaf5-97e6-4499-8c32-46b2d32eb718",
# META       "known_lakehouses": [
# META         {
# META           "id": "bf243886-c544-49a3-be29-3518ea12fc80"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode_outer, col, max as spark_max,current_timestamp
from pyspark.sql.types import StructType, ArrayType
import requests, json, os

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
# META   "language_group": "synapse_pyspark"
# META }

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

# CELL ********************

from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode_outer, col
from pyspark.sql.types import StructType, ArrayType
import requests, json, os

# ---------------- Spark Session ----------------
spark = SparkSession.builder.getOrCreate()

# ---------------- Apaleo credentials ----------------
client_id = "FFHG-SP-ALLES"
client_secret = "vjHKFWlh30LfeKonuChEuxbW3V70vU"
token_url = "https://identity.apaleo.com/connect/token"

# ---------------- Get access token ----------------
token_response = requests.post(
    token_url,
    data={"grant_type": "client_credentials"},
    auth=(client_id, client_secret)
)
access_token = token_response.json()["access_token"]

# ---------------- Call Apaleo API ----------------
url = "https://api.apaleo.com/inventory/v1/properties?pageNumber=1&pageSize=50"
headers = {"Authorization": f"Bearer {access_token}"}

response = requests.get(url, headers=headers)
if response.status_code != 200:
    raise Exception(f"Failed to fetch properties: {response.text}")

props_json = response.json()

# ---------------- Flatten Function ----------------
def flatten_df(df):
    while True:
        struct_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
        array_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType)]

        if not struct_cols and not array_cols:
            break

        for col_name in struct_cols:
            expanded = [
                col(f"{col_name}.{c}").alias(f"{col_name}_{c}") 
                for c in df.select(f"{col_name}.*").columns
            ]
            df = df.select("*", *expanded).drop(col_name)

        for col_name in array_cols:
            df = df.withColumn(col_name, explode_outer(col_name))

    return df

# ---------------- Convert JSON to DataFrame ----------------
df = spark.read.json(spark.sparkContext.parallelize([json.dumps(props_json)]))

# Flatten the DataFrame
flat_df = flatten_df(df)

# ---------------- Show in Notebook ----------------
flat_df.show(truncate=False)

# ---------------- Save as Delta table in Fabric Lakehouse ----------------
flat_df.write.format("delta").mode("overwrite").save("/lakehouse/default/Files/flat_properties")

# (Optional) Save as Spark SQL Table
flat_df.write.mode("overwrite").saveAsTable("flat_properties_table")

print("✅ Data flattened and saved to Lakehouse & SQL Table successfully!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# CELL ********************

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
