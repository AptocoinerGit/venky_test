# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5b18df32-d724-45d6-8818-c07c3e258af9",
# META       "default_lakehouse_name": "Bronze_Layer",
# META       "default_lakehouse_workspace_id": "1130aaf5-97e6-4499-8c32-46b2d32eb718",
# META       "known_lakehouses": [
# META         {
# META           "id": "5b18df32-d724-45d6-8818-c07c3e258af9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
import json
from pyspark.sql import SparkSession

# 1. Spark session
spark = SparkSession.builder.getOrCreate()

# 2. Apaleo credentials
client_id = "FFHG-SP-ALLES"
client_secret = "vjHKFWlh30LfeKonuChEuxbW3V70vU"
token_url = "https://identity.apaleo.com/connect/token"

# 3. Get access token
token_response = requests.post(
    token_url,
    data={"grant_type": "client_credentials"},
    auth=(client_id, client_secret)
)

if token_response.status_code != 200:
    raise Exception(f"Token request failed: {token_response.text}")

access_token = token_response.json()["access_token"]

# 4. Call Properties API
url = "https://api.apaleo.com/inventory/v1/properties?pageNumber=1&pageSize=50"
headers = {"Authorization": f"Bearer {access_token}"}

response = requests.get(url, headers=headers)

if response.status_code != 200:
    raise Exception(f"API request failed: {response.text}")

data = response.json()

# 5. Extract property list
properties = data.get("properties", [])

# 6. Convert to Spark DataFrame
df = spark.createDataFrame(properties)

# 7. Save into Lakehouse "Bronze_Layer"
df.write.mode("overwrite").saveAsTable("Bronze_Layer.Bronze_Properties")

print(" Data saved into Bronze_Layer table: Properties")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
from pyspark.sql import SparkSession

# 1. Spark session
spark = SparkSession.builder.getOrCreate()

# 2. Apaleo credentials
client_id = "FFHG-SP-ALLES"
client_secret = "vjHKFWlh30LfeKonuChEuxbW3V70vU"
token_url = "https://identity.apaleo.com/connect/token"

# 3. Get access token
token_response = requests.post(
    token_url,
    data={"grant_type": "client_credentials"},
    auth=(client_id, client_secret)
)

if token_response.status_code != 200:
    raise Exception(f"Token request failed: {token_response.text}")

access_token = token_response.json()["access_token"]

# 4. Call Properties API
url = "https://api.apaleo.com/booking/v1/reservations?pageNumber=1&pageSize=500"
headers = {"Authorization": f"Bearer {access_token}"}

response = requests.get(url, headers=headers)

if response.status_code != 200:
    raise Exception(f"API request failed: {response.text}")

data = response.json()

# 5. Extract reservations list
reservations = data.get("reservations", [])

# 6. Convert to Spark DataFrame
df = spark.createDataFrame(reservations)

# 7. Save into Lakehouse "Bronze_Layer"
df.write.mode("overwrite").saveAsTable("Bronze_Layer.Bronze_Reservations")

print(" Data saved into Bronze_Layer table: Reservations")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
from pyspark.sql import SparkSession

# 1. Spark session
spark = SparkSession.builder.getOrCreate()

# 2. Apaleo credentials
client_id = "FFHG-SP-ALLES"
client_secret = "vjHKFWlh30LfeKonuChEuxbW3V70vU"
token_url = "https://identity.apaleo.com/connect/token"

# 3. Get access token
token_response = requests.post(
    token_url,
    data={"grant_type": "client_credentials"},
    auth=(client_id, client_secret)
)

if token_response.status_code != 200:
    raise Exception(f"Token request failed: {token_response.text}")

access_token = token_response.json()["access_token"]

# 4. Call Folios API (Finance)
url = "https://api.apaleo.com/finance/v1/folios?pageNumber=1&pageSize=500"
headers = {"Authorization": f"Bearer {access_token}"}

response = requests.get(url, headers=headers)

if response.status_code != 200:
    raise Exception(f"API request failed: {response.text}")

data = response.json()

# 5. Extract folios list
folios = data.get("folios", [])

# 6. Convert to Spark DataFrame
if folios:
    df = spark.createDataFrame(folios)

    # 7. Save into Lakehouse "Bronze_Layer"
    df.write.mode("overwrite").saveAsTable("Bronze_Layer.Bronze_Folios")

    print(" Data saved into Bronze_Layer table: Folios")
else:
    print(" No folios found in API response")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
from pyspark.sql import SparkSession

# 1. Spark session
spark = SparkSession.builder.getOrCreate()

# 2. Apaleo credentials
client_id = "FFHG-SP-ALLES"
client_secret = "vjHKFWlh30LfeKonuChEuxbW3V70vU"
token_url = "https://identity.apaleo.com/connect/token"

# 3. Get access token
token_response = requests.post(
    token_url,
    data={"grant_type": "client_credentials"},
    auth=(client_id, client_secret)
)
access_token = token_response.json()["access_token"]

# 4. Call Reservations API for a single property
property_id = "BER"  # <--- put the propertyId here
url = f"https://api.apaleo.com/booking/v1/reservations?propertyId={property_id}&pageNumber=1&pageSize=500"
headers = {"Authorization": f"Bearer {access_token}"}

response = requests.get(url, headers=headers)
data = response.json()

# 5. Extract reservations list
reservations = data.get("reservations", [])

# 6. Convert to Spark DataFrame
df = spark.createDataFrame(reservations)

# 7. Save into Lakehouse
df.write.mode("overwrite").saveAsTable(f"Bronze_Layer.Reservations_{property_id}")

print(f"Reservations for property {property_id} saved into Bronze_Layer table")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze_Layer.reservations")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import * 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze_Layer.Silver_Reservations")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType, MapType

def fully_flatten(df):
    """
    Recursively flattens a DataFrame:
    - StructType → creates separate columns
    - ArrayType of StructType → explodes and flattens
    - MapType → creates separate columns for each key
    """
    while True:
        struct_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
        array_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType) and isinstance(f.dataType.elementType, StructType)]
        map_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, MapType)]

        if not struct_cols and not array_cols and not map_cols:
            break

        # Flatten Structs
        for col in struct_cols:
            nested_cols = [F.col(f"{col}.{c.name}").alias(f"{col}_{c.name}") for c in df.schema[col].dataType.fields]
            df = df.select("*", *nested_cols).drop(col)

        # Explode Arrays of Structs
        for col in array_cols:
            df = df.withColumn(col, F.explode_outer(F.col(col)))

        # Flatten Maps
        for col in map_cols:
            # Get all keys in the map dynamically from first row
            keys = df.select(F.explode(F.map_keys(F.col(col)))).distinct().rdd.flatMap(lambda x: x).collect()
            for key in keys:
                # Create a column for each key
                df = df.withColumn(f"{col}_{key}", F.col(col)[key])
            df = df.drop(col)  # drop original map column

    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Assume df is your raw DataFrame from Apaleo
df_flat = fully_flatten(df)

# Save into Silver layer
df_flat.write.mode("overwrite").saveAsTable("Bronze_Layer.Silver_Reservations")
print("✅ Fully flattened data saved into Bronze_Layer.Reservations")


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
