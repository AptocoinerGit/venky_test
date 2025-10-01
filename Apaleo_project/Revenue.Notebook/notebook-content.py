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

# CELL ********************


import requests
import json
import os
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

# ✅ 4. List of property IDs
property_ids = ["BER", "MUC", "LND", "PAR", "VIE"]   

# 5. Loop through each property
for property_id in property_ids:
    print(f"🔄 Fetching data for Property: {property_id}")

    # API URL with dynamic propertyId
    url = f"https://api.apaleo.com/reports/v1/reports/revenues?propertyId={property_id}&from=2025-01-01&to=2025-09-19&pageSize=500"
    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"❌ Failed for {property_id}: {response.text}")
        continue

    # 6. Get raw API response
    raw_json = response.json()
    formatted_json = json.dumps(raw_json, indent=4)

    # 7. Define Lakehouse path for each property
    output_path = f"/lakehouse/default/Files/Bronze_Layer/Apaleo/revenues/{property_id}_revenues.json"

    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Save as multiline JSON
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(formatted_json)

    print(f"✅ Saved {property_id} data to: {output_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.getOrCreate()

property_ids = ["BER", "MUC", "LND", "PAR", "VIE"]

dfs = []
for pid in property_ids:
    path = f"Files/Bronze_Layer/Apaleo/revenues/{pid}_revenues.json"
    
    # Read JSON and add a PropertyId column
    df = (
        spark.read.option("multiLine", True).json(path)
        .withColumn("PropertyId", lit(pid))
    )
    
    dfs.append(df)

# ✅ Consolidate all into one DataFrame
df_all = dfs[0]
for df in dfs[1:]:
    df_all = df_all.unionByName(df, allowMissingColumns=True)

df_all.printSchema()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, ArrayType

def flatten_df(df: DataFrame) -> DataFrame:
    """ Recursively flatten structs and explode arrays """
    while True:
        # Find all struct and array columns
        struct_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
        array_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType)]

        if not struct_cols and not array_cols:
            break

        # Flatten struct columns
        for col_name in struct_cols:
            expanded = [F.col(f"{col_name}.{c}").alias(f"{col_name}_{c}") for c in df.select(f"{col_name}.*").columns]
            df = df.select("*", *expanded).drop(col_name)

        # Explode array columns
        for col_name in array_cols:
            df = df.withColumn(col_name, F.explode_outer(col_name))

    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_flat = flatten_df(df_all)

df_flat.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_flat.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("DataLH.Silver_Revenues")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
