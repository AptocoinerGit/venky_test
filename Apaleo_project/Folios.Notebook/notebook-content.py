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

# 4. Call Properties API
url = "https://api.apaleo.com/finance/v1/folios?pageNumber=1&pageSize=500"
headers = {"Authorization": f"Bearer {access_token}"}

response = requests.get(url, headers=headers)

if response.status_code != 200:
    raise Exception(f"API request failed: {response.text}")

# 5. Get raw API response
raw_json = response.json()   # parse JSON
formatted_json = json.dumps(raw_json, indent=4)  # pretty-print (multiline)

# 6. Define Lakehouse Files path
output_path = "/lakehouse/default/Files/Bronze_Layer/Apaleo/folios/folios.json"

# 7. Ensure directory exists
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# 8. Save as multiline JSON
with open(output_path, "w", encoding="utf-8") as f:
    f.write(formatted_json)

print(f"✅ Raw multiline JSON saved to Lakehouse Files: {output_path}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Path to your saved Bronze JSON
input_path = "Files/Bronze_Layer/Apaleo/folios/folios.json"

# Read multiline JSON
df_properties = spark.read.option("multiLine", True).json(input_path)


df_properties.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_exploded = df_properties.withColumn("folio", F.explode("folios"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

def flatten_structs(df: DataFrame) -> DataFrame:
    flat_cols = []
    nested_cols = []
    
    for field in df.schema.fields:
        col_name = field.name
        if field.dataType.simpleString().startswith("struct"):
            # Add nested struct for flattening later
            nested_cols.append(col_name)
        else:
            flat_cols.append(F.col(col_name))
    
    if not nested_cols:
        return df
    
    # Explode nested structs with alias to avoid ambiguity
    select_exprs = flat_cols.copy()
    for nc in nested_cols:
        for nested_field in df.select(f"{nc}.*").columns:
            select_exprs.append(F.col(f"{nc}.{nested_field}").alias(f"{nc}_{nested_field}"))
    
    return df.select(select_exprs)

# --- Apply recursively until fully flattened ---
df_flat = df_exploded.select("folio.*")
while any(f.dataType.simpleString().startswith("struct") for f in df_flat.schema.fields):
    df_flat = flatten_structs(df_flat)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_flat.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("DataLH.Silver_folios")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
