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
# 1. Import required libraries
# ----------------------------------------------------
import requests
import json
import os
from pyspark.sql import SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

# ----------------------------------------------------
# 2. Start Spark session (needed for Fabric notebooks)
# ----------------------------------------------------
spark = SparkSession.builder.getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

# ----------------------------------------------------
# 3. Apaleo API credentials (used to get access token)
# ----------------------------------------------------
client_id = "FFHG-SP-ALLES"
client_secret = "vjHKFWlh30LfeKonuChEuxbW3V70vU"
token_url = "https://identity.apaleo.com/connect/token"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

# ----------------------------------------------------
# 4. Request an access token from Apaleo
#    (token is like a temporary password)
# ----------------------------------------------------
token_response = requests.post(
    token_url,
    data={"grant_type": "client_credentials"},
    auth=(client_id, client_secret)
)

if token_response.status_code != 200:
    raise Exception(f"Token request failed: {token_response.text}")

# Extract access token from response
access_token = token_response.json()["access_token"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

#Save files in the JSON 

import requests
import json
import os
from datetime import datetime, timedelta

# List of property IDs
property_ids = ["BER", "MUC", "LND", "PAR", "VIE"]   

# Your access token
access_token = access_token  # already obtained

# Define start and end dates
start_date = datetime(2020, 1, 1)
end_date = datetime.today()

# Outer loop: property_id
for property_id in property_ids:
    print(f"Fetching data for Property: {property_id}")
    
    # Inner loop: date range
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        
        # Build API URL for this property and date
        url = f"https://api.apaleo.com/reports/v1/reports/property-performance?propertyId={property_id}&from={date_str}&to={date_str}&pageSize=500"
        
        # Add authorization header
        headers = {"Authorization": f"Bearer {access_token}"}
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            # Parse JSON
            data = response.json()
            formatted_json = json.dumps(data, indent=4)
            
            # Define file path (one file per property per date)
            output_path = f"/lakehouse/default/Files/Bronze_Layer/Apaleo/property-performance/{property_id}_{date_str}_property-performance.json"
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Save JSON to file
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(formatted_json)
            
            print(f"Saved data for Property {property_id} on {date_str} to: {output_path}")
        else:
            print(f"Failed to fetch data for Property {property_id} on {date_str}: {response.text}")
        
        current_date += timedelta(days=1)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# ----------------------------------------------------
# 5. Load Data into DataFrame. 
# ----------------------------------------------------
import requests
import json
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import Row

# --- Spark session ---
spark = SparkSession.builder.getOrCreate()

# --- Inputs ---
property_ids = ["BER", "MUC", "LND", "PAR", "VIE"]
access_token = access_token   # already obtained earlier

# Define start and end dates
start_date = datetime(2024, 3, 19)
end_date = datetime.today()

# --- Collect rows from API ---
rows = []

current_date = start_date
while current_date <= end_date:
    date_str = current_date.strftime("%Y-%m-%d")
    for pid in property_ids:
        print(f"Fetching {pid} for {date_str}")

        url = (
            "https://api.apaleo.com/reports/v1/reports/property-performance"
            f"?propertyId={pid}&from={date_str}&to={date_str}&pageSize=500"
        )
        headers = {"Authorization": f"Bearer {access_token}"}
        resp = requests.get(url, headers=headers)

        if resp.status_code == 200:
            data = resp.json()
            # Wrap into Row so we can push to Spark
            rows.append(Row(PropertyId=pid, BusinessDate=date_str, data=json.dumps(data)))
        else:
            print(f"***Failed {pid} {date_str}: {resp.status_code} {resp.text[:200]}")

    current_date += timedelta(days=1)

# --- Convert to DataFrame ---
df = spark.createDataFrame(rows)

# --- Show output ---
df.printSchema()
df.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# ----------------------------------------------------
# 6. Display Output DataFrame. 
# ----------------------------------------------------
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# ----------------------------------------------------
# 7. Expand Data Column Into DataFrame. 
# ----------------------------------------------------

from pyspark.sql import functions as F, types as T

# df is your existing dataframe with columns: PropertyId, BusinessDate, data (JSON string)

# ---- 1) Infer a schema from one non-null JSON string in `data` ----
sample_json = (
    df.select("data")
      .filter(F.col("data").isNotNull())
      .limit(1)
      .collect()[0][0]
)

json_schema = (
    spark.read.json(spark.sparkContext.parallelize([sample_json]))
         .schema
)

# ---- 2) Parse `data` JSON into a struct column `parsed` ----
df_parsed = df.withColumn("parsed", F.from_json(F.col("data"), json_schema))

# ---- 3) Bring parsed fields top-level alongside your tags ----
df_struct = df_parsed.select("PropertyId", "BusinessDate", F.col("parsed.*"))

# ---- 4) Generic flattener for nested structs (recursively) ----
def flatten_struct(df_in, sep="_"):
    """
    Recursively flattens all StructType columns.
    e.g. grossUnitRevenue.amount -> grossUnitRevenue_amount
    """
    flat_cols = []
    nested_cols = []

    for field in df_in.schema.fields:
        col_name = field.name
        dtype = field.dataType
        if isinstance(dtype, T.StructType):
            # expand struct with prefix
            for child in dtype.fields:
                child_name = child.name
                flat_cols.append(F.col(f"`{col_name}`.`{child_name}`").alias(f"{col_name}{sep}{child_name}"))
            nested_cols.append(col_name)
        else:
            flat_cols.append(F.col(f"`{col_name}`"))

    df_out = df_in.select(*flat_cols)

    # If any of the newly expanded columns are themselves structs, keep flattening
    if any(isinstance(f.dataType, T.StructType) for f in df_out.schema.fields):
        return flatten_struct(df_out, sep=sep)
    else:
        return df_out

# Keep PropertyId & BusinessDate separate, flatten only the parsed fields
tag_cols = ["PropertyId", "BusinessDate"]
parsed_cols = [c for c in df_struct.columns if c not in tag_cols]

df_to_flatten = df_struct.select(*tag_cols, *parsed_cols)
df_flat = flatten_struct(df_to_flatten)

# ---- 5) (Optional) order columns: tags first, then the rest ----
other_cols = [c for c in df_flat.columns if c not in tag_cols]
df_final = df_flat.select(*tag_cols, *other_cols)

# ---- 6) Inspect ----
df_final.printSchema()
df_final.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# ----------------------------------------------------
# 7. Display Output DataFrame. 
# ----------------------------------------------------
display(df_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# ----------------------------------------------------
# 8. Load DataFrame Into Lakehouse Table.  -- append mode.
# ----------------------------------------------------

from pyspark.sql import functions as F

table_name = "property_performance"

# Ensure BusinessDate is DATE for consistency
df_to_write = df_final.withColumn("BusinessDate", F.to_date("BusinessDate"))

# Create the table on first run (empty) so schema is registered
if not spark.catalog.tableExists(table_name):
    (df_to_write.limit(0)
        .write
        .format("delta")
        .mode("overwrite")
        # If you want partitions on first creation, uncomment:
        # .partitionBy("BusinessDate")
        .saveAsTable(table_name))

# Append new rows
(df_to_write
    .write
    .format("delta")
    .mode("append")
    # If schema may evolve later, uncomment:
    # .option("mergeSchema", "true")
    .saveAsTable(table_name))

# Quick check
spark.read.table(table_name).show(20, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC ----------------------------------------------------
# MAGIC -- 8. Check Output Table. 
# MAGIC ----------------------------------------------------
# MAGIC 
# MAGIC SELECT * FROM property_performance;
# MAGIC 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --TRUNCATE TABLE Dev_Bronze_Lakehouse.property_performance 


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

# Not in use code 

import requests
import json
import os
from datetime import datetime, timedelta

# List of property IDs
property_ids = ["BER", "MUC", "LND", "PAR", "VIE"]   

# Your access token
access_token = access_token  # already obtained

# Define start and end dates
start_date = datetime(2025, 9, 22)
end_date = datetime(2025, 9, 24)

# Outer loop: property_id
for property_id in property_ids:
    print(f"Fetching data for Property: {property_id}")
    
    # Inner loop: date range
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        
        # Build API URL for this property and date
        url = f"https://api.apaleo.com/reports/v1/reports/property-performance?propertyId={property_id}&from={date_str}&to={date_str}&pageSize=500"
        
        # Add authorization header
        headers = {"Authorization": f"Bearer {access_token}"}
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            # Parse JSON
            data = response.json()
            formatted_json = json.dumps(data, indent=4)
            
            # Define file path (one file per property per date)
            output_path = f"/lakehouse/default/Files/Bronze_Layer/Apaleo/property-performance/{property_id}_{date_str}_property-performance.json"
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Save JSON to file
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(formatted_json)
            
            print(f"Saved data for Property {property_id} on {date_str} to: {output_path}")
        else:
            print(f"Failed to fetch data for Property {property_id} on {date_str}: {response.text}")
        
        current_date += timedelta(days=1)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
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
