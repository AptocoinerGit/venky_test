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

df = spark.sql("SELECT * FROM Bronze_Layer.properties")
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
df_flat.write.mode("overwrite").saveAsTable("Bronze_Layer.Silver_Properties")
print("✅ Fully flattened data saved into Bronze_Layer.Silver_Properties")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze_Layer.Silver_Properties LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType, MapType, StringType
import json

def auto_flatten(df, max_json_parse=10):
    """
    Fully recursive flattening:
    - StructType → separate columns
    - ArrayType of StructType → explode & flatten
    - MapType → separate columns per key
    - JSON strings → parse automatically
    max_json_parse: number of rows to sample for auto JSON detection
    """
    while True:
        struct_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
        array_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType) and isinstance(f.dataType.elementType, StructType)]
        map_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, MapType)]
        string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]

        # Detect JSON strings automatically
        json_string_cols = []
        for col in string_cols:
            sample = df.select(col).limit(max_json_parse).rdd.flatMap(lambda x: x).collect()
            if any(isinstance(x, str) and x.strip().startswith("{") and x.strip().endswith("}") for x in sample):
                json_string_cols.append(col)

        if not struct_cols and not array_cols and not map_cols and not json_string_cols:
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
            keys = df.select(F.explode(F.map_keys(F.col(col)))).distinct().rdd.flatMap(lambda x: x).collect()
            for key in keys:
                df = df.withColumn(f"{col}_{key}", F.col(col)[key])
            df = df.drop(col)

        # Parse JSON strings dynamically
        for col in json_string_cols:
            # Infer schema from first non-null row
            sample_json = df.select(col).filter(F.col(col).isNotNull()).limit(1).collect()
            if sample_json:
                json_str = sample_json[0][0]
                try:
                    parsed = json.loads(json_str)
                    if isinstance(parsed, dict):
                        # Create a StructType dynamically
                        fields = [StructType([StructField(k, StringType(), True)]) if isinstance(v, dict) else StructField(k, StringType(), True) for k, v in parsed.items()]
                        # Actually we don't need to create schema, just use from_json with MapType
                        df = df.withColumn(col+"_map", F.from_json(F.col(col), MapType(StringType(), StringType())))
                        keys = df.select(F.explode(F.map_keys(F.col(col+"_map")))).distinct().rdd.flatMap(lambda x: x).collect()
                        for key in keys:
                            df = df.withColumn(f"{col}_{key}", F.col(col+"_map")[key])
                        df = df.drop(col).drop(col+"_map")
                except:
                    continue

    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Assume df is your raw DataFrame from Apaleo
df_flat = auto_flatten(df)

# Save into Silver layer
df_flat.write.mode("overwrite").saveAsTable("Bronze_Layer.Silver_Reservations")
print("✅ Fully flattened data saved into Bronze_Layer.Reservations")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
