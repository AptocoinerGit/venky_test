# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import requests
import json
import os
import datetime as dt

from pyspark.sql import SparkSession

# 1. Spark session
spark = SparkSession.builder.getOrCreate()

# 2. Apaleo credentials (make sure these have reports.read scope!)
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

# 4. Property IDs
property_ids = ["BER", "MUC", "LND", "PAR", "VIE"]

# 5. Helper: split big ranges into 30-day chunks
def daterange_chunks(start, end, days=30):
    cur = start
    while cur < end:
        nxt = min(cur + dt.timedelta(days=days), end)
        yield cur, nxt
        cur = nxt

# 6. Loop through properties and date chunks
for property_id in property_ids:
    print(f"🔄 Fetching data for Property: {property_id}")

    for start, stop in daterange_chunks(dt.date(2024, 1, 1), dt.date(2025, 9, 17), 30):
        url = "https://api.apaleo.com/reports/v1/reports/property-performance"
        params = {
            "propertyId": property_id,
            "from": start.isoformat(),
            "to": stop.isoformat(),
            "includeBusinessDays": "true",
            "includeUnitGroups": "true",
            "pageSize": "500"
        }
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            print(f"❌ Failed for {property_id} ({start} → {stop}): {response.text}")
            continue

        # JSON payload
        raw_json = response.json()
        formatted_json = json.dumps(raw_json, indent=4)

        # Bronze Lakehouse path
        output_path = f"/lakehouse/default/Files/Bronze_Layer/Apaleo/property-performance/{property_id}{start}{stop}.json"

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(formatted_json)

        print(f"✅ Saved {property_id} {start} → {stop} to: {output_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
