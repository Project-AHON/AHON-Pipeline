# Extract PSA census population data from the PXWeb API

import requests

# PXWeb API endpoint for the 2024 census table:
# Total Population, Urban Population, and Percent Urban by geographic location
API_URL = (
    "https://openstat.psa.gov.ph/PXWeb/api/v1/en/DB/1A/PO_2024/0241A6DPUP1.px"
)

# Destination: the source volume, one folder per dataset (naming standard)
VOLUME_ROOT = "/Volumes/ahon/reference/source"
DATASET_NAME = "psa_population"
TARGET_PATH = f"{VOLUME_ROOT}/{DATASET_NAME}/2024_population_urban.csv"

# POST query: select all geographic locations and all parameters, return as CSV
query_body = {
    "query": [
        {
            "code": "Geographic Location",
            "selection": {"filter": "all", "values": ["*"]},
        },
        {
            "code": "Parameter",
            "selection": {"filter": "all", "values": ["*"]},
        },
    ],
    "response": {"format": "csv"},
}

resp = requests.post(
    API_URL, json=query_body, headers={"Accept": "text/csv"}, timeout=60
)
resp.raise_for_status()

# Save the raw CSV response to the volume
dbutils.fs.mkdirs(f"{VOLUME_ROOT}/{DATASET_NAME}")
dbutils.fs.put(TARGET_PATH, resp.text, overwrite=True)

# Show what landed
lines = resp.text.strip().splitlines()
print(f"Saved {len(resp.text):,} bytes to {TARGET_PATH}")
print(f"Lines: {len(lines)}")
print(f"Header: {lines[0]}")
