# PSA census population into bronze


import csv
import io
import uuid

from delta.tables import DeltaTable
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, sha2
from pyspark.sql.types import StructField, StructType, StringType

spark = SparkSession.builder.getOrCreate()

# Settings: change here if the team renames anything
VOLUME_ROOT = "/Volumes/ahon/reference/source"
DATASET_NAME = "psa_population"
SOURCE_PATH = f"{VOLUME_ROOT}/{DATASET_NAME}/2024_population_urban.csv"
TABLE_NAME = "ahon.bronze.psa_population_raw"
CENSUS_YEAR = "2024"
MERGE_KEYS = ["geographic_location", "census_year"]
RAW_COLS = [
    "geographic_location",
    "total_population",
    "urban_population",
    "percent_urban",
    "census_year",
]

# Read the CSV saved by the extract step and build full hierarchical geographic paths.
# The PXWeb CSV uses dot-indentation for hierarchy (2 dots per level); leaf names
# like "San Isidro" appear in multiple provinces, so the full path is needed for
# unique merge keys.
rows = []
parents: list[tuple[int, str]] = []
with open(SOURCE_PATH, encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for row in reader:
        loc = row["Geographic Location"]
        name = loc.lstrip(".")
        level = (len(loc) - len(name)) // 2
        while parents and parents[-1][0] >= level:
            parents.pop()
        parents.append((level, name))
        geographic_location = " > ".join(n for _, n in parents)
        rows.append(
            Row(
                geographic_location=geographic_location,
                total_population=row["Total Population"],
                urban_population=row["Urban Population"],
                percent_urban=row["Percent Urban"],
                census_year=CENSUS_YEAR,
            )
        )

schema = StructType(
    [
        StructField("geographic_location", StringType(), True),
        StructField("total_population", StringType(), True),
        StructField("urban_population", StringType(), True),
        StructField("percent_urban", StringType(), True),
        StructField("census_year", StringType(), True),
    ]
)
df = spark.createDataFrame(rows, schema)

# Provenance columns and SHA-256 row hash
batch_id = str(uuid.uuid4())
df = df.withColumns(
    {
        "source_name": lit("PSA PXWeb API"),
        "source_ref": lit(SOURCE_PATH),
        "ingested_at": current_timestamp(),
        "batch_id": lit(batch_id),
        "row_hash": sha2(concat_ws("|", *[col(c) for c in RAW_COLS]), 256),
    }
)

# Create the bronze table if it does not exist
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        geographic_location     STRING,
        total_population        STRING,
        urban_population        STRING,
        percent_urban           STRING,
        census_year             STRING,
        source_name             STRING,
        source_ref              STRING,
        ingested_at             TIMESTAMP,
        batch_id                STRING,
        row_hash                STRING
    ) USING DELTA
    """
)

# Merge (upsert) on geographic_location + census_year
condition = " AND ".join(
    f"target.{key} <=> source.{key}" for key in MERGE_KEYS
)
(
    DeltaTable.forName(spark, TABLE_NAME)
    .alias("target")
    .merge(df.alias("source"), condition)
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# Show the result
columns = RAW_COLS + ["source_name", "source_ref", "ingested_at", "batch_id", "row_hash"]
print(f"Batch ID   : {batch_id}")
print(f"Merged {df.count()} rows into {TABLE_NAME}")
print(f"Merge keys : {MERGE_KEYS}")
print(f"Columns    : {columns}")
