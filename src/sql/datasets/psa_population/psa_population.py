# Generated from: psa_population.ipynb
# Converted at: 2026-10-05T21:41:24.523Z
# Next step (optional): refactor into modules & generate tests with RunCell
# Quick start: pip install runcell

from pyspark.sql.functions import col, current_timestamp, sha2, concat_ws, lit
from delta.tables import DeltaTable
import uuid

VOLUME_PATH = "/Volumes/ahon/bronze/population_raw"
TABLE_NAME  = "ahon.bronze.psa_population_raw"

# Generate a batch ID for this ingestion run
batch_id = str(uuid.uuid4())

# Read CSV files via read_files (supports skipRows for the 2-line preamble).
# Files use ';' delimiter and have 2 title rows before the header.
# 2020: Geographic Location, Total Population, Household Population, Number of Households
# 2024: same + Average Household Size
df_2020 = spark.sql(f"""
    SELECT
        `Geographic Location`     AS geographic_location,
        `Total Population`         AS total_population,
        `Household Population`     AS household_population,
        `Number of Households`     AS number_of_households,
        CAST(NULL AS STRING)       AS average_household_size,
        '2020'                     AS census_year,
        '2020 population.csv'      AS source_file
    FROM read_files(
        '{VOLUME_PATH}/2020 population.csv',
        format => 'csv',
        header => true,
        delimiter => ';',
        skipRows => 2,
        inferSchema => false
    )
""")

df_2024 = spark.sql(f"""
    SELECT
        `Geographic Location`     AS geographic_location,
        `Total Population`         AS total_population,
        `Household Population`     AS household_population,
        `Number of Households`     AS number_of_households,
        `Average Household Size`   AS average_household_size,
        '2024'                     AS census_year,
        '2024 population.csv'      AS source_file
    FROM read_files(
        '{VOLUME_PATH}/2024 population.csv',
        format => 'csv',
        header => true,
        delimiter => ';',
        skipRows => 2,
        inferSchema => false
    )
""")

# Combine both years
raw_df = df_2020.unionByName(df_2024)

# Add metadata columns
raw_df = (
    raw_df
    .withColumn("source_name", lit("PSA Population CSV"))
    .withColumn("source_ref", lit(VOLUME_PATH))
    .withColumn("ingested_at", current_timestamp())
    .withColumn("batch_id", lit(batch_id))
)

# Add row_hash: SHA-256 of all raw column values concatenated with "|"
raw_cols = ["geographic_location", "total_population", "household_population",
            "number_of_households", "average_household_size", "census_year"]

source_df = raw_df.withColumn("row_hash", sha2(concat_ws("|", *[col(c) for c in raw_cols]), 256))

# Create the bronze table
spark.sql(f"""
CREATE OR REPLACE TABLE {TABLE_NAME} (
    geographic_location     STRING,
    total_population        STRING,
    household_population    STRING,
    number_of_households    STRING,
    average_household_size  STRING,
    census_year             STRING,
    source_file             STRING,
    source_name             STRING,
    source_ref              STRING,
    ingested_at             TIMESTAMP,
    batch_id                STRING,
    row_hash                STRING
) USING DELTA
""")

# MERGE INTO: upsert source into target
# Merge on geographic_location + census_year (null-safe equality)
merge_keys = ["geographic_location", "census_year"]
merge_condition = " AND ".join([f"target.{k} <=> source.{k}" for k in merge_keys])

delta_table = DeltaTable.forName(spark, TABLE_NAME)

(
    delta_table.alias("target")
    .merge(source_df.alias("source"), merge_condition)
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"Batch ID   : {batch_id}")
print(f"Merged into: {TABLE_NAME}")   
print(f"Merge keys : {merge_keys}")
