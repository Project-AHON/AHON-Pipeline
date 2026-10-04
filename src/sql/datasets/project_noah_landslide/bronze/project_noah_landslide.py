# ruff: noqa: F821
from pyspark.sql import functions as F

# Configuration

CATALOG = "ahon"
BRONZE_SCHEMA = "bronze"

BASE_PATH = "/Volumes/ahon/bronze/noah"


landslide_df = (
    spark.read
    .parquet(f"{BASE_PATH}/landslide_chunks/*.parquet")
)

landslide_df = (
    landslide_df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_name", F.lit("NOAH Landslide Hazard"))
    .withColumn("_source_ref", F.lit("landslide_chunks"))
    .withColumn("_batch_id", F.date_format(
        F.current_timestamp(),
        "yyyyMMdd_HHmmss"
    ))
)

(
    landslide_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        f"{CATALOG}.{BRONZE_SCHEMA}.noah_landslide"
    )
)