from pyspark.sql import functions as F

# Configuration

CATALOG = "ahon"
BRONZE_SCHEMA = "bronze"

BASE_PATH = "/Volumes/ahon/bronze/noah"

# Flood

flood_df = (
    spark.read
    .parquet(f"{BASE_PATH}/flood_25yr.parquet")
)

flood_df = (
    flood_df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_name", F.lit("NOAH Flood Hazard"))
    .withColumn("_source_ref", F.lit("flood_25yr.parquet"))
    .withColumn("_batch_id", F.date_format(
        F.current_timestamp(),
        "yyyyMMdd_HHmmss"
    ))
)

(
    flood_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.noah_flood_25yr")
)