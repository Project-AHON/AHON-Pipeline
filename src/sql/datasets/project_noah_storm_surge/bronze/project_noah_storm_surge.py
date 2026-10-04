from pyspark.sql import functions as F

# Configuration

CATALOG = "ahon"
BRONZE_SCHEMA = "bronze"

BASE_PATH = "/Volumes/ahon/bronze/noah"

storm_surge_df = (
    spark.read
    .parquet(f"{BASE_PATH}/storm_surge.parquet")
)

storm_surge_df = (
    storm_surge_df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_name", F.lit("NOAH Storm Surge"))
    .withColumn("_source_ref", F.lit("storm_surge.parquet"))
    .withColumn("_batch_id", F.date_format(
        F.current_timestamp(),
        "yyyyMMdd_HHmmss"
    ))
)

(
    storm_surge_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        f"{CATALOG}.{BRONZE_SCHEMA}.noah_storm_surge"
    )
)