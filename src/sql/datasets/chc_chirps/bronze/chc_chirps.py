# ruff: noqa: F821
from pyspark.sql import functions as F

# Configuration

CATALOG = "ahon"
BRONZE_SCHEMA = "bronze"

BASE_PATH = "/Volumes/ahon/reference/source/chc_chirps"

SOURCE_NAME = "chc_chirps"
SOURCE_REF = "https://data.chc.ucsb.edu/products/CHIRPS/v3.0/monthly/global/tifs"


# Read CHIRPS source files

chirps_df = (
    spark.read
    .format("binaryFile")
    .load(f"{BASE_PATH}/*.tif")
)


# Add metadata and provenance

chirps_df = (
    chirps_df
    .withColumn(
        "_ingested_at",
        F.current_timestamp()
    )
    .withColumn(
        "_source_name",
        F.lit(SOURCE_NAME)
    )
    .withColumn(
        "_source_ref",
        F.lit(SOURCE_REF)
    )
    .withColumn(
        "_batch_id",
        F.concat(
            F.lit(SOURCE_NAME),
            F.lit("_"),
            F.date_format(
                F.current_timestamp(),
                "yyyyMMdd_HHmmss"
            )
        )
    )
    .withColumn(
        "_row_hash",
        F.sha2(
            F.col("content"),
            256
        )
    )
)


# Write to Bronze Delta table
(
    chirps_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        f"{CATALOG}.{BRONZE_SCHEMA}.chc_chirps"
    )
)