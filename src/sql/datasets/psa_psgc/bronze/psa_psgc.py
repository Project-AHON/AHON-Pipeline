from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Configuration

RAW_PATH = "/Volumes/ahon_dev/reference/source/psa_psgc"

BRONZE_TABLE = "ahon_dev.bronze.psa_psgc"

SOURCE_REF = "https://classification.psa.gov.ph/psgc"

# Read raw PSA PSGC data

raw_df = (
    spark.read
    .option("multiLine", "true")
    .json(f"{RAW_PATH}/*.json")
)

raw_count = raw_df.count()

if raw_count == 0:
    raise RuntimeError(
        f"No PSA PSGC source files were found in {RAW_PATH}"
    )

print(f"Raw PSA PSGC records: {raw_count:,}")


bronze_df = raw_df.select(
    F.col("code")
        .cast("string")
        .alias("psgc_code"),

    F.col("area_name")
        .cast("string")
        .alias("area_name"),

    F.col("correspondence_code")
        .cast("string")
        .alias("correspondence_code"),

    F.col("geographic_level")
        .cast("string")
        .alias("geographic_level"),

    F.col("reg")
        .cast("string")
        .alias("region_code"),

    F.col("prv")
        .cast("string")
        .alias("province_code"),

    F.col("mun")
        .cast("string")
        .alias("municipality_code"),

    F.col("bgy")
        .cast("string")
        .alias("barangay_code"),

    F.col("old_name")
        .cast("string")
        .alias("old_name"),

    F.col("city_class")
        .cast("string")
        .alias("city_class"),

    F.col("income_classification")
        .cast("string")
        .alias("income_classification"),

    F.col("urban_rural")
        .cast("string")
        .alias("urban_rural"),

    F.col("island_region")
        .cast("string")
        .alias("island_region"),

    F.col("status")
        .cast("string")
        .alias("status"),

    # Added by our extraction layer
    F.col("_api_period")
        .cast("string")
        .alias("_api_period"),

    # Provided by the PSA PSGC API
    F.col("version")
        .cast("string")
        .alias("version"),

    # Preserve the nested populations structure
    # as JSON in Bronze.
    F.to_json(F.col("populations"))
        .alias("populations_json"),
)


bronze_df = (
    bronze_df
    .withColumn(
        "_source_name",
        F.lit("PSA PSGC API"),
    )
    .withColumn(
        "_source_ref",
        F.lit(SOURCE_REF),
    )
    .withColumn(
        "_ingested_at",
        F.current_timestamp(),
    )
    .withColumn(
        "_batch_id",
        F.date_format(
            F.current_timestamp(),
            "yyyyMMddHHmmss",
        ),
    )
)



hash_columns = [
    "psgc_code",
    "area_name",
    "correspondence_code",
    "geographic_level",
    "region_code",
    "province_code",
    "municipality_code",
    "barangay_code",
    "old_name",
    "city_class",
    "income_classification",
    "urban_rural",
    "island_region",
    "status",
    "_api_period",
    "version",
    "populations_json",
]


bronze_df = bronze_df.withColumn(
    "_row_hash",
    F.sha2(
        F.to_json(
            F.struct(
                *[
                    F.col(column)
                    for column in hash_columns
                ]
            )
        ),
        256,
    ),
)


bronze_df = bronze_df.dropDuplicates(
    ["_row_hash"]
)

unique_count = bronze_df.count()

print(
    f"Unique PSA PSGC records in batch: "
    f"{unique_count:,}"
)


spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
        psgc_code STRING,
        area_name STRING,
        correspondence_code STRING,
        geographic_level STRING,
        region_code STRING,
        province_code STRING,
        municipality_code STRING,
        barangay_code STRING,
        old_name STRING,
        city_class STRING,
        income_classification STRING,
        urban_rural STRING,
        island_region STRING,
        status STRING,
        _api_period STRING,
        version STRING,
        populations_json STRING,
        _source_name STRING,
        _source_ref STRING,
        _ingested_at TIMESTAMP,
        _batch_id STRING,
        _row_hash STRING
    )
    USING DELTA
    """
)

delta_table = DeltaTable.forName(
    spark,
    BRONZE_TABLE,
)

(
    delta_table.alias("target")
    .merge(
        bronze_df.alias("source"),
        "target._row_hash = source._row_hash",
    )
    .whenNotMatchedInsertAll()
    .execute()
)

final_count = spark.table(
    BRONZE_TABLE
).count()

print(
    f"Bronze table '{BRONZE_TABLE}' "
    f"now contains {final_count:,} records."
)
