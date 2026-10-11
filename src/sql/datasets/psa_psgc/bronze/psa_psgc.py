%python

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# ============================================================
# 1. Configuration
# ============================================================

RAW_PATH = "/Volumes/ahon_dev/reference/source/psa_psgc"
BRONZE_TABLE = "ahon_dev.bronze.psa_psgc"
SOURCE_REF = "https://classification.psa.gov.ph/psgc"

EXPECTED_PERIODS = {
    "April_2024",
    "Q2_2021",
    "Q2_2024",
    "Q4_2023",
}


# ============================================================
# 2. Read raw JSON files
#    Use Unity Catalog-compatible file metadata.
# ============================================================

raw_df = (
    spark.read
    .option("multiLine", "true")
    .json(f"{RAW_PATH}/*.json")
    .withColumn(
        "_source_file_path",
        F.col("_metadata.file_path"),
    )
)

print("Raw JSON columns:", raw_df.columns)

if "results" not in raw_df.columns:
    raise RuntimeError(
        f"Expected a 'results' column. Found: {raw_df.columns}"
    )


# ============================================================
# 3. Extract individual records and derive _api_period
#    from the source filename.
# ============================================================

records_df = (
    raw_df
    .select(
        F.explode_outer("results").alias("record"),
        "_source_file_path",
    )
    .filter(F.col("record").isNotNull())
)

raw_records_df = records_df.select(
    "record.*",
    "_source_file_path",
)

if raw_records_df.limit(1).count() == 0:
    raise RuntimeError(
        f"No PSGC records found in {RAW_PATH}"
    )

raw_records_df = raw_records_df.withColumn(
    "_api_period",
    F.regexp_extract(
        F.col("_source_file_path"),
        r"([^/]+)\.json$",
        1,
    ),
)

found_periods = {
    row["_api_period"]
    for row in raw_records_df.select("_api_period").distinct().collect()
}

invalid_periods = found_periods - EXPECTED_PERIODS

if "" in found_periods or invalid_periods:
    raise RuntimeError(
        "Unrecognized snapshot filename(s). "
        f"Found periods: {sorted(found_periods)}"
    )

print("Snapshot periods:", sorted(found_periods))
print(f"Raw PSGC records: {raw_records_df.count():,}")


# ============================================================
# 4. Validate required source columns
# ============================================================

required_columns = [
    "code",
    "area_name",
    "geographic_level",
]

missing_columns = [
    column
    for column in required_columns
    if column not in raw_records_df.columns
]

if missing_columns:
    raise RuntimeError(
        f"Required columns missing: {missing_columns}. "
        f"Available columns: {raw_records_df.columns}"
    )


# ============================================================
# 5. Transform source records into the Bronze schema
#    _api_period is the only snapshot-period column.
# ============================================================

def source_column(column_name, data_type="string"):
    """Cast an available source column or return typed NULL."""
    if column_name in raw_records_df.columns:
        return F.col(column_name).cast(data_type)

    return F.lit(None).cast(data_type)


bronze_df = raw_records_df.select(
    source_column("code").alias("psgc_code"),
    source_column("area_name").alias("area_name"),
    source_column("correspondence_code").alias("correspondence_code"),
    source_column("geographic_level").alias("geographic_level"),
    source_column("reg").alias("region_code"),
    source_column("prv").alias("province_code"),
    source_column("mun").alias("municipality_code"),
    source_column("bgy").alias("barangay_code"),
    source_column("old_name").alias("old_name"),
    source_column("city_class").alias("city_class"),
    source_column("income_classification").alias(
        "income_classification"
    ),
    source_column("urban_rural").alias("urban_rural"),
    source_column("island_region").alias("island_region"),
    source_column("status").alias("status"),
    F.col("_api_period"),
    (
        F.to_json(F.col("populations")).alias("populations_json")
        if "populations" in raw_records_df.columns
        else F.lit(None).cast("string").alias("populations_json")
    ),
)


# ============================================================
# 6. Add provenance and audit columns
# ============================================================

bronze_df = (
    bronze_df
    .withColumn("_source_name", F.lit("PSA PSGC API"))
    .withColumn("_source_ref", F.lit(SOURCE_REF))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn(
        "_batch_id",
        F.date_format(
            F.current_timestamp(),
            "yyyyMMddHHmmss",
        ),
    )
)


# ============================================================
# 7. Generate SHA-256 row hash
#    Include _api_period to distinguish snapshots.
# ============================================================

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
    "populations_json",
]

bronze_df = bronze_df.withColumn(
    "_row_hash",
    F.sha2(
        F.to_json(
            F.struct(
                *[F.col(column) for column in hash_columns]
            )
        ),
        256,
    ),
)

# Intentionally do not dropDuplicates().
# Preserve source records, including repeated business keys.


# ============================================================
# 8. Validate snapshot periods and required identifiers
#    Do not enforce uniqueness of psgc_code + _api_period.
# ============================================================

invalid_records = bronze_df.filter(
    F.col("psgc_code").isNull()
    | (F.trim(F.col("psgc_code")) == "")
    | F.col("_api_period").isNull()
    | (F.trim(F.col("_api_period")) == "")
)

if invalid_records.limit(1).count() > 0:
    invalid_records.show(10, truncate=False)
    raise RuntimeError(
        "Records have missing PSGC codes or snapshot periods. "
        "Bronze was not written."
    )

print(f"Records ready for Bronze: {bronze_df.count():,}")


# ============================================================
# 9. Create the corrected Bronze table
# ============================================================

spark.sql("CREATE SCHEMA IF NOT EXISTS ahon_dev.bronze")

# IMPORTANT:
# This rebuilds Bronze from the complete raw snapshot files.
# It removes the obsolete `version` column and old rows whose
# _api_period was NULL. It preserves repeated source records.
#
# Run only when all required source snapshots are available.

(
    bronze_df
    .select(
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
        "populations_json",
        "_source_name",
        "_source_ref",
        "_ingested_at",
        "_batch_id",
        "_row_hash",
    )
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)


# ============================================================
# 10. Verify the Bronze table
# ============================================================

final_df = spark.table(BRONZE_TABLE)

print(f"Bronze table: {BRONZE_TABLE}")
print(f"Total Bronze records: {final_df.count():,}")

display(
    final_df
    .groupBy("_api_period")
    .count()
    .orderBy("_api_period")
)

display(
    final_df
    .orderBy("_api_period", "psgc_code")
    .limit(10)
)
