
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# ============================================================
# 1. Configuration
# ============================================================

RAW_PATH = "/Volumes/ahon_dev/reference/source/psa_psgc"
BRONZE_TABLE = "ahon_dev.bronze.psa_psgc"
SOURCE_REF = "https://classification.psa.gov.ph/psgc"


# ============================================================
# 2. Read raw PSA PSGC API response files
# ============================================================

raw_df = (
    spark.read
    .option("multiLine", "true")
    .json(f"{RAW_PATH}/*.json")
)

print("Raw JSON columns:", raw_df.columns)

if "results" not in raw_df.columns:
    raise RuntimeError(
        f"Expected a 'results' column. Found: {raw_df.columns}"
    )


# ============================================================
# 3. Extract individual records from the results array
# ============================================================

records_df = (
    raw_df
    .select(
        F.explode_outer("results").alias("record")
    )
    .filter(F.col("record").isNotNull())
)

raw_records_df = records_df.select("record.*")

if raw_records_df.limit(1).count() == 0:
    raise RuntimeError(
        f"No PSGC records found in {RAW_PATH}"
    )

print("PSGC record columns:", raw_records_df.columns)

raw_count = raw_records_df.count()
print(f"Raw PSA PSGC records: {raw_count:,}")


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
        f"Required columns missing from PSGC records: "
        f"{missing_columns}. Available columns: "
        f"{raw_records_df.columns}"
    )


# ============================================================
# 5. Transform source records into the Bronze schema
# ============================================================

def source_column(column_name, data_type="string"):
    """
    Return a source column cast to the requested type.
    If an optional field is absent, return NULL.
    """
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
    source_column("income_classification").alias("income_classification"),
    source_column("urban_rural").alias("urban_rural"),
    source_column("island_region").alias("island_region"),
    source_column("status").alias("status"),
    source_column("_api_period").alias("_api_period"),
    source_column("version").alias("version"),
    F.to_json(F.col("populations")).alias("populations_json")
    if "populations" in raw_records_df.columns
    else F.lit(None).cast("string").alias("populations_json"),
)


# ============================================================
# 6. Add provenance and audit columns
# ============================================================

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


# ============================================================
# 7. Generate SHA-256 row hash
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
    "version",
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


# ============================================================
# 8. Remove duplicate rows within the current batch
# ============================================================

bronze_df = bronze_df.dropDuplicates(["_row_hash"])

unique_count = bronze_df.count()

print(
    f"Unique PSA PSGC records in batch: {unique_count:,}"
)


# ============================================================
# 9. Create the Bronze Delta table if it does not exist
# ============================================================

spark.sql(f"""
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
""")


# ============================================================
# 10. Merge records into Bronze using the row hash
# ============================================================

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


# ============================================================
# 11. Validate the final Bronze table
# ============================================================

final_count = spark.table(BRONZE_TABLE).count()

print(
    f"Bronze table '{BRONZE_TABLE}' "
    f"now contains {final_count:,} records."
)


# ============================================================
# 12. Display a sample of Bronze records
# ============================================================

display(
    spark.table(BRONZE_TABLE)
    .orderBy("_api_period", "psgc_code")
    .limit(10)
)
