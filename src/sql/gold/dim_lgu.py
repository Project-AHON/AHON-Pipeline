%python
"""
AHON Gold dim_lgu transformation.

Source: ahon_dev.silver.psa_psgc_clean
Target: ahon_dev.gold.dim_lgu

Creates one geographic dimension from a single approved PSGC snapshot.
Silver remains the source of truth for cleaned area names and hierarchy codes.
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURATION
# ============================================================

dbutils.widgets.text("catalog", "ahon_dev")
CATALOG = dbutils.widgets.get("catalog")

dbutils.widgets.text("source_period", "Q2_2024")
SOURCE_PERIOD = dbutils.widgets.get("source_period")

SOURCE_TABLE = f"{CATALOG}.silver.psa_psgc_clean"
GOLD_SCHEMA = f"{CATALOG}.gold"
TARGET_TABLE = f"{GOLD_SCHEMA}.dim_lgu"

DIMENSION_COLUMNS = [
    "psgc_code",
    "area_name",
    "geographic_level",
    "region_code",
    "province_code",
    "municipality_code",
    "barangay_code",
]


# ============================================================
# 2. VALIDATE SOURCE
# ============================================================

if not spark.catalog.tableExists(SOURCE_TABLE):
    raise RuntimeError(f"Source table does not exist: {SOURCE_TABLE}")

source_df = spark.table(SOURCE_TABLE)

required_source_columns = [
    "psgc_code",
    "area_name",
    "geographic_level",
    "region_psgc",
    "province_psgc",
    "citymun_psgc",
    "_api_period",
]

missing_columns = [
    column
    for column in required_source_columns
    if column not in source_df.columns
]

if missing_columns:
    raise ValueError(
        f"Missing required Silver columns: {missing_columns}"
    )


# ============================================================
# 3. SELECT AND VALIDATE THE APPROVED PSGC SNAPSHOT
# ============================================================

# Silver retains multiple API snapshots. Select one approved period
# so dim_lgu has one current-state row per PSGC geographic entity.
snapshot_df = source_df.filter(F.col("_api_period") == SOURCE_PERIOD)

if snapshot_df.limit(1).count() == 0:
    raise ValueError(
        f"No Silver records found for API period {SOURCE_PERIOD}."
    )

# Check the source grain BEFORE selecting or deduplicating records.
# Do not silently discard conflicting records with dropDuplicates().
source_duplicate_codes = (
    snapshot_df
    .groupBy("psgc_code")
    .count()
    .filter(F.col("count") > 1)
)

if source_duplicate_codes.limit(1).count() > 0:
    print(
        f"Duplicate PSGC codes found in Silver for {SOURCE_PERIOD}:"
    )
    source_duplicate_codes.show(20, truncate=False)
    raise ValueError(
        "The selected Silver snapshot is not unique by psgc_code. "
        "Resolve duplicate source records before building dim_lgu."
    )

dim_lgu = (
    snapshot_df
    .select(
        F.col("psgc_code").cast("string").alias("psgc_code"),
        F.col("area_name").alias("area_name"),
        F.col("geographic_level").alias("geographic_level"),

        # These hierarchy fields use standardized codes derived in Silver.
        F.col("region_psgc").alias("region_code"),
        F.col("province_psgc").alias("province_code"),
        F.col("citymun_psgc").alias("municipality_code"),

        # For BGY rows, barangay_code stores the full 10-digit PSGC code.
        # It is NULL for all other geographic levels.
        F.when(
            F.col("geographic_level") == "BGY",
            F.col("psgc_code").cast("string"),
        ).otherwise(F.lit(None).cast("string")).alias("barangay_code"),
    )
)


# ============================================================
# 4. DATA QUALITY VALIDATION
# ============================================================

invalid_required = dim_lgu.filter(
    F.col("psgc_code").isNull()
    | (~F.col("psgc_code").rlike(r"^[0-9]{10}$"))
    | F.col("area_name").isNull()
    | (F.trim(F.col("area_name")) == "")
    | F.col("geographic_level").isNull()
)

if invalid_required.limit(1).count() > 0:
    invalid_required.show(20, truncate=False)
    raise ValueError(
        "dim_lgu contains invalid PSGC codes, missing area names, "
        "or missing geographic levels."
    )

duplicate_codes = (
    dim_lgu
    .groupBy("psgc_code")
    .count()
    .filter(F.col("count") > 1)
)

if duplicate_codes.limit(1).count() > 0:
    duplicate_codes.show(20, truncate=False)
    raise ValueError("Duplicate PSGC codes found in dim_lgu source.")


# ============================================================
# 5. CREATE GOLD SCHEMA
# ============================================================

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")


# ============================================================
# 6. CREATE OR UPSERT DIMENSION
# Existing target rows absent from this snapshot are intentionally retained.
# This is a non-destructive upsert, not a full snapshot replacement.
# ============================================================


if not spark.catalog.tableExists(TARGET_TABLE):
    (
        dim_lgu
        .select(*DIMENSION_COLUMNS)
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_TABLE)
    )
    print(f"Created Gold dimension: {TARGET_TABLE}")


else:
    target_df = spark.table(TARGET_TABLE)

    missing_target_columns = [
        column for column in DIMENSION_COLUMNS
        if column not in target_df.columns
    ]

    if missing_target_columns:
        raise ValueError(
            "Existing dim_lgu schema does not match the expected model. "
            f"Missing columns: {missing_target_columns}. "
            "Review the schema before migrating it."
        )

    target = DeltaTable.forName(spark, TARGET_TABLE)

    update_columns = [
        column for column in DIMENSION_COLUMNS
        if column != "psgc_code"
    ]

    (
        target.alias("target")
        .merge(
            dim_lgu.select(*DIMENSION_COLUMNS).alias("source"),
            "target.psgc_code = source.psgc_code",
        )
        .whenMatchedUpdate(
            set={column: f"source.{column}" for column in update_columns}
        )
        .whenNotMatchedInsert(
            values={column: f"source.{column}" for column in DIMENSION_COLUMNS}
        )
        .execute()
    )

    print(f"Upserted Gold dimension: {TARGET_TABLE}")


# ============================================================
# 7. VERIFY RESULTS
# ============================================================

final_dim_lgu = spark.table(TARGET_TABLE)

print(f"Target table: {TARGET_TABLE}")
print(f"Source PSGC snapshot: {SOURCE_PERIOD}")
print(f"Dimension rows: {final_dim_lgu.count():,}")

display(
    final_dim_lgu
    .groupBy("geographic_level")
    .count()
    .orderBy("geographic_level")
)

display(
    final_dim_lgu
    .orderBy("geographic_level", "area_name")
    .limit(25)
)
