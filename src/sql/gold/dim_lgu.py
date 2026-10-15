"""
AHON Gold dim_lgu transformation.

Source: <catalog>.silver.psa_psgc_clean
Target: <catalog>.gold.dim_lgu

Creates one geographic dimension from a single approved PSGC snapshot.
Silver remains the source of truth for cleaned area names and hierarchy codes.
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from quality import check, unique

# ============================================================
# 1. CONFIGURATION
# ============================================================

dbutils.widgets.text("catalog", "")
CATALOG = dbutils.widgets.get("catalog")

dbutils.widgets.text("source_period", "")
SOURCE_PERIOD = dbutils.widgets.get("source_period")

SOURCE_TABLE = f"{CATALOG}.silver.psa_psgc_clean"
GOLD_SCHEMA = f"{CATALOG}.gold"
TARGET_TABLE = f"{GOLD_SCHEMA}.dim_lgu"
RUN_ID = globals().get("PIPELINE_RUN_ID", "interactive")

DIMENSION_COLUMNS = [
    "source_period",
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

check(spark, CATALOG, RUN_ID, "selected_snapshot_nonempty", SOURCE_TABLE,
      int(not snapshot_df.limit(1).count()))

# Check the source grain BEFORE selecting or deduplicating records.
# Do not silently discard conflicting records with dropDuplicates().
source_duplicate_codes = (
    snapshot_df
    .groupBy("psgc_code")
    .count()
    .filter(F.col("count") > 1)
)

check(spark, CATALOG, RUN_ID, "selected_snapshot_unique", SOURCE_TABLE,
      source_duplicate_codes.count())

dim_lgu = (
    snapshot_df
    .select(
        F.lit(SOURCE_PERIOD).alias("source_period"),
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

check(spark, CATALOG, RUN_ID, "required_geographic_values", TARGET_TABLE, invalid_required.count())

duplicate_codes = (
    dim_lgu
    .groupBy("psgc_code")
    .count()
    .filter(F.col("count") > 1)
)

check(spark, CATALOG, RUN_ID, "dimension_source_unique", TARGET_TABLE, duplicate_codes.count())


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
        .mode("error")
        .saveAsTable(TARGET_TABLE)
    )
    print(f"Created Gold dimension: {TARGET_TABLE}")


else:
    target_df = spark.table(TARGET_TABLE)
    if "source_period" in target_df.columns:
        check(spark, CATALOG, RUN_ID, "dimension_snapshot_requires_review", TARGET_TABLE,
              target_df.where(F.col("source_period").isNull() | (F.col("source_period") != SOURCE_PERIOD)).count())

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

    if {f.name: f.dataType for f in target_df.schema if f.name in DIMENSION_COLUMNS} != {f.name: f.dataType for f in dim_lgu.select(*DIMENSION_COLUMNS).schema}:
        raise ValueError("Existing dimension types require reviewed migration")
    unique(spark, CATALOG, RUN_ID, target_df, ["psgc_code"], TARGET_TABLE)

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
            condition=" OR ".join(f"NOT(target.`{column}` <=> source.`{column}`)" for column in update_columns),
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
