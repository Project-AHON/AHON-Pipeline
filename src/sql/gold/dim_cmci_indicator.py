from pyspark.sql import functions as F

from cmci_common import (
    EXPECTED_INDICATOR_COUNT,
    INDICATORS_BY_PILLAR,
)

# =====================================================
# CONFIG
# =====================================================

TARGET_TABLE = "ahon.gold.dim_cmci_indicator"

# =====================================================
# STARTUP
# =====================================================

print("CMCI Gold indicator dimension build started")
print("Target: " + TARGET_TABLE)
print(
    "Expected indicators: "
    + str(EXPECTED_INDICATOR_COUNT)
)

# =====================================================
# VALIDATE TARGET TABLE
# =====================================================

try:
    target_df = spark.table(
        TARGET_TABLE
    )

    print(
        "Required table found: "
        + TARGET_TABLE
    )

except Exception as error:
    raise RuntimeError(
        "Required table cannot be accessed: "
        + TARGET_TABLE
        + " | "
        + str(error)
    ) from error

required_target_columns = {
    "indicator_code",
    "indicator_name",
    "pillar_name",
}

missing_target_columns = (
    required_target_columns
    - set(target_df.columns)
)

if missing_target_columns:
    raise RuntimeError(
        "Gold indicator dimension is missing required columns: "
        + str(
            sorted(
                missing_target_columns
            )
        )
    )

# =====================================================
# BUILD INDICATOR RECORDS
# =====================================================

indicator_records = []

for (
    pillar_name,
    pillar_indicators,
) in INDICATORS_BY_PILLAR.items():

    for (
        indicator_name,
        indicator_code,
    ) in pillar_indicators.items():

        indicator_records.append(
            {
                "indicator_code": indicator_code,
                "indicator_name": indicator_name,
                "pillar_name": pillar_name,
            }
        )

# =====================================================
# VALIDATE CONFIGURATION
# =====================================================

if (
    len(indicator_records)
    != EXPECTED_INDICATOR_COUNT
):
    raise RuntimeError(
        "Indicator count mismatch. Expected "
        + str(EXPECTED_INDICATOR_COUNT)
        + ", found "
        + str(len(indicator_records))
    )

indicator_codes = [
    record["indicator_code"]
    for record in indicator_records
]

if (
    len(set(indicator_codes))
    != EXPECTED_INDICATOR_COUNT
):
    raise RuntimeError(
        "Indicator configuration contains duplicate "
        "indicator codes"
    )

indicator_names = [
    record["indicator_name"]
    for record in indicator_records
]

if (
    len(set(indicator_names))
    != EXPECTED_INDICATOR_COUNT
):
    raise RuntimeError(
        "Indicator configuration contains duplicate "
        "indicator names"
    )

# =====================================================
# CREATE DIMENSION DATAFRAME
# =====================================================

dimension_df = (
    spark.createDataFrame(
        indicator_records
    )
    .select(
        F.trim(
            F.col("indicator_code")
        ).alias(
            "indicator_code"
        ),
        F.trim(
            F.col("indicator_name")
        ).alias(
            "indicator_name"
        ),
        F.trim(
            F.col("pillar_name")
        ).alias(
            "pillar_name"
        ),
    )
)

# =====================================================
# VALIDATE REQUIRED VALUES
# =====================================================

missing_value_count = (
    dimension_df
    .where(
        F.col("indicator_code").isNull()
        | (F.col("indicator_code") == "")
        | F.col("indicator_name").isNull()
        | (F.col("indicator_name") == "")
        | F.col("pillar_name").isNull()
        | (F.col("pillar_name") == "")
    )
    .count()
)

if missing_value_count > 0:
    raise RuntimeError(
        "Indicator dimension contains "
        + str(missing_value_count)
        + " rows with missing required values"
    )

# =====================================================
# VALIDATE DATAFRAME COUNT
# =====================================================

dimension_count = (
    dimension_df.count()
)

if (
    dimension_count
    != EXPECTED_INDICATOR_COUNT
):
    raise RuntimeError(
        "Indicator dimension DataFrame count mismatch. "
        "Expected "
        + str(EXPECTED_INDICATOR_COUNT)
        + ", found "
        + str(dimension_count)
    )

print()
print("INDICATOR DIMENSION SOURCE VALIDATED")
print(
    "Indicators: "
    + str(dimension_count)
)

# =====================================================
# MERGE INTO GOLD
# =====================================================

TEMP_VIEW = "cmci_indicator_dimension_source"

dimension_df.createOrReplaceTempView(
    TEMP_VIEW
)

spark.sql(
    f"""
    MERGE INTO {TARGET_TABLE} AS target

    USING {TEMP_VIEW} AS source

    ON target.indicator_code = source.indicator_code

    WHEN MATCHED AND (
        NOT (
            target.indicator_name
            <=> source.indicator_name
        )
        OR NOT (
            target.pillar_name
            <=> source.pillar_name
        )
    )
    THEN UPDATE SET
        target.indicator_name =
            source.indicator_name,
        target.pillar_name =
            source.pillar_name

    WHEN NOT MATCHED
    THEN INSERT (
        indicator_code,
        indicator_name,
        pillar_name
    )
    VALUES (
        source.indicator_code,
        source.indicator_name,
        source.pillar_name
    )

    WHEN NOT MATCHED BY SOURCE
    THEN DELETE
    """
)

spark.catalog.dropTempView(
    TEMP_VIEW
)

print()
print("Gold indicator dimension MERGE completed")

# =====================================================
# VERIFY SAVED DIMENSION
# =====================================================

saved_df = spark.table(
    TARGET_TABLE
)

saved_count = (
    saved_df.count()
)

if (
    saved_count
    != EXPECTED_INDICATOR_COUNT
):
    raise RuntimeError(
        "Saved Gold indicator dimension count mismatch. "
        "Expected "
        + str(EXPECTED_INDICATOR_COUNT)
        + ", found "
        + str(saved_count)
    )

duplicate_code_count = (
    saved_df
    .groupBy(
        "indicator_code"
    )
    .count()
    .where(
        F.col("count") > 1
    )
    .count()
)

if duplicate_code_count > 0:
    raise RuntimeError(
        "Saved Gold indicator dimension contains "
        "duplicate indicator codes"
    )

# =====================================================
# VERIFY SAVED VALUES
# =====================================================

comparison_df = (
    saved_df.alias("gold")
    .join(
        dimension_df.alias("source"),
        on="indicator_code",
        how="full",
    )
)

mismatch_count = (
    comparison_df
    .where(
        ~F.col(
            "gold.indicator_name"
        ).eqNullSafe(
            F.col(
                "source.indicator_name"
            )
        )
        | ~F.col(
            "gold.pillar_name"
        ).eqNullSafe(
            F.col(
                "source.pillar_name"
            )
        )
    )
    .count()
)

if mismatch_count > 0:
    raise RuntimeError(
        "Saved Gold indicator dimension contains "
        + str(mismatch_count)
        + " rows that do not match CMCI configuration"
    )

# =====================================================
# VERIFY PILLAR DISTRIBUTION
# =====================================================

pillar_counts = (
    saved_df
    .groupBy(
        "pillar_name"
    )
    .count()
    .orderBy(
        "pillar_name"
    )
    .collect()
)

print()
print("CMCI INDICATORS BY PILLAR")

for row in pillar_counts:
    print(
        row["pillar_name"]
        + ": "
        + str(row["count"])
    )

# =====================================================
# FINAL SUMMARY
# =====================================================

print()
print("CMCI GOLD INDICATOR DIMENSION COMPLETE")
print(
    "Indicators: "
    + str(saved_count)
)
print(
    "Pillars: "
    + str(len(pillar_counts))
)