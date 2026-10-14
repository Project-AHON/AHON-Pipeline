from datetime import datetime, timezone

from pyspark.sql import functions as F

# =====================================================
# CONFIG
# =====================================================

FACT_TABLE = "ahon.gold.fact_cmci_indicator"
INDICATOR_TABLE = "ahon.gold.dim_cmci_indicator"
TARGET_TABLE = "ahon.platinum.cmci_pillar_score"

EXPECTED_LGU_COUNT = 1634

EXPECTED_YEARS = [
    2014,
    2015,
    2016,
    2017,
    2018,
    2019,
    2020,
    2021,
    2022,
    2023,
    2024,
]

EXPECTED_INDICATORS_BY_PILLAR = {
    "Economic Dynamism": 4,
    "Government Efficiency": 10,
    "Infrastructure": 10,
    "Resiliency": 10,
    "Innovation": 1,
}

EXPECTED_YEAR_COUNT = len(EXPECTED_YEARS)
EXPECTED_PILLAR_COUNT = len(
    EXPECTED_INDICATORS_BY_PILLAR
)

EXPECTED_ROW_COUNT = (
    EXPECTED_LGU_COUNT
    * EXPECTED_YEAR_COUNT
    * EXPECTED_PILLAR_COUNT
)

# =====================================================
# SPARK CONFIG
# =====================================================

spark.conf.set(
    "spark.sql.session.timeZone",
    "UTC",
)

# =====================================================
# STARTUP
# =====================================================

print("CMCI Platinum pillar score build started")
print("Gold fact: " + FACT_TABLE)
print("Indicator dimension: " + INDICATOR_TABLE)
print("Target: " + TARGET_TABLE)
print("Expected LGUs: " + str(EXPECTED_LGU_COUNT))
print("Expected years: " + str(EXPECTED_YEAR_COUNT))
print("Expected pillars: " + str(EXPECTED_PILLAR_COUNT))
print("Expected rows: " + str(EXPECTED_ROW_COUNT))

# =====================================================
# VALIDATE REQUIRED TABLES
# =====================================================

required_tables = [
    FACT_TABLE,
    INDICATOR_TABLE,
    TARGET_TABLE,
]

for table_name in required_tables:
    try:
        spark.table(
            table_name
        ).limit(1).collect()

        print(
            "Required table found: "
            + table_name
        )

    except Exception as error:
        raise RuntimeError(
            "Required table cannot be accessed: "
            + table_name
            + " | "
            + str(error)
        ) from error

# =====================================================
# VALIDATE REQUIRED COLUMNS
# =====================================================

required_fact_columns = {
    "psgc_code",
    "year",
    "indicator_code",
    "raw_score",
}

actual_fact_columns = set(
    spark.table(
        FACT_TABLE
    ).columns
)

missing_fact_columns = (
    required_fact_columns
    - actual_fact_columns
)

if missing_fact_columns:
    raise RuntimeError(
        "Gold fact is missing required columns: "
        + str(
            sorted(
                missing_fact_columns
            )
        )
    )

required_indicator_columns = {
    "indicator_code",
    "indicator_name",
    "pillar_name",
}

actual_indicator_columns = set(
    spark.table(
        INDICATOR_TABLE
    ).columns
)

missing_indicator_columns = (
    required_indicator_columns
    - actual_indicator_columns
)

if missing_indicator_columns:
    raise RuntimeError(
        "Indicator dimension is missing required columns: "
        + str(
            sorted(
                missing_indicator_columns
            )
        )
    )

required_target_columns = {
    "psgc_code",
    "year",
    "pillar_name",
    "pillar_score_raw",
    "indicators_expected",
    "indicators_available",
    "indicators_missing",
    "coverage_pct",
    "score_status",
    "platinum_processed_timestamp",
}

actual_target_columns = set(
    spark.table(
        TARGET_TABLE
    ).columns
)

missing_target_columns = (
    required_target_columns
    - actual_target_columns
)

if missing_target_columns:
    raise RuntimeError(
        "Platinum target is missing required columns: "
        + str(
            sorted(
                missing_target_columns
            )
        )
    )

# =====================================================
# VALIDATE INDICATOR DIMENSION
# =====================================================

indicator_df = spark.table(
    INDICATOR_TABLE
)

actual_pillar_counts = {
    row["pillar_name"]: row["indicator_count"]
    for row in (
        indicator_df
        .groupBy(
            "pillar_name"
        )
        .agg(
            F.countDistinct(
                "indicator_code"
            ).alias(
                "indicator_count"
            )
        )
        .collect()
    )
}

if (
    actual_pillar_counts
    != EXPECTED_INDICATORS_BY_PILLAR
):
    raise RuntimeError(
        "CMCI pillar configuration mismatch. Expected "
        + str(EXPECTED_INDICATORS_BY_PILLAR)
        + ", found "
        + str(actual_pillar_counts)
    )

print(
    "Indicator pillar configuration validated: "
    + str(actual_pillar_counts)
)

# =====================================================
# BUILD PILLAR-LEVEL SOURCE
# =====================================================

processed_timestamp = datetime.now(
    timezone.utc
)

fact_df = (
    spark.table(
        FACT_TABLE
    )
    .alias(
        "fact"
    )
)

dimension_df = (
    indicator_df
    .alias(
        "indicator"
    )
)

joined_df = (
    fact_df
    .join(
        dimension_df,
        on="indicator_code",
        how="inner",
    )
)

pillar_score_df = (
    joined_df
    .groupBy(
        "psgc_code",
        "year",
        "pillar_name",
    )
    .agg(
        F.countDistinct(
            "indicator_code"
        ).alias(
            "indicators_expected"
        ),
        F.count(
            "raw_score"
        ).alias(
            "indicators_available"
        ),
        F.round(
            F.sum("raw_score"),
            4,
        ).alias(
            "available_score_sum"
        ),
    )
    .withColumn(
        "indicators_missing",
        (
            F.col("indicators_expected")
            - F.col("indicators_available")
        ).cast("int"),
    )
    .withColumn(
        "coverage_pct",
        F.round(
            F.lit(100.0)
            * F.col("indicators_available")
            / F.col("indicators_expected"),
            2,
        ),
    )
    .withColumn(
        "score_status",
        F.when(
            F.col("indicators_available")
            == F.col("indicators_expected"),
            F.lit("COMPLETE"),
        ).otherwise(
            F.lit("INSUFFICIENT_DATA")
        ),
    )
    .withColumn(
        "pillar_score_raw",
        F.when(
            F.col("score_status")
            == "COMPLETE",
            F.col("available_score_sum"),
        ).otherwise(
            F.lit(None).cast("double")
        ),
    )
    .withColumn(
        "platinum_processed_timestamp",
        F.lit(
            processed_timestamp
        ).cast("timestamp"),
    )
    .select(
        F.trim(
            F.col("psgc_code")
        ).alias(
            "psgc_code"
        ),
        F.col("year")
        .cast("int")
        .alias(
            "year"
        ),
        F.col(
            "pillar_name"
        ),
        F.col(
            "pillar_score_raw"
        ).cast(
            "double"
        ),
        F.col(
            "indicators_expected"
        ).cast(
            "int"
        ),
        F.col(
            "indicators_available"
        ).cast(
            "int"
        ),
        F.col(
            "indicators_missing"
        ).cast(
            "int"
        ),
        F.col(
            "coverage_pct"
        ).cast(
            "double"
        ),
        F.col(
            "score_status"
        ),
        F.col(
            "platinum_processed_timestamp"
        ),
    )
)

# =====================================================
# VALIDATE REQUIRED KEYS
# =====================================================

missing_key_count = (
    pillar_score_df
    .where(
        F.col("psgc_code").isNull()
        | (F.trim("psgc_code") == "")
        | F.col("year").isNull()
        | F.col("pillar_name").isNull()
        | (F.trim("pillar_name") == "")
        | F.col("score_status").isNull()
    )
    .count()
)

if missing_key_count > 0:
    raise RuntimeError(
        "Platinum source contains "
        + str(missing_key_count)
        + " rows with missing required values"
    )

# =====================================================
# VALIDATE GRAIN
# =====================================================

duplicate_key_count = (
    pillar_score_df
    .groupBy(
        "psgc_code",
        "year",
        "pillar_name",
    )
    .count()
    .where(
        F.col("count") > 1
    )
    .count()
)

if duplicate_key_count > 0:
    raise RuntimeError(
        "Platinum source contains "
        + str(duplicate_key_count)
        + " duplicate LGU-year-pillar keys"
    )

# =====================================================
# VALIDATE DOMAINS
# =====================================================

invalid_year_count = (
    pillar_score_df
    .where(
        ~F.col("year").isin(
            EXPECTED_YEARS
        )
    )
    .count()
)

if invalid_year_count > 0:
    raise RuntimeError(
        "Platinum source contains "
        + str(invalid_year_count)
        + " rows outside the expected year domain"
    )

expected_pillars = list(
    EXPECTED_INDICATORS_BY_PILLAR.keys()
)

invalid_pillar_count = (
    pillar_score_df
    .where(
        ~F.col("pillar_name").isin(
            expected_pillars
        )
    )
    .count()
)

if invalid_pillar_count > 0:
    raise RuntimeError(
        "Platinum source contains unexpected pillar names"
    )

# =====================================================
# VALIDATE EXPECTED INDICATORS BY PILLAR
# =====================================================

expected_pillar_rows = [
    (
        pillar_name,
        expected_count,
    )
    for (
        pillar_name,
        expected_count,
    ) in EXPECTED_INDICATORS_BY_PILLAR.items()
]

expected_pillar_df = spark.createDataFrame(
    expected_pillar_rows,
    "pillar_name string, expected_count int",
)

invalid_expected_count = (
    pillar_score_df
    .alias(
        "score"
    )
    .join(
        expected_pillar_df.alias(
            "expected"
        ),
        on="pillar_name",
        how="left",
    )
    .where(
        F.col(
            "expected.expected_count"
        ).isNull()
        | (
            F.col(
                "score.indicators_expected"
            )
            != F.col(
                "expected.expected_count"
            )
        )
    )
    .count()
)

if invalid_expected_count > 0:
    raise RuntimeError(
        "Platinum source contains "
        + str(invalid_expected_count)
        + " rows with an incorrect expected indicator count"
    )

# =====================================================
# VALIDATE COVERAGE LOGIC
# =====================================================

invalid_coverage_count = (
    pillar_score_df
    .where(
        (F.col("indicators_available") < 0)
        | (
            F.col("indicators_available")
            > F.col("indicators_expected")
        )
        | (F.col("indicators_missing") < 0)
        | (
            F.col("indicators_missing")
            != (
                F.col("indicators_expected")
                - F.col("indicators_available")
            )
        )
        | (F.col("coverage_pct") < 0)
        | (F.col("coverage_pct") > 100)
    )
    .count()
)

if invalid_coverage_count > 0:
    raise RuntimeError(
        "Platinum source contains "
        + str(invalid_coverage_count)
        + " rows with invalid coverage calculations"
    )

# =====================================================
# VALIDATE STATUS AND SCORE LOGIC
# =====================================================

invalid_complete_count = (
    pillar_score_df
    .where(
        (
            F.col("score_status")
            == "COMPLETE"
        )
        & (
            (
                F.col("indicators_available")
                != F.col("indicators_expected")
            )
            | F.col(
                "pillar_score_raw"
            ).isNull()
        )
    )
    .count()
)

if invalid_complete_count > 0:
    raise RuntimeError(
        "Platinum source contains "
        + str(invalid_complete_count)
        + " invalid COMPLETE rows"
    )

invalid_incomplete_count = (
    pillar_score_df
    .where(
        (
            F.col("score_status")
            == "INSUFFICIENT_DATA"
        )
        & (
            (
                F.col("indicators_available")
                >= F.col("indicators_expected")
            )
            | F.col(
                "pillar_score_raw"
            ).isNotNull()
        )
    )
    .count()
)

if invalid_incomplete_count > 0:
    raise RuntimeError(
        "Platinum source contains "
        + str(invalid_incomplete_count)
        + " invalid INSUFFICIENT_DATA rows"
    )

invalid_status_count = (
    pillar_score_df
    .where(
        ~F.col("score_status").isin(
            "COMPLETE",
            "INSUFFICIENT_DATA",
        )
    )
    .count()
)

if invalid_status_count > 0:
    raise RuntimeError(
        "Platinum source contains unexpected score statuses"
    )

# =====================================================
# VALIDATE TOTAL COVERAGE
# =====================================================

source_row_count = (
    pillar_score_df.count()
)

if source_row_count != EXPECTED_ROW_COUNT:
    raise RuntimeError(
        "Platinum row-count mismatch. Expected "
        + str(EXPECTED_ROW_COUNT)
        + ", found "
        + str(source_row_count)
    )

distinct_lgu_count = (
    pillar_score_df
    .select(
        "psgc_code"
    )
    .distinct()
    .count()
)

if distinct_lgu_count != EXPECTED_LGU_COUNT:
    raise RuntimeError(
        "Platinum LGU coverage mismatch. Expected "
        + str(EXPECTED_LGU_COUNT)
        + ", found "
        + str(distinct_lgu_count)
    )

distinct_year_count = (
    pillar_score_df
    .select(
        "year"
    )
    .distinct()
    .count()
)

if distinct_year_count != EXPECTED_YEAR_COUNT:
    raise RuntimeError(
        "Platinum year coverage mismatch. Expected "
        + str(EXPECTED_YEAR_COUNT)
        + ", found "
        + str(distinct_year_count)
    )

distinct_pillar_count = (
    pillar_score_df
    .select(
        "pillar_name"
    )
    .distinct()
    .count()
)

if distinct_pillar_count != EXPECTED_PILLAR_COUNT:
    raise RuntimeError(
        "Platinum pillar coverage mismatch. Expected "
        + str(EXPECTED_PILLAR_COUNT)
        + ", found "
        + str(distinct_pillar_count)
    )

print()
print("PLATINUM CMCI SOURCE VALIDATED")
print("Rows: " + str(source_row_count))
print("LGUs: " + str(distinct_lgu_count))
print("Years: " + str(distinct_year_count))
print("Pillars: " + str(distinct_pillar_count))

# =====================================================
# MERGE INTO PLATINUM
# =====================================================

TEMP_VIEW = "cmci_platinum_pillar_score_source"

pillar_score_df.createOrReplaceTempView(
    TEMP_VIEW
)

spark.sql(
    f"""
    MERGE INTO {TARGET_TABLE} AS target

    USING {TEMP_VIEW} AS source

    ON target.psgc_code = source.psgc_code
    AND target.year = source.year
    AND target.pillar_name = source.pillar_name

    WHEN MATCHED AND (
        NOT (
            target.pillar_score_raw
            <=> source.pillar_score_raw
        )
        OR target.indicators_expected
            <> source.indicators_expected
        OR target.indicators_available
            <> source.indicators_available
        OR target.indicators_missing
            <> source.indicators_missing
        OR NOT (
            target.coverage_pct
            <=> source.coverage_pct
        )
        OR target.score_status
            <> source.score_status
    )
    THEN UPDATE SET
        target.pillar_score_raw =
            source.pillar_score_raw,
        target.indicators_expected =
            source.indicators_expected,
        target.indicators_available =
            source.indicators_available,
        target.indicators_missing =
            source.indicators_missing,
        target.coverage_pct =
            source.coverage_pct,
        target.score_status =
            source.score_status,
        target.platinum_processed_timestamp =
            source.platinum_processed_timestamp

    WHEN NOT MATCHED
    THEN INSERT (
        psgc_code,
        year,
        pillar_name,
        pillar_score_raw,
        indicators_expected,
        indicators_available,
        indicators_missing,
        coverage_pct,
        score_status,
        platinum_processed_timestamp
    )
    VALUES (
        source.psgc_code,
        source.year,
        source.pillar_name,
        source.pillar_score_raw,
        source.indicators_expected,
        source.indicators_available,
        source.indicators_missing,
        source.coverage_pct,
        source.score_status,
        source.platinum_processed_timestamp
    )

    WHEN NOT MATCHED BY SOURCE
    THEN DELETE
    """
)

spark.catalog.dropTempView(
    TEMP_VIEW
)

print()
print("Platinum MERGE completed")

# =====================================================
# VERIFY SAVED TABLE
# =====================================================

saved_df = spark.table(
    TARGET_TABLE
)

saved_row_count = (
    saved_df.count()
)

if saved_row_count != EXPECTED_ROW_COUNT:
    raise RuntimeError(
        "Saved Platinum row-count mismatch. Expected "
        + str(EXPECTED_ROW_COUNT)
        + ", found "
        + str(saved_row_count)
    )

saved_duplicate_count = (
    saved_df
    .groupBy(
        "psgc_code",
        "year",
        "pillar_name",
    )
    .count()
    .where(
        F.col("count") > 1
    )
    .count()
)

if saved_duplicate_count > 0:
    raise RuntimeError(
        "Saved Platinum table contains duplicate "
        "LGU-year-pillar keys"
    )

# =====================================================
# VERIFY VALUES AGAINST SOURCE
# =====================================================

comparison_df = (
    saved_df
    .alias(
        "saved"
    )
    .join(
        pillar_score_df.alias(
            "source"
        ),
        on=[
            "psgc_code",
            "year",
            "pillar_name",
        ],
        how="full",
    )
)

mismatch_count = (
    comparison_df
    .where(
        (
            ~F.round(
                F.col("saved.pillar_score_raw"),
                4,
            ).eqNullSafe(
                F.round(
                    F.col("source.pillar_score_raw"),
                    4,
                )
            )
        )
        | (
            F.col("saved.indicators_expected")
            != F.col("source.indicators_expected")
        )
        | (
            F.col("saved.indicators_available")
            != F.col("source.indicators_available")
        )
        | (
            F.col("saved.indicators_missing")
            != F.col("source.indicators_missing")
        )
        | (
            ~F.round(
                F.col("saved.coverage_pct"),
                2,
            ).eqNullSafe(
                F.round(
                    F.col("source.coverage_pct"),
                    2,
                )
            )
        )
        | (
            F.col("saved.score_status")
            != F.col("source.score_status")
        )
    )
    .count()
)

if mismatch_count > 0:
    raise RuntimeError(
        "Saved Platinum table contains "
        + str(mismatch_count)
        + " rows that do not match the source calculation"
    )

# =====================================================
# FINAL SUMMARY
# =====================================================

status_rows = (
    saved_df
    .groupBy(
        "score_status"
    )
    .count()
    .orderBy(
        "score_status"
    )
    .collect()
)

print()
print("CMCI PLATINUM PILLAR SCORE COMPLETE")
print("Rows: " + str(saved_row_count))

for row in status_rows:
    print(
        row["score_status"]
        + ": "
        + str(row["count"])
    )