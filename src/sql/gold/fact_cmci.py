from pyspark.sql import functions as F

# =====================================================
# CONFIG
# =====================================================

TARGET_TABLE = "ahon.gold.fact_cmci_indicator"

EXPECTED_LGU_COUNT = 1634

EXPECTED_YEARS = [
    "2014",
    "2015",
    "2016",
    "2017",
    "2018",
    "2019",
    "2020",
    "2021",
    "2022",
    "2023",
    "2024",
]

EXPECTED_YEAR_COUNT = len(EXPECTED_YEARS)
EXPECTED_INDICATOR_COUNT = 35

EXPECTED_ROW_COUNT = (
    EXPECTED_LGU_COUNT
    * EXPECTED_YEAR_COUNT
    * EXPECTED_INDICATOR_COUNT
)

# =====================================================
# SILVER INDICATOR CONFIG
# =====================================================

SILVER_CONFIG = {
    "ahon.silver.cmci_economic_dynamism": {
        "les": "local_economy_size",
        "leg": "local_economy_growth",
        "sle": "active_establishments",
        "job": "employment_generation",
    },
    "ahon.silver.cmci_government_efficiency": {
        "cnd": "compliance_national_directives",
        "pipu": "investment_promotion_unit",
        "bre": "arta_citizens_charter",
        "rat": "local_resource_generation",
        "chs": "health_services_capacity",
        "css": "school_services_capacity",
        "lgua": "performance_recognition",
        "bpls": "business_permits",
        "pao": "peace_and_order",
        "sp": "social_protection",
    },
    "ahon.silver.cmci_infrastructure": {
        "road": "road_network",
        "dtp": "distance_to_ports",
        "abu": "basic_utilities",
        "trans": "transportation_vehicles",
        "edu": "education",
        "hea": "health",
        "inv": "lgu_investment",
        "acc": "accommodation_capacity",
        "ict": "information_technology_capacity",
        "atm": "financial_technology_capacity",
    },
    "ahon.silver.cmci_resiliency": {
        "lup": "land_use_plan",
        "drrp": "disaster_risk_reduction_plan",
        "add": "annual_disaster_drill",
        "ews": "early_warning_system",
        "drrmp": "budget_for_drrmp",
        "lra": "local_risk_assessments",
        "ei": "emergency_infrastructure",
        "util": "utilities",
        "ep": "employed_population",
        "ss": "sanitary_system",
    },
    "ahon.silver.cmci_innovation": {
        "intc": "internet_capability",
    },
}

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

print("CMCI Gold fact build started")
print("Target: " + TARGET_TABLE)
print("Expected LGUs: " + str(EXPECTED_LGU_COUNT))
print("Expected years: " + str(EXPECTED_YEAR_COUNT))
print("Expected indicators: " + str(EXPECTED_INDICATOR_COUNT))
print("Expected fact rows: " + str(EXPECTED_ROW_COUNT))

# =====================================================
# VALIDATE INDICATOR CONFIG
# =====================================================

indicator_codes = []

for indicator_mapping in SILVER_CONFIG.values():
    indicator_codes.extend(
        indicator_mapping.keys()
    )

if len(indicator_codes) != EXPECTED_INDICATOR_COUNT:
    raise RuntimeError(
        "Expected "
        + str(EXPECTED_INDICATOR_COUNT)
        + " configured indicators, found "
        + str(len(indicator_codes))
    )

if len(set(indicator_codes)) != EXPECTED_INDICATOR_COUNT:
    raise RuntimeError(
        "Gold indicator configuration contains duplicate codes"
    )

# =====================================================
# VALIDATE REQUIRED TABLES
# =====================================================

required_tables = (
    list(SILVER_CONFIG.keys())
    + [TARGET_TABLE]
)

for table_name in required_tables:
    try:
        spark.table(table_name).limit(1).collect()

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
# BUILD LONG-FORM FACT ROWS
# =====================================================

fact_dataframes = []

for table_name, indicator_mapping in (
    SILVER_CONFIG.items()
):
    silver_df = spark.table(
        table_name
    )

    required_columns = {
        "psgc_code",
        "year",
        *indicator_mapping.values(),
    }

    missing_columns = (
        required_columns
        - set(silver_df.columns)
    )

    if missing_columns:
        raise RuntimeError(
            "Silver table is missing required columns: "
            + table_name
            + " | "
            + str(sorted(missing_columns))
        )

    for (
        indicator_code,
        silver_column,
    ) in indicator_mapping.items():

        indicator_df = (
            silver_df
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
                F.lit(
                    indicator_code
                ).alias(
                    "indicator_code"
                ),
                F.col(
                    silver_column
                )
                .cast("double")
                .alias(
                    "raw_score"
                ),
            )
        )

        fact_dataframes.append(
            indicator_df
        )

if len(fact_dataframes) != EXPECTED_INDICATOR_COUNT:
    raise RuntimeError(
        "Expected "
        + str(EXPECTED_INDICATOR_COUNT)
        + " indicator DataFrames, created "
        + str(len(fact_dataframes))
    )

# =====================================================
# UNION ALL INDICATORS
# =====================================================

fact_df = fact_dataframes[0]

for indicator_df in fact_dataframes[1:]:
    fact_df = fact_df.unionByName(
        indicator_df
    )

# =====================================================
# VALIDATE REQUIRED KEYS
# =====================================================

missing_key_count = (
    fact_df
    .where(
        F.col("psgc_code").isNull()
        | (F.trim("psgc_code") == "")
        | F.col("year").isNull()
        | F.col("indicator_code").isNull()
        | (F.trim("indicator_code") == "")
    )
    .count()
)

if missing_key_count > 0:
    raise RuntimeError(
        "Gold fact source contains "
        + str(missing_key_count)
        + " rows with missing keys"
    )

# =====================================================
# VALIDATE YEAR DOMAIN
# =====================================================

expected_years_int = [
    int(year)
    for year in EXPECTED_YEARS
]

invalid_year_count = (
    fact_df
    .where(
        ~F.col("year").isin(
            expected_years_int
        )
    )
    .count()
)

if invalid_year_count > 0:
    raise RuntimeError(
        "Gold fact source contains "
        + str(invalid_year_count)
        + " rows outside the expected year range"
    )

# =====================================================
# VALIDATE INDICATOR DOMAIN
# =====================================================

invalid_indicator_count = (
    fact_df
    .where(
        ~F.col("indicator_code").isin(
            indicator_codes
        )
    )
    .count()
)

if invalid_indicator_count > 0:
    raise RuntimeError(
        "Gold fact source contains unexpected indicator codes"
    )

# ====================================================
# VALIDATE FACT GRAIN
# =====================================================

duplicate_key_count = (
    fact_df
    .groupBy(
        "psgc_code",
        "year",
        "indicator_code",
    )
    .count()
    .where(
        F.col("count") > 1
    )
    .count()
)

if duplicate_key_count > 0:
    raise RuntimeError(
        "Gold fact source contains "
        + str(duplicate_key_count)
        + " duplicate PSGC-year-indicator keys"
    )

# =====================================================
# VALIDATE COVERAGE
# =====================================================

fact_row_count = fact_df.count()

if fact_row_count != EXPECTED_ROW_COUNT:
    raise RuntimeError(
        "Gold fact coverage mismatch. Expected "
        + str(EXPECTED_ROW_COUNT)
        + " rows, found "
        + str(fact_row_count)
    )

distinct_lgu_count = (
    fact_df
    .select(
        "psgc_code"
    )
    .distinct()
    .count()
)

if distinct_lgu_count != EXPECTED_LGU_COUNT:
    raise RuntimeError(
        "Gold fact LGU coverage mismatch. Expected "
        + str(EXPECTED_LGU_COUNT)
        + ", found "
        + str(distinct_lgu_count)
    )

distinct_year_count = (
    fact_df
    .select(
        "year"
    )
    .distinct()
    .count()
)

if distinct_year_count != EXPECTED_YEAR_COUNT:
    raise RuntimeError(
        "Gold fact year coverage mismatch. Expected "
        + str(EXPECTED_YEAR_COUNT)
        + ", found "
        + str(distinct_year_count)
    )

distinct_indicator_count = (
    fact_df
    .select(
        "indicator_code"
    )
    .distinct()
    .count()
)

if (
    distinct_indicator_count
    != EXPECTED_INDICATOR_COUNT
):
    raise RuntimeError(
        "Gold fact indicator coverage mismatch. Expected "
        + str(EXPECTED_INDICATOR_COUNT)
        + ", found "
        + str(distinct_indicator_count)
    )

print()
print("GOLD FACT SOURCE VALIDATED")
print("Rows: " + str(fact_row_count))
print("LGUs: " + str(distinct_lgu_count))
print("Years: " + str(distinct_year_count))
print(
    "Indicators: "
    + str(distinct_indicator_count)
)

# =====================================================
# PREPARE MERGE SOURCE
# =====================================================

TEMP_VIEW = "cmci_gold_fact_source"

fact_df.createOrReplaceTempView(
    TEMP_VIEW
)

# =====================================================
# MERGE INTO GOLD
# =====================================================

spark.sql(
    f"""
    MERGE INTO {TARGET_TABLE} AS target

    USING {TEMP_VIEW} AS source

    ON target.psgc_code = source.psgc_code
    AND target.year = source.year
    AND target.indicator_code = source.indicator_code

    WHEN MATCHED AND NOT (
        target.raw_score <=> source.raw_score
    )
    THEN UPDATE SET
        target.raw_score = source.raw_score

    WHEN NOT MATCHED
    THEN INSERT (
        psgc_code,
        year,
        indicator_code,
        raw_score
    )
    VALUES (
        source.psgc_code,
        source.year,
        source.indicator_code,
        source.raw_score
    )

    WHEN NOT MATCHED BY SOURCE
    THEN DELETE
    """
)

spark.catalog.dropTempView(
    TEMP_VIEW
)

print()
print("Gold MERGE completed")

# =====================================================
# VERIFY SAVED GOLD TABLE
# =====================================================

saved_df = spark.table(
    TARGET_TABLE
)

saved_row_count = (
    saved_df.count()
)

if saved_row_count != EXPECTED_ROW_COUNT:
    raise RuntimeError(
        "Saved Gold fact row count mismatch. Expected "
        + str(EXPECTED_ROW_COUNT)
        + ", found "
        + str(saved_row_count)
    )

saved_duplicate_count = (
    saved_df
    .groupBy(
        "psgc_code",
        "year",
        "indicator_code",
    )
    .count()
    .where(
        F.col("count") > 1
    )
    .count()
)

if saved_duplicate_count > 0:
    raise RuntimeError(
        "Saved Gold fact contains duplicate "
        "PSGC-year-indicator keys"
    )

# =====================================================
# VERIFY VALUES AGAINST SOURCE
# =====================================================

comparison_df = (
    saved_df.alias("gold")
    .join(
        fact_df.alias("source"),
        on=[
            "psgc_code",
            "year",
            "indicator_code",
        ],
        how="full",
    )
)

mismatch_count = (
    comparison_df
    .where(
        ~F.col("gold.raw_score").eqNullSafe(
            F.col("source.raw_score")
        )
    )
    .count()
)

if mismatch_count > 0:
    raise RuntimeError(
        "Saved Gold fact contains "
        + str(mismatch_count)
        + " values that do not match Silver"
    )

# =====================================================
# FINAL SUMMARY
# =====================================================

null_score_count = (
    saved_df
    .where(
        F.col("raw_score").isNull()
    )
    .count()
)

print()
print("CMCI GOLD FACT BUILD COMPLETE")
print(
    "Rows: "
    + str(saved_row_count)
)
print(
    "LGUs: "
    + str(EXPECTED_LGU_COUNT)
)
print(
    "Years: "
    + str(EXPECTED_YEAR_COUNT)
)
print(
    "Indicators: "
    + str(EXPECTED_INDICATOR_COUNT)
)
print(
    "Null scores preserved from Silver: "
    + str(null_score_count)
)