# Databricks notebook source
"""Run data quality checks on the silver PSA population table."""

import sys

from pyspark.sql import Column, SparkSession
from pyspark.sql.functions import col, count, lit, trim

# COMMAND ----------

SILVER_TABLE = "ahon.silver.psa_population"
BRONZE_TABLE = "ahon.bronze.psa_population_raw"
REJECT_TABLE = "ahon.silver.psa_population_rejects"
MERGE_KEYS = ["geographic_location", "census_year"]
MAX_PERCENT = 100
# geo_level: 0 national, then the first level that has each path part
REGION_LEVEL = 1
PROVINCE_LEVEL = 2
CITY_LEVEL = 3  # city/municipality, the deepest level expected
MARKER_REGEX = "[0-9]+/"  # a PSA footnote marker such as "1/"
# Reported only, never counted as a failure
INFO_CHECKS = {"total_rows", "rejects_table_keys"}

# COMMAND ----------


def report(message: str) -> None:
    """Write a line to stdout, which Databricks shows as cell output."""
    sys.stdout.write(f"{message}\n")


def status_for(check: str, value: int) -> str:
    """Return INFO, PASS or FAIL for one printed check line."""
    if check in INFO_CHECKS:
        return "INFO"
    return "PASS" if value == 0 else "FAIL"


def build_row_checks() -> dict[str, Column]:
    """Return the row-level checks, each true for a failing row.

    The conditions are built inside a function, not at import time, because
    pyspark's col() needs an active Spark session.
    """
    geo_level = col("geo_level")
    region = col("region")
    province = col("province")
    city = col("city_municipality")
    # A path part the level needs is NULL
    missing_geo_part = (
        ((geo_level >= REGION_LEVEL) & region.isNull())
        | ((geo_level >= PROVINCE_LEVEL) & province.isNull())
        | ((geo_level >= CITY_LEVEL) & city.isNull())
    )
    # A path part is filled in that the level says should not exist
    extra_geo_part = (
        ((geo_level < REGION_LEVEL) & region.isNotNull())
        | ((geo_level < PROVINCE_LEVEL) & province.isNotNull())
        | ((geo_level < CITY_LEVEL) & city.isNotNull())
    )
    return {
        "null_geographic_location": col("geographic_location").isNull(),
        "empty_geographic_location": trim(col("geographic_location")) == "",
        "null_total_population": col("total_population").isNull(),
        "null_urban_population": col("urban_population").isNull(),
        "null_percent_urban": col("percent_urban").isNull(),
        "null_census_year": col("census_year").isNull(),
        "negative_total_population": col("total_population") < 0,
        "negative_urban_population": col("urban_population") < 0,
        "percent_urban_out_of_range": (
            (col("percent_urban") < 0) | (col("percent_urban") > MAX_PERCENT)
        ),
        "urban_gt_total_population": (
            col("urban_population") > col("total_population")
        ),
        "null_rural_population": (
            col("total_population").isNotNull()
            & col("urban_population").isNotNull()
            & col("rural_population").isNull()
        ),
        "negative_rural_population": col("rural_population") < 0,
        "rural_population_mismatch": (
            col("total_population").isNotNull()
            & col("urban_population").isNotNull()
            & col("rural_population").isNotNull()
            & (
                col("rural_population")
                != col("total_population") - col("urban_population")
            )
        ),
        "footnote_marker_in_name": (
            col("geographic_location").rlike(MARKER_REGEX)
        ),
        "null_geo_level": geo_level.isNull(),
        "geo_level_out_of_range": (
            (geo_level < 0) | (geo_level > CITY_LEVEL)
        ),
        "missing_geo_part": missing_geo_part,
        "extra_geo_part": extra_geo_part,
    }


def count_reject_keys(spark: SparkSession) -> int:
    """Count distinct path and year pairs in the rejects table, or 0.

    The rejects table is only created the first time a row is rejected, and
    it keeps every earlier run, so this is a history count, not a run count.
    """
    if not spark.catalog.tableExists(REJECT_TABLE):
        return 0
    return spark.table(REJECT_TABLE).select(*MERGE_KEYS).distinct().count()


def run_silver_dq_checks(spark: SparkSession) -> dict[str, int]:
    """Run data quality checks on the silver table and print a summary.

    Returns a dict mapping each check name to its failing row count, where 0
    means the check passed. The total row count is included for reference.
    bronze_silver_row_gap is bronze rows minus silver rows: 0 when every
    bronze row reached silver, above 0 when rows were rejected, and below 0
    when silver holds rows that bronze no longer has.
    """
    silver = spark.table(SILVER_TABLE)
    silver_rows = silver.count()

    results = {"total_rows": silver_rows}
    results.update(
        {
            name: silver.filter(failing).count()
            for name, failing in build_row_checks().items()
        }
    )
    results["duplicate_merge_keys"] = (
        silver.groupBy(*MERGE_KEYS)
        .agg(count(lit(1)).alias("n"))
        .filter(col("n") > 1)
        .count()
    )
    bronze_rows = spark.table(BRONZE_TABLE).count()
    results["bronze_silver_row_gap"] = bronze_rows - silver_rows
    results["rejects_table_keys"] = count_reject_keys(spark)

    report("-- Silver DQ Checks --")
    for check, value in results.items():
        report(f"  {check:40s} {value:>10,}  {status_for(check, value)}")
    return results


def main() -> None:
    """Run silver DQ checks and print the summary."""
    spark = SparkSession.builder.getOrCreate()
    run_silver_dq_checks(spark)


# COMMAND ----------

if __name__ == "__main__":
    main()