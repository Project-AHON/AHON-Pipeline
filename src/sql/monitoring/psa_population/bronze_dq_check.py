# Databricks notebook source
"""Run data quality checks on the bronze PSA population table."""

import sys

from pyspark.sql import Column, SparkSession
from pyspark.sql.functions import col, count, expr, lit

# COMMAND ----------

BRONZE_TABLE = "ahon.bronze.psa_population_raw"
MERGE_KEYS = ["geographic_location", "census_year"]
NULL_CHECK_COLS = (
    "geographic_location",
    "total_population",
    "urban_population",
    "percent_urban",
    "census_year",
)
NUMERIC_COLS = ("total_population", "urban_population", "percent_urban")
# Empty, whitespace-only, or dots-only (PXWeb marks missing values with dots)
BLANK_REGEX = r"^[\s.]*$"
EMPTY_CHECK_COLS = (
    "geographic_location",
    "total_population",
    "urban_population",
    "percent_urban",
)
# Reported only, never counted as a failure
INFO_CHECKS = {"total_rows"}

# COMMAND ----------


def report(message: str) -> None:
    """Write a line to stdout, which Databricks shows as cell output."""
    sys.stdout.write(f"{message}\n")


def status_for(check: str, value: int) -> str:
    """Return INFO, PASS or FAIL for one printed check line."""
    if check in INFO_CHECKS:
        return "INFO"
    return "PASS" if value == 0 else "FAIL"


def non_numeric(column: str) -> Column:
    """Return a condition that is true when a non-null value is not numeric."""
    cast = expr(f"try_cast({column} AS DOUBLE)")
    return col(column).isNotNull() & cast.isNull()


def build_row_checks() -> dict[str, Column]:
    """Return the row-level checks, each true for a failing row.

    The conditions are built inside a function, not at import time, because
    pyspark's col() needs an active Spark session.
    """
    # Empty-value checks for required STRING fields
    checks: dict[str, Column] = {
        f"empty_{name}": col(name).rlike(BLANK_REGEX)
        for name in EMPTY_CHECK_COLS
    }
    checks.update(
        {f"null_{name}": col(name).isNull() for name in NULL_CHECK_COLS}
    )
    checks.update(
        {f"non_numeric_{name}": non_numeric(name) for name in NUMERIC_COLS}
    )
    return checks


def run_bronze_dq_checks(spark: SparkSession) -> dict[str, int]:
    """Run data quality checks on the bronze table and print a summary.

    Bronze stores population columns as STRING, so the checks include
    non-numeric value detection in addition to nulls and duplicates. Returns a
    dict mapping each check name to its failing row count, where 0 means the
    check passed. The total row count is included for reference.
    """
    bronze = spark.table(BRONZE_TABLE)

    results = {"total_rows": bronze.count()}
    results.update(
        {
            name: bronze.filter(failing).count()
            for name, failing in build_row_checks().items()
        }
    )
    results["duplicate_merge_keys"] = (
        bronze.groupBy(*MERGE_KEYS)
        .agg(count(lit(1)).alias("n"))
        .filter(col("n") > 1)
        .count()
    )

    report("-- Bronze DQ Checks --")
    for check, value in results.items():
        report(f"  {check:40s} {value:>10,}  {status_for(check, value)}")
    return results


def main() -> None:
    """Run bronze DQ checks and print the summary."""
    spark = SparkSession.builder.getOrCreate()
    run_bronze_dq_checks(spark)


# COMMAND ----------

if __name__ == "__main__":
    main()