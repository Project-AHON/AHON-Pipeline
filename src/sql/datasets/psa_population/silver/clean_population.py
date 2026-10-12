# Databricks notebook source
"""Silver layer for PSA population: clean, type, and derive from bronze."""

import sys
import uuid
from functools import reduce
from operator import or_

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as sf

# COMMAND ----------

BRONZE_TABLE = "ahon.bronze.psa_population_raw"
SILVER_TABLE = "ahon.silver.psa_population"
REJECT_TABLE = "ahon.silver.psa_population_rejects"
MERGE_KEYS = ["geographic_location", "census_year"]
PATH_SEPARATOR = " > "  # joins the levels of the hierarchical geographic path
# PSA footnote marker such as "1/" at the end of a path level
FOOTNOTE_PATTERN = r"(\s*\d+/)+(?=\s*>|$)"
# Exact silver columns and types; the column order is derived from this string
SILVER_SCHEMA = (
    "geographic_location STRING, total_population BIGINT, "
    "urban_population BIGINT, rural_population BIGINT, percent_urban DOUBLE, "
    "census_year INT, geo_level INT, region STRING, province STRING, "
    "city_municipality STRING, source_name STRING, source_ref STRING, "
    "ingested_at TIMESTAMP, batch_id STRING, row_hash STRING"
)
SILVER_COLS = [column.split()[0] for column in SILVER_SCHEMA.split(", ")]
HASH_COLS = [
    "geographic_location",
    "total_population",
    "urban_population",
    "rural_population",
    "percent_urban",
    "census_year",
]
# A record without these cannot be loaded into silver; it goes to REJECT_TABLE.
REQUIRED_COLS = [c for c in HASH_COLS if c != "rural_population"]
UPDATE_CONDITION = "NOT (target.row_hash <=> source.row_hash)"

# COMMAND ----------


def report(message: str) -> None:
    """Write a line to stdout, which Databricks shows as cell output."""
    sys.stdout.write(f"{message}\n")


def to_long(name: str) -> Column:
    """Cast a STRING such as '1234.00' to BIGINT, giving NULL if invalid."""
    cast = f"try_cast(try_cast({name} AS DOUBLE) AS BIGINT)"
    return sf.expr(cast).alias(name)


def path_part(position: int) -> Column:
    """Return one level of the geographic path (1-based), or NULL."""
    path = f"split(geographic_location, '{PATH_SEPARATOR}')"
    return sf.expr(f"try_element_at({path}, {position})")


def clean_location() -> Column:
    """Strip the PSA footnote marker and surrounding whitespace."""
    location = sf.col("geographic_location")
    cleaned = sf.regexp_replace(location, FOOTNOTE_PATTERN, "")
    return sf.trim(cleaned).alias("geographic_location")


def build_silver_frame(spark: SparkSession, batch_id: str) -> DataFrame:
    """Read bronze, cast types, derive geography and rural population."""
    # geo_level: 0 national, 1 region, 2 province, 3 city/municipality
    typed = spark.table(BRONZE_TABLE).select(
        clean_location(),
        to_long("total_population"),
        to_long("urban_population"),
        sf.expr("try_cast(percent_urban AS DOUBLE)").alias("percent_urban"),
        sf.expr("try_cast(census_year AS INT)").alias("census_year"),
        sf.col("source_name"),
    )
    levels = sf.split(sf.col("geographic_location"), PATH_SEPARATOR)
    derived = typed.withColumns(
        {
            "rural_population": sf.col("total_population")
            - sf.col("urban_population"),
            "geo_level": sf.size(levels) - 1,
            "region": path_part(2),
            "province": path_part(3),
            "city_municipality": path_part(4),
            "source_ref": sf.lit(BRONZE_TABLE),
            "ingested_at": sf.current_timestamp(),
            "batch_id": sf.lit(batch_id),
        }
    )
    hash_input = sf.concat_ws("|", *[sf.col(c) for c in HASH_COLS])
    hashed = derived.withColumns({"row_hash": sf.sha2(hash_input, 256)})
    return hashed.select(*SILVER_COLS)


def reject_reason() -> Column:
    """Return a comma-separated list of reasons a row is rejected, or ''."""
    missing = reduce(or_, [sf.col(c).isNull() for c in REQUIRED_COLS])
    missing |= sf.col("geographic_location") == ""
    negative = sf.col("rural_population") < 0
    return sf.concat_ws(
        ",",
        sf.when(missing, sf.lit("missing_required_field")),
        sf.when(negative, sf.lit("negative_rural_population")),
    )


def split_valid_and_rejected(frame: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split rows into those safe to merge and those to quarantine."""
    flagged = frame.withColumns({"reject_reason": reject_reason()})
    valid = flagged.filter(sf.col("reject_reason") == "").select(*SILVER_COLS)
    rejected = flagged.filter(sf.col("reject_reason") != "")
    return valid, rejected.select(*SILVER_COLS, "reject_reason")


def assert_loadable(valid: DataFrame, valid_count: int) -> None:
    """Fail before the merge if there is nothing to load or merge keys collide.

    Stripping the footnote marker changes the key, so 'PHILIPPINES 1/' and
    'PHILIPPINES' in the same census year would now collide in the merge.
    """
    if valid_count == 0:
        msg = "No valid rows to load; refusing to empty the silver table"
        raise ValueError(msg)
    duplicates = valid_count - valid.dropDuplicates(MERGE_KEYS).count()
    if duplicates:
        msg = (
            f"{duplicates} rows share a merge key after cleaning; "
            "check bronze "
            "for rows that differ only by footnote marker"
        )
        raise ValueError(msg)


def merge_into_silver(spark: SparkSession, frame: DataFrame) -> None:
    """Create the silver table if missing, then sync it with the valid rows.

    Matched rows are updated only when their content hash changed, so unchanged
    rows keep their provenance. Silver rows with no match in the source are
    deleted (old rows with a footnote marker, and rows now rejected), so silver
    holds exactly the valid bronze rows and a rerun leaves no duplicates.
    """
    spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_TABLE} ({SILVER_SCHEMA})")
    condition = " AND ".join(f"target.{k} <=> source.{k}" for k in MERGE_KEYS)
    (
        DeltaTable.forName(spark, SILVER_TABLE)
        .alias("target")
        .merge(frame.alias("source"), condition)
        .whenMatchedUpdateAll(condition=UPDATE_CONDITION)
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )


def assert_silver_matches(spark: SparkSession, expected: int) -> None:
    """Fail if silver does not hold exactly the rows that were merged."""
    actual = spark.table(SILVER_TABLE).count()
    if actual != expected:
        msg = f"{SILVER_TABLE} has {actual:,} rows, expected {expected:,}"
        raise RuntimeError(msg)


def write_rejects(rejected: DataFrame) -> int:
    """Append rejected rows to the quarantine table and return how many."""
    rejected_count = rejected.count()
    if rejected_count:
        writer = rejected.write.format("delta").mode("append")
        writer.option("mergeSchema", "true").saveAsTable(REJECT_TABLE)
    return rejected_count


def main() -> None:
    """Build the silver table from bronze, then print a summary."""
    spark = SparkSession.builder.getOrCreate()
    batch_id = str(uuid.uuid4())
    frame = build_silver_frame(spark, batch_id)
    valid, rejected = split_valid_and_rejected(frame)
    valid_count = valid.count()
    assert_loadable(valid, valid_count)
    merge_into_silver(spark, valid)
    assert_silver_matches(spark, valid_count)
    rejected_count = write_rejects(rejected)

    report(f"Silver table : {SILVER_TABLE}")
    report(f"Reject table : {REJECT_TABLE}")
    report(f"Batch ID     : {batch_id}")
    report(f"Merge keys   : {MERGE_KEYS}")
    report(f"Silver rows  : {valid_count:,}")
    report(f"Rejected rows: {rejected_count:,}")

# COMMAND ----------

if __name__ == "__main__":
    main()
