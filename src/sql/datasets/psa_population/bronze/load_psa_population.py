# Databricks notebook source
"""Load the PSA census population CSV into the bronze Delta table."""

from __future__ import annotations

import csv
import sys
import uuid
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, expr, lit, sha2
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# COMMAND ----------

# Settings: change here if the team renames anything
VOLUME_ROOT = Path("/Volumes/ahon/reference/source")
DATASET_NAME = "psa_population"
SOURCE_PATH = VOLUME_ROOT / DATASET_NAME / "2024_population_urban.csv"
TABLE_NAME = "ahon.bronze.psa_population_raw"
CENSUS_YEAR = 2024
MERGE_KEYS = ["geographic_location", "census_year"]
RAW_COLS = [
    "geographic_location",
    "total_population",
    "urban_population",
    "percent_urban",
    "census_year",
]
INT_COLS = {"census_year"}  # every other raw column is kept as published (STRING)
PROVENANCE_COLS = ["source_name", "source_ref", "ingested_at", "batch_id", "row_hash"]
TABLE_COLS = RAW_COLS + PROVENANCE_COLS  # exact column list and order of the table
SCHEMA = StructType(
    [
        StructField(c, IntegerType() if c in INT_COLS else StringType(), True)
        for c in RAW_COLS
    ]
)

# Matched rows are updated only when the content hash differs (null-safe)
UPDATE_CONDITION = "NOT (target.row_hash <=> source.row_hash)"

CREATE_TABLE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        geographic_location     STRING,
        total_population        STRING,
        urban_population        STRING,
        percent_urban           STRING,
        census_year             INT,
        source_name             STRING,
        source_ref              STRING,
        ingested_at             TIMESTAMP,
        batch_id                STRING,
        row_hash                STRING
    ) USING DELTA
"""

# COMMAND ----------


def report(message: str) -> None:
    """Write a line to stdout, which Databricks shows as cell output."""
    sys.stdout.write(f"{message}\n")


def read_source_rows(
    path: Path, census_year: int
) -> list[tuple[str, str, str, str, int]]:
    """Read the extract CSV and build full hierarchical geographic paths.

    The PXWeb CSV uses dot-indentation for hierarchy (2 dots per level). Leaf
    names like "San Isidro" appear in multiple provinces, so the full path is
    needed for unique merge keys.
    """
    rows: list[tuple[str, str, str, str, int]] = []
    parents: list[tuple[int, str]] = []
    with path.open(encoding="utf-8-sig", newline="") as csv_file:
        for row in csv.DictReader(csv_file):
            loc = row["Geographic Location"]
            name = loc.lstrip(".")
            level = (len(loc) - len(name)) // 2
            while parents and parents[-1][0] >= level:
                parents.pop()
            parents.append((level, name))
            rows.append(
                (
                    " > ".join(n for _, n in parents),
                    row["Total Population"],
                    row["Urban Population"],
                    row["Percent Urban"],
                    census_year,
                )
            )
    return rows


def add_provenance(frame: DataFrame, batch_id: str) -> DataFrame:
    """Add provenance columns and a SHA-256 row hash."""
    return frame.withColumns(
        {
            "source_name": lit("PSA PXWeb API"),
            "source_ref": lit(str(SOURCE_PATH)),
            "ingested_at": current_timestamp(),
            "batch_id": lit(batch_id),
            # concat_ws hashes census_year as text, so the hash input is "2024"
            "row_hash": sha2(concat_ws("|", *[col(c) for c in RAW_COLS]), 256),
        }
    )


def conform_table_schema(spark: SparkSession) -> bool:
    """Rebuild the table in place if its columns differ from the expected ones.

    Fixes tables from earlier versions: census_year stored as STRING, or extra
    columns left behind by a manual migration. The rebuild is one atomic
    create-or-replace, so a failure leaves the table as it was and the table
    history is kept. Does nothing, and returns False, when the table already
    matches, so it is safe to run on every load.
    """
    table = spark.table(TABLE_NAME)
    year_type = table.schema["census_year"].dataType
    if year_type == IntegerType() and table.schema.fieldNames() == TABLE_COLS:
        return False
    if year_type not in (IntegerType(), StringType()):
        msg = f"{TABLE_NAME}.census_year has unexpected type {year_type.simpleString()}"
        raise RuntimeError(msg)

    # try_cast gives NULL instead of failing, so bad values can be counted first
    int_year = expr("try_cast(census_year AS INT)")
    bad_rows = table.filter(col("census_year").isNotNull() & int_year.isNull()).count()
    if bad_rows:
        msg = f"{bad_rows} rows in {TABLE_NAME} have a census_year that is not an int"
        raise RuntimeError(msg)

    columns = [int_year.alias(c) if c == "census_year" else col(c) for c in TABLE_COLS]
    table.select(*columns).writeTo(TABLE_NAME).using("delta").createOrReplace()
    return True


def merge_into_bronze(spark: SparkSession, frame: DataFrame) -> None:
    """Create the bronze table if missing, then upsert on the merge keys.

    Matched rows are only updated when their content hash has changed, so
    unchanged rows keep their original provenance columns.
    """
    spark.sql(CREATE_TABLE_SQL)
    if conform_table_schema(spark):
        report(f"Rebuilt {TABLE_NAME} to match the expected columns (census_year INT)")
    condition = " AND ".join(f"target.{key} <=> source.{key}" for key in MERGE_KEYS)
    (
        DeltaTable.forName(spark, TABLE_NAME)
        .alias("target")
        .merge(frame.alias("source"), condition)
        .whenMatchedUpdateAll(condition=UPDATE_CONDITION)
        .whenNotMatchedInsertAll()
        .execute()
    )


def main() -> None:
    """Read the extracted CSV, add provenance, and merge into bronze."""
    spark = SparkSession.builder.getOrCreate()

    rows = read_source_rows(SOURCE_PATH, CENSUS_YEAR)
    batch_id = str(uuid.uuid4())
    frame = add_provenance(spark.createDataFrame(rows, SCHEMA), batch_id)
    merge_into_bronze(spark, frame)

    report(f"Batch ID   : {batch_id}")
    report(f"Read {len(rows)} rows from {SOURCE_PATH}")
    report(f"Merge keys : {MERGE_KEYS}")
    report(f"Columns    : {TABLE_COLS}")


# COMMAND ----------

if __name__ == "__main__":
    main()