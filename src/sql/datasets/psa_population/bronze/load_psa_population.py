# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
"""Load the PSA census population CSV into the bronze Delta table.

The table is created by src/sql/00_setup/bronze_psa.sql, which must run first.
This notebook only checks that the table exists and merges rows into it.
"""

import csv
import sys
import uuid
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, sha2

# COMMAND ----------

# Settings: change here if the team renames anything
VOLUME_ROOT = Path("/Volumes/ahon/reference/source")
DATASET_NAME = "psa_population"
SOURCE_PATH = VOLUME_ROOT / DATASET_NAME / "2024_population_urban.csv"
TABLE_NAME = "ahon.bronze.psa_population_raw"
DDL_PATH = "src/sql/00_setup/bronze_psa.sql"  # creates TABLE_NAME
CENSUS_YEAR = 2024
MERGE_KEYS = ["geographic_location", "census_year"]
RAW_COLS = [
    "geographic_location",
    "total_population",
    "urban_population",
    "percent_urban",
    "census_year",
]
# CSV columns that must be non-empty in every row before bronze is written
REQUIRED_SOURCE_COLS = (
    "Geographic Location",
    "Total Population",
    "Urban Population",
    "Percent Urban",
)
PROVENANCE_COLS = [
    "source_name",
    "source_ref",
    "ingested_at",
    "batch_id",
    "row_hash",
]
# The columns of the table, as in DDL_PATH
TABLE_COLS = RAW_COLS + PROVENANCE_COLS
# Column types the table must have, as DESCRIBE TABLE reports them
EXPECTED_TYPES = dict.fromkeys(TABLE_COLS, "string")
EXPECTED_TYPES.update({"census_year": "int", "ingested_at": "timestamp"})
# Schema of the rows read from the CSV, as a DDL string
SCHEMA = ", ".join(f"{c} {EXPECTED_TYPES[c]}" for c in RAW_COLS)
SourceRow = tuple[str, str, str, str, int]  # one parsed CSV row

# Matched rows are updated only when the content hash differs (null-safe)
UPDATE_CONDITION = "NOT (target.row_hash <=> source.row_hash)"

# COMMAND ----------


class SourceValidationError(ValueError):
    """Raised when the source CSV has a row with an empty required value."""


def report(message: str) -> None:
    """Write a line to stdout, which Databricks shows as cell output."""
    sys.stdout.write(f"{message}\n")


def is_blank(value: str) -> bool:
    """Return True for an empty, whitespace-only, or dots-only value."""
    return not value.strip().lstrip(".").strip()


def read_source_rows(path: Path, census_year: int) -> list[SourceRow]:
    """Read the CSV into rows with full paths (region > province > city).

    Raises SourceValidationError on an empty required value, before anything
    reaches bronze.
    """
    rows: list[SourceRow] = []
    parents: list[tuple[int, str]] = []
    with path.open(encoding="utf-8-sig", newline="") as csv_file:
        for line_no, row in enumerate(csv.DictReader(csv_file), start=1):
            if any(is_blank(row.get(c) or "") for c in REQUIRED_SOURCE_COLS):
                msg = (
                    f"Data row {line_no} in {path} "
                    "has an empty required value"
                )
                raise SourceValidationError(msg)
            loc = row["Geographic Location"]
            name = loc.lstrip(".")
            # 2 dots per level; the full path is the key (leaf names repeat)
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
            # census_year is hashed as text, so the hash input is "2024"
            "row_hash": sha2(concat_ws("|", *[col(c) for c in RAW_COLS]), 256),
        }
    )


def check_table_ready(spark: SparkSession) -> None:
    """Fail fast if the bronze table is missing or differs from its DDL."""
    if not spark.catalog.tableExists(TABLE_NAME):
        msg = f"{TABLE_NAME} does not exist; run {DDL_PATH} first"
        raise RuntimeError(msg)
    actual: dict[str, str] = {}
    # DESCRIBE TABLE instead of .schema, which Spark Connect restricts
    for row in spark.sql(f"DESCRIBE TABLE {TABLE_NAME}").collect():
        if not row["col_name"] or row["col_name"].startswith("#"):
            break  # the column list ends at the first blank or '#' line
        actual[row["col_name"]] = row["data_type"]
    if actual != EXPECTED_TYPES:
        msg = f"{TABLE_NAME} differs from {DDL_PATH}; found {actual}"
        raise RuntimeError(msg)


def merge_into_bronze(spark: SparkSession, frame: DataFrame) -> None:
    """Upsert the rows into the bronze table on the merge keys.

    Matched rows are only updated when their content hash has changed.
    """
    condition = " AND ".join(
        f"target.{key} <=> source.{key}" for key in MERGE_KEYS
    )
    (
        DeltaTable.forName(spark, TABLE_NAME)
        .alias("target")
        .merge(frame.alias("source"), condition)
        .whenMatchedUpdateAll(condition=UPDATE_CONDITION)
        .whenNotMatchedInsertAll()
        .execute()
    )


def main() -> None:
    """Check the bronze table, then merge the extracted CSV into it."""
    spark = SparkSession.builder.getOrCreate()
    check_table_ready(spark)

    rows = read_source_rows(SOURCE_PATH, CENSUS_YEAR)
    batch_id = str(uuid.uuid4())
    frame = add_provenance(spark.createDataFrame(rows, SCHEMA), batch_id)
    merge_into_bronze(spark, frame)

    report(f"Batch ID   : {batch_id}")
    report(f"Read {len(rows)} rows from {SOURCE_PATH}")
    report(f"Merge keys : {MERGE_KEYS}")


# COMMAND ----------

if __name__ == "__main__":
    main()
