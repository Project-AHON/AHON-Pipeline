"""Ingest PSA census population CSVs (2020, 2024) into a bronze Delta table."""

import uuid

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, sha2

VOLUME_PATH = "/Volumes/ahon/bronze/population_raw"
TABLE_NAME = "ahon.bronze.psa_population_raw"
MERGE_KEYS = ["geographic_location", "census_year"]
RAW_COLS = [
    "geographic_location",
    "total_population",
    "household_population",
    "number_of_households",
    "average_household_size",
    "census_year",
]

# Files use ';' as delimiter and have 2 title rows before the header.
# 2020: Geographic Location, Total Population, Household Population,
#       Number of Households
# 2024: same columns plus Average Household Size
AVG_SIZE_BY_YEAR = {
    "2020": "CAST(NULL AS STRING)",
    "2024": "`Average Household Size`",
}


def read_census_file(spark: SparkSession, year: str) -> DataFrame:
    """Read one census CSV from the volume as an all-string DataFrame."""
    file_name = f"{year} population.csv"
    return spark.sql(
        f"""
        SELECT
            `Geographic Location`   AS geographic_location,
            `Total Population`      AS total_population,
            `Household Population`  AS household_population,
            `Number of Households`  AS number_of_households,
            {AVG_SIZE_BY_YEAR[year]} AS average_household_size,
            '{year}'                AS census_year,
            '{file_name}'           AS source_file
        FROM read_files(
            '{VOLUME_PATH}/{file_name}',
            format => 'csv',
            header => true,
            delimiter => ';',
            skipRows => 2,
            inferSchema => false
        )
        """
    )


def add_metadata(df: DataFrame, batch_id: str) -> DataFrame:
    """Add provenance columns and a SHA-256 hash of the raw columns."""
    return (
        df.withColumn("source_name", lit("PSA Population CSV"))
        .withColumn("source_ref", lit(VOLUME_PATH))
        .withColumn("ingested_at", current_timestamp())
        .withColumn("batch_id", lit(batch_id))
        .withColumn(
            "row_hash",
            sha2(concat_ws("|", *[col(c) for c in RAW_COLS]), 256),
        )
    )


def create_bronze_table(spark: SparkSession) -> None:
    """Create (or replace) the bronze Delta table."""
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {TABLE_NAME} (
            geographic_location     STRING,
            total_population        STRING,
            household_population    STRING,
            number_of_households    STRING,
            average_household_size  STRING,
            census_year             STRING,
            source_file             STRING,
            source_name             STRING,
            source_ref              STRING,
            ingested_at             TIMESTAMP,
            batch_id                STRING,
            row_hash                STRING
        ) USING DELTA
        """
    )


def merge_into_bronze(spark: SparkSession, source_df: DataFrame) -> None:
    """Upsert source rows on the merge keys (null-safe equality)."""
    condition = " AND ".join(
        f"target.{key} <=> source.{key}" for key in MERGE_KEYS
    )
    (
        DeltaTable.forName(spark, TABLE_NAME)
        .alias("target")
        .merge(source_df.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def main() -> None:
    """Run the ingestion."""
    spark = SparkSession.builder.getOrCreate()
    batch_id = str(uuid.uuid4())

    raw_df = read_census_file(spark, "2020").unionByName(
        read_census_file(spark, "2024")
    )
    source_df = add_metadata(raw_df, batch_id)

    create_bronze_table(spark)
    merge_into_bronze(spark, source_df)

    print(f"Batch ID   : {batch_id}")
    print(f"Merged {source_df.count()} rows into {TABLE_NAME}")
    print(f"Merge keys : {MERGE_KEYS}")
    print(f"Columns    : {source_df.columns}")


if __name__ == "__main__":
    main()
