"""AHON Gold fact_population transformation.

Source: ahondev.silver.psa_psgc and ahondev.gold.dim_lgu
Target: ahondev.gold.fact_population

Grain: one barangay per census year, keyed by (barangay_code, population_year).
barangay_code references dim_lgu.psgc_code. Population is a barangay total,
not an exposed-population measure.
"""

import sys
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

dbutils.widgets.text("census_year", "2024")
CENSUS_YEAR = dbutils.widgets.get("census_year")

dbutils.widgets.text("source_version", "Q2_2024")
SOURCE_VERSION = dbutils.widgets.get("source_version")

# Silver PSGC lives in the "ahondev" catalog; Gold also in "ahondev".
GOLD_CATALOG = "ahondev"
SOURCE_TABLE = f"{GOLD_CATALOG}.silver.psa_psgc"
DIM_TABLE = f"{GOLD_CATALOG}.gold.dim_lgu"
TARGET_TABLE = f"{GOLD_CATALOG}.gold.fact_population"
KEY_COLS = ["barangay_code", "population_year"]
FACT_SCHEMA = (
    "population_key BIGINT NOT NULL, "
    "barangay_code STRING NOT NULL, barangay_name STRING NOT NULL, "
    "municipality_code STRING NOT NULL, province_code STRING, "
    "region_code STRING NOT NULL, population BIGINT NOT NULL, "
    "population_year INT NOT NULL"
)
FACT_COLS = [column.split()[0] for column in FACT_SCHEMA.split(", ")]
VALUE_COLS = [c for c in FACT_COLS if c not in KEY_COLS]

# PSGC silver populations_json is an array of {id, population, year, psgc}
POP_JSON_SCHEMA = ArrayType(
    StructType([
        StructField("id", StringType()),
        StructField("population", StringType()),
        StructField("year", StringType()),
        StructField("psgc", StringType()),
    ])
)

SOURCE_REQUIRED = [
    "psgc_code",
    "geographic_name",
    "geographic_level",
    "_api_period",
    "populations_json",
]
DIM_REQUIRED = [
    "psgc_code",
    "geographic_level",
    "region_code",
    "province_code",
    "municipality_code",
]


def report(message: str) -> None:
    """Write a line to stdout, which Databricks shows as cell output."""
    sys.stdout.write(f"{message}\n")


def fail(message: str) -> None:
    """Raise a ValueError with the given message."""
    raise ValueError(message)


def require_columns(frame: DataFrame, table: str, columns: list[str]) -> None:
    """Fail if the table lacks any column this notebook reads."""
    frame_columns = set(frame.columns)
    missing = [c for c in columns if c not in frame_columns]
    if missing:
        fail(f"{table} is missing required columns: {missing}")


def load_source(spark: SparkSession) -> DataFrame:
    """Extract barangay-level population from the PSGC silver table."""
    silver = spark.table(SOURCE_TABLE)
    require_columns(silver, SOURCE_TABLE, SOURCE_REQUIRED)
    bgy = silver.filter(
        (sf.col("geographic_level") == "Bgy")
        & (sf.col("_api_period") == SOURCE_VERSION)
    )
    if bgy.limit(1).count() == 0:
        fail(
            f"No barangay (Bgy) rows in {SOURCE_TABLE} "
            f"for API period {SOURCE_VERSION}"
        )
    exploded = (
        bgy.withColumn(
            "pop_records", sf.from_json(sf.col("populations_json"), POP_JSON_SCHEMA)
        )
        .withColumn("pop", sf.explode("pop_records"))
        .filter(sf.col("pop.year") == CENSUS_YEAR)
    )
    if exploded.limit(1).count() == 0:
        fail(f"No population records for census year {CENSUS_YEAR} in {SOURCE_TABLE}")
    cleaned_population = sf.expr(
        "try_cast(trim(regexp_replace(pop.population, ',', '')) AS BIGINT)"
    )
    return exploded.select(
        sf.col("psgc_code").cast("string").alias("barangay_code"),
        sf.col("geographic_name").alias("barangay_name"),
        cleaned_population.alias("population"),
        sf.lit(int(CENSUS_YEAR)).alias("population_year"),
    )


def load_dim(spark: SparkSession) -> DataFrame:
    """Read the barangay rows of dim_lgu."""
    dim = spark.table(DIM_TABLE)
    require_columns(dim, DIM_TABLE, DIM_REQUIRED)
    return dim.filter(sf.col("geographic_level") == "Bgy").select(
        sf.col("psgc_code").cast("string").alias("barangay_code"),
        "municipality_code",
        "province_code",
        "region_code",
    )


def join_to_dim(source: DataFrame, dim: DataFrame) -> DataFrame:
    """Inner join source barangays to dim_lgu on psgc_code."""
    return source.join(dim, "barangay_code", "inner")


def assert_joined(joined: DataFrame, source_count: int) -> None:
    """Fail if any source barangay has no dim_lgu match."""
    joined_count = joined.count()
    if joined_count != source_count:
        missing = source_count - joined_count
        fail(
            f"{missing:,} source barangays have no dim_lgu match; "
            f"source={source_count:,}, joined={joined_count:,}"
        )


def build_fact(joined: DataFrame) -> DataFrame:
    """Select the fact columns and add the surrogate key."""
    key = sf.xxhash64(*[sf.col(c) for c in KEY_COLS])  # stable across reruns
    return joined.withColumns({"population_key": key}).select(*FACT_COLS)


def assert_valid_fact(fact: DataFrame) -> None:
    """Fail on duplicate keys, null required values or negative population."""
    required = [c for c in FACT_COLS if c != "province_code"]
    null_rows = fact.filter(
        reduce(lambda a, b: a | b, [sf.col(c).isNull() for c in required])
    )
    if null_rows.limit(1).count():
        null_rows.show(20, truncate=False)
        fail("fact_population has null values in required columns")
    if fact.filter(sf.col("population") < 0).limit(1).count():
        fail("fact_population has negative population values")
    duplicates = fact.groupBy(*KEY_COLS).count().filter("count > 1")
    if duplicates.limit(1).count():
        duplicates.show(20, truncate=False)
        fail("Duplicate (barangay_code, population_year) in fact_population")


def upsert_fact(spark: SparkSession, fact: DataFrame) -> None:
    """Create the fact table if missing, then upsert on the grain keys."""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE} ({FACT_SCHEMA})")
    on_keys = " AND ".join(f"target.{k} = source.{k}" for k in KEY_COLS)
    changed = " OR ".join(
        f"NOT (target.{c} <=> source.{c})" for c in VALUE_COLS
    )
    (
        DeltaTable.forName(spark, TARGET_TABLE)
        .alias("target")
        .merge(fact.alias("source"), on_keys)
        .whenMatchedUpdate(
            condition=changed,
            set={c: f"source.{c}" for c in VALUE_COLS},
        )
        .whenNotMatchedInsert(values={c: f"source.{c}" for c in FACT_COLS})
        .execute()
    )


def main() -> None:
    """Build fact_population from PSGC silver and dim_lgu, then print a summary."""
    spark = SparkSession.builder.getOrCreate()
    source = load_source(spark)
    source_count = source.count()
    if source_count == 0:
        fail(
            f"No barangay population rows in {SOURCE_TABLE} "
            f"for year {CENSUS_YEAR}"
        )
    # Drop barangays with no population data (source value "-" or null)
    source = source.filter(sf.col("population").isNotNull())
    matched = join_to_dim(source, load_dim(spark))
    assert_joined(matched, source.count())
    fact = build_fact(matched)
    assert_valid_fact(fact)
    upsert_fact(spark, fact)

    final = spark.table(TARGET_TABLE)
    report(f"Target table : {TARGET_TABLE}")
    report(f"Source rows  : {source_count:,}")
    report(f"Fact rows    : {final.count():,}")
    final.groupBy("population_year").count().orderBy("population_year").show()


if __name__ == "__main__":
    main()
