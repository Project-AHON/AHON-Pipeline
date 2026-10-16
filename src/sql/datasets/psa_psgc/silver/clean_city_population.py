"""Type reported city/municipality population records; retain missing values."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
from quality import check, unique, upsert
from runtime import parser


def transform(spark, args):
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType, StructField, StructType

    table = "silver.psgc_city_population_clean"
    source = spark.table(f"{args.catalog}.silver.psa_psgc_clean").where(
        (F.col("_api_period") == args.source_period) & F.col("geographic_level").isin("CITY", "MUN")
    )
    unique(spark, args.catalog, args.batch_id, source, ["psgc_code"], "city_population_snapshot")
    schema = ArrayType(StructType([StructField(c, StringType()) for c in ["id", "population", "year", "psgc"]]))
    parsed = source.withColumn("_population", F.from_json("populations_json", schema))
    check(spark, args.catalog, args.batch_id, "city_population_json", table,
          parsed.where("populations_json IS NOT NULL AND _population IS NULL").count())
    frame = parsed.withColumn("pop", F.explode_outer("_population"))
    check(spark, args.catalog, args.batch_id, "city_without_population_records", table,
          frame.where("pop IS NULL").count(), "WARN")
    # Gold supplies the city/year grid without inventing source observations.
    frame = frame.where("pop IS NOT NULL").select(
        "psgc_code", "area_name", "geographic_level",
        F.col("pop.population").alias("population_raw"), F.col("pop.year").alias("year_raw"),
        F.col("pop.psgc").alias("population_psgc_raw"),
        "_api_period", "_source_name", "_source_ref", "_ingested_at", "_batch_id", "_row_hash",
    )
    frame = frame.withColumn("population_year", F.expr("CASE WHEN year_raw RLIKE '^[0-9]{4}$' THEN try_cast(year_raw AS INT) END"))
    value = "trim(regexp_replace(population_raw, ',', ''))"
    frame = frame.withColumn("population", F.expr(f"CASE WHEN {value} RLIKE '^[0-9]+$' THEN try_cast({value} AS BIGINT) END"))
    frame = frame.withColumn("disposition", F.expr("""CASE
      WHEN population_year IS NULL THEN 'invalid_year'
      WHEN population_psgc_raw IS NOT NULL AND (NOT trim(population_psgc_raw) RLIKE '^[0-9]{8,10}$'
        OR lpad(trim(population_psgc_raw), 10, '0') <> psgc_code) THEN 'different_population_psgc'
      WHEN population_raw IS NULL OR trim(population_raw) IN ('', '-') THEN 'missing_population'
      WHEN population IS NULL THEN 'invalid_population'
      ELSE 'accepted' END"""))
    frame = frame.withColumn("_processed_at", F.current_timestamp())
    unique(spark, args.catalog, args.batch_id, frame, ["psgc_code", "population_year", "_api_period"], table)
    upsert(spark, frame, f"{args.catalog}.{table}", ["psgc_code", "population_year", "_api_period"],
           "s._ingested_at >= t._ingested_at AND NOT(t._row_hash <=> s._row_hash)")
    # Gold blocks invalid values in its selected year; historical dispositions remain visible.
    check(spark, args.catalog, args.batch_id, "invalid_city_population_records", table,
          frame.where("disposition LIKE 'invalid_%' OR disposition = 'different_population_psgc'").count(), "WARN")


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    arguments = parser(__doc__)
    arguments.add_argument("--source-period", required=True)
    transform(SparkSession.builder.getOrCreate(), arguments.parse_args())
