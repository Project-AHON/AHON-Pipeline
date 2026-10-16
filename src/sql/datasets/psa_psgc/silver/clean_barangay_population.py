"""Extract typed barangay census observations from the selected PSGC snapshot."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
from quality import check, unique, upsert
from runtime import parser


def transform(spark, args):
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType, StructField, StructType

    source = spark.table(f"{args.catalog}.silver.psa_psgc_clean").where(
        (F.col("_api_period") == args.source_period)
        & (F.col("geographic_level") == "BGY")
    )
    unique(spark, args.catalog, args.batch_id, source, ["psgc_code"], "barangay_snapshot")
    schema = ArrayType(StructType([StructField(c, StringType()) for c in ["id", "population", "year", "psgc"]]))
    parsed = source.withColumn("_population", F.from_json("populations_json", schema))
    check(spark, args.catalog, args.batch_id, "population_json", "silver.psa_psgc_clean",
          parsed.where("populations_json IS NOT NULL AND _population IS NULL").count())
    frame = parsed.withColumn("pop", F.explode_outer("_population"))
    # Do not invent a census year for an absent array. Report missing source coverage.
    check(spark, args.catalog, args.batch_id, "no_population_records", "silver.psa_psgc_clean",
          frame.where("pop IS NULL").count(), "WARN")
    frame = frame.where("pop IS NOT NULL").select(
        F.col("psgc_code").alias("barangay_code"), F.col("area_name").alias("barangay_name"),
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
        OR lpad(trim(population_psgc_raw), 10, '0') <> barangay_code) THEN 'different_population_psgc'
      WHEN population_raw IS NULL OR trim(population_raw) IN ('', '-') THEN 'missing_population'
      WHEN population IS NULL THEN 'invalid_population'
      ELSE 'accepted' END"""))
    frame = frame.withColumn("_processed_at", F.current_timestamp())
    # All parsed observations, including missing and invalid values, remain in Silver.
    unique(spark, args.catalog, args.batch_id, frame,
           ["barangay_code", "population_year", "_api_period"], "silver.psgc_population_clean")
    upsert(spark, frame, f"{args.catalog}.silver.psgc_population_clean",
           ["barangay_code", "population_year", "_api_period"], "NOT(t._row_hash <=> s._row_hash)")
    check(spark, args.catalog, args.batch_id, "invalid_population_records", "silver.psgc_population_clean",
          frame.where("disposition LIKE 'invalid_%' OR disposition = 'different_population_psgc'").count())


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    p = parser(__doc__)
    p.add_argument("--source-period", required=True)
    transform(SparkSession.builder.getOrCreate(), p.parse_args())
