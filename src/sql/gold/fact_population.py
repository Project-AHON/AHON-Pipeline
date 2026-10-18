"""Canonical population fact: one city/municipality per census year."""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from quality import check, unique, upsert


def main():
    spark = SparkSession.builder.getOrCreate()
    catalog = dbutils.widgets.get("catalog")
    period = dbutils.widgets.get("source_version")
    year = int(dbutils.widgets.get("census_year"))
    run_id = globals().get("PIPELINE_RUN_ID", "interactive")
    source_table = f"{catalog}.silver.psgc_city_population_clean"
    target = f"{catalog}.gold.fact_population"
    recorded = spark.table(source_table).where((F.col("_api_period") == period) & (F.col("population_year") == year))
    current = spark.table(f"{catalog}.silver.psa_psgc_clean").where(
        (F.col("_api_period") == period) & F.col("geographic_level").isin("CITY", "MUN")
    ).select("psgc_code", "_api_period", "_row_hash")
    unique(spark, catalog, run_id, current, ["psgc_code"], "current_city_population_source")
    source = recorded.join(current, ["psgc_code", "_api_period", "_row_hash"], "inner")
    check(spark, catalog, run_id, "stale_city_census_observations", source_table,
          recorded.count() - source.count(), "WARN")
    dimension = spark.table(f"{catalog}.gold.dim_lgu")
    unique(spark, catalog, run_id, dimension, ["psgc_code"], "city_population_dimension")
    check(spark, catalog, run_id, "city_population_dimension_snapshot", target,
          dimension.where(F.col("source_period").isNull() | (F.col("source_period") != period)).count())
    cities = dimension.where(F.col("geographic_level").isin("CITY", "MUN"))
    check(spark, catalog, run_id, "city_population_source_nonempty", source_table,
          int(not source.limit(1).count()))
    unique(spark, catalog, run_id, source, ["psgc_code", "population_year"], source_table)
    check(spark, catalog, run_id, "accepted_city_population_dispositions", source_table,
          source.where("disposition IS NULL OR disposition NOT IN ('accepted', 'missing_population')").count())
    check(spark, catalog, run_id, "city_population_reference_coverage", source_table,
          source.join(cities.select("psgc_code"), "psgc_code", "left_anti").count())
    query = Path(__file__).with_name("fact_population_candidate.sql").read_text(encoding="utf-8")
    fact = spark.sql(query, args={"source_period": period, "census_year": year})
    check(spark, catalog, run_id, "city_population_grid_cardinality", target, abs(fact.count() - cities.count()))
    check(spark, catalog, run_id, "city_population_required_values", target,
          fact.where("psgc_code IS NULL OR area_name IS NULL OR region_code IS NULL OR population_year IS NULL").count())
    check(spark, catalog, run_id, "city_population_nonnegative", target, fact.where("population < 0").count())
    check(spark, catalog, run_id, "missing_city_population", target, fact.where("population IS NULL").count(), "WARN")
    unique(spark, catalog, run_id, fact, ["psgc_code", "population_year"], target)
    if spark.catalog.tableExists(target):
        existing = spark.table(target)
        check(spark, catalog, run_id, "city_population_schema_requires_migration", target,
              int("barangay_code" in existing.columns or "psgc_code" not in existing.columns))
        check(spark, catalog, run_id, "city_population_target_snapshot", target,
              existing.where(F.col("source_period").isNull() | (F.col("source_period") != period)).count())
    fact = fact.withColumn("population_key", F.xxhash64("psgc_code", "population_year"))
    fact = fact.withColumn("_row_hash", F.sha2(F.to_json(F.struct(*fact.columns), options={"ignoreNullFields": "false", "timeZone": "UTC"}), 256))
    fact = fact.withColumn("_processed_at", F.current_timestamp())
    upsert(spark, fact, target, ["psgc_code", "population_year"], "NOT(t._row_hash <=> s._row_hash)")
    fact.groupBy("population_year", "geographic_level", "coverage_status").count().show()


if __name__ == "__main__":
    main()
