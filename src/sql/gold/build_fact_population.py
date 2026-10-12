"""Census source geography retained; mappings require region, province and locality."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from quality import check, unique, upsert
from runtime import parser


def build(spark, args):
    from pyspark.sql import functions as F

    silver = spark.table(f"{args.catalog}.silver.psa_population_clean")
    psgc = spark.table(f"{args.catalog}.silver.psa_psgc_clean").where(
        F.col("_api_period") == args.source_period
    )
    unique(
        spark,
        args.catalog,
        args.batch_id,
        psgc,
        ["psgc_code"],
        "population_psgc_snapshot",
    )
    region = psgc.where("geographic_level = 'REG'").select(
        F.col("psgc_code").alias("region_psgc"), F.col("area_name").alias("region_name")
    )
    reference = psgc.where("geographic_level IN ('CITY','MUN')").join(
        region, "region_psgc", "left"
    )

    def normalize(c):
        return F.lower(F.regexp_replace(F.trim(c), r"\s+", " "))

    reference = reference.select(
        "psgc_code",
        normalize(F.col("region_name")).alias("region_key"),
        normalize(F.col("province_name")).alias("province_key"),
        normalize(F.col("area_name")).alias("locality_key"),
    )
    reference = reference.groupBy("region_key", "province_key", "locality_key").agg(
        F.sort_array(F.collect_set("psgc_code")).alias("candidate_codes")
    )
    fact = (
        silver.withColumn("region_key", normalize(F.col("region")))
        .withColumn("province_key", normalize(F.col("province")))
        .withColumn("locality_key", normalize(F.col("city_municipality")))
    )
    fact = fact.join(reference, ["region_key", "province_key", "locality_key"], "left")
    fact = fact.withColumn(
        "psgc_code",
        F.when(
            (F.col("geo_level") == 3) & (F.size("candidate_codes") == 1),
            F.element_at("candidate_codes", 1),
        ),
    )
    fact = fact.withColumn(
        "mapping_status",
        F.when(F.col("psgc_code").isNotNull(), "exact_context")
        .when(F.size("candidate_codes") > 1, "ambiguous")
        .otherwise("unmatched_or_aggregate"),
    )
    fact = fact.withColumn("source_period", F.lit(args.source_period)).withColumn(
        "population_fact_key",
        F.sha2(F.to_json(F.struct("geographic_location", "census_year")), 256),
    )
    check(
        spark,
        args.catalog,
        args.batch_id,
        "no_population_fanout",
        "gold.fact_population",
        abs(fact.count() - silver.count()),
    )
    unique(
        spark,
        args.catalog,
        args.batch_id,
        fact,
        ["population_fact_key"],
        "gold.fact_population",
    )
    upsert(
        spark,
        fact,
        f"{args.catalog}.gold.fact_population",
        ["population_fact_key"],
        "NOT(t._observation_id <=> s._observation_id) OR NOT(t.psgc_code <=> s.psgc_code)",
    )


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    p = parser(__doc__)
    p.add_argument("--source-period", default="Q2_2024")
    build(SparkSession.builder.getOrCreate(), p.parse_args())
