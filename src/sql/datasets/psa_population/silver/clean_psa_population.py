"""Type explicitly landed census observations; preserve rejects on replay."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bronze"))
from load_psa_population import RAW_COLUMNS, source_frame
from quality import check, unique, upsert
from runtime import parser, provenance


def transform(spark, args):
    from pyspark.sql import functions as F

    paths = provenance(
        source_frame(spark, args), "psa_population", args.batch_id, RAW_COLUMNS
    ).select("_source_digest", "_row_hash", "geographic_path")
    unique(
        spark,
        args.catalog,
        args.batch_id,
        paths,
        ["_source_digest", "_row_hash"],
        "population_source_context",
    )
    frame = spark.table(f"{args.catalog}.bronze.psa_population").join(
        paths, ["_source_digest", "_row_hash"]
    )
    frame = frame.withColumn(
        "geographic_location", F.regexp_replace("geographic_path", r"\s*\d+/", "")
    ).drop("geographic_path")
    for c in ["total_population", "urban_population"]:
        frame = frame.withColumn(
            c,
            F.expr(
                f"CASE WHEN trim({c}) RLIKE '^[+]?[0-9]+([.]0+)?$' THEN try_cast(try_cast({c} AS DECIMAL(38,0)) AS BIGINT) END"
            ),
        )
    frame = frame.withColumn(
        "percent_urban", F.expr("try_cast(percent_urban AS DOUBLE)")
    ).withColumn(
        "rural_population", F.col("total_population") - F.col("urban_population")
    )
    frame = frame.withColumn(
        "geo_level", F.size(F.split("geographic_location", " > ")) - 1
    )
    for name, pos in [("region", 2), ("province", 3), ("city_municipality", 4)]:
        frame = frame.withColumn(
            name, F.expr(f"try_element_at(split(geographic_location, ' > '), {pos})")
        )
    invalid = "total_population IS NULL OR urban_population IS NULL OR percent_urban IS NULL OR isnan(percent_urban) OR NOT percent_urban BETWEEN 0 AND 100 OR total_population < urban_population OR geographic_location IS NULL OR trim(geographic_location) = ''"
    frame = frame.withColumn("has_dq_issues", F.expr(invalid)).withColumn(
        "_processed_at", F.current_timestamp()
    )
    rejected = frame.where("has_dq_issues").withColumn(
        "disposition", F.lit("invalid_population_or_arithmetic")
    )
    upsert(
        spark,
        rejected,
        f"{args.catalog}.silver.psa_population_reject",
        ["_observation_id"],
    )
    accepted = frame.where("NOT has_dq_issues")
    unique(
        spark,
        args.catalog,
        args.batch_id,
        accepted,
        ["geographic_location", "census_year"],
        "silver.psa_population_clean",
    )
    upsert(
        spark,
        accepted,
        f"{args.catalog}.silver.psa_population_clean",
        ["geographic_location", "census_year"],
        "s._ingested_at >= t._ingested_at AND NOT(t._observation_id <=> s._observation_id)",
    )
    check(
        spark,
        args.catalog,
        args.batch_id,
        "population_rejects",
        "silver.psa_population_reject",
        rejected.count(),
        "WARN",
    )
    check(
        spark,
        args.catalog,
        args.batch_id,
        "source_accounting",
        "silver.psa_population_clean",
        abs(paths.count() - accepted.count() - rejected.count()),
    )


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    transform(SparkSession.builder.getOrCreate(), parser(__doc__).parse_args())
