"""Fresh-catalog observation contract: raw text + file digest + ordinal, never event identity."""

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from earthquake_contract import LOCALITY_PATTERN, SOURCE_COLUMNS
from quality import check, unique, upsert
from runtime import batch_path, completed_batch, parser, provenance


def bronze(spark, args):
    from pyspark.sql import functions as F

    directory = batch_path(args, "philvolcs_earthquake_data")
    records = []
    for item in completed_batch(directory)["files"]:
        with (directory / item["name"]).open(
            encoding="utf-8-sig", newline=""
        ) as stream:
            reader = csv.reader(stream, strict=True)
            if next(reader, None) != SOURCE_COLUMNS:
                raise ValueError("Earthquake source columns changed")
            for ordinal, row in enumerate(reader, 1):
                if len(row) != len(SOURCE_COLUMNS):
                    raise ValueError(
                        "Malformed CSV row retained in landing; no Bronze commit"
                    )
                records.append((*row, ordinal, str(directory / item["name"])))
    schema = (
        ", ".join(f"`{c}` string" for c in SOURCE_COLUMNS)
        + ", _source_row long, _source_ref string"
    )
    frame = spark.createDataFrame(records, schema)
    frame = provenance(
        frame, "philvolcs_earthquake_data", args.batch_id, SOURCE_COLUMNS
    )
    frame = frame.withColumn(
        "_observation_id",
        F.sha2(F.to_json(F.struct("_source_digest", "_source_row")), 256),
    )
    unique(
        spark,
        args.catalog,
        args.batch_id,
        frame,
        ["_observation_id"],
        "bronze.philvolcs_earthquake_observation",
    )
    upsert(
        spark,
        frame,
        f"{args.catalog}.bronze.philvolcs_earthquake_observation",
        ["_observation_id"],
    )
    check(
        spark,
        args.catalog,
        args.batch_id,
        "landing_rows",
        "bronze.philvolcs_earthquake_observation",
        abs(
            len(records)
            - sum(item["rows"] for item in completed_batch(directory)["files"])
        ),
    )


def silver(spark, args):
    from pyspark.sql import functions as F

    directory = batch_path(args, "philvolcs_earthquake_data")
    digests = [item["sha256"] for item in completed_batch(directory)["files"]]
    frame = spark.table(
        f"{args.catalog}.bronze.philvolcs_earthquake_observation"
    ).where(F.col("_source_digest").isin(digests))
    # Wall-clock text stays a wall-clock TIMESTAMP_NTZ. UTC interpretation withheld.
    frame = frame.withColumn(
        "event_time",
        F.expr(
            "try_cast(try_to_timestamp(upper(`Date-Time`), 'dd MMMM yyyy - hh:mm a') AS TIMESTAMP_NTZ)"
        ),
    )
    for raw, clean in [
        ("Latitude", "latitude"),
        ("Longitude", "longitude"),
        ("Depth", "depth_km"),
        ("Magnitude", "magnitude"),
    ]:
        frame = frame.withColumnRenamed(raw, "_raw_" + clean).withColumn(
            clean, F.expr(f"try_cast(`_raw_{clean}` AS DOUBLE)")
        )
    frame = frame.withColumn("source_year", F.expr("CASE WHEN Year RLIKE '^[0-9]{4}$' THEN try_cast(Year AS INT) END"))
    frame = frame.withColumn("source_month", F.expr("month(try_to_timestamp(concat('01 ', Month, ' 2000'), 'dd MMMM yyyy'))"))
    invalid = "event_time IS NULL OR latitude IS NULL OR longitude IS NULL OR depth_km IS NULL OR magnitude IS NULL OR NOT latitude BETWEEN -90 AND 90 OR NOT longitude BETWEEN -180 AND 180 OR depth_km < 0 OR magnitude < 0 OR source_year IS NULL OR source_month IS NULL OR source_year <> year(event_time) OR source_month <> month(event_time)"
    invalid += " OR " + " OR ".join(
        f"isnan({c}) OR abs({c}) = double('Infinity')"
        for c in ["latitude", "longitude", "depth_km", "magnitude"]
    )
    frame = frame.withColumn("has_dq_issues", F.expr(invalid)).withColumn(
        "_psgc_snapshot", F.lit(args.source_period)
    )
    frame = frame.withColumn(
        "locality_token",
        F.lower(F.trim(F.regexp_extract("Location", LOCALITY_PATTERN, 1))),
    )
    frame = frame.withColumn(
        "province_token",
        F.lower(F.trim(F.regexp_extract("Location", LOCALITY_PATTERN, 2))),
    )
    reference = spark.table(f"{args.catalog}.silver.psa_psgc_clean").where(
        (F.col("_api_period") == args.source_period)
        & F.col("geographic_level").isin("CITY", "MUN")
    )
    unique(
        spark,
        args.catalog,
        args.batch_id,
        reference,
        ["psgc_code"],
        "silver.psa_psgc_clean",
    )
    check(spark, args.catalog, args.batch_id, "nonempty_reference", "silver.psa_psgc_clean",
          0 if reference.limit(1).count() else 1)
    reference = reference.select(
        "psgc_code",
        F.lower(F.trim("area_name")).alias("locality_token"),
        F.lower(F.trim("province_name")).alias("province_token"),
    )
    candidates = reference.groupBy("locality_token", "province_token").agg(
        F.sort_array(F.collect_set("psgc_code")).alias("candidate_codes")
    )
    frame = frame.join(
        candidates, ["locality_token", "province_token"], "left"
    ).withColumn(
        "candidate_codes",
        F.coalesce("candidate_codes", F.array().cast("array<string>")),
    )
    frame = frame.withColumn(
        "candidate_status",
        F.when(F.col("locality_token") == "", "unparsed")
        .when(F.size("candidate_codes") > 1, "ambiguous")
        .when(F.size("candidate_codes") == 1, "single_candidate")
        .otherwise("unmatched"),
    )
    frame = frame.withColumn("psgc_code", F.lit(None).cast("string")).withColumn(
        "geographic_assignment_status", F.lit("WITHHELD_LOCALITY_IS_NOT_EXPOSURE")
    )
    frame = frame.withColumn("_processed_at", F.current_timestamp())
    check(spark, args.catalog, args.batch_id, "no_locality_join_fanout", "earthquake_candidate",
          abs(frame.count() - spark.table(f"{args.catalog}.bronze.philvolcs_earthquake_observation").where(F.col("_source_digest").isin(digests)).count()))
    unique(
        spark,
        args.catalog,
        args.batch_id,
        frame,
        ["_observation_id"],
        "silver.philvolcs_earthquake_observation_clean",
    )
    upsert(
        spark,
        frame,
        f"{args.catalog}.silver.philvolcs_earthquake_observation_clean",
        ["_observation_id"],
        "NOT(t.candidate_codes <=> s.candidate_codes) OR NOT(t._psgc_snapshot <=> s._psgc_snapshot)",
    )
    upsert(
        spark,
        frame.where("has_dq_issues"),
        f"{args.catalog}.silver.philvolcs_earthquake_reject",
        ["_observation_id"],
    )
    check(
        spark,
        args.catalog,
        args.batch_id,
        "invalid_observations",
        "silver.philvolcs_earthquake_reject",
        frame.where("has_dq_issues").count(),
        "WARN",
    )
    check(
        spark,
        args.catalog,
        args.batch_id,
        "observation_accounting",
        "silver.philvolcs_earthquake_observation_clean",
        frame.count()
        - (
            frame.where("has_dq_issues").count()
            + frame.where("NOT has_dq_issues").count()
        ),
    )


def main():
    from pyspark.sql import SparkSession

    p = parser(__doc__)
    p.add_argument("--stage", choices=["bronze", "silver"], required=True)
    p.add_argument("--source-period", required=True)
    args = p.parse_args()
    globals()[args.stage](SparkSession.builder.getOrCreate(), args)


if __name__ == "__main__":
    main()
