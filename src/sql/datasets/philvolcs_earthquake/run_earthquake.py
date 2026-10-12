"""One earthquake-only Spark Python job task. SQL reports precede target writes."""

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from earthquake_contract import (
    LINEAGE,
    LOCALITY_PATTERN,
    SOURCE_COLUMNS,
    check_schema,
    digest,
    land_source,
    record_landing_contract,
    report,
    stop_on_failures,
    validate_landing,
)

SQL_ROOT = Path(__file__).resolve().parents[2]
DATASET_ROOT = Path(__file__).resolve().parent
BRONZE = "bronze.philvolcs_earthquake_data"
SILVER = "silver.philvolcs_earthquake_data_clean"
BRONZE_TYPES = dict(zip(SOURCE_COLUMNS, [
    "string", "double", "double", "double", "double", "string", "string", "int",
])) | dict(zip(LINEAGE, ["string", "string", "timestamp", "string", "string"]))


def build_candidates(spark, source_period):
    from pyspark.sql import functions as F

    def normalized(column):
        return F.lower(F.regexp_replace(F.trim(column), r"\s+", " "))

    keys = spark.table("earthquake_scope").select(
        "_row_hash",
        normalized(F.regexp_extract("Location", LOCALITY_PATTERN, 1)).alias("locality_token"),
        normalized(F.regexp_extract("Location", LOCALITY_PATTERN, 2)).alias("province_token"),
    ).fillna({"locality_token": "", "province_token": ""})
    keys.createOrReplaceTempView("earthquake_description_keys")
    # Distinct is on reference keys, never on observations.
    keys.select("locality_token", "province_token").distinct().createOrReplaceTempView(
        "earthquake_distinct_localities"
    )
    reference = None
    if spark.catalog.tableExists("silver.psa_psgc_clean"):
        reference = spark.table("silver.psa_psgc_clean").where(
            F.col("_api_period") == source_period
        )
    if reference is None or not reference.limit(1).count():
        spark.sql("""
          CREATE OR REPLACE TEMP VIEW earthquake_lgu_candidates AS
          SELECT locality_token, province_token, cast(array() AS ARRAY<STRING>) AS candidate_codes,
            'blocked_reference' AS candidate_status, cast(NULL AS STRING) AS candidate_method
          FROM earthquake_distinct_localities
        """)
        return
    metrics = reference.agg(
        F.count("*").alias("n"), F.countDistinct("psgc_code").alias("codes"),
        F.count_if(~F.col("psgc_code").rlike("^[0-9]{10}$")
                   | F.col("psgc_code").isNull()).alias("bad_codes"),
    ).first()
    if metrics.n != metrics.codes or metrics.bad_codes:
        raise ValueError("Reference codes invalid or duplicated in selected snapshot")
    reference.createOrReplaceTempView("earthquake_psgc_snapshot")
    ref = reference.where(F.col("geographic_level").isin("CITY", "MUN")).select(
        "psgc_code", normalized(F.col("area_name")).alias("name"),
        normalized(F.col("province_name")).alias("province"),
    )
    ref.createOrReplaceTempView("earthquake_locality_reference")
    spark.sql("""
      CREATE OR REPLACE TEMP VIEW earthquake_lgu_candidates AS
      WITH matched AS (
        SELECT k.locality_token, k.province_token,
          sort_array(collect_list(r.psgc_code)) AS candidate_codes
        FROM earthquake_distinct_localities k LEFT JOIN earthquake_locality_reference r
          ON k.locality_token = r.name AND k.province_token = r.province
          AND k.locality_token <> '' AND k.province_token <> ''
        GROUP BY k.locality_token, k.province_token
      )
      SELECT *, CASE WHEN locality_token = '' OR province_token = '' THEN 'unparsed'
          WHEN size(candidate_codes) = 1 THEN 'single_candidate'
          WHEN size(candidate_codes) > 1 THEN 'ambiguous' ELSE 'unmatched' END AS candidate_status,
        CASE WHEN size(candidate_codes) > 0 THEN 'exact_locality_province_candidate'
             ELSE NULL END AS candidate_method
      FROM matched
    """)
    # Independent cities without a province never use name-only fallback.


def arguments():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--catalog", required=True)
    p.add_argument("--source-path", required=True, help="Immutable landed CSV, never scraper output")
    p.add_argument("--source-sha256", required=True)
    p.add_argument("--land-from", help="Optional validated file to copy once into source-path")
    p.add_argument("--batch-id", required=True, help="One stable ID per run; reuse on retries")
    p.add_argument("--source-period", default="Q2_2024", help="Same PSGC snapshot as dim_lgu")
    p.add_argument("--expected-periods", required=True, help="Comma-separated YYYY-MM periods")
    p.add_argument("--hash-schema", required=True, help="Reviewed JSON of legacy inferred column types")
    p.add_argument("--stage", choices=["bronze", "silver", "gold"], default="silver")
    p.add_argument("--reprocess", action="store_true", help="Deliberate full historical Silver rebuild candidate")
    p.add_argument("--check-only", action="store_true", help="Validate candidates, never write tables")
    args = p.parse_args()
    if not re.fullmatch(r"[a-z][a-z0-9_]*", args.catalog):
        p.error("catalog must be a lowercase SQL identifier")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", args.batch_id):
        p.error("batch-id must be a single safe component")
    if not args.source_path.startswith(f"/Volumes/{args.catalog}/reference/"):
        p.error("source-path must be an immutable file in the configured reference volume")
    periods = args.expected_periods.split(",")
    if not all(re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", v) for v in periods):
        p.error("expected-periods must contain YYYY-MM values")
    args.periods = periods
    return args


def execute_sql(spark, path, checks=False):
    outputs = []
    for statement in Path(path).read_text(encoding="utf-8").split("-- COMMAND ----------"):
        result = spark.sql(statement.strip())
        if checks:
            # SQL returns a bounded rule summary, never a growing event dataset.
            rows = [r.asDict() for r in result.collect()]
            for row in rows:
                report(row)
            if rows and "action" in rows[0]:
                stop_on_failures(rows)
            outputs.extend(rows)
    return outputs


def schema_types(frame):
    return {f.name: f.dataType.simpleString() for f in frame.schema.fields}


def preflight_target(spark, name, expected, nullable=False, keys=None):
    if not spark.catalog.tableExists(name):
        return
    frame = spark.table(name)
    check_schema(schema_types(frame), expected)
    required_nullable = {"event_time", "latitude", "longitude", "depth", "magnitude", "month", "year"}
    if nullable and any(not f.nullable for f in frame.schema.fields if f.name in required_nullable):
        raise ValueError("Legacy Silver NOT NULL constraints require reviewed migration")
    if name == SILVER:
        ddl = spark.sql(f"SHOW CREATE TABLE {name}").first()[0]
        if re.search(r"\bPRIMARY KEY\b", ddl, re.IGNORECASE):
            raise ValueError("Legacy Silver event PK must be removed in reviewed migration")
    if keys is not None:
        frame = frame.join(keys.select("_row_hash"), "_row_hash", "left_semi")
        if name == SILVER:
            paired = frame.alias("t").join(keys.alias("b"), "_row_hash")
            lineage_changed = " OR ".join(f"NOT(t.`{c}` <=> b.`{c}`)" for c in LINEAGE)
            if paired.where(lineage_changed).limit(1).count():
                raise ValueError("Existing Silver lineage differs from Bronze; reviewed migration required")
    if frame.where("_row_hash IS NULL OR NOT _row_hash RLIKE '^[0-9a-f]{64}$'").limit(1).count():
        raise ValueError("Existing target has invalid observation hashes")
    if frame.groupBy("_row_hash").count().where("count > 1").limit(1).count():
        raise ValueError("Existing target observation identity is not unique")


def merge_candidate(spark, name, view, columns, update=False):
    """Atomic Delta MERGE. Equal rows, including timestamps, remain unchanged."""
    from pyspark.sql import functions as F

    candidate = spark.table(view).select(*columns)
    if not spark.catalog.tableExists(name):
        # New target only. Existing schemas are NEVER automatically replaced.
        candidate.where(F.lit(False)).write.format("delta").mode("error").saveAsTable(name)
    names = ", ".join(f"`{c}`" for c in columns)
    values = ", ".join(f"s.`{c}`" for c in columns)
    change = ""
    if update:
        business = [c for c in columns if c not in LINEAGE + ["_processed_at"]]
        different = " OR ".join(f"NOT(t.`{c}` <=> s.`{c}`)" for c in business)
        # Existing provenance must equal Bronze; candidate DQ checks this first.
        change = f"WHEN MATCHED AND ({different}) THEN UPDATE SET " + ", ".join(
            f"t.`{c}` = s.`{c}`" for c in columns if c != "_row_hash"
        )
    spark.sql(f"""
      MERGE INTO {name} t USING {view} s ON t._row_hash = s._row_hash
      {change}
      WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})
    """)


def prepare_gold_review(spark):
    """Read-only scoped evidence; never infer an approved event or geography policy."""
    from pyspark.sql import functions as F

    scope = spark.table("earthquake_silver_check")
    review = scope
    if spark.catalog.tableExists(SILVER):
        # Semi join returns each historical observation once, even if multiple
        # scope signatures share coordinates. Also inspect signature collisions
        # outside those coordinates; id alone is not an approved event identity.
        keys = scope.select("id", "event_time", "latitude", "longitude").distinct()
        p, k = spark.table(SILVER).alias("p"), keys.alias("k")
        same_signature = F.col("p.id") == F.col("k.id")
        same_point_time = (
            F.col("k.event_time").isNotNull()
            & F.col("k.latitude").isNotNull() & F.col("k.longitude").isNotNull()
            & (F.col("p.event_time") == F.col("k.event_time"))
            & (F.col("p.latitude") == F.col("k.latitude"))
            & (F.col("p.longitude") == F.col("k.longitude"))
        )
        previous = p.join(k, same_signature | same_point_time, "left_semi").join(
            scope.select("_row_hash"), "_row_hash", "left_anti",
        )
        review = scope.unionByName(previous)
    review.createOrReplaceTempView("earthquake_event_review")

    # dim_lgu has no snapshot column and retains target-only rows. Compare ALL
    # seven projected attributes to the selected reference, not just membership.
    dimension_columns = ["psgc_code", "area_name", "geographic_level", "region_code",
                         "province_code", "municipality_code", "barangay_code"]
    reference_columns = ["psgc_code", "area_name", "geographic_level", "region_psgc",
                         "province_psgc", "citymun_psgc", "_api_period"]
    schema = ", ".join(f"{c} string" for c in dimension_columns)
    period = spark.table("earthquake_config").select("source_period").first()[0]
    available = []
    for table, required, view in [
        ("silver.psa_psgc_clean", reference_columns, "earthquake_gold_reference"),
        ("gold.dim_lgu", dimension_columns, "earthquake_gold_dimension"),
    ]:
        supported = spark.catalog.tableExists(table)
        if supported:
            frame = spark.table(table)
            types = schema_types(frame)
            supported = all(types.get(c) == "string" for c in required)
        if not supported:
            # Keep independent observation accounting runnable with explicit
            # BLOCKED prerequisite results; never substitute another snapshot.
            frame = spark.createDataFrame([], schema)
        elif table == "silver.psa_psgc_clean":
            frame = frame.where(F.col("_api_period") == period).select(
                "psgc_code", "area_name", "geographic_level",
                F.col("region_psgc").alias("region_code"),
                F.col("province_psgc").alias("province_code"),
                F.col("citymun_psgc").alias("municipality_code"),
                F.when(F.col("geographic_level") == "BGY", F.col("psgc_code"))
                .otherwise(F.lit(None).cast("string")).alias("barangay_code"),
            )
        else:
            frame = frame.select(*dimension_columns)
        frame.createOrReplaceTempView(view)
        available.append(supported)
    spark.createDataFrame([tuple(available)],
                          "reference_supported boolean, dimension_supported boolean"
                          ).createOrReplaceTempView("earthquake_gold_prerequisites")


def main():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    args = arguments()
    if args.land_from:
        land_source(args.land_from, args.source_path, args.source_sha256, args.periods)
    landing = validate_landing(args.source_path, args.source_sha256, args.periods)
    report({"landing": landing, "batch_id": args.batch_id, "source_ref": args.source_path})
    spark = SparkSession.builder.getOrCreate()
    # Reproduce legacy wall-clock parsing/signature. Do not relabel event_time UTC.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql(f"USE CATALOG `{args.catalog}`")
    spark.createDataFrame([(
        args.source_path, args.batch_id, args.source_period,
        datetime.now(timezone.utc), landing["raw_rows"],
        len(landing["requested_missing_periods"]), len(args.periods),
        landing["period_counts"].get("invalid_period", 0),
        landing["blank_required"], landing["invalid_events"],
    )], "source_path string, batch_id string, source_period string, as_of timestamp, raw_rows long, missing_periods long, requested_periods long, raw_invalid_periods long, raw_blank_required long, raw_invalid_events long"
    ).createOrReplaceTempView("earthquake_config")

    # Exact main read_files inference retained. Pin its reviewed schema because
    # changing inference on a smaller batch can otherwise change every row hash.
    raw = spark.sql("""SELECT `Date-Time`, Latitude, Longitude, Depth, Magnitude,
      Location, Month, Year FROM read_files(:path, format => 'csv', header => true,
      inferColumnTypes => true, mode => 'FAILFAST')""", args={"path": args.source_path})
    check_schema(schema_types(raw), json.loads(Path(args.hash_schema).read_text(encoding="utf-8")))
    if raw.count() != landing["raw_rows"]:
        raise ValueError("Raw Spark count differs from CSV; no target written")
    raw.createOrReplaceTempView("earthquake_raw")
    execute_sql(spark, DATASET_ROOT / "bronze/philvolcs_earthquake.sql")
    preflight_target(spark, BRONZE, BRONZE_TYPES, keys=spark.table("earthquake_bronze_candidate"))
    spark.table("earthquake_bronze_candidate").createOrReplaceTempView("earthquake_bronze_check")
    execute_sql(spark, SQL_ROOT / "monitoring/philvolcs_earthquake/bronze_dq.sql", checks=True)
    # Inference/typed Bronze cannot prove raw-text fidelity. Stop if distinct
    # raw textual observations collapse to the same legacy inferred signature.
    text = spark.read.option("header", True).option("inferSchema", False).option(
        "mode", "FAILFAST"
    ).csv(args.source_path).select(*SOURCE_COLUMNS)
    raw_distinct = text.distinct().count()
    typed_distinct = spark.table("earthquake_bronze_observations").select("source_signature").distinct().count()
    if raw_distinct != typed_distinct:
        raise ValueError("Distinct raw text collapses under legacy inference; reviewed hash migration required")
    # Check integer period text before legacy Year casts can hide a fraction.
    # Recreate the pinned inferred serialization solely to relate text evidence
    # to existing hashes; keep the original file as the fidelity authority.
    text_hash = F.sha2(F.concat_ws("||", *[
        F.col(f"`{field.name}`").cast(field.dataType) for field in raw.schema.fields
    ]), 256)
    raw_period_issues = text.withColumn("_row_hash", text_hash).select(
        "_row_hash", (~F.coalesce(F.trim(F.col("Year")).rlike("^[0-9]{4}$"), F.lit(False))).cast("int").alias("bad_year")
    ).groupBy("_row_hash").agg(F.max("bad_year").cast("boolean").alias("_raw_period_invalid"))
    if not args.check_only:
        record_landing_contract(args.source_path, landing, schema_types(raw))
    report({"raw_rows": landing["raw_rows"], "distinct_raw_observations": raw_distinct,
            "repeated_raw_rows": landing["raw_rows"] - raw_distinct})
    # Candidate versus existing business values, with first-ingestion lineage intact.
    if spark.catalog.tableExists(BRONZE):
        existing = spark.table(BRONZE)
        incoming = spark.table("earthquake_bronze_candidate")
        joined = incoming.alias("s").join(existing.alias("t"), "_row_hash")
        changed = " OR ".join(f"NOT(s.`{c}` <=> t.`{c}`)" for c in SOURCE_COLUMNS)
        if joined.where(F.expr(changed)).limit(1).count():
            raise ValueError("Existing hash has different values; no target written")
        if args.check_only:
            # A dry candidate must also keep actual first-ingestion provenance.
            existing.select(*BRONZE_TYPES).join(incoming.select("_row_hash"),
                "_row_hash", "left_semi").unionByName(
                    incoming.select(*BRONZE_TYPES).join(existing.select("_row_hash"),
                        "_row_hash", "left_anti")
                ).createOrReplaceTempView("earthquake_bronze_check")
    if digest(args.source_path) != args.source_sha256:
        raise ValueError("Landing mutated during candidate build")
    if not args.check_only:
        merge_candidate(spark, BRONZE, "earthquake_bronze_candidate", list(BRONZE_TYPES))
        spark.table(BRONZE).join(spark.table("earthquake_bronze_candidate").select("_row_hash"),
                                 "_row_hash", "left_semi").createOrReplaceTempView("earthquake_bronze_check")
        execute_sql(spark, SQL_ROOT / "monitoring/philvolcs_earthquake/bronze_dq.sql", checks=True)
    if args.stage == "bronze":
        return

    bronze = spark.table("earthquake_bronze_check")
    if args.reprocess:
        if args.check_only and not spark.catalog.tableExists(BRONZE):
            raise ValueError("Historical reprocess needs an existing Bronze target")
        bronze = spark.table(BRONZE)
    bronze = bronze.join(raw_period_issues, "_row_hash", "left")
    # Candidate scope is only the explicitly landed file's hashes. MERGE updates
    # new/changed rows; unchanged lineage/timestamps remain intact. This also
    # retries a previously blocked reference without silently skipping its rows.
    # Historical reference changes require deliberate --reprocess.
    bronze.createOrReplaceTempView("earthquake_scope")
    scoped_rows = bronze.count()
    report({"silver_scope_observations": scoped_rows, "mode": "historical" if args.reprocess else "incremental"})
    if not scoped_rows:
        raise ValueError("Empty requested Silver scope; no Silver target written")
    build_candidates(spark, args.source_period)
    execute_sql(spark, DATASET_ROOT / "silver/philvolcs_earthquake_clean.sql")
    candidate = spark.table("earthquake_silver_candidate")
    candidate.createOrReplaceTempView("earthquake_silver_check")
    preflight_target(spark, SILVER, schema_types(candidate), nullable=True, keys=bronze)
    execute_sql(spark, SQL_ROOT / "monitoring/philvolcs_earthquake/silver_dq.sql", checks=True)
    if digest(args.source_path) != args.source_sha256:
        raise ValueError("Landing mutated before Silver write")
    if not args.check_only:
        merge_candidate(spark, SILVER, "earthquake_silver_candidate", candidate.columns, update=True)
        spark.table(SILVER).join(bronze.select("_row_hash"), "_row_hash", "left_semi").createOrReplaceTempView(
            "earthquake_silver_check"
        )
        execute_sql(spark, SQL_ROOT / "monitoring/philvolcs_earthquake/silver_dq.sql", checks=True)
    # Bounded summary only. UTC lag uses the publisher's UTC+8 listing convention.
    report(spark.sql("""SELECT count(*) AS observations, count_if(has_dq_issues) AS flagged,
      count_if(psgc_candidate_status = 'single_candidate') AS exact_candidates,
      max(event_time) AS max_source_wall_time,
      max(to_utc_timestamp(event_time, 'Asia/Manila')) AS max_event_utc,
      max(_ingested_at) AS max_ingestion_utc,
      max(timestampdiff(SECOND, to_utc_timestamp(event_time, 'Asia/Manila'), _ingested_at))
        AS maximum_ingestion_lag_seconds,
      'Measured only; no cadence/SLA approved' AS timeliness_status
      FROM earthquake_silver_check""").first().asDict())
    if args.stage == "gold":
        # Reached only after Bronze and Silver stop checks and verification pass.
        # Gold/dimension targets are never created or changed by this diagnostic.
        prepare_gold_review(spark)
        execute_sql(spark, SQL_ROOT / "gold/build_fact_earthquake_event.sql", checks=True)


if __name__ == "__main__":
    main()
