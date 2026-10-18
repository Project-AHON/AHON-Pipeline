"""CMCI source observations and transparent component outputs; no preparedness score."""

import base64
import csv
import hashlib
import json
import sys
import tempfile
import time
from pathlib import Path

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from cmci_common import create_http_session
from quality import check, unique, upsert
from runtime import (
    batch_is_complete,
    batch_path,
    completed_batch,
    merge_observations,
    parser,
    provenance,
    publish_batch,
)

APPROVED_INDICATORS = {}
INDICATOR_PILLAR_LOOKUP = {}


def configure_indicators(path):
    """Read reviewed source definitions as data; no embedded indicator identities or counts."""
    with Path(path).open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    if not rows or any(not all(row.get(key, "").strip() for key in ("indicator_code", "indicator_name", "pillar_name")) for row in rows):
        raise ValueError("Indicator CSV requires code, source name and pillar")
    if len({row["indicator_code"] for row in rows}) != len(rows) or len({row["indicator_name"] for row in rows}) != len(rows):
        raise ValueError("Indicator CSV source names and codes must be unique")
    APPROVED_INDICATORS.clear()
    APPROVED_INDICATORS.update({row["indicator_name"]: row["indicator_code"] for row in rows})
    INDICATOR_PILLAR_LOOKUP.clear()
    INDICATOR_PILLAR_LOOKUP.update({row["indicator_name"]: row["pillar_name"] for row in rows})


def parse_response(html, mappings, years):
    """Reuse #60's strict response contract; missing indicator values stay missing."""
    soup = BeautifulSoup(html, "html.parser")
    if not soup.title or "Data Portal Results" not in soup.title.get_text():
        raise ValueError("Unexpected CMCI response page")
    result, seen = [], set()
    by_name = {m["cmci_name"]: m for m in mappings}
    if len(by_name) != len(mappings):
        raise ValueError("CMCI mapping source names are not unique")
    for table in soup.find_all("table"):
        heading = table.find_previous("h2")
        label = heading.get_text(" ", strip=True) if heading else ""
        if label not in APPROVED_INDICATORS or label in seen:
            raise ValueError("Unexpected or duplicate indicator table")
        seen.add(label)
        rows = [
            [c.get_text(" ", strip=True) for c in r.find_all(["th", "td"])]
            for r in table.find_all("tr")
        ]
        rows = [r for r in rows if r]
        if (
            not rows
            or rows[0] != ["Province / LGU", *years]
            or len(rows) != len(mappings) + 1
        ):
            raise ValueError("CMCI years or row coverage changed")
        names = set()
        for row in rows[1:]:
            if len(row) != len(years) + 1 or row[0] not in by_name or row[0] in names:
                raise ValueError("Unexpected or duplicate CMCI locality")
            names.add(row[0])
            for year, value in zip(years, row[1:]):
                result.append(
                    (
                        by_name[row[0]]["psgc_code"],
                        row[0],
                        int(year),
                        APPROVED_INDICATORS[label],
                        value,
                    )
                )
        if names != set(by_name):
            raise ValueError("Incomplete CMCI locality coverage")
    if seen != set(APPROVED_INDICATORS):
        raise ValueError("Incomplete CMCI indicator coverage")
    return result


def reviewed_mapping(path, source_period):
    if not path or not Path(path).is_file():
        raise ValueError("CMCI extraction requires a readable reviewed mapping CSV for the selected PSGC snapshot")
    with Path(path).open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    if not rows:
        raise ValueError("Reviewed CMCI mapping is empty")
    import re

    for row in rows:
        if (
            not re.fullmatch(r"\d{10}", row.get("psgc_code", ""))
            or row.get("source_period") != source_period
            or row.get("reviewed", "").lower() != "true"
            or not row.get("reviewer")
            or not row.get("evidence")
            or not row.get("cmci_name")
        ):
            raise ValueError(
                "CMCI mapping requires code, snapshot, reviewer and evidence"
            )
    if len({r["psgc_code"] for r in rows}) != len(rows) or len(
        {r["cmci_name"] for r in rows}
    ) != len(rows):
        raise ValueError("Reviewed CMCI mapping is not one-to-one")
    return sorted(rows, key=lambda r: r["psgc_code"])


def extract(args):
    directory = batch_path(args, "cmci_data_portal")
    if batch_is_complete(directory):
        return completed_batch(directory)
    mappings = reviewed_mapping(args.mapping_file, args.source_period)
    if args.run_mode == "test":
        mappings = mappings[:args.test_lgu_limit]
    years = [str(y) for y in range(args.start_year, args.end_year + 1)]
    with tempfile.TemporaryDirectory() as temp, create_http_session(args.http_retries, args.http_backoff, args.portal_url, args.user_agent) as session:
        session.get(args.portal_url, timeout=args.request_timeout).raise_for_status()
        files = []
        for offset in range(0, len(mappings), args.request_batch_size):
            batch = mappings[offset : offset + args.request_batch_size]
            payload = [("chk-lgu[]", m["cmci_name"]) for m in batch]
            payload += [("chk-year[]", y) for y in years]
            payload += [("chk-indicators[]", c) for c in APPROVED_INDICATORS.values()]
            response = session.post(args.process_url, data=payload, timeout=args.request_timeout)
            response.raise_for_status()
            html = response.text
            values = parse_response(html, batch, years)
            file = Path(temp) / f"batch_{offset:05d}.json"
            file.write_text(
                json.dumps(
                    {
                        "mappings": batch,
                        "years": years,
                        "html": html,
                        "response_bytes_base64": base64.b64encode(
                            response.content
                        ).decode("ascii"),
                        "response_sha256": hashlib.sha256(response.content).hexdigest(),
                        "source_period": args.source_period,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            files.append((file, len(values)))
            time.sleep(args.request_delay)
        publish_batch(directory, files)


def bronze(spark, args):
    from pyspark.sql import functions as F

    directory = batch_path(args, "cmci_data_portal")
    manifest = completed_batch(directory)
    records = []
    for item in manifest["files"]:
        file = directory / item["name"]
        payload = json.loads(file.read_text(encoding="utf-8"))
        if hashlib.sha256(base64.b64decode(payload["response_bytes_base64"], validate=True)).hexdigest() != payload["response_sha256"]:
            raise ValueError("CMCI raw response bytes do not match their source digest")
        records.append(
            (
                payload["html"],
                json.dumps(payload["mappings"], sort_keys=True),
                json.dumps(payload["years"]),
                payload["source_period"],
                payload["response_sha256"],
                str(file),
            )
        )
    frame = spark.createDataFrame(
        records,
        "response_html string, mappings_json string, years_json string, source_period string, response_sha256 string, _source_ref string",
    )
    raw = [c for c in frame.columns if not c.startswith("_")]
    frame = provenance(frame, "cmci_data_portal", args.batch_id, raw)
    merge_observations(
        spark, frame, f"{args.catalog}.bronze.cmci_raw_indicator_batch_html"
    )
    # Persist the verified map alongside the source version, not inferred fuzzy matches.
    mappings = []
    for item in manifest["files"]:
        mappings.extend(json.loads((directory / item["name"]).read_text(encoding="utf-8"))["mappings"])
    mapping = spark.createDataFrame(
        [
            (
                m["psgc_code"],
                m["cmci_name"],
                args.source_period,
                m["reviewer"],
                m["evidence"],
            )
            for m in mappings
        ],
        "psgc_code string, cmci_name string, source_period string, reviewer string, evidence string",
    )
    dimension = spark.table(f"{args.catalog}.gold.dim_lgu").where(
        (F.col("source_period") == args.source_period)
        & F.col("geographic_level").isin("CITY", "MUN")
    )
    unique(spark, args.catalog, args.batch_id, dimension, ["psgc_code"], "cmci_dimension")
    unique(spark, args.catalog, args.batch_id, mapping, ["psgc_code", "source_period"], "cmci_mapping_codes")
    unique(spark, args.catalog, args.batch_id, mapping, ["cmci_name", "source_period"], "cmci_mapping_names")
    check(
        spark,
        args.catalog,
        args.batch_id,
        "mapping_snapshot",
        "reference.cmci_lgu_map",
        mapping.join(
            dimension,
            "psgc_code",
            "left_anti",
        ).count(),
    )
    upsert(spark, mapping, f"{args.catalog}.reference.cmci_lgu_map",
           ["psgc_code", "source_period"],
           "NOT(t.cmci_name <=> s.cmci_name) OR NOT(t.evidence <=> s.evidence)")


def silver(spark, args):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    directory = batch_path(args, "cmci_data_portal")
    digests = [item["sha256"] for item in completed_batch(directory)["files"]]
    source = spark.table(f"{args.catalog}.bronze.cmci_raw_indicator_batch_html").where(
        F.col("_source_digest").isin(digests) & (F.col("_batch_id") == args.batch_id)
    )
    records = []
    # Bound driver parsing by requested scope; HTML parsing cannot be expressed in SQL.
    for row in source.toLocalIterator():
        parsed = parse_response(
            row.response_html, json.loads(row.mappings_json), json.loads(row.years_json)
        )
        for code, name, year, indicator, raw in parsed:
            records.append(
                (
                    code,
                    name,
                    year,
                    indicator,
                    raw,
                    row.source_period,
                    row._source_ref,
                    row._row_hash,
                    row._observation_id,
                    row._ingested_at,
                    row._batch_id,
                )
            )
    if not records:
        raise ValueError("No CMCI observations in the selected landed batch")
    frame = spark.createDataFrame(
        records,
        "psgc_code string, cmci_name string, year int, indicator_code string, raw_value string, source_period string, _source_ref string, _row_hash string, _observation_id string, _ingested_at timestamp, _batch_id string",
    )
    score = F.expr(
        "CASE WHEN trim(raw_value) IN ('', '-') THEN NULL ELSE try_cast(raw_value AS DOUBLE) END"
    )
    frame = frame.withColumn("raw_score", score).withColumn(
        "has_dq_issues",
        F.expr(
            "trim(raw_value) NOT IN ('','-') AND (raw_score IS NULL OR isnan(raw_score) OR abs(raw_score) = double('Infinity') OR raw_score < 0)"
        ),
    )
    frame = frame.withColumn("_processed_at", F.current_timestamp())
    unique(spark, args.catalog, args.batch_id, frame,
           ["_observation_id", "psgc_code", "year", "indicator_code"], "cmci_parsed_observations")
    check(spark, args.catalog, args.batch_id, "cmci_parse_accounting", "silver.cmci_indicator_clean",
          abs(frame.count() - sum(item["rows"] for item in completed_batch(directory)["files"])))
    upsert(
        spark,
        frame.where("has_dq_issues"),
        f"{args.catalog}.silver.cmci_reject",
        ["_observation_id", "psgc_code", "year", "indicator_code"],
    )
    valid = frame.where("NOT has_dq_issues")
    keys = ["psgc_code", "year", "indicator_code"]
    valid = (
        valid.withColumn(
            "_rank",
            F.row_number().over(
                Window.partitionBy(*keys).orderBy(
                    F.col("_ingested_at").desc(), F.col("_observation_id").desc()
                )
            ),
        )
        .where("_rank = 1")
        .drop("_rank")
    )
    unique(
        spark, args.catalog, args.batch_id, valid, keys, "silver.cmci_indicator_clean"
    )
    upsert(
        spark,
        valid,
        f"{args.catalog}.silver.cmci_indicator_clean",
        keys,
        "s._ingested_at >= t._ingested_at AND NOT(t._observation_id <=> s._observation_id)",
    )
    check(
        spark,
        args.catalog,
        args.batch_id,
        "invalid_indicator_values",
        "silver.cmci_reject",
        frame.where("has_dq_issues").count(),
        "WARN",
    )


def gold(spark, args):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    rows = [
        (code, label, INDICATOR_PILLAR_LOOKUP[label], args.metric_version)
        for label, code in APPROVED_INDICATORS.items()
    ]
    dim = spark.createDataFrame(
        rows,
        "indicator_code string, indicator_name string, pillar_name string, metric_version string",
    )
    dimension_table = f"{args.catalog}.gold.dim_cmci_indicator"
    if spark.catalog.tableExists(dimension_table):
        existing = spark.table(dimension_table).select(*dim.columns)
        difference = existing.exceptAll(dim).unionByName(dim.exceptAll(existing))
        check(spark, args.catalog, args.batch_id, "cmci_definition_requires_review",
              dimension_table, difference.count())
    upsert(
        spark,
        dim,
        dimension_table,
        ["indicator_code"],
    )
    fact = spark.table(f"{args.catalog}.silver.cmci_indicator_clean")
    # A later invalid correction must not make the previous accepted value look current.
    # Preserve Silver history, but withhold that business key's analytical value.
    keys = ["psgc_code", "year", "indicator_code"]
    rejected = spark.table(f"{args.catalog}.silver.cmci_reject")
    masked = rejected.join(fact.select(*keys), keys, "left_semi").select(*fact.columns)
    masked = masked.withColumn("raw_score", F.lit(None).cast("double"))
    fact = (
        fact.unionByName(masked)
        .withColumn(
            "_latest",
            F.row_number().over(
                Window.partitionBy(*keys).orderBy(
                    F.col("_ingested_at").desc(), F.col("_observation_id").desc()
                )
            ),
        )
        .where("_latest = 1")
        .drop("_latest")
    )
    dimension = spark.table(f"{args.catalog}.gold.dim_lgu").where(
        (F.col("source_period") == args.source_period)
        & F.col("geographic_level").isin("CITY", "MUN")
    )
    unique(spark, args.catalog, args.batch_id, dimension, ["psgc_code"], "cmci_dimension")
    check(spark, args.catalog, args.batch_id, "cmci_snapshot_alignment", "gold.fact_cmci_indicator",
          fact.where(F.col("source_period") != args.source_period).count())
    check(
        spark,
        args.catalog,
        args.batch_id,
        "lgu_reference",
        "gold.fact_cmci_indicator",
        fact.join(dimension, "psgc_code", "left_anti").count(),
    )
    upsert(
        spark,
        fact,
        f"{args.catalog}.gold.fact_cmci_indicator",
        ["psgc_code", "year", "indicator_code"],
        "s._ingested_at >= t._ingested_at AND NOT(t._observation_id <=> s._observation_id)",
    )


def platinum(spark, args):
    from pyspark.sql import functions as F

    fact = spark.table(f"{args.catalog}.gold.fact_cmci_indicator")
    dim = spark.table(f"{args.catalog}.gold.dim_cmci_indicator")
    expected = dim.groupBy("pillar_name").agg(
        F.count("*").alias("expected_indicator_count")
    )
    scope = (
        fact.select("psgc_code", "year", "source_period").distinct().crossJoin(expected)
    )
    metrics = (
        fact.join(dim, "indicator_code")
        .groupBy("psgc_code", "year", "source_period", "pillar_name")
        .agg(
            F.count("raw_score").alias("available_indicator_count"),
            F.max("_ingested_at").alias("source_ingested_at"),
        )
    )
    result = scope.join(
        metrics, ["psgc_code", "year", "source_period", "pillar_name"], "left"
    )
    result = result.withColumn(
        "coverage_status",
        F.when(
            F.col("available_indicator_count") == F.col("expected_indicator_count"),
            "COMPLETE",
        ).otherwise("INSUFFICIENT_DATA"),
    ).withColumn("metric_version", F.lit(args.metric_version))
    unique(
        spark,
        args.catalog,
        args.batch_id,
        result,
        ["psgc_code", "year", "pillar_name"],
        "platinum.cmci_pillar_coverage",
    )
    upsert(
        spark,
        result,
        f"{args.catalog}.platinum.cmci_pillar_coverage",
        ["psgc_code", "year", "pillar_name"],
        "NOT(t.available_indicator_count <=> s.available_indicator_count) OR NOT(t.coverage_status <=> s.coverage_status) OR NOT(t.source_ingested_at <=> s.source_ingested_at)",
    )


def main():
    p = parser(__doc__)
    p.add_argument(
        "--stage",
        choices=["extract", "bronze", "silver", "gold", "platinum"],
        required=True,
    )
    p.add_argument("--mapping-file", default="")
    p.add_argument("--run-mode", choices=["test", "full"], required=True)
    p.add_argument("--indicator-file", required=True)
    p.add_argument("--test-lgu-limit", type=int, required=True)
    p.add_argument("--source-period", required=True)
    p.add_argument("--start-year", type=int, required=True)
    p.add_argument("--end-year", type=int, required=True)
    p.add_argument("--metric-version", required=True)
    p.add_argument("--portal-url")
    p.add_argument("--process-url")
    p.add_argument("--request-timeout", type=float)
    p.add_argument("--request-batch-size", type=int)
    p.add_argument("--request-delay", type=float)
    p.add_argument("--http-retries", type=int)
    p.add_argument("--http-backoff", type=float)
    p.add_argument("--user-agent")
    args = p.parse_args()
    configure_indicators(args.indicator_file)
    if args.test_lgu_limit < 1:
        raise ValueError("test-lgu-limit must be positive")
    if args.start_year > args.end_year:
        raise ValueError("start-year must not exceed end-year; source response validates requested coverage")
    if args.stage == "extract":
        if not args.portal_url or not args.process_url or not args.user_agent or args.request_timeout is None or args.request_batch_size is None or args.request_batch_size < 1 or args.request_delay is None or args.request_delay < 0 or args.http_retries is None or args.http_backoff is None:
            raise ValueError("CMCI extraction requires explicit source URLs and request/retry settings")
        extract(args)
    else:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        globals()[args.stage](spark, args)


if __name__ == "__main__":
    main()
