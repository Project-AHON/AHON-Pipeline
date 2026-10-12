"""CMCI source observations and transparent component outputs; no preparedness score."""

import base64
import csv
import hashlib
import json
import sys
import tempfile
from pathlib import Path

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from cmci_common import (
    APPROVED_INDICATORS,
    INDICATOR_PILLAR_LOOKUP,
    PORTAL_URL,
    PROCESS_URL,
    create_http_session,
)
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
    years = [str(y) for y in range(args.start_year, args.end_year + 1)]
    with tempfile.TemporaryDirectory() as temp, create_http_session() as session:
        session.get(PORTAL_URL, timeout=60).raise_for_status()
        files = []
        for offset in range(0, len(mappings), 10):
            batch = mappings[offset : offset + 10]
            payload = [("chk-lgu[]", m["cmci_name"]) for m in batch]
            payload += [("chk-year[]", y) for y in years]
            payload += [("chk-indicators[]", c) for c in APPROVED_INDICATORS.values()]
            response = session.post(PROCESS_URL, data=payload, timeout=60)
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
        publish_batch(directory, files)


def bronze(spark, args):
    from pyspark.sql import functions as F

    directory = batch_path(args, "cmci_data_portal")
    manifest = completed_batch(directory)
    records = []
    for item in manifest["files"]:
        file = directory / item["name"]
        payload = json.loads(file.read_text(encoding="utf-8"))
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
    mappings = reviewed_mapping(args.mapping_file, args.source_period)
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
    upsert(
        spark,
        mapping,
        f"{args.catalog}.reference.cmci_lgu_map",
        ["psgc_code", "source_period"],
        "NOT(t.cmci_name <=> s.cmci_name) OR NOT(t.evidence <=> s.evidence)",
    )
    check(
        spark,
        args.catalog,
        args.batch_id,
        "mapping_snapshot",
        "reference.cmci_lgu_map",
        mapping.join(
            spark.table(f"{args.catalog}.gold.dim_lgu").where(
                F.col("source_period") == args.source_period
            ),
            "psgc_code",
            "left_anti",
        ).count(),
    )


def silver(spark, args):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    directory = batch_path(args, "cmci_data_portal")
    digests = [item["sha256"] for item in completed_batch(directory)["files"]]
    source = spark.table(f"{args.catalog}.bronze.cmci_raw_indicator_batch_html").where(
        F.col("_source_digest").isin(digests)
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
        (code, label, INDICATOR_PILLAR_LOOKUP[label], "selected_35_v1")
        for label, code in APPROVED_INDICATORS.items()
    ]
    dim = spark.createDataFrame(
        rows,
        "indicator_code string, indicator_name string, pillar_name string, metric_version string",
    )
    upsert(
        spark,
        dim,
        f"{args.catalog}.gold.dim_cmci_indicator",
        ["indicator_code"],
        "NOT(t.indicator_name <=> s.indicator_name)",
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
            F.sum("raw_score").alias("pillar_score_raw"),
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
    ).withColumn("metric_version", F.lit("selected_35_v1"))
    unique(
        spark,
        args.catalog,
        args.batch_id,
        result,
        ["psgc_code", "year", "pillar_name"],
        "platinum.cmci_pillar_score",
    )
    upsert(
        spark,
        result,
        f"{args.catalog}.platinum.cmci_pillar_score",
        ["psgc_code", "year", "pillar_name"],
        "NOT(t.pillar_score_raw <=> s.pillar_score_raw) OR NOT(t.available_indicator_count <=> s.available_indicator_count)",
    )


def main():
    p = parser(__doc__)
    p.add_argument(
        "--stage",
        choices=["extract", "bronze", "silver", "gold", "platinum"],
        required=True,
    )
    p.add_argument("--mapping-file", required=True)
    p.add_argument("--source-period", default="Q2_2024")
    p.add_argument("--start-year", type=int, default=2014)
    p.add_argument("--end-year", type=int, default=2024)
    args = p.parse_args()
    if not 2014 <= args.start_year <= args.end_year <= 2024:
        raise ValueError(
            "Supported CMCI window is 2014–2024; review new source contract before extending"
        )
    if args.stage == "extract":
        extract(args)
    else:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        globals()[args.stage](spark, args)


if __name__ == "__main__":
    main()
