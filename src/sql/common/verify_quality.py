"""Final job barrier: verify current-run GX evidence and actual terminal outputs."""

import json

from quality import QualityError, check
from runtime import parser

# One success marker from each terminal branch. ALL_SUCCESS dependencies guarantee
# the writers finished; these markers prevent stale tables from passing the audit.
REQUIRED_RESULTS = {
    ("no_population_fanout", "gold.fact_population_total"),
    ("sql_execution", "src/sql/platinum/population_components.sql"),
    ("sql_execution", "src/sql/platinum/funding_components.sql"),
    ("sql_execution", "src/sql/platinum/earthquake_components.sql"),
    ("unique_psgc_code_year_pillar_name", "platinum.cmci_pillar_coverage"),
    ("osm_invalid_geometry", "silver.osm_critical_infrastructures_clean"),
}

OUTPUT_KEYS = {
    "gold.fact_population_total": ["population_fact_key"],
    "platinum.population_components": ["psgc_code", "population_year"],
    "platinum.funding_components": [
        "fiscal_year", "region", "province", "lgu_name", "lgu_type"
    ],
    "platinum.earthquake_observation_summary": [
        "source_year", "source_month", "source_period"
    ],
    "platinum.cmci_pillar_coverage": ["psgc_code", "year", "pillar_name"],
    "silver.osm_critical_infrastructures_clean": ["_observation_id"],
}


def evidence_failures(rows):
    """Validate bounded monitoring records; malformed/missing evidence fails closed."""
    failures = []
    present = set()
    for row in rows:
        # A repaired audit must re-evaluate its own prior failure, rather than
        # treating that historical verdict as an immutable source-rule failure.
        if row["check_id"].startswith("pipeline_audit_"):
            continue
        key = (row["check_id"], row["table_name"])
        try:
            result = json.loads(row["validation_result"])
            count = row["failed_count"]
            severity = row["severity"]
            success = count == 0
            disposition = "PASS" if success else "BLOCKED" if severity == "ERROR" else "WARN"
            meta = result["meta"]
            expectation = result["expectation_config"]
            kwargs = expectation["kwargs"]
            valid = (
                type(count) is int and count >= 0
                and severity in {"ERROR", "WARN"}
                and row["disposition"] == disposition
                and result["success"] is success
                and meta["engine"] == "great_expectations"
                and meta["gx_version"] == "1.24.0"
                and expectation["type"] == "expect_column_values_to_be_between"
                and kwargs["column"] == "failed_count"
                and kwargs["min_value"] == kwargs["max_value"] == 0
                and expectation["meta"]["check_id"] == key[0]
                and expectation["meta"]["table_name"] == key[1]
            )
        except (TypeError, KeyError, ValueError):
            valid = False
        if not valid or row["disposition"] == "BLOCKED":
            failures.append("Invalid or blocking GX evidence: " + str(key))
        else:
            present.add(key)
    failures.extend("Missing same-run GX result: " + str(key)
                    for key in sorted(REQUIRED_RESULTS - present))
    return failures


def verify(spark, args):
    from pyspark.sql import functions as F

    # No source observations are collected: this reads only the current delivery's
    # small rule-result log. Older runs cannot satisfy the required branch markers.
    rows = [row.asDict() for row in spark.table(f"{args.catalog}.monitoring.dq_result")
            .where(F.col("run_id") == args.batch_id).limit(10001).collect()]
    failures = evidence_failures(rows)
    if len(rows) > 10000:
        failures.append("Unexpectedly large rule log; inspect before increasing the audit bound")
    for failure in failures[:20]:
        print(failure)
    check(spark, args.catalog, args.batch_id, "pipeline_audit_gx_evidence",
          "pipeline", len(failures))
    for table, keys in OUTPUT_KEYS.items():
        frame = spark.table(f"{args.catalog}.{table}")
        check(spark, args.catalog, args.batch_id, "pipeline_audit_output_nonempty",
              table, int(not frame.limit(1).count()))
        duplicates = frame.groupBy(*keys).count().where("count > 1").count()
        check(spark, args.catalog, args.batch_id, "pipeline_audit_unique_keys",
              table, duplicates)
    print(json.dumps({"status": "PASS", "batch_id": args.batch_id,
                      "gx_results_checked": len(rows), "outputs_checked": list(OUTPUT_KEYS)}))


def main():
    from pyspark.sql import SparkSession

    args = parser(__doc__).parse_args()
    spark = SparkSession.builder.getOrCreate()
    try:
        verify(spark, args)
    except QualityError:
        raise
    except Exception:
        try:
            check(spark, args.catalog, args.batch_id, "pipeline_audit_execution", "pipeline", 1)
        except QualityError:
            pass
        raise


if __name__ == "__main__":
    main()
