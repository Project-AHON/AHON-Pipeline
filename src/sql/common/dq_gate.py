# %pip install great_expectations==1.23.1
"""
Shared data quality gate for the medallion layers.

run_dq_gate() runs a list of Great Expectations (GX) rules on a Spark DataFrame
as one suite, writes one row per rule to monitoring.dq_result (the same table
the pipeline's quality.check() writes), then raises an error if any stop rule
FAILed or a check hit an ERROR. WARN rows (flag, fix and report rules) are
recorded but do not stop the run.

Each rule maps onto the dq_result columns as:
    run_id      the pipeline batch_id (run_id argument), "interactive" in a notebook
    check_id    <source_name>_<layer>_<rule>
    table_name  <layer>.<table_name>, same style as the pipeline checks
    failed_count, checked_at
    severity    ERROR for stop/quarantine rules, WARN for the rest
    disposition PASS, WARN, BLOCKED (a stop rule failed) or ERROR (the check crashed)
    validation_result  JSON with the gate details: rule_type, action, rows_checked,
                failed_pct, sample_keys, message, dq_run_id, layer, source_name, batch_ids

rule_type stays inside validation_result: a new dq_result column would break the
MERGE in quality.check(), which writes the same table. Read it back with
get_json_object(validation_result, '$.meta.rule_type').

Usage from a gate file:
    from dq_gate import rule, run_dq_gate
    E = gx.expectations
    rules = [rule(E.ExpectColumnValuesToNotBeNull(column="x"), "x_not_null", "null", "stop")]
    run_dq_gate(spark, df, rules, source_name=..., layer=..., table_name=...,
                key_columns=[...], results_table=f"{CATALOG}.monitoring.dq_result",
                run_id=RUN_ID)
"""

import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import great_expectations as gx

try:
    from quality import ensure_monitoring
except ImportError:
    # A plain notebook cannot import quality.py: create dq_result the same way it does
    def ensure_monitoring(spark, catalog):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.monitoring")
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {catalog}.monitoring.dq_result (
          run_id STRING, check_id STRING, table_name STRING, failed_count BIGINT,
          severity STRING, disposition STRING, checked_at TIMESTAMP,
          validation_result STRING) USING DELTA""")


# ============================================================
# 1. RULE SETTINGS
# ============================================================

# A rule's action sets its GX severity and the status it gets when it fails
SEVERITY = {
    "stop": "critical",
    "quarantine": "critical",
    "flag": "warning",
    "fix": "warning",
    "report": "info",
}
BLOCKING_ACTIONS = ("stop", "quarantine")

# Gate status -> dq_result.disposition, in the pipeline's words
DISPOSITION = {"PASS": "PASS", "WARN": "WARN", "FAIL": "BLOCKED", "ERROR": "ERROR"}

RULE_TYPES = (
    "null", "unique", "range", "accepted_values",
    "referential_integrity", "volume", "reconciliation", "spatial",
)

_data_source = None


def rule(expectation, rule_name, rule_type, action):
    """Tag one GX expectation with its rule name, rule_type and action."""
    if action not in SEVERITY:
        raise ValueError(f"Unknown action '{action}' for rule {rule_name}")
    if rule_type not in RULE_TYPES:
        raise ValueError(f"Unknown rule_type '{rule_type}' for rule {rule_name}")

    expectation.severity = SEVERITY[action]
    expectation.meta = {"rule": rule_name, "rule_type": rule_type, "action": action}
    return expectation


# ============================================================
# 2. GX CONNECTION
# ============================================================

def _get_data_source():
    # One in-memory GX context per Python session.
    # persist=False because serverless compute does not allow caching a DataFrame.
    global _data_source
    if _data_source is None:
        context = gx.get_context(mode="ephemeral")
        _data_source = context.data_sources.add_spark(name="ahon_spark", persist=False)
    return _data_source


def _get_batch(df, table_name):
    # Reuse the asset when the same table is checked twice in one session
    data_source = _get_data_source()
    try:
        asset = data_source.get_asset(table_name)
    except LookupError:
        asset = data_source.add_dataframe_asset(name=table_name)
    try:
        batch_definition = asset.get_batch_definition("whole_table")
    except KeyError:
        batch_definition = asset.add_batch_definition_whole_dataframe("whole_table")
    return batch_definition.get_batch(batch_parameters={"dataframe": df})


# ============================================================
# 3. ONE GX RESULT -> ONE dq_result ROW
# ============================================================

def result_status(success, action, error):
    """PASS, WARN, FAIL or ERROR for one rule."""
    if error:
        return "ERROR"
    if success:
        return "PASS"
    if action in BLOCKING_ACTIONS:
        return "FAIL"
    return "WARN"


def failed_percent(rows_failed, rows_checked):
    """rows_failed as a percent of rows_checked, 4 decimals (0 when nothing was checked)."""
    if rows_checked == 0:
        return Decimal(0)
    return round(Decimal(rows_failed) * 100 / Decimal(rows_checked), 4)


def sample_keys(details, key_columns, limit=5):
    """Up to `limit` failing rows as "key1 / key2 / ...", or failing values when GX gave no keys."""
    keys = details.get("unexpected_index_list") or []
    if keys and isinstance(keys[0], dict):
        samples = [" / ".join(str(k.get(c)) for c in key_columns) for k in keys]
    else:
        samples = details.get("partial_unexpected_list") or []
    return ", ".join(str(s) for s in samples[:limit]) or None


# ============================================================
# 4. RUN THE GATE
# ============================================================

def run_dq_gate(spark, df, rules, source_name, layer, table_name, key_columns, results_table,
                run_id=None):
    """Run the rules on df, write the results to results_table, raise on FAIL or ERROR."""
    run_id = run_id or "interactive"
    catalog = results_table.split(".")[0]
    # GX needs an active context before a suite can hold expectations
    _get_data_source()
    suite = gx.ExpectationSuite(name=f"{source_name}_{layer}_{table_name}")
    for expectation in rules:
        suite.add_expectation(expectation)

    # COMPLETE returns every failing row's key columns to the driver: fine for
    # tables of thousands of rows; use a smaller result_format for very large tables.
    results = _get_batch(df, table_name).validate(
        suite,
        result_format={"result_format": "COMPLETE", "unexpected_index_column_names": key_columns},
    )

    dq_run_id = str(uuid.uuid4())
    checked_at = datetime.now(timezone.utc)
    rows_in_table = df.count()
    batch_ids = None
    if "_batch_id" in df.columns:
        batch_ids = ",".join(sorted(str(r["_batch_id"]) for r in df.select("_batch_id").distinct().collect()))

    result_rows = []
    meta_types = {}
    for r in results.results:
        config = r.expectation_config
        meta = config.meta
        details = r.result or {}
        error = (r.exception_info or {}).get("exception_message")

        # A row count check has no failing rows: count the difference from the expected number
        if config.type == "expect_table_row_count_to_equal":
            rows_checked = rows_in_table
            rows_failed = abs(rows_in_table - config.kwargs["value"])
        else:
            rows_checked = details.get("element_count", rows_in_table)
            rows_failed = details.get("unexpected_count", 0)

        meta_types[f"{source_name}_{layer}_{meta['rule']}"] = meta["rule_type"]
        status = result_status(r.success, meta["action"], error)
        # Same shape as quality.check(): GX success flag plus a "meta" object
        # (tests/integration/verify_pipeline.py reads $.meta.engine)
        details_json = json.dumps({"success": bool(r.success), "meta": {
            "engine": "great_expectations", "gx_version": gx.__version__,
            "dq_run_id": dq_run_id, "source_name": source_name, "layer": layer,
            "rule_type": meta["rule_type"], "action": meta["action"],
            "rows_checked": rows_checked,
            "failed_pct": float(failed_percent(rows_failed, rows_checked)),
            "sample_keys": sample_keys(details, key_columns),
            "message": error, "batch_ids": batch_ids,
        }})
        result_rows.append((
            run_id, f"{source_name}_{layer}_{meta['rule']}", f"{layer}.{table_name}",
            int(rows_failed), "ERROR" if meta["action"] in BLOCKING_ACTIONS else "WARN",
            DISPOSITION[status], checked_at, details_json,
        ))

    # Same columns and MERGE key as quality.check(), so a rerun replaces its rows
    ensure_monitoring(spark, catalog)
    results_df = spark.createDataFrame(
        result_rows,
        "run_id string, check_id string, table_name string, failed_count long, severity string, "
        "disposition string, checked_at timestamp, validation_result string",
    )
    results_df.createOrReplaceTempView("dq_gate_candidate")
    spark.sql(f"""MERGE INTO {results_table} t
      USING dq_gate_candidate s ON t.run_id=s.run_id AND t.check_id=s.check_id AND t.table_name=s.table_name
      WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")

    print(f"DQ gate {layer}.{table_name}, run_id {run_id}, dq_run_id {dq_run_id}")
    for row in result_rows:
        print(f"  {row[5]:7}  {meta_types[row[1]]:22}  {row[1]}  failed={row[3]}")

    blocking = [row[1] for row in result_rows if row[5] in ("BLOCKED", "ERROR")]
    if blocking:
        raise RuntimeError(f"DQ gate failed for {layer}.{table_name} (run_id {run_id}): " + ", ".join(blocking))
    print(f"DQ gate passed for {table_name}.")
    return dq_run_id
