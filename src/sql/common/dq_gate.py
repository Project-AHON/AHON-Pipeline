# %pip install great_expectations==1.23.1
"""
Legacy standalone data quality gate for the medallion layers (decision 0018).

run_dq_gate() runs a list of Great Expectations (GX) rules on a Spark DataFrame
as one suite, appends one legacy 15-field row per rule to the supplied results table, then raises
an error if any stop rule FAILed or a check hit an ERROR. WARN rows (flag,
fix and report rules) are recorded but do not stop the run.

Usage from a gate file:
    from dq_gate import rule, run_dq_gate
    E = gx.expectations
    rules = [rule(E.ExpectColumnValuesToNotBeNull(column="x"), "x_not_null", "null", "stop")]
    run_dq_gate(spark, df, rules, source_name=..., layer=..., table_name=...,
                key_columns=[...], results_table="ahon_dev.monitoring.dq_results")
"""

import uuid
from datetime import datetime, timezone
from decimal import Decimal

import great_expectations as gx


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

def run_dq_gate(spark, df, rules, source_name, layer, table_name, key_columns, results_table):
    """Run the rules on df, append the results to results_table, raise on FAIL or ERROR."""
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

        result_rows.append((
            dq_run_id, f"{source_name}_{layer}_{meta['rule']}", meta["rule_type"],
            source_name, batch_ids, layer, table_name, checked_at,
            rows_checked, rows_failed, failed_percent(rows_failed, rows_checked),
            result_status(r.success, meta["action"], error), meta["action"],
            sample_keys(details, key_columns), error,
        ))

    # Schema copied from the table so the types always match
    results_df = spark.createDataFrame(result_rows, schema=spark.table(results_table).schema)
    results_df.write.mode("append").saveAsTable(results_table)

    print(f"DQ gate {table_name} ({layer}), dq_run_id {dq_run_id}")
    for row in result_rows:
        print(f"  {row[11]:5}  {row[1]}  rows_failed={row[9]}  {row[13] or ''}")

    blocking = [row[1] for row in result_rows if row[11] in ("FAIL", "ERROR")]
    if blocking:
        raise RuntimeError(f"DQ gate failed for {table_name} (dq_run_id {dq_run_id}): " + ", ".join(blocking))
    print(f"DQ gate passed for {table_name}.")
    return dq_run_id
