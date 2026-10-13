"""Small read-only Gold fixtures; export with --emit for a dev SQL warehouse.

Python tests check the executable SQL and stop gate, not a Delta MERGE that does
not yet exist. The emitted SQL executes the actual Gold checks on synthetic data.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
DATASET = ROOT / "src/sql/datasets/philvolcs_earthquake"
sys.path.insert(0, str(DATASET))
from earthquake_contract import stop_on_failures

SQL = ROOT / "src/sql/gold/build_fact_earthquake_event.sql"
SCENARIOS = ["normal", "missing_dimension", "missing_reference", "snapshot_mismatch",
             "dimension_attributes", "dimension_extra_code", "missing_lineage",
             "signature_collision", "changed_signature", "unchanged_second_run"]


def fixture_query(scenario):
    """CTEs only: no pipeline/source/target table reads or writes."""
    assert scenario in SCENARIOS
    reference = """SELECT * FROM VALUES
      ('0100101000', 'Example', 'MUN', '0100000000', '0100100000', '0100101000', cast(NULL AS STRING))
      AS r(psgc_code, area_name, geographic_level, region_code, province_code, municipality_code, barangay_code)"""
    dimension = "SELECT * FROM earthquake_gold_reference"
    if scenario == "missing_dimension":
        dimension += " WHERE false"
    elif scenario == "dimension_attributes":
        dimension = "SELECT * EXCEPT(area_name), 'Wrong name' AS area_name FROM earthquake_gold_reference"
    elif scenario == "dimension_extra_code":
        dimension += " UNION ALL SELECT '0100102000', 'Old extra town', 'MUN', '0100000000', '0100100000', '0100102000', NULL"
    if scenario == "missing_reference":
        reference += " WHERE false"
    ctes = [
        ("earthquake_config", "SELECT 'Q2_2024' AS source_period, timestamp('2026-10-12 00:00:00') AS as_of"),
        ("earthquake_gold_reference", reference),
        ("earthquake_gold_dimension", dimension),
        ("earthquake_gold_prerequisites", (f"SELECT {'false' if scenario == 'missing_reference' else 'true'} AS reference_supported, "
         f"{'false' if scenario == 'missing_dimension' else 'true'} AS dimension_supported")),
        ("fixture_values", """SELECT * FROM VALUES
          ('normal', '2024-01-01 01:00:00', 10D, 120D, 5D, 3D, false, true),
          ('unmatched', '2024-01-01 02:00:00', 11D, 121D, 5D, 3D, false, true),
          ('invalid', NULL, 10D, 120D, 5D, 3D, true, true),
          ('duplicate', '2024-01-01 03:00:00', 12D, 122D, 5D, 3D, false, true),
          ('duplicate_previous', '2024-01-01 03:00:00', 12D, 122D, 5D, 3D, false, false),
          ('correction', '2024-01-01 04:00:00', 13D, 123D, 6D, 4D, false, true),
          ('correction_previous', '2024-01-01 04:00:00', 13D, 123D, 5D, 3D, false, false),
          ('nan', '2024-01-01 05:00:00', double('NaN'), 120D, 5D, 3D, true, true),
          ('infinity', '2024-01-01 06:00:00', 10D, 120D, double('Infinity'), 3D, true, true)
          AS v(label, raw_time, latitude, longitude, depth, magnitude, has_dq_issues, in_scope)"""),
        ("fixture_typed", "SELECT *, cast(raw_time AS TIMESTAMP) AS event_time FROM fixture_values"),
        ("fixture_signatures", "SELECT *, xxhash64(event_time, latitude, longitude, depth, magnitude) AS signature FROM fixture_typed"),
    ]
    signature = "signature"
    if scenario == "signature_collision":
        signature = "CASE WHEN label = 'correction_previous' THEN (SELECT signature FROM fixture_signatures WHERE label = 'normal') ELSE signature END"
    elif scenario == "changed_signature":
        signature = "CASE WHEN label = 'normal' THEN signature + 1 ELSE signature END"
    source_ref = "cast(NULL AS STRING)" if scenario == "missing_lineage" else "'/fixture/immutable.csv'"
    snapshot = "'Q2_2026'" if scenario == "snapshot_mismatch" else "'Q2_2024'"
    # Candidate codes deliberately exist on one row; assigned psgc_code stays NULL.
    ctes += [("fixture_observations", f"""SELECT {signature} AS id, event_time,
      latitude, longitude, depth, magnitude, cast(month(event_time) AS INT) AS month,
      cast(year(event_time) AS INT) AS year, has_dq_issues,
      'philvolcs_earthquake_data' AS _source_name, {source_ref} AS _source_ref,
      timestamp('2026-10-11 00:00:00') AS _ingested_at, 'original_batch' AS _batch_id,
      sha2(label, 256) AS _row_hash, timestamp('2026-10-11 01:00:00') AS _processed_at,
      {snapshot} AS _psgc_snapshot, cast(NULL AS STRING) AS psgc_code,
      'blocked_meaning' AS psgc_match_status, cast(NULL AS STRING) AS psgc_match_method,
      CASE WHEN label = 'normal' THEN array('0100101000') ELSE cast(array() AS ARRAY<STRING>) END AS psgc_candidate_codes,
      label, in_scope FROM fixture_signatures"""),
        ("earthquake_silver_check", "SELECT * FROM fixture_observations WHERE in_scope"),
        ("earthquake_event_review", "SELECT * FROM fixture_observations")]
    checks = None
    for chunk in SQL.read_text(encoding="utf-8").split("-- COMMAND ----------"):
        if "CREATE OR REPLACE TEMP VIEW " in chunk:
            statement = chunk[chunk.index("CREATE OR REPLACE TEMP VIEW "):].strip().rstrip(";")
            name, query = statement.removeprefix("CREATE OR REPLACE TEMP VIEW ").split(" AS", 1)
            ctes.append((name.strip(), query))
        elif "WITH observation_metrics AS" in chunk:
            checks = chunk.strip().removeprefix("WITH ").rstrip(";")
    assert checks is not None
    prefix = "WITH " + ",\n".join(f"{name} AS ({query})" for name, query in ctes)
    return prefix + ",\n" + checks


def all_scenarios_query():
    queries = []
    for scenario in SCENARIOS:
        checks = fixture_query(scenario)
        queries.append(f"SELECT '{scenario}' AS scenario, checks.* FROM ({checks}) checks")
        # Same CTEs expose disposition totals without running the final blocking
        # assertion. These measured counts are observations, not event counts.
        prefix = checks.split(",\nobservation_metrics AS", 1)[0]
        accounting = prefix + """ SELECT concat('accounting_', review_disposition) AS check_name,
          'Auditability' AS attribute, 'gold.fact_earthquake_event (unmaterialized)' AS table_name,
          'synthetic scoped observations' AS scope, 'report' AS action, 0L AS failed_rows,
          count(*) AS denominator, 'Fixture disposition count' AS expected_result,
          'MEASURED' AS status, (SELECT as_of FROM earthquake_config) AS as_of
          FROM earthquake_gold_accounting GROUP BY review_disposition"""
        queries.append(f"SELECT '{scenario}' AS scenario, checks.* FROM ({accounting}) checks")
    return "\nUNION ALL\n".join(queries)


def test_no_gold_writes_without_approved_contract():
    spec = importlib.util.spec_from_file_location("earthquake_runner", DATASET / "run_earthquake.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)

    class Result:
        def collect(self):
            return [Row()]

    class Row:
        def asDict(self):
            return {"check_name": "gold_fact_contract", "action": "stop", "status": "BLOCKED"}

    class Session:
        def __init__(self):
            self.statements = []

        def sql(self, statement):
            self.statements.append(statement)
            if "WITH observation_metrics AS" in statement:
                return Result()
            return type("Empty", (), {"collect": lambda self: []})()

    session = Session()
    with pytest.raises(ValueError, match="gold_fact_contract"):
        runner.execute_sql(session, SQL, checks=True)
    assert not any("MERGE INTO" in s or "CREATE TABLE" in s or "saveAsTable" in s
                   for s in session.statements)
    assert not any("assert_true(false" in s for s in session.statements)


@pytest.mark.parametrize("status", ["FAIL", "BLOCKED", "NOT_RUN"])
def test_gold_stop_failure_prevents_progress(status):
    with pytest.raises(ValueError, match="gold_dimension_snapshot_equivalence"):
        stop_on_failures([{"check_name": "gold_dimension_snapshot_equivalence",
                           "action": "stop", "status": status}])


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_fixture_sql_is_read_only_and_uses_actual_checks(scenario):
    query = fixture_query(scenario)
    assert "gold_dimension_snapshot_equivalence" in query
    assert "gold_fact_contract" in query
    assert "MERGE INTO" not in query and "CREATE TABLE" not in query


if __name__ == "__main__" and "--emit" in sys.argv:
    print(all_scenarios_query())
