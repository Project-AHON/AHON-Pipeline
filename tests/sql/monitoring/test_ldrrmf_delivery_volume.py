"""The LDRRMF silver gate reconciles one delivery, so a corrected rejected report does not block later runs."""
import ast
from datetime import datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
GATE = ROOT / "src/sql/monitoring/blgf_ldrrmf_annual_lgu/silver_dq_gate.py"
BRONZE = "_batch_id string, _ingested_at timestamp, _source_ref string, region string, province string, lgu_name string, lgu_type string"
SILVER = "fiscal_year int, region string, province string, lgu_name string, lgu_type string"
REJECT = "fiscal_year int, _batch_id string, lgu_name string"
FIRST = datetime(2026, 10, 1)
SECOND = FIRST + timedelta(days=7)


@pytest.fixture(scope="module")
def spark():
    pyspark = pytest.importorskip("pyspark")  # runs where Spark is installed; CI has no Spark
    try:
        return pyspark.sql.SparkSession.builder.master("local[1]").getOrCreate()
    except Exception as error:  # e.g. Databricks Connect's pyspark allows only remote sessions
        pytest.skip(f"no local Spark session: {error}")


@pytest.fixture(scope="module")
def delivery_volume_check():
    """Load the gate's own function without running the gate."""
    tree = ast.parse(GATE.read_text(encoding="utf-8"))
    selected = [node for node in tree.body
                if (isinstance(node, ast.FunctionDef) and node.name == "delivery_volume_check")
                or (isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "LGU_KEY" for t in node.targets))]
    namespace = {}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(GATE), "exec"), namespace)
    return namespace["delivery_volume_check"]


def report(batch, at, lgu_name):
    return (batch, at, "FY2024-LDRRMF-by-LGU.xlsx", "Region IV-A", "Batangas", lgu_name, "Municipality")


def unaccounted(spark, check, bronze, silver, rejects, batch_id):
    result = check(spark.createDataFrame(bronze, BRONZE), spark.createDataFrame(silver, SILVER),
                   spark.createDataFrame(rejects, REJECT), batch_id)
    return {r.fiscal_year: (r.rows_unaccounted, r.reject_rows) for r in result.collect()}


# Delivery 1 reports "Sto Tomas" and is rejected (the build stops, silver keeps nothing from it).
# Delivery 2 corrects the name to "Santo Tomas" and is built into silver.
BRONZE_ROWS = [
    report("delivery-1", FIRST, "Calaca"), report("delivery-1", FIRST, "Sto Tomas"),
    report("delivery-2", SECOND, "Calaca"), report("delivery-2", SECOND, "Santo Tomas"),
]
SILVER_ROWS = [(2024, "Region IV-A", "Batangas", "Calaca", "Municipality"),
               (2024, "Region IV-A", "Batangas", "Santo Tomas", "Municipality")]
REJECT_ROWS = [(2024, "delivery-1", "Sto Tomas")]


def test_corrected_delivery_is_not_blocked_by_the_old_rejected_report(spark, delivery_volume_check):
    assert unaccounted(spark, delivery_volume_check, BRONZE_ROWS, SILVER_ROWS, REJECT_ROWS, "delivery-2") == {2024: (0, 0)}


def test_replaying_the_corrected_delivery_still_passes(spark, delivery_volume_check):
    replay = BRONZE_ROWS + [report("delivery-2", SECOND + timedelta(hours=1), "Santo Tomas")]
    assert unaccounted(spark, delivery_volume_check, replay, SILVER_ROWS, REJECT_ROWS, "delivery-2") == {2024: (0, 0)}


def test_report_missing_from_silver_is_still_caught(spark, delivery_volume_check):
    silver = SILVER_ROWS[:1]
    assert unaccounted(spark, delivery_volume_check, BRONZE_ROWS, silver, REJECT_ROWS, "delivery-2") == {2024: (1, 0)}


def test_rejected_delivery_reports_its_rejects(spark, delivery_volume_check):
    assert unaccounted(spark, delivery_volume_check, BRONZE_ROWS, SILVER_ROWS, REJECT_ROWS, "delivery-1") == {2024: (1, 1)}


def test_notebook_run_checks_the_latest_delivery(spark, delivery_volume_check):
    assert unaccounted(spark, delivery_volume_check, BRONZE_ROWS, SILVER_ROWS, REJECT_ROWS, "interactive") == {2024: (0, 0)}
