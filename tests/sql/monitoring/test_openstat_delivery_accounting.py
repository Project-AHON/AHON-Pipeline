"""The OpenSTAT silver gate accounts for one delivery at observation grain, so valid refreshes do not warn."""
import ast
from datetime import datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
GATE = ROOT / "src/sql/monitoring/psa_population/openstat_silver_dq_gate.py"
BRONZE = "_batch_id string, _ingested_at timestamp, _observation_id string"
LANDED = "_observation_id string"
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
def delivery_accounting():
    """Load the gate's own function without running the gate."""
    tree = ast.parse(GATE.read_text(encoding="utf-8"))
    selected = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "delivery_accounting"]
    namespace = {}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(GATE), "exec"), namespace)
    return namespace["delivery_accounting"]


def unaccounted(spark, check, bronze, silver, rejects, batch_id):
    frames = [spark.createDataFrame(bronze, BRONZE)] + [spark.createDataFrame([(o,) for o in ids], LANDED)
                                                        for ids in (silver, rejects)]
    row = check(*frames, batch_id).first()
    return row.delivery_rows, row.rows_unaccounted


# Two valid deliveries of the same three locations. Silver merges by location and census year,
# so after delivery 2 it holds delivery 2's observations only.
BRONZE_ROWS = [("delivery-1", FIRST, f"d1-{n}") for n in "abc"] + [("delivery-2", SECOND, f"d2-{n}") for n in "abc"]


def test_second_valid_delivery_does_not_warn(spark, delivery_accounting):
    assert unaccounted(spark, delivery_accounting, BRONZE_ROWS, ["d2-a", "d2-b", "d2-c"], [], "delivery-2") == (3, 0)


def test_rejected_row_also_in_silver_is_counted_once(spark, delivery_accounting):
    # The writer can upsert a rejected row into silver as well as into the reject table
    assert unaccounted(spark, delivery_accounting, BRONZE_ROWS, ["d2-a", "d2-b", "d2-c"], ["d2-c"], "delivery-2") == (3, 0)


def test_rejected_row_only_in_reject_table_is_accounted(spark, delivery_accounting):
    assert unaccounted(spark, delivery_accounting, BRONZE_ROWS, ["d2-a", "d2-b"], ["d2-c"], "delivery-2") == (3, 0)


def test_observation_missing_from_both_tables_is_reported(spark, delivery_accounting):
    assert unaccounted(spark, delivery_accounting, BRONZE_ROWS, ["d2-a", "d2-b"], [], "delivery-2") == (3, 1)


def test_notebook_run_checks_the_latest_delivery(spark, delivery_accounting):
    assert unaccounted(spark, delivery_accounting, BRONZE_ROWS, ["d2-a", "d2-b", "d2-c"], [], "interactive") == (3, 0)
