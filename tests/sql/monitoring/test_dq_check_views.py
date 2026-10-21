"""The dashboard views keep failed task runs in the health signal until the script succeeds again."""
from datetime import datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
VIEWS_SQL = ROOT / "src/sql/00_setup/07_dq_check_views.sql"
SCRIPT = "src/sql/gold/dim_lgu.py"
START = datetime(2026, 10, 21, 1, 0, 0)
SCHEMA = (
    "run_id string, check_id string, table_name string, failed_count long, severity string, "
    "disposition string, checked_at timestamp, validation_result string"
)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pyspark = pytest.importorskip("pyspark")  # runs where Spark is installed; CI has no Spark
    warehouse = tmp_path_factory.mktemp("warehouse")
    try:
        session = (pyspark.sql.SparkSession.builder.master("local[1]")
                   .config("spark.sql.warehouse.dir", str(warehouse))
                   .config("spark.sql.session.timeZone", "UTC")
                   .getOrCreate())
    except Exception as error:  # e.g. Databricks Connect's pyspark allows only remote sessions
        pytest.skip(f"no local Spark session: {error}")
    session.sql("CREATE DATABASE IF NOT EXISTS monitoring")
    yield session
    session.sql("DROP DATABASE IF EXISTS monitoring CASCADE")


def current_status(spark, rows):
    """Load dq_result rows, create the views from the repo SQL and return dq_check_status."""
    spark.createDataFrame(rows, SCHEMA).write.mode("overwrite").saveAsTable("monitoring.dq_result")
    for statement in VIEWS_SQL.read_text(encoding="utf-8").split("-- COMMAND ----------"):
        sql = "\n".join(line for line in statement.splitlines() if not line.strip().startswith("--")).strip()
        if sql:
            spark.sql(sql.rstrip(";"))
    return {(r.check_stage, r.table_name, r.check_id): r for r in spark.table("monitoring.dq_check_status").collect()}


def gate_pass(at):
    meta = '{"success": true, "meta": {"dq_run_id": "r", "source_name": "psa_psgc", "layer": "gold", "rule_type": "null"}}'
    return ("run-1", "psa_psgc_gold_psgc_code_not_null", "gold.dim_lgu", 0, "ERROR", "PASS", at, meta)


def task_run(run_id, failed, at):
    return (run_id, "python_transformation", SCRIPT, failed, "ERROR", "BLOCKED" if failed else "PASS", at, "{}")


def test_failed_task_is_a_critical_failure(spark):
    status = current_status(spark, [gate_pass(START), task_run("run-2", 1, START + timedelta(hours=3))])
    failed = status[("Task run", SCRIPT, "python_transformation")]
    assert failed.is_critical and failed.status == "BLOCKED"
    assert failed.dataset == "PSGC" and failed.layer == "task" and failed.check_type == "execution"
    assert sum(r.is_critical for r in status.values()) == 1


def test_task_run_clears_when_the_script_succeeds_again(spark):
    status = current_status(spark, [
        gate_pass(START),
        task_run("run-2", 1, START + timedelta(hours=3)),
        task_run("run-3", 0, START + timedelta(hours=5)),
    ])
    assert not any(stage == "Task run" for stage, _, _ in status)
    assert not any(r.is_critical for r in status.values())


def test_successful_task_runs_do_not_count_as_checks(spark):
    status = current_status(spark, [gate_pass(START), task_run("run-1", 0, START)])
    assert list(status) == [("DQ gate", "gold.dim_lgu", "psa_psgc_gold_psgc_code_not_null")]
