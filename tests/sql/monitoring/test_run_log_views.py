"""The run log views count every task state and give unfinished runs no end time."""
import importlib.util
from datetime import datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
CATALOG = "spark_catalog"  # the local session catalog stands in for the Unity Catalog one
START = datetime(2026, 10, 21, 1, 0, 0)
JOB = ("w1", "j1")

JOBS = "workspace_id string, job_id string, name string, change_time timestamp, delete_time timestamp"
RUNS = (
    "workspace_id string, job_id string, run_id string, period_start_time timestamp, "
    "period_end_time timestamp, trigger_type string, run_type string, result_state string, "
    "termination_code string, job_parameters map<string, map<string, string>>"
)
TASKS = (
    "workspace_id string, job_id string, run_id string, job_run_id string, task_key string, "
    "period_start_time timestamp, period_end_time timestamp, result_state string, termination_code string"
)
DQ = (
    "run_id string, check_id string, table_name string, failed_count long, severity string, "
    "disposition string, checked_at timestamp, validation_result string"
)


def load_views_module():
    spec = importlib.util.spec_from_file_location("run_log_views", ROOT / "src/sql/monitoring/run_log_views.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # not __main__, so nothing runs against Spark here
    return module


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
    for schema in ("lakeflow", "monitoring"):
        session.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    yield session
    for schema in ("lakeflow", "monitoring"):
        session.sql(f"DROP DATABASE IF EXISTS {schema} CASCADE")


def at(minutes):
    return START + timedelta(minutes=minutes)


def params(batch_id="{{job.run_id}}", catalog=CATALOG):
    return {"batch_id": {"default": "{{job.run_id}}", "value": batch_id},
            "catalog": {"default": catalog, "value": catalog}}


def run_slice(run_id, start, end, state=None, code=None, **kwargs):
    return (*JOB, run_id, at(start), at(end), "ONETIME", "JOB_RUN", state, code, params(**kwargs))


def task_slice(run_id, task_key, start, end, state=None, attempt=1):
    return (*JOB, f"{run_id}-{task_key}-{attempt}", run_id, task_key, at(start), at(end), state, None)


def build(spark, runs, tasks, dq=()):
    """Load fixture system tables and dq_result, create the views and return both as dicts."""
    spark.createDataFrame([(*JOB, "[dev crystal] ahon_pipeline", at(-60), None)], JOBS) \
        .write.mode("overwrite").saveAsTable("lakeflow.jobs")
    spark.createDataFrame(runs, RUNS).write.mode("overwrite").saveAsTable("lakeflow.job_run_timeline")
    spark.createDataFrame(tasks, TASKS).write.mode("overwrite").saveAsTable("lakeflow.job_task_run_timeline")
    spark.createDataFrame(list(dq), DQ).write.mode("overwrite").saveAsTable("monitoring.dq_result")
    for statement in load_views_module().view_statements(CATALOG, lakeflow="lakeflow"):
        spark.sql(statement)
    run_log = {r.job_run_id: r for r in spark.table("monitoring.run_log").collect()}
    task_log = {(r.job_run_id, r.task_key): r for r in spark.table("monitoring.task_run_log").collect()}
    return run_log, task_log


def counters_add_up(run):
    parts = (run.tasks_succeeded, run.tasks_failed, run.tasks_skipped, run.tasks_blocked,
             run.tasks_cancelled, run.tasks_running, run.tasks_other)
    return sum(parts) == run.tasks_total


def test_failed_gate_counts_blocked_descendants(spark):
    tasks = [
        task_slice("r1", "setup", 0, 5, "SUCCEEDED"),
        task_slice("r1", "psgc_silver", 5, 10, "SUCCEEDED"),
        task_slice("r1", "dq_psgc_silver", 10, 12, "FAILED"),
        task_slice("r1", "dim_lgu", 12, 12, "BLOCKED"),
        task_slice("r1", "dq_psgc_gold", 12, 12, "BLOCKED"),
    ]
    dq = [("r1", "psa_psgc_silver_rows", "silver.psa_psgc_clean", 3, "ERROR", "BLOCKED", at(12), "{}")]
    run_log, task_log = build(spark, [run_slice("r1", 0, 12, "ERROR", "RUN_EXECUTION_ERROR")], tasks, dq)
    run = run_log["r1"]
    assert run.batch_id == "r1"  # unresolved {{job.run_id}} falls back to the job run id
    assert (run.tasks_total, run.tasks_succeeded, run.tasks_failed, run.tasks_blocked) == (5, 2, 1, 2)
    assert run.tasks_skipped == 0 and run.tasks_running == 0 and run.tasks_other == 0
    assert counters_add_up(run)
    assert run.dq_blocked == 1
    assert task_log[("r1", "dim_lgu")].result_state == "BLOCKED"


def test_unfinished_run_has_no_end_time_until_its_final_slice(spark):
    # Two slices without a result: the run and its task are still going.
    runs = [run_slice("r2", 0, 60), run_slice("r2", 60, 80)]
    tasks = [task_slice("r2", "setup", 0, 60), task_slice("r2", "setup", 60, 80)]
    run_log, task_log = build(spark, runs, tasks)
    run, task = run_log["r2"], task_log[("r2", "setup")]
    assert run.result_state is None and run.ended_at is None and run.duration_seconds is None
    assert run.last_seen_at == at(80) and run.elapsed_seconds == 80 * 60
    assert run.tasks_running == 1 and counters_add_up(run)
    assert task.ended_at is None and task.duration_seconds is None and task.elapsed_seconds == 80 * 60

    # The final slice arrives with a result: now the run has an end and a duration.
    runs.append(run_slice("r2", 80, 105, "SUCCEEDED", "SUCCESS"))
    tasks.append(task_slice("r2", "setup", 80, 105, "SUCCEEDED"))
    run_log, task_log = build(spark, runs, tasks)
    run, task = run_log["r2"], task_log[("r2", "setup")]
    assert run.result_state == "SUCCEEDED" and run.ended_at == at(105)
    assert run.duration_seconds == 105 * 60 == run.elapsed_seconds
    assert run.tasks_succeeded == 1 and run.tasks_running == 0 and counters_add_up(run)
    assert task.ended_at == at(105) and task.duration_seconds == 105 * 60


def test_only_this_catalogs_runs_are_kept(spark):
    runs = [run_slice("r3", 0, 5, "SUCCEEDED", "SUCCESS", batch_id="crystal-run-001"),
            run_slice("r4", 0, 5, "SUCCEEDED", "SUCCESS", catalog="ahon")]
    run_log, _ = build(spark, runs, [task_slice("r3", "setup", 0, 5, "SUCCEEDED")])
    assert set(run_log) == {"r3"}
    assert run_log["r3"].batch_id == "crystal-run-001"
