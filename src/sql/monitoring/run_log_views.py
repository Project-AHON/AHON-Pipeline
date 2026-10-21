"""
Pipeline run log views.

Creates two read-only views in monitoring for the pipeline runs dashboard:
- monitoring.task_run_log: one row per task per job run (start, end, duration,
  final state, attempts).
- monitoring.run_log: one row per job run (start, end, duration, final state,
  task counts by state, and DQ counts from monitoring.dq_result matched on batch_id).

Both read the Databricks system tables system.lakeflow.jobs, job_run_timeline
and job_task_run_timeline, so the job's run_as user needs SELECT on them. Only
runs of the AHON bundle jobs (ahon_pipeline, ahon_catalog_setup,
ahon_data_quality) whose catalog parameter is this catalog are kept.

The timeline tables split a run or task into time slices. result_state is NULL
on every slice except the last one of a finished run or task, so a NULL final
state means "still running", and the end of its latest slice is only the last
time it was seen, not its end. ended_at and duration_seconds are therefore set
only once a final state exists; last_seen_at and elapsed_seconds are always set.

This is a Python file, not SQL, because the catalog name has to be written into
the view: a view cannot use a parameter, and current_catalog() would depend on
the reader's session.

Safe to rerun: CREATE OR REPLACE only changes the view definitions.
"""

JOB_NAMES = "ahon_(pipeline|catalog_setup|data_quality)$"


def view_statements(catalog, lakeflow="system.lakeflow"):
    """The CREATE VIEW statements; lakeflow is the schema holding the system tables."""
    # Latest name per job; job_parameters holds {"default": ..., "value": ...} per parameter.
    # batch_id falls back to the job run id when the stored value is the unresolved '{{job.run_id}}'.
    ahon_job_runs = f"""
jobs AS (
  SELECT workspace_id, job_id, job_name
  FROM (
    SELECT workspace_id, job_id, name AS job_name,
           row_number() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS name_rank
    FROM {lakeflow}.jobs
  )
  WHERE name_rank = 1
),
run_slices AS (
  SELECT r.*, j.job_name,
         r.job_parameters['batch_id']['value'] AS batch_param
  FROM {lakeflow}.job_run_timeline r
  JOIN jobs j ON r.workspace_id = j.workspace_id AND r.job_id = j.job_id
  WHERE r.job_parameters['catalog']['value'] = '{catalog}'
    AND j.job_name RLIKE '{JOB_NAMES}'
),
runs AS (
  SELECT
    workspace_id,
    job_id,
    job_name,
    run_id AS job_run_id,
    CASE WHEN max(batch_param) IS NULL OR max(batch_param) LIKE '{{{{%'
         THEN CAST(run_id AS STRING) ELSE max(batch_param) END AS batch_id,
    max(trigger_type)                         AS trigger_type,
    max(run_type)                             AS run_type,
    min(period_start_time)                    AS started_at,
    max(period_end_time)                      AS last_seen_at,
    max_by(result_state, period_end_time)     AS result_state,
    max_by(termination_code, period_end_time) AS termination_code
  FROM run_slices
  GROUP BY workspace_id, job_id, job_name, run_id
)"""

    # Create the task run log view
    # Groups a task's time slices (and retries) back into one row per task per job run.
    # result_state is the latest slice's state: SUCCEEDED, FAILED, ERROR, TIMED_OUT,
    # SKIPPED (not selected with --only), BLOCKED (an upstream task, e.g. a DQ gate, failed),
    # CANCELLED, or NULL while the task is still running.
    task_view = f"""
CREATE OR REPLACE VIEW {catalog}.monitoring.task_run_log
COMMENT 'One row per task per AHON job run, from system.lakeflow tables'
AS
WITH {ahon_job_runs},
task_slices AS (
  SELECT
    r.job_name,
    r.batch_id,
    t.job_run_id,
    t.task_key,
    min(t.period_start_time)                      AS started_at,
    max(t.period_end_time)                        AS last_seen_at,
    max_by(t.result_state, t.period_end_time)     AS result_state,
    max_by(t.termination_code, t.period_end_time) AS termination_code,
    count(DISTINCT t.run_id)                      AS attempts
  FROM {lakeflow}.job_task_run_timeline t
  JOIN runs r ON t.workspace_id = r.workspace_id AND t.job_id = r.job_id AND t.job_run_id = r.job_run_id
  GROUP BY r.job_name, r.batch_id, t.job_run_id, t.task_key
)
SELECT
  job_name,
  batch_id,
  job_run_id,
  task_key,
  started_at,
  CASE WHEN result_state IS NOT NULL THEN last_seen_at END AS ended_at,
  CASE WHEN result_state IS NOT NULL
       THEN timestampdiff(SECOND, started_at, last_seen_at) END AS duration_seconds,
  last_seen_at,
  timestampdiff(SECOND, started_at, last_seen_at) AS elapsed_seconds,
  result_state,
  termination_code,
  attempts
FROM task_slices
"""

    # Create the run log view
    # One row per job run. The task counters add up to tasks_total; tasks_other catches
    # any final state not listed. Runs that reuse a batch_id share one set of DQ counts,
    # because dq_result keeps one row per run_id, check and table.
    run_view = f"""
CREATE OR REPLACE VIEW {catalog}.monitoring.run_log
COMMENT 'One row per AHON job run, from system.lakeflow tables and monitoring.dq_result'
AS
WITH {ahon_job_runs},
tasks AS (
  SELECT job_run_id,
         count(*)                                                    AS tasks_total,
         count_if(result_state = 'SUCCEEDED')                        AS tasks_succeeded,
         count_if(result_state IN ('FAILED', 'ERROR', 'TIMED_OUT'))  AS tasks_failed,
         count_if(result_state = 'SKIPPED')                          AS tasks_skipped,
         count_if(result_state = 'BLOCKED')                          AS tasks_blocked,
         count_if(result_state = 'CANCELLED')                        AS tasks_cancelled,
         count_if(result_state IS NULL)                              AS tasks_running,
         count_if(result_state NOT IN ('SUCCEEDED', 'FAILED', 'ERROR', 'TIMED_OUT',
                                       'SKIPPED', 'BLOCKED', 'CANCELLED')) AS tasks_other
  FROM {catalog}.monitoring.task_run_log
  GROUP BY job_run_id
),
dq AS (
  SELECT run_id,
         count_if(disposition = 'PASS')    AS dq_pass,
         count_if(disposition = 'WARN')    AS dq_warn,
         count_if(disposition = 'BLOCKED') AS dq_blocked,
         count_if(disposition = 'ERROR')   AS dq_error
  FROM {catalog}.monitoring.dq_result
  GROUP BY run_id
)
SELECT
  r.job_name, r.batch_id, r.job_run_id, r.trigger_type, r.run_type,
  r.started_at,
  CASE WHEN r.result_state IS NOT NULL THEN r.last_seen_at END AS ended_at,
  CASE WHEN r.result_state IS NOT NULL
       THEN timestampdiff(SECOND, r.started_at, r.last_seen_at) END AS duration_seconds,
  r.last_seen_at,
  timestampdiff(SECOND, r.started_at, r.last_seen_at) AS elapsed_seconds,
  r.result_state, r.termination_code,
  coalesce(t.tasks_total, 0) AS tasks_total, coalesce(t.tasks_succeeded, 0) AS tasks_succeeded,
  coalesce(t.tasks_failed, 0) AS tasks_failed, coalesce(t.tasks_skipped, 0) AS tasks_skipped,
  coalesce(t.tasks_blocked, 0) AS tasks_blocked, coalesce(t.tasks_cancelled, 0) AS tasks_cancelled,
  coalesce(t.tasks_running, 0) AS tasks_running, coalesce(t.tasks_other, 0) AS tasks_other,
  coalesce(d.dq_pass, 0) AS dq_pass, coalesce(d.dq_warn, 0) AS dq_warn,
  coalesce(d.dq_blocked, 0) AS dq_blocked, coalesce(d.dq_error, 0) AS dq_error
FROM runs r
LEFT JOIN tasks t ON r.job_run_id = t.job_run_id
LEFT JOIN dq d ON r.batch_id = d.run_id
"""
    return [task_view, run_view]


if __name__ == "__main__":  # run_python.py runs this file as __main__ with spark and PIPELINE_CATALOG set
    CATALOG = globals()["PIPELINE_CATALOG"]
    for statement in view_statements(CATALOG):
        spark.sql(statement)
    print(f"Created {CATALOG}.monitoring.task_run_log and {CATALOG}.monitoring.run_log")
