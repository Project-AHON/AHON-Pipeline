"""
Pipeline run log views.

Creates two read-only views in monitoring for the pipeline runs dashboard:
- monitoring.task_run_log: one row per task per job run (start, end, duration,
  final state, attempts).
- monitoring.run_log: one row per job run (start, end, duration, final state,
  task counts, and DQ counts from monitoring.dq_result matched on batch_id).

Both read the Databricks system tables system.lakeflow.jobs, job_run_timeline
and job_task_run_timeline, so the job's run_as user needs SELECT on them. Runs
appear a few minutes after they happen. Only runs of the AHON bundle jobs
(ahon_pipeline, ahon_catalog_setup, ahon_data_quality) whose catalog parameter
is this catalog are kept.

This is a Python file, not SQL, because the catalog name has to be written into
the view: a view cannot use a parameter, and current_catalog() would depend on
the reader's session.

Safe to rerun: CREATE OR REPLACE only changes the view definitions.
"""

CATALOG = globals()["PIPELINE_CATALOG"]
JOB_NAMES = "ahon_(pipeline|catalog_setup|data_quality)$"

# Latest name per job; job_parameters is a map of {"default": ..., "value": ...} per parameter.
# batch_id falls back to the job run id when the stored value is the unresolved '{{job.run_id}}'.
AHON_JOB_RUNS = f"""
jobs AS (
  SELECT workspace_id, job_id, name AS job_name
  FROM system.lakeflow.jobs
  QUALIFY row_number() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
),
run_slices AS (
  SELECT r.*, j.job_name,
         r.job_parameters['batch_id']['value'] AS batch_param
  FROM system.lakeflow.job_run_timeline r
  JOIN jobs j USING (workspace_id, job_id)
  WHERE r.job_parameters['catalog']['value'] = '{CATALOG}'
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
    max(period_end_time)                      AS ended_at,
    max_by(result_state, period_end_time)     AS result_state,
    max_by(termination_code, period_end_time) AS termination_code
  FROM run_slices
  GROUP BY workspace_id, job_id, job_name, run_id
)"""

# Create the task run log view
# The system table splits long tasks into time slices; group them back into one
# row per task per job run. result_state is the last slice's state (null while
# running): SUCCEEDED, FAILED, SKIPPED (e.g. after a gate BLOCKs) or CANCELLED.
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.monitoring.task_run_log
COMMENT 'One row per task per AHON job run, from system.lakeflow tables'
AS
WITH {AHON_JOB_RUNS}
SELECT
  r.job_name,
  r.batch_id,
  t.job_run_id,
  t.task_key,
  min(t.period_start_time)                      AS started_at,
  max(t.period_end_time)                        AS ended_at,
  timestampdiff(SECOND, min(t.period_start_time), max(t.period_end_time)) AS duration_seconds,
  max_by(t.result_state, t.period_end_time)     AS result_state,
  max_by(t.termination_code, t.period_end_time) AS termination_code,
  count(DISTINCT t.run_id)                      AS attempts
FROM system.lakeflow.job_task_run_timeline t
JOIN runs r ON t.workspace_id = r.workspace_id AND t.job_id = r.job_id AND t.job_run_id = r.job_run_id
GROUP BY r.job_name, r.batch_id, t.job_run_id, t.task_key
""")

# Create the run log view
# One row per job run, with task counts from task_run_log and DQ counts from
# dq_result. Runs that reuse a batch_id share one set of DQ counts, because
# dq_result keeps one row per run_id, check and table.
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.monitoring.run_log
COMMENT 'One row per AHON job run, from system.lakeflow tables and monitoring.dq_result'
AS
WITH {AHON_JOB_RUNS},
tasks AS (
  SELECT job_run_id,
         count(*)                                                    AS tasks_total,
         count_if(result_state = 'SUCCEEDED')                        AS tasks_succeeded,
         count_if(result_state IN ('FAILED', 'ERROR', 'TIMED_OUT'))  AS tasks_failed,
         count_if(result_state = 'SKIPPED')                          AS tasks_skipped,
         count_if(result_state = 'CANCELLED')                        AS tasks_cancelled
  FROM {CATALOG}.monitoring.task_run_log
  GROUP BY job_run_id
),
dq AS (
  SELECT run_id,
         count_if(disposition = 'PASS')    AS dq_pass,
         count_if(disposition = 'WARN')    AS dq_warn,
         count_if(disposition = 'BLOCKED') AS dq_blocked,
         count_if(disposition = 'ERROR')   AS dq_error
  FROM {CATALOG}.monitoring.dq_result
  GROUP BY run_id
)
SELECT
  r.job_name, r.batch_id, r.job_run_id, r.trigger_type, r.run_type,
  r.started_at, r.ended_at,
  timestampdiff(SECOND, r.started_at, r.ended_at) AS duration_seconds,
  r.result_state, r.termination_code,
  t.tasks_total, t.tasks_succeeded, t.tasks_failed, t.tasks_skipped, t.tasks_cancelled,
  coalesce(d.dq_pass, 0) AS dq_pass, coalesce(d.dq_warn, 0) AS dq_warn,
  coalesce(d.dq_blocked, 0) AS dq_blocked, coalesce(d.dq_error, 0) AS dq_error
FROM runs r
LEFT JOIN tasks t ON r.job_run_id = t.job_run_id
LEFT JOIN dq d ON r.batch_id = d.run_id
""")
print(f"Created {CATALOG}.monitoring.task_run_log and {CATALOG}.monitoring.run_log")
