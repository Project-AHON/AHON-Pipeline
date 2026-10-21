# Pipeline runs and evidence

Databricks Jobs is the source of task execution state. The `run_log_views` task makes it queryable in the catalog through two views, `monitoring.run_log` and `monitoring.task_run_log`, and the AHON Pipeline Runs dashboard shows them (see [Run log views](#run-log-views)). `monitoring.dq_result` stores individual check results.

## Evidence to retain

For each verified run, record:

- Repository commit SHA and deployed bundle target.
- Databricks job run URL and task outcomes.
- Catalog, `batch_id` and selected source periods.
- Pinned source revision and completed landing manifests.
- Check results for the selected `run_id`.
- Unresolved warnings, rejected records and skipped tasks.
- Replay comparison when claiming idempotency.

Do not record tokens or secret values. No new cloud run is claimed by this documentation update.

## Current verification record

No complete live-source pipeline run has been verified for the current commit.

| Field | Evidence |
| --- | --- |
| Commit SHA | Not yet recorded |
| Bundle target | Not yet recorded |
| Databricks job run | Not yet recorded |
| Catalog | Not yet recorded |
| Batch ID | Not yet recorded |
| Tasks verified | Not yet recorded |
| Source periods | Not yet recorded |
| Expected versus actual rows | Not yet recorded |
| DQ outcomes | Not yet recorded |
| Same-batch replay | Not yet verified |
| Evidence type | None recorded for the current commit |

Until these fields are completed from a single reproducible run, the repository must not claim that the full live-source pipeline is verified, reproducible, or ready for handoff.

## Verification record template

Copy this section for each verified run.

- **Verification date:**
- **Commit SHA:**
- **Bundle target:**
- **Databricks workspace:**
- **Job name:**
- **Job run URL or ID:**
- **Catalog:**
- **Batch ID:**
- **Source periods:**
- **Pinned source revision:**
- **Command executed:**
- **Tasks selected:**
- **Successful tasks:**
- **Failed or skipped tasks:**
- **Expected versus actual row counts:**
- **DQ PASS results:**
- **DQ WARN results and explanations:**
- **DQ BLOCKED results:**
- **Rejected or unresolved records:**
- **Replay command and result:**
- **Evidence type:** live sources or synthetic fixtures
- **Verified by:**

## Interpreting outcomes

A successful task is not evidence that every branch ran. Check skipped, failed and excluded tasks separately. A partial run verifies only its selected tasks and prerequisites. A same-batch replay verifies reuse of a completed delivery; it does not verify a new source delivery.

## Run log views

`src/sql/monitoring/run_log_views.py` (task `run_log_views`, after `setup`) creates two read-only views on the Databricks system tables `system.lakeflow.jobs`, `job_run_timeline` and `job_task_run_timeline`. They keep only runs of the AHON bundle jobs (`ahon_pipeline`, `ahon_catalog_setup`, `ahon_data_quality`) whose `catalog` parameter is the catalog the views live in. Nothing is copied: each query reads the system tables.

The timeline tables split a run or task into time slices. Only the last slice of a finished run or task has a result; earlier slices have a NULL `result_state`. So `ended_at` and `duration_seconds` stay NULL until the final result arrives, and `last_seen_at` and `elapsed_seconds` show progress while it is still running.

**`monitoring.run_log`**: one row per job run. Key: `job_run_id`.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `job_name` | string | Deployed job name | Dev targets add a prefix, for example `[dev crystal_manas] ahon_pipeline` |
| `batch_id` | string | The `batch_id` job parameter; the `run_id` in `dq_result` | The job run id when left at its default `{{job.run_id}}` |
| `job_run_id` | string | Databricks job run id | Kept as the string the system tables use |
| `trigger_type` | string | How the run started | For example `ONETIME` |
| `run_type` | string | Run type | For example `JOB_RUN` |
| `started_at` | timestamp (UTC) | Run start | |
| `ended_at` | timestamp (UTC) | Run end | NULL until the run has a final result |
| `duration_seconds` | bigint | `ended_at` minus `started_at` | Includes time in queue. NULL until the run finishes |
| `last_seen_at` | timestamp (UTC) | End of the run's latest time slice | Equals `ended_at` once the run finishes |
| `elapsed_seconds` | bigint | `last_seen_at` minus `started_at` | Time so far for a run still in progress |
| `result_state` | string | Final state of the run | `SUCCEEDED`, `ERROR`, `CANCELLED`; NULL while running |
| `termination_code` | string | Why the run ended | For example `SUCCESS`, `RUN_EXECUTION_ERROR`, `USER_CANCELLED` |
| `tasks_total` | bigint | Tasks in the run | Equals the sum of the seven task counters below |
| `tasks_succeeded` | bigint | Tasks with state `SUCCEEDED` | |
| `tasks_failed` | bigint | Tasks with state `FAILED`, `ERROR` or `TIMED_OUT` | |
| `tasks_skipped` | bigint | Tasks with state `SKIPPED` | Not selected with `--only` |
| `tasks_blocked` | bigint | Tasks with state `BLOCKED` | An upstream task failed, for example a DQ gate, so the task did not run |
| `tasks_cancelled` | bigint | Tasks with state `CANCELLED` | |
| `tasks_running` | bigint | Tasks with no final result yet | |
| `tasks_other` | bigint | Tasks with any other final state | Expected to be 0 |
| `dq_pass`, `dq_warn`, `dq_blocked`, `dq_error` | bigint | `dq_result` rows for this `batch_id`, by disposition | Includes execution markers |

**`monitoring.task_run_log`**: one row per task per job run. Key: `job_run_id`, `task_key`.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `job_name`, `batch_id`, `job_run_id` | | As in `run_log` | |
| `task_key` | string | Task name in the job | |
| `started_at` | timestamp (UTC) | Start of the task's first time slice | |
| `ended_at` | timestamp (UTC) | Task end | NULL until the task has a final result |
| `duration_seconds` | bigint | `ended_at` minus `started_at` | NULL until the task finishes |
| `last_seen_at`, `elapsed_seconds` | | As in `run_log` | |
| `result_state` | string | Final state of the task | `SUCCEEDED`, `FAILED`, `ERROR`, `TIMED_OUT`, `SKIPPED`, `BLOCKED`, `CANCELLED`; NULL while running |
| `termination_code` | string | Why the task ended | |
| `attempts` | bigint | Task runs for this task key | More than 1 after a retry or repair |

Things to know:

- The job's `run_as` identity needs `SELECT` on `system.lakeflow`.
- System table data can take up to about an hour to appear, so a new run can be missing or still show as running for a while.
- Runs that reuse a `batch_id` share one set of DQ counts, because `dq_result` keeps one row per `run_id`, check and table. Leave `batch_id` at its default to get counts per run.
- Rows written per table per run are not recorded.

## Pipeline runs dashboard

The AHON Pipeline Runs dashboard (`resources/dashboards/ahon_pipeline_runs.lvdash.json`) reads the two views. The **Runs** page shows the number of runs, average duration, when the last run ended, runs by status, duration over time and the run history. The **Tasks** page shows, for the run picked in its Run filter, each task's duration and final state; with no run picked it shows the tasks of every run. Average duration and the last run use finished runs only. Times are in UTC. Like the data quality dashboard, it is defined in `databricks.yml`, and `bundle deploy` creates it with the bundle's `catalog` and `warehouse_id`. Edit the JSON, not the dashboard in the workspace: a deploy overwrites changes made there.

## DQ evidence

Use the selected `run_id` when reviewing `monitoring.dq_result`. WARN findings require an explanation. BLOCKED findings require investigation and rerunning the affected path after the cause is corrected. Earlier successful statements and tasks are not rolled back.

See [DQ results](../data-quality/dq-result.md) and the [runbook](runbook.md).
