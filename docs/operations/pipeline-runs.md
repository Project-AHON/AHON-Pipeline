# Pipeline runs and evidence

Databricks Jobs is the source of task execution state. The `run_log_views` task makes it queryable in the catalog through two views, `monitoring.run_log` and `monitoring.task_run_log` (see [Run log views](#run-log-views)). `monitoring.dq_result` stores individual check results.

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

**`monitoring.run_log`**: one row per job run. Key: `job_run_id`.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `job_name` | string | Deployed job name | Dev targets add a prefix, e.g. `[dev crystal_manas] ahon_pipeline` |
| `batch_id` | string | The `batch_id` job parameter; the `run_id` in `dq_result` | The job run id when left at its default `{{job.run_id}}` |
| `job_run_id` | bigint | Databricks job run id | |
| `trigger_type` | string | How the run started | e.g. `ONETIME` |
| `run_type` | string | Run type | e.g. `JOB_RUN` |
| `started_at` | timestamp (UTC) | Run start | |
| `ended_at` | timestamp (UTC) | Run end | |
| `duration_seconds` | bigint | `ended_at` minus `started_at` | Includes time in queue |
| `result_state` | string | Final state of the run | `SUCCEEDED`, `ERROR`, `CANCELLED`; null while running |
| `termination_code` | string | Why the run ended | e.g. `SUCCESS`, `RUN_EXECUTION_ERROR`, `USER_CANCELLED` |
| `tasks_total` | bigint | Tasks in the run | |
| `tasks_succeeded` | bigint | Tasks with state `SUCCEEDED` | |
| `tasks_failed` | bigint | Tasks with state `FAILED`, `ERROR` or `TIMED_OUT` | |
| `tasks_skipped` | bigint | Tasks with state `SKIPPED` | After a gate BLOCKs, or not selected with `--only` |
| `tasks_cancelled` | bigint | Tasks with state `CANCELLED` | |
| `dq_pass`, `dq_warn`, `dq_blocked`, `dq_error` | bigint | `dq_result` rows for this `batch_id` by disposition | Includes execution markers |

**`monitoring.task_run_log`**: one row per task per job run. Key: `job_run_id`, `task_key`.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `job_name`, `batch_id`, `job_run_id` | | As in `run_log` | |
| `task_key` | string | Task name in the job | |
| `started_at`, `ended_at` | timestamp (UTC) | First and last time slice of the task | |
| `duration_seconds` | bigint | `ended_at` minus `started_at` | |
| `result_state` | string | Final state of the task | `SUCCEEDED`, `FAILED`, `SKIPPED`, `CANCELLED`; null while running |
| `termination_code` | string | Why the task ended | |
| `attempts` | bigint | Task runs for this task key | More than 1 after a retry or repair |

Things to know:

- The job's `run_as` user needs SELECT on `system.lakeflow`.
- System tables lag a few minutes, so a new run can be missing or show a null state for a while.
- Runs that reuse a `batch_id` share one set of DQ counts, because `dq_result` keeps one row per `run_id`, check and table. Leave `batch_id` at its default to get counts per run.
- Rows written per table per run are not recorded.

## DQ evidence

Use the selected `run_id` when reviewing `monitoring.dq_result`. WARN findings require an explanation. BLOCKED findings require investigation and rerunning the affected path after the cause is corrected. Earlier successful statements and tasks are not rolled back.

See [DQ results](../data-quality/dq-result.md) and the [runbook](runbook.md).
