# Pipeline runs and evidence

Databricks Jobs is the source of task execution state. `monitoring.dq_result` stores individual build-check results; it is not a complete pipeline-run ledger. The current setup does not create `monitoring.pipeline_run`.

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

## DQ evidence

Use the selected `run_id` when reviewing `monitoring.dq_result`. WARN findings require an explanation. BLOCKED findings require investigation and rerunning the affected path after the cause is corrected. Earlier successful statements and tasks are not rolled back.

See [DQ results](../data-quality/dq-result.md) and the [runbook](runbook.md).
