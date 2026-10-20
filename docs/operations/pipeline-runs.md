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

## Interpreting outcomes

A successful task is not evidence that every branch ran. Check skipped, failed and excluded tasks separately. A partial run verifies only its selected tasks and prerequisites. A same-batch replay verifies reuse of a completed delivery; it does not verify a new source delivery.

## DQ evidence

Use the selected `run_id` when reviewing `monitoring.dq_result`. WARN findings require an explanation. BLOCKED findings require investigation and rerunning the affected path after the cause is corrected. Earlier successful statements and tasks are not rolled back.

See [DQ results](../data-quality/dq-result.md) and the [runbook](runbook.md).
