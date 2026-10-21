# Data quality results

Build and layer-gate checks share `<catalog>.monitoring.dq_result` with eight columns: run_id, check_id, table_name, failed_count, severity, disposition, checked_at and validation_result.

The MERGE key is `(run_id, check_id, table_name)`. A same-batch retry replaces the corresponding result; it does not prove an entire pipeline run succeeded. Use Databricks Jobs task state and the selected run_id together. Latest-per-check views can mix deliveries.

Gate metadata in validation_result includes engine, GX version, source, layer, rule type, action, rows checked and sample keys. Dispositions are PASS, WARN, BLOCKED or ERROR. Blocking/error findings stop dependent tasks, without rolling back earlier writes.

failed_count is a failure metric: row failures, a row-count difference, or one failed contract where GX supplies no row metric. A failed nonempty-table rule reports at least one, even though the empty table has zero rows. PASS reports zero.

Legacy standalone monitoring scripts are not evidence of scheduled execution. The maintained graph is in resources/jobs/ahon_job.yml. See [gates](dq-gates.md) and [run evidence](../operations/pipeline-runs.md).
