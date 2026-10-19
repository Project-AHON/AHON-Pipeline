# Data quality results

Every check in the `ahon_pipeline` job writes its result to `<catalog>.monitoring.dq_result`, through `check()` in `src/sql/common/quality.py`.

- **One row is:** one check on one table in one batch
- **Key:** `run_id` + `check_id` + `table_name`. A rerun of the same check in the same batch replaces its row.
- **Created by:** the `setup` task, and again by `check()` if missing ([decision 0011](../decisions/0011-shared-gx-quality-gates.md))

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `run_id` | string | The batch that ran the check | The job's `batch_id`; by default the job run ID |
| `check_id` | string | The rule that was checked | e.g. `component_totals`, `unique_psgc_code`. `python_transformation` and `sql_execution` are execution markers for the script itself |
| `table_name` | string | Table checked | For an execution marker: the script file |
| `failed_count` | bigint | Rows that broke the rule | 0 means it passed |
| `severity` | string | How serious a failure is | `ERROR` (stops the task) or `WARN` (rows kept) |
| `disposition` | string | Result of the check | `PASS`; `WARN` (a WARN rule found rows); `BLOCKED` (an ERROR rule failed, the task stopped before writing) |
| `checked_at` | timestamp (UTC) | When the check ran | |
| `validation_result` | string | GX result as JSON | Includes the expectation metadata and GX version. NULL on rows written before this column was added |

```sql
-- Latest result of every check
SELECT table_name, check_id, severity, disposition, failed_count, checked_at
FROM monitoring.dq_result
QUALIFY row_number() OVER (PARTITION BY table_name, check_id ORDER BY checked_at DESC) = 1
ORDER BY disposition, table_name, check_id;
```

## Gate results

The layer gates ([dq-gates](dq-gates.md)) are meant to record their results here too. Their code (`run_dq_gate()` in `src/sql/common/dq_gate.py`) does not yet: it writes a separate table, `monitoring.dq_results`, created by hand with `src/sql/00_setup/07_dq_results.sql`. That table has one row per rule per gate run, with `dq_rule_id`, `rule_type`, `layer`, `rows_checked`, `rows_failed`, `failed_pct`, `status` (PASS, WARN, FAIL, ERROR), `action` and up to 5 `sample_keys`. Aligning the gates with `dq_result` comes before they are added to the job.

## Known issues

- **CMCI writes elsewhere.** `tests/sql/monitoring/cmci_quality.py` still writes to `quality.dq_results` with its own columns. Moving it is follow-up work with its owner.

## Open

- How the gates' extra fields (`layer`, `rule_type`, `action`, `sample_keys`, failed percent) fit into `dq_result`.
- Whether a per-run summary table (like CMCI's `dq_run_log`) is needed, or a query is enough.
- The draft `dq_quarantine` and `dq_audit` tables are not built.
