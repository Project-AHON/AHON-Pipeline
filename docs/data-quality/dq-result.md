# Data quality results

Every check in the `ahon_pipeline` job writes its result to `<catalog>.monitoring.dq_result`, through `check()` in `src/sql/common/quality.py`. The layer gates write here too, through `run_dq_gate()` in `src/sql/common/dq_gate.py` (see [Gate results](#gate-results)).

- **One row is:** one check on one table in one batch
- **Key:** `run_id` + `check_id` + `table_name`. A rerun of the same check in the same batch replaces its row.
- **Created by:** the `setup` task, and again by `check()` if missing ([decision 0016](../decisions/0016-shared-gx-quality-gates.md))

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

The layer gates ([dq-gates](dq-gates.md)) write one row per rule to the same table, with the same columns and key:

| Column | Gate value |
| --- | --- |
| `run_id` | The job's `batch_id`, or `interactive` when run by hand |
| `check_id` | `<dataset>_<layer>_<rule name>`, e.g. `psa_psgc_silver_psgc_code_10_digits` |
| `table_name` | `<layer>.<table>`, e.g. `silver.psa_psgc_clean` |
| `failed_count` | Rows that broke the rule |
| `severity` | `ERROR` for `stop` and `quarantine` rules, `WARN` for the rest |
| `disposition` | `PASS`, `WARN`, `BLOCKED` (a stop rule failed) or `ERROR` (the check could not run) |
| `validation_result` | JSON: `success` plus a `meta` object with `engine`, `gx_version`, `dq_run_id`, `source_name`, `layer`, `rule_type`, `action`, `rows_checked`, `failed_pct`, up to 5 `sample_keys`, `message` and `batch_ids` |

`check_id` starts with the dataset, so gate rows and the build's own checks can be told apart. Read the gate details from the JSON:

```sql
-- Gate rules that did not pass, latest run first
SELECT run_id, table_name, check_id, disposition, failed_count,
  get_json_object(validation_result, '$.meta.rule_type') AS rule_type,
  get_json_object(validation_result, '$.meta.rows_checked') AS rows_checked,
  get_json_object(validation_result, '$.meta.sample_keys') AS sample_keys
FROM monitoring.dq_result
WHERE get_json_object(validation_result, '$.meta.dq_run_id') IS NOT NULL
  AND disposition <> 'PASS'
ORDER BY checked_at DESC;
```

## Known issues

- **CMCI writes elsewhere.** `tests/sql/monitoring/cmci_quality.py` still writes to `quality.dq_results` with its own columns. Moving it is follow-up work with its owner.

## Open

- Whether `rule_type` becomes its own column. Adding it needs `quality.py` and `dq_gate.py` changed together, because `check()` writes every column of the table.
- Whether a per-run summary table (like CMCI's `dq_run_log`) is needed, or a query is enough.
- The draft `dq_quarantine` and `dq_audit` tables are not built.