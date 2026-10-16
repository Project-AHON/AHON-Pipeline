# dq_results

- **Source:** written by the DQ gates through `run_dq_gate()` in `src/sql/common/dq_gate.py` ([decision 0010](../../decisions/0010-dq-gates-with-great-expectations.md))
- **Coverage:** every DQ gate run (bronze to silver, silver to gold, gold to platinum), all datasets. Piloted on LDRRMF in a notebook (2026-10-15); no gate is in the repo yet.
- **Owner:** Crystal

## Monitoring: `ahon.monitoring.dq_results`

- **One row is:** one rule checked in one DQ gate run
- **Key:** `dq_run_id` + `dq_rule_id`
- **Created by:** `src/sql/00_setup/07_dq_results.sql` (added with the first gate). Append only: rows are never edited or deleted, except test rows.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `dq_run_id` | string | One id per gate run; all rules checked in that run share it | UUID. Kept apart from a future pipeline `run_id` |
| `dq_rule_id` | string | The rule that was checked | `<dataset>_<layer>_<rule>`, e.g. `blgf_ldrrmf_annual_lgu_silver_overspent`. Unique across datasets |
| `rule_type` | string | Kind of check | null, unique, range, accepted_values, referential_integrity, volume, reconciliation, spatial |
| `_source_name` | string | Dataset checked | Same value as `_source_name` in bronze |
| `_batch_id` | string | Load run(s) the checked rows came from | Comma-separated. NULL when the table has no `_batch_id` (e.g. gold) |
| `layer` | string | Layer of the table checked | bronze, silver, gold, platinum |
| `table_name` | string | Table checked, without catalog and schema | |
| `checked_at` | timestamp (UTC) | When the gate ran | |
| `rows_checked` | bigint | Rows the rule looked at | For a row count rule: rows in the table |
| `rows_failed` | bigint | Rows that broke the rule | For a row count rule: the difference from the expected count |
| `failed_pct` | decimal(7,4) | `rows_failed` as a percent of `rows_checked` | 0 when no rows were checked |
| `status` | string | Result of the rule | PASS; FAIL (a stop or quarantine rule broke, the run stops); WARN (a flag, fix or report rule found rows); ERROR (the check could not run, the run stops) |
| `action` | string | What the rule does when it fails | stop, quarantine, flag, fix, report. Copied from the rule at run time |
| `sample_keys` | string | Up to 5 failing rows | Key values joined with " / ", e.g. `Romblon / Romblon / 2020`; failing values when the gate gives no keys |
| `message` | string | Short note | The error text when status is ERROR |

## Example query

```sql
-- Latest result of every rule
SELECT dq_rule_id, layer, status, rows_failed, sample_keys, checked_at
FROM ahon.monitoring.dq_results
QUALIFY row_number() OVER (PARTITION BY dq_rule_id ORDER BY checked_at DESC) = 1
ORDER BY status, dq_rule_id;
```

## Known issues

- **CMCI writes elsewhere.** `tests/sql/monitoring/cmci_quality.py` still writes to `ahon.quality.dq_results` with its own columns. Moving it here is follow-up work with its owner.
- **Dev only so far.** The table exists on `ahon_dev` (created for the pilot).

## Open

- Whether a per-run summary table (like CMCI's `dq_run_log`) is needed, or a query on this table is enough.
- The draft `dq_quarantine` and `dq_audit` tables are not built yet.
