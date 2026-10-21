# Data quality results

Every check in the `ahon_pipeline` job writes its result to one table, `<catalog>.monitoring.dq_result` ([decision 0020](../decisions/0020-one-dq-results-table.md)). Two kinds of check write to it:

- **Build checks:** `check()` and `unique()` in `src/sql/common/quality.py`, called inside each dataset build. A failed ERROR check stops the build before it writes its table.
- **Layer gates:** `run_dq_gate()` in `src/sql/common/dq_gate.py`, run as their own job tasks after a build ([dq-gates](dq-gates.md)). A gate checks a table that is already written; a failed stop rule stops the next layer.

The table itself:

- **One row is:** one check on one table in one batch.
- **Key:** `run_id` + `check_id` + `table_name`. A rerun of the same check in the same batch replaces its row; a new batch adds new rows.
- **Created by:** the `setup` task, and again by `check()` or `run_dq_gate()` if it is missing.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `run_id` | string | The batch that ran the check | The job's `batch_id`, by default the job run ID. `interactive` for a gate run in a notebook without `PIPELINE_RUN_ID` |
| `check_id` | string | The rule that was checked | Build checks: e.g. `component_totals`, `unique_psgc_code`. Gates: `<dataset>_<layer>_<rule>`, e.g. `blgf_ldrrmf_annual_lgu_silver_overspent`. `python_transformation` and `sql_execution` are execution markers for the script itself |
| `table_name` | string | Table checked | Gates use `<layer>.<table>`. For an execution marker: the script file |
| `failed_count` | bigint | How many failed | Usually failing rows. For a row-count rule, the distance from the expected count; for a whole-table rule with no row count, at least 1. 0 means it passed |
| `severity` | string | How serious a failure is | `ERROR` (stops) or `WARN` (rows kept). For gates, `stop` and `quarantine` rules are `ERROR`, the rest `WARN` |
| `disposition` | string | Result of the check | `PASS`; `WARN` (a WARN rule found rows); `BLOCKED` (an ERROR rule failed and the task stopped); `ERROR` (a gate check could not run, for example a missing column, and the task stopped) |
| `checked_at` | timestamp (UTC) | When the check ran | |
| `validation_result` | string | Details as JSON | For gates: `$.meta.rule_type`, `action`, `layer`, `source_name`, `rows_checked`, `failed_pct`, `sample_keys` (up to 5 failing rows), GX version |

A BLOCKED or ERROR result does not roll back tables written earlier in the run. Read results together with the job run's task states: a task that crashed before writing any result does not appear here, and a skipped task writes nothing.

## Dashboard views

The `dashboard_views` task creates two views on this table for the AHON Data Quality Monitoring dashboard (`src/sql/00_setup/07_dq_check_views.sql`):

- `monitoring.dq_check_result`: every row, with dataset, layer, check type and owner added.
- `monitoring.dq_check_status`: the latest result of each check.

## Queries

```sql
-- Everything one run found that was not a PASS
SELECT table_name, check_id, severity, disposition, failed_count,
       get_json_object(validation_result, '$.meta.sample_keys') AS sample_keys
FROM monitoring.dq_result
WHERE run_id = '<batch_id>' AND disposition <> 'PASS'
ORDER BY disposition, table_name, check_id;

-- Latest result of every check
SELECT table_name, check_id, severity, disposition, failed_count, checked_at
FROM monitoring.dq_result
QUALIFY row_number() OVER (PARTITION BY table_name, check_id ORDER BY checked_at DESC) = 1
ORDER BY disposition, table_name, check_id;

-- Gate results by rule type
SELECT get_json_object(validation_result, '$.meta.rule_type') AS rule_type, disposition, count(*) AS checks
FROM monitoring.dq_result
WHERE run_id = '<batch_id>' AND get_json_object(validation_result, '$.meta.rule_type') IS NOT NULL
GROUP BY ALL
ORDER BY rule_type, disposition;
```

## Known warnings

From the production run on `ahon` (batch `prod-verify-003`, 2026-10-21, commit `62fa65b`; CMCI in full mode; PHIVOLCS 2024-01 to 2026-09). See [Pipeline runs](../operations/pipeline-runs.md). No check was BLOCKED. These WARNs are expected until the cause is fixed; a new WARN, or a count that grows, needs a look.

| Dataset | Check | Rows | Cause |
| --- | --- | --- | --- |
| PSGC | broken `ñ` in `area_name` (silver; gold `dim_lgu`) | 1,824 (gold 456) | Encoding in the PSA source (`Ã±`). The LDRRMF matcher repairs it when matching |
| PSGC | `region_found` (silver) | 30,648 | `Q2_2021` only: PSA's old 9-digit codes padded to 10 digits give wrong regions. Not used downstream (`dim_lgu` is `Q2_2024`) |
| PSGC | rows in `psa_psgc_reject` | 12 | 2 each in Q2_2024, April_2024 and Q4_2023, 6 in Q2_2021 |
| PSGC | `rows_per_period_match_bronze` (silver) | 4 periods | The gate counts every bronze delivery, so it warns once bronze holds a second batch. Fix pending: compare the current batch only |
| PSA Population | non-accepted records (census silver) | 16 | Source records outside the accepted levels |
| PSA Population | `population` NULL (census gold) | 19 | Sulu municipalities with no 2024 count (`MISSING`) |
| PSA OpenSTAT | rows not mapped to PSGC (gold) | 44 | OpenSTAT names with no PSGC match; this chain is not used downstream |
| BLGF LDRRMF | `psgc_matched` (silver, gold) | 235 | 5 old Maguindanao province rows, plus 230 that the LGU map marks `needs_review` since #94 (NCR, HUCs, cityhood changes). Fix pending |
| BLGF LDRRMF | `overspent`, `zero_appropriation`, `negative_amount` (silver) | 388, 187, 3 (407 rows flagged) | As reported by BLGF; kept and flagged with `has_dq_issues` |
| BLGF LDRRMF | negative utilization; `lgu_type` vs PSGC level (gold) | 2; 4 | Negative amounts in the source; Baliuag's cityhood |
| BLGF LDRRMF | `dim_lgu_has_ldrrmf_latest_year` (gold) | 43 | Cities and municipalities without FY2024 LDRRMF: the unmatched cities above and the 8 new BARMM towns |
| PHIVOLCS earthquakes | `in_ph_bounds` (silver) | 698 | Coordinates outside the Philippine bounding box; cause to check |
| PHIVOLCS earthquakes | `candidate_coverage` (silver) | 59,861 | The locality parser does not handle the source's spacing and degree sign; fix pending |
| PHIVOLCS earthquakes | `event_signature_unique` (silver) | 87 | The same event appears more than once in the source; cause to check |
| PHIVOLCS earthquakes | `event_time_parsed`, `event_values_finite` (silver) | 33, 24 | Date-time or numeric values that could not be read; cause to check |
| PHIVOLCS earthquakes | rows in `philvolcs_earthquake_reject` | 54 | Rows flagged invalid by the checks above |
| CMCI | `raw_score_present` (silver, gold) | 132,797 | Source shows `-` or blank |
| CMCI | `coverage_complete` (platinum) | 30,503 | Pillars with `INSUFFICIENT_DATA` |

## Not used any more

- `monitoring.dq_results` (with an s) and `07_dq_results.sql` were the gates' first results table. The gates now write here, and the setup file was replaced by `07_dq_check_views.sql`.
- `tests/sql/monitoring/cmci_quality.py` wrote to `quality.dq_results`. It is not in any job and is to be removed.

## Open

- A real `rule_type` column. Today it is inside `validation_result`; adding a column needs `quality.py` and `dq_gate.py` to change together.
- The draft `dq_quarantine` and `dq_audit` tables are not built.

See [gates](dq-gates.md), [handling findings](handling-rejects.md) and [run evidence](../operations/pipeline-runs.md).
