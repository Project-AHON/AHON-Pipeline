# Data quality

How AHON checks its data, where the results go, and how to read them.

## How checks work

Dataset builders evaluate checks around their writes; a task may contain several independently committed statements. Checks use the shared Great Expectations (GX Core) helper `check()` in `src/sql/common/quality.py`. Spark computes the failure count over the complete candidate input; GX expects each rule's count to equal zero. No source rows are sampled or collected into pandas for these checks.

Each check has a severity:

| Severity | When the rule breaks | Result |
| --- | --- | --- |
| `ERROR` | The result is saved, then a `QualityError` stops the task before its table is written. Dependent tasks are skipped. Earlier successful writes remain. | `BLOCKED` |
| `WARN` | Rows stay in the table, marked with `has_dq_issues` where the table has that column. | `WARN` |
| (rule passes) | Nothing to do. | `PASS` |

A GX dependency or runtime failure stops the task; it never becomes an unvalidated pass.

Reject behavior is dataset-specific. LDRRMF and invalid PSGC-code rejects can block the corresponding build. Other rejected or flagged records are retained with warnings. Consult the table dictionary for the exact disposition. Blocking checks prevent the associated downstream write where evaluated before that write. Earlier successful statements and upstream tasks are not rolled back.

File integrity, authentication, parsing structure and schema checks are part of ingestion and the write step, not `dq_result` rules. A failed delivery is not deleted to hide it.

## What is checked, by dataset

| Dataset | Rule families |
| --- | --- |
| PSGC | Valid code strings, snapshot selection, unique geographic keys, lookup cardinality and explicit rejects |
| PSA Population | City/municipality/census keys, missing versus invalid or absent observations, dimension alignment, source accounting and full LGU/year grid |
| CMCI | Reviewed mapping identity, parser accounting, indicator keys, current invalid corrections and reviewed definition/version |
| BLGF LDRRMF | Original entity/year uniqueness, required amounts, component totals, year range, map/dimension alignment and retained financial warnings |
| PHIVOLCS earthquakes | Observation identity, typed invalid records, source accounting and locality-candidate cardinality; LGU assignment is not done yet |

The checks for each table are listed on its [data dictionary](../data/data-dictionary/README.md) page.

## Results table

Every check writes one row to `<catalog>.monitoring.dq_result`, keyed by `(run_id, check_id, table_name)`. A rerun of the same check in the same run replaces its row.

| Column | Type | Meaning |
| --- | --- | --- |
| `run_id` | string | The batch that ran the check (`batch_id`; by default the job run ID) |
| `check_id` | string | The rule's name, for example `component_totals` |
| `table_name` | string | The table checked |
| `failed_count` | bigint | Failure metric: rows, duplicate-key groups, a count difference or a binary contract failure, depending on the check |
| `severity` | string | `ERROR` or `WARN` |
| `disposition` | string | `PASS`, `WARN` or `BLOCKED` |
| `checked_at` | timestamp (UTC) | When the check ran |
| `validation_result` | string | GX result JSON with the expectation metadata and GX version. NULL on older rows until their checks rerun. |

The table is created by the `setup` task. Full column details: [dq-results](dq-result.md).

## Reading the results

Latest result of every check (an overview that can combine different runs, not evidence that one complete run succeeded):

```sql
SELECT table_name, check_id, severity, disposition, failed_count, checked_at
FROM monitoring.dq_result
QUALIFY ROW_NUMBER() OVER (PARTITION BY table_name, check_id ORDER BY checked_at DESC) = 1
ORDER BY disposition, table_name, check_id;
```

- Before treating a gold table as current, check that its latest checks are `PASS` or an expected `WARN`.
- After a partial run, or a failed attempt followed by a successful retry, look at the selected task and the current `checked_at` window. Older rows from earlier deliveries are history, not current failures.
- Scripts executed through `run_python.py` and `run_sql.py` write an execution marker (`check_id` `python_transformation` or `sql_execution`, `table_name` = the script file). It is `BLOCKED` when the script failed unexpectedly, and a successful retry resets it to `PASS`. Directly launched extractors and dataset entry points do not necessarily emit the same marker.

When a check is `BLOCKED`, follow [Recover from a blocked check](../operations/runbook.md#recover-from-a-blocked-check) in the runbook.

## Layer gates

The design for Great Expectations gates between layers (bronze to silver, silver to gold, gold to platinum) is in [dq-gates](dq-gates.md). Gate files are templates in `src/sql/monitoring/<dataset>/` and are not yet tasks in the job; only the LDRRMF silver and gold rules have been run, in a notebook.

## Related

- [Runbook](../operations/runbook.md): running, rerunning and recovering
- [Naming standard](../architecture/naming-standard.md): `monitoring` schema and `dq_` tables
- Decisions: [0016 shared GX quality gates](../decisions/0016-shared-gx-quality-gates.md), [0018 DQ gates with Great Expectations](../decisions/0018-dq-gates-with-great-expectations.md), [0008 silver provenance and DQ flag](../decisions/0008-silver-provenance-and-dq-flag.md)
