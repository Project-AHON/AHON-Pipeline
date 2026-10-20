# Data quality

How AHON checks its data, where the results go, and how to read them.

## How checks work

Data is checked in two places:

- **Build checks:** every build checks its own output before it writes.
- **Layer gates:** after a build, a Great Expectations gate checks the finished table before the next layer reads it (see [Layer gates](#layer-gates)).

Build checks use the shared Great Expectations (GX Core) helper `check()` in `src/sql/common/quality.py`. Spark computes the failure count over the complete candidate input; GX expects each rule's count to equal zero. No source rows are sampled or collected into pandas for these checks.

Each build check has a severity:

| Severity | When the rule breaks | Result |
| --- | --- | --- |
| `ERROR` | The result is saved, then a `QualityError` stops the task before its table is written. Dependent tasks are skipped and tables keep their last accepted state. | `BLOCKED` |
| `WARN` | Rows stay in the table, marked with `has_dq_issues` where the table has that column. | `WARN` |
| (rule passes) | Nothing to do. | `PASS` |

A GX dependency or runtime failure stops the task; it never becomes an unvalidated pass.

Rows that fail a silver stop rule go to the dataset's `_reject` table (for example `silver.psa_psgc_reject`), so they can be inspected without stopping the run. [Handling rejects and DQ findings](handling-rejects.md) says what to do with them.

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

Every build check and every gate rule writes one row to `<catalog>.monitoring.dq_result`, keyed by `(run_id, check_id, table_name)`. A rerun of the same check in the same run replaces its row.

| Column | Type | Meaning |
| --- | --- | --- |
| `run_id` | string | The batch that ran the check (`batch_id`; by default the job run ID) |
| `check_id` | string | The rule's name, for example `component_totals`; gate rules are named `<dataset>_<layer>_<rule>` |
| `table_name` | string | The table checked; gate rows use `<layer>.<table>` |
| `failed_count` | bigint | Rows that broke the rule |
| `severity` | string | `ERROR` or `WARN` |
| `disposition` | string | `PASS`, `WARN` or `BLOCKED` |
| `checked_at` | timestamp (UTC) | When the check ran |
| `validation_result` | string | GX result JSON with the expectation metadata and GX version; gate rows also carry `rule_type`, `action`, `rows_checked`, `failed_pct` and `sample_keys`. NULL on older rows until their checks rerun. |

The table is created by the `setup` task. Full column details: [dq-result](dq-result.md).

## Reading the results

Latest result of every check:

```sql
SELECT table_name, check_id, severity, disposition, failed_count, checked_at
FROM monitoring.dq_result
QUALIFY ROW_NUMBER() OVER (PARTITION BY table_name, check_id ORDER BY checked_at DESC) = 1
ORDER BY disposition, table_name, check_id;
```

- Before treating a gold table as current, check that its latest checks are `PASS` or an expected `WARN`.
- After a partial run, or a failed attempt followed by a successful retry, look at the selected task and the current `checked_at` window. Older rows from earlier deliveries are history, not current failures.
- Each script also writes an execution marker (`check_id` `python_transformation` or `sql_execution`, `table_name` = the script file). It is `BLOCKED` when the script failed unexpectedly, and a successful retry resets it to `PASS`.

When a check is `WARN` or `BLOCKED`, follow [Handling rejects and DQ findings](handling-rejects.md) to find the cause and decide what to do. To rerun after a `BLOCKED` check, follow [Recover from a blocked check](../operations/runbook.md#recover-from-a-blocked-check) in the runbook.

## Layer gates

Great Expectations gates check each table between layers (bronze to silver, silver to gold, gold to platinum). Each gate is a task in `ahon_pipeline`, named `dq_<dataset>_<layer>`, that runs after the build of the table it checks. The next build depends on the gate:

- a **stop** rule that fails gives `BLOCKED`, fails the gate task and skips the layers after it;
- a **report** rule that fails gives `WARN` and the run continues.

| Dataset | Silver | Gold | Platinum |
| --- | --- | --- | --- |
| PSGC | `dq_psgc_silver` | `dq_psgc_gold` | |
| PSA Population | `dq_population_silver` | `dq_population_gold` | |
| BLGF LDRRMF | `dq_ldrrmf_silver` | `dq_ldrrmf_gold` | |
| PHIVOLCS earthquakes | `dq_earthquake_silver` | | |
| CMCI | `dq_cmci_silver` | `dq_cmci_gold` | `dq_cmci_platinum` |

Gate files are in `src/sql/monitoring/<dataset>/<layer>_dq_gate.py`. Rules, results and how to add a gate: [dq-gates](dq-gates.md).

## Related

- [DQ gates](dq-gates.md): the gate rules per dataset and how to add a gate
- [Handling rejects and DQ findings](handling-rejects.md): what to do when a check is `WARN` or `BLOCKED`
- [Runbook](../operations/runbook.md): running, rerunning and recovering
- [Naming standard](../architecture/naming-standard.md): `monitoring` schema and `dq_` tables
- Decisions: [0011 shared GX quality gates](../decisions/0011-shared-gx-quality-gates.md), [0012 DQ gates with Great Expectations](../decisions/0012-dq-gates-with-great-expectations.md), [0007 silver provenance and DQ flag](../decisions/0007-silver-provenance-and-dq-flag.md)