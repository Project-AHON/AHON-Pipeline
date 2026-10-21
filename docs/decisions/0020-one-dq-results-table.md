# 0020: One data quality results table, and gates that block the next build

- **Status:** Proposed
- **Date:** 2026-10-20
- **Updates:** [0018](0018-dq-gates-with-great-expectations.md) (where the gates write, and how they run in the job); keeps [0016](0016-shared-gx-quality-gates.md)

## Context

Two results tables existed. The checks inside each build write `monitoring.dq_result` ([0016](0016-shared-gx-quality-gates.md)): one row per check per batch, with PASS, WARN or BLOCKED. The layer gates were designed to write `monitoring.dq_results` ([0018](0018-dq-gates-with-great-expectations.md)): one row per rule per gate run, with PASS, WARN, FAIL or ERROR, sample failing keys and a failed percent. The names differ by one letter, the job creates only `dq_result`, and the gate table had to be created by hand. The gates were also not job tasks yet, so nothing stopped a later layer from reading a table that failed its gate.

## Decision

1. **One results table.** `monitoring.dq_result` is the one results table for the pipeline. The build checks and the layer gates both write to it, and the docs and the dashboard read it.
2. **Gate rows use the same 8 columns.** `run_id` is the batch, `check_id` is `<dataset>_<layer>_<rule>`, `table_name` is `<layer>.<table>`, and gate FAIL is written as `BLOCKED`. A rule that cannot run is written as `ERROR`.
3. **Gate details stay in `validation_result`.** `rule_type`, `action`, `rows_checked`, `failed_pct`, `sample_keys`, `dq_run_id` and `layer` go in `validation_result.meta`. No columns are added for now.
4. **Gates are job tasks that block.** Each gate is a task in `ahon_pipeline`, named `dq_<dataset>_<layer>`, that runs after its build. Every build that reads the checked table depends on the gate. A stop rule FAIL fails the gate task, so the layers after it are skipped. A report or flag rule gives WARN and the run continues.

## Why

- `dq_result` is the table the job already creates and uses on every run.
- One table answers "what passed, what was flagged, what stopped" in one query.
- Keeping the 8 columns means `quality.check()` and every existing row keep working. Its MERGE uses `UPDATE SET *`, which breaks if the table has columns it does not write.
- A gate that only records a failure, while the next layer still reads the table, does not protect the dashboard.

## Alternatives considered

- **Keep two tables:** two places to look, and names that are easy to confuse.
- **Move the build checks to `dq_results`:** changes every build and the setup task for no gain.
- **A view over both tables:** hides the difference but keeps two sets of statuses.
- **Add `rule_type` and the other gate fields as columns now:** easier to query, but `quality.py` and `dq_gate.py` must then change together. Left for a later PR.
- **Gates at the end of each branch, with nothing depending on them:** simpler job file, but a failed gate would not stop the next layer.

## Consequences

- `src/sql/common/dq_gate.py` writes to `dq_result` with the same MERGE key as `quality.check()` (`run_id`, `check_id`, `table_name`), so a rerun replaces its rows.
- Twelve gate tasks with independent census and OpenSTAT paths in `resources/jobs/ahon_job.yml`; `tests/common/test_job_contract.py` checks the task count and the gate dependencies.
- Filtering by rule type needs `get_json_object(validation_result, '$.meta.rule_type')`. The dashboard needs a view that adds dataset, layer and rule type as columns.
- `monitoring.dq_results`, `src/sql/00_setup/07_dq_results.sql` and the old per-dataset DQ files are no longer used and are to be removed.
- A failing gate now skips the builds after it. A broken gate rule can stop a run, so changing a stop rule needs the same care as changing a build.
- What to do with WARN and BLOCKED results: [handling rejects and DQ findings](../data-quality/handling-rejects.md).

## Open

- When to add `rule_type` (and possibly `layer`) as real columns.
- Whether a per-run summary view is needed for the dashboard, or a query on `dq_result` is enough.
