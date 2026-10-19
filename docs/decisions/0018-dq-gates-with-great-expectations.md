# 0018: Data quality gates with Great Expectations

- **Status:** Proposed
- **Date:** 2026-10-15

## Context

Each dataset checks its own data in its own way: `assert_true` stop checks in the LDRRMF SQL, `*_dq.sql` files for PSGC and earthquakes, Python checks for population, and a CMCI-only Great Expectations (GX) job writing to `ahon.quality` (decision 0009-cmci). Results are not stored in one place, so nobody can see in one query what passed, what was flagged and what stopped a run. The draft DQ table design (`monitoring.dq_ruleset`, `dq_results`, `dq_quarantine`, `dq_audit`) was never built.

## Decision

1. **Gate positions.** The pipeline has four DQ points:
   - Landing to bronze (DQ Inspect): plain code in the load scripts (file layout, header, row count per file). No GX, because some datasets go straight to bronze and GX checks tables, not files.
   - Bronze to silver, silver to gold, gold to platinum (DQ Gates): GX, run on the table the layer just wrote.
2. **One shared gate.** Every gate calls `run_dq_gate()` in `src/sql/common/dq_gate.py`. It runs the rules as one GX suite directly on the Spark table, writes one row per rule to `monitoring.dq_results`, then stops the job on any `FAIL` or `ERROR`. `WARN` is recorded and does not stop the job.
3. **Rules live in code.** Each rule is a GX expectation tagged with `meta` = rule name, `rule_type` and `action`. There is no `dq_ruleset` table; changing a rule is a PR.
4. **Action sets severity and status.** `stop` / `quarantine` = GX `critical`, a failure is `FAIL`. `flag` / `fix` = `warning`, `report` = `info`, a failure is `WARN`. A check that cannot run is `ERROR`.
5. **`rule_type`** is one of: null, unique, range, accepted_values, referential_integrity, volume, reconciliation, spatial.
6. **`monitoring.dq_results`** (append only) follows the draft design with three changes: `dq_run_id` added (one id per gate run, kept apart from a future pipeline `run_id`), `rule_id` renamed `dq_rule_id` (`<dataset>_<layer>_<rule>`), and `rule_type` added. It lives in `monitoring`, as decision 0001 says, not in `quality`.
7. **Row flags stay in the builds.** Columns such as `has_dq_issues` are still set by the silver SQL. GX decides whether the table passes; it does not mark rows.
8. **Stop checks inside the builds stay.** `assert_true` checks run before a table is replaced, so a bad build never overwrites a good table. The GX gate repeats the stop rules after the write and adds the recorded history, flag and report rules, and cross-table checks.
9. **Version.** `great-expectations==1.23.1`, the same version as the CMCI job.

## Why

- One results table answers "what passed, what was flagged, what stopped" for every dataset and layer.
- GX runs the counting itself on Spark and returns sample failing keys (`unexpected_index_column_names`), so the checks need less custom SQL.
- Rules in code are reviewed and versioned like the rest of the pipeline, with no table to keep in sync.
- The LDRRMF pilot on `ahon_dev` (2026-10-15) reproduced every count in the LDRRMF profile: silver 14 stop rules PASS, 3 flag rules WARN (negative_amount 3 rows, zero_appropriation 187, overspent 388); gold 5 stop rules PASS, 1 report WARN (5 old Maguindanao province rows). A deliberate failing rule stopped the gate as expected.

## Alternatives considered

- **Keep per-dataset SQL checks only:** no shared results, every dataset reinvents the reporting.
- **Metrics in SQL, GX only compares percentages (current CMCI pattern):** keeps the data in Spark, but GX then adds little beyond an `if`, and gives no sample keys.
- **`dq_ruleset` table read by the code:** more flexible, but a second copy of the rules that can drift from what runs. Too much for the capstone.
- **GX at DQ Inspect too:** GX cannot read raw Excel or JSON files before they are loaded.
- **Databricks Lakeflow (DLT) expectations or Databricks Labs DQX:** less setup, but the CMCI work already uses GX and the team wanted to learn it.

## Consequences

- New: `src/sql/00_setup/07_dq_results.sql`, `src/sql/common/dq_gate.py`, `src/sql/monitoring/<dataset>/<layer>_dq_gate.py` per dataset. The file map per dataset is in [dq-gates.md](../operations/monitoring/dq-gates.md).
- Jobs get a gate task after each silver and gold build once the job YAML is exported (`resources/jobs/ahon_job.yml` is still a placeholder).
- GX must be installed on the job environment (`pyproject.toml`).
- CMCI (`tests/sql/monitoring/cmci_quality.py`) still writes to `ahon.quality.dq_results`; aligning it is follow-up work with its owner.
- The draft `dq_quarantine` and `dq_audit` tables are not built yet.
- Some stop rules hold values that change with new data (year range, expected row counts) and must be updated in the same PR as the new data.

## Open

- Should `ahon.quality` be dropped once CMCI moves to `monitoring.dq_results`?
- Is a per-run summary table (`dq_run_log`, as in CMCI) needed, or is a query on `dq_results` enough?
- Do gates run on every job run, or only after a load that changed data?