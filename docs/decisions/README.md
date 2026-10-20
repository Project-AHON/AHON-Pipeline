# Decision records

This folder holds the decisions that shape the AHON pipeline design, one file per decision. Each record says what was decided, why, what else was considered and what changes as a result, so the reasoning is still available after the people involved have moved on.

## When to write one

Write a record when a decision changes the design or someone might question it later: naming standards, storage layout, load strategy, tooling choices, how a measure is calculated. Group related choices into one record instead of writing one per detail. Do not write one for routine work or for choices that are easy to change.

## How to add one

1. Copy [0000-short-title.md](0000-short-title.md).
2. Rename it to the next free number and a short title, for example `0021-load-strategy.md`. Check this list and open PRs first, so two records do not take the same number.
3. Fill in each section, keeping it to a few lines.
4. Add it to the list below.

Numbers are never reused. A decision is never edited to change its meaning once it is Accepted: if it changes, write a new record and mark the old one "Superseded by NNNN" (or "Partly superseded by NNNN", saying which part). Fixing typos, adding links and updating the status line are fine. A Proposed record can still be edited.

## Statuses

- **Proposed:** written, not yet agreed.
- **Accepted:** agreed and in effect.
- **Partly superseded by NNNN:** some of it was replaced by a later record; the record says which part still applies.
- **Superseded:** replaced by a later record.

## Decisions

| No. | Title | Date | Status |
| --- | --- | --- | --- |
| [0001](0001-naming-standard.md) | Naming standard | 2026-10-04 | Accepted |
| [0002](0002-team-workflow.md) | Team workflow | 2026-10-04 | Accepted |
| [0003](0003-repository-structure.md) | Repository structure | 2026-10-04 | Accepted |
| [0004](0004-source-file-landing-hugging-face.md) | Land source files through Hugging Face | 2026-10-04 | Accepted |
| [0005](0005-raw-file-volume-in-reference-schema.md) | Raw files land in a volume in the reference schema | 2026-10-05 | Accepted |
| [0006](0006-cmci-mapping-and-ingestion.md) | CMCI locality mapping and incremental ingestion | 2026-10-06 | Partly superseded by 0015 |
| [0007](0007-cmci-silver.md) | Store CMCI silver data by pillar | 2026-10-09 | Superseded by 0015 |
| [0008](0008-silver-provenance-and-dq-flag.md) | Silver keeps provenance columns and marks DQ problems in one column | 2026-10-09 | Accepted |
| [0009](0009-dab-and-jobs.md) | Configure Databricks Asset Bundle deployment | 2026-10-11 | Partly superseded by 0015 |
| [0010](0010-cmci-gold-analytical-model.md) | CMCI gold analytical model | 2026-10-12 | Partly superseded by 0015 |
| [0011](0011-cmci-great-expectations-data-quality.md) | CMCI Great Expectations data quality framework | 2026-10-12 | Superseded by 0016 and 0018 |
| [0012](0012-psgc-snapshot-and-codes-in-silver.md) | One PSGC snapshot, with PSGC codes attached in silver | 2026-10-12 | Accepted |
| [0013](0013-ldrrmf-psgc-matching-in-silver.md) | Match LDRRMF to PSGC codes in silver through a reference map | 2026-10-12 | Accepted |
| [0014](0014-earthquake-observations-and-locality-candidates.md) | Keep earthquake observations and report locality candidates | 2026-10-12 | Proposed |
| [0015](0015-reconciled-source-contracts.md) | One executable pipeline and reconciled source contracts | 2026-10-14 | Accepted |
| [0016](0016-shared-gx-quality-gates.md) | Use Great Expectations inside every build's checks | 2026-10-15 | Accepted |
| [0017](0017-city-municipality-analysis-grain.md) | Analyse at city and municipality level | 2026-10-15 | Accepted |
| [0018](0018-dq-gates-with-great-expectations.md) | Data quality gates with Great Expectations | 2026-10-15 | Partly superseded by 0020 |
| [0019](0019-risk-and-preparedness-scores.md) | Risk and preparedness scores for the dashboard | 2026-10-16 | Proposed |
| [0020](0020-one-dq-results-table.md) | One data quality results table, and gates that block the next build | 2026-10-20 | Proposed |
