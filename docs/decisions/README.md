# Decision records

This folder holds the decisions that shape the AHON pipeline design, one file
per decision. Each record says what was decided, why, what else was
considered and what changes as a result, so the reasoning is still
available after the people involved have moved on.

## When to write one

Write a record when a decision changes the design or someone might question
it later: naming standards, storage layout, load strategy, tooling choices.
Group related choices into one record instead of writing one per detail.
Do not write one for routine work or for choices that are easy to change.

## How to add one

1. Copy [0000-short-title.md](0000-short-title.md).
2. Rename it to the next number and a short title, for example
   `0002-load-strategy.md`.
3. Fill in each section, keeping it to a few lines.
4. Add it to the list below.

Numbers are never reused. A decision is never edited to change its meaning:
if it changes, write a new record and mark the old one
"Superseded by NNNN". Fixing typos or adding links is fine.

## Statuses

- **Proposed:** written, not yet agreed.
- **Accepted:** agreed and in effect.
- **Superseded by NNNN:** replaced by a later record.

## Decisions

| No. | Title | Status |
| --- | --- | --- |
| [0001](0001-naming-standard.md) | Naming standard | Accepted, except where marked open |
| [0002](0002-team-workflow.md) | Team workflow | Accepted |
| [0003](0003-repository-structure-initial.md) | Repository structure | Accepted |
| [0004](0004-source-file-landing-hugging-face.md) | Land source files through Hugging Face | Accepted |
| [0005](0005-cmci-mapping-and-ingestion.md) | CMCI mapping and ingestion | Implementation update proposed in 0010 |
| [0006](0006-cmci-silver.md) | CMCI Silver | Implementation update proposed in 0010 |
| [0007](0007-silver-provenance-and-dq-flag.md) | Silver keeps provenance columns and marks data quality problems in one column | Proposed |
| [0008](0008-ldrrmf-psgc-matching-in-silver.md) | Match LDRRMF to PSGC codes in silver through a reference map | Proposed |
| [0007 DAB](0007-DAB-and-jobs.md) | Bundle and personal development workflow | Implementation update proposed in 0010 |
| [0008 CMCI](0008-cmci-gold-analytical-model.md) | CMCI Gold model | Implementation update proposed in 0010 |
| [0009 CMCI](0009-cmci-great-expectations-data-quality.md) | Prior CMCI GX quality workflow | Implementation update proposed in 0010 |
| [0009 earthquake](0009-earthquake-observations-and-locality-candidates.md) | Earthquake observations and locality candidates | Proposed |
| [0010](0010-reconciled-source-contracts.md) | Executable source and analytical contracts | Proposed for review, locally implemented |
| [0011](0011-shared-gx-quality-gates.md) | Shared GX quality gates and explicit production identity | Implemented at user request; pending team review |
| [0012](0012-city-municipality-analysis-grain.md) | City/municipality analysis and population fact grain | Implemented at user request; supersedes 0010's population grain |
