# 0006: Silver keeps provenance columns and marks data quality problems in one column

- **Status:** Proposed
- **Date:** 2026-10-09

## Context

The first silver table, `blgf_ldrrmf_annual_lgu_clean`, cleans bronze without dropping or correcting rows. Two things needed a decision: what silver does with the bronze provenance columns, and how it shows that a row breaks a data quality rule. The shared data quality tables (`dq_ruleset`, `dq_results`) are not defined yet, so silver cannot depend on them.

## Decision

- Silver carries the five bronze provenance columns forward (`_source_name`, `_source_ref`, `_ingested_at`, `_batch_id`, `_row_hash`) and adds `_processed_at`, the time silver was built.
- Silver marks data quality problems in one boolean column, `has_dq_issues`. It is true when any flag rule fails. There is no column per rule.
- Which rule a row breaks is found with a query that re-applies the rules, joined back to silver by `_source_ref` and `_row_hash`. For this dataset the query is in `check_blgf_ldrrmf_annual_lgu_clean.sql`.
- Flagged rows stay in silver and keep their original values. Only stop rules fail the build.

## Why

- With the provenance columns, any silver row can be traced to one bronze row and one source file.
- One column does not grow with every new rule. Per-rule columns would change the table each time a check is added.
- Users can filter out flagged rows today, before the shared data quality tables exist.
- Silver stays unchanged when those tables are defined later. The finder query can feed them.

## Alternatives considered

- **One flag column per rule:** rejected. It grows with every new check.
- **A column holding a list of rule codes:** rejected for now. It needs the rule list and code names to be agreed first.
- **Dropping or correcting bad rows in silver:** rejected. Some values, such as negative expenditures, may have a meaning, so they are flagged and decided on in gold.

## Consequences

- Other silver tables are expected to follow the same pattern.
- A row can be flagged without saying why, until the shared data quality tables exist.

## Open

- Whether `has_dq_issues` stays once `dq_results` exists, or becomes derived from it.
- Which report-only checks, if any, should later set `has_dq_issues`.