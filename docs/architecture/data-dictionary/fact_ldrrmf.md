# fact_ldrrmf

Target: `<catalog>.gold.fact_ldrrmf`. Source: validated `silver.blgf_ldrrmf_annual_lgu_clean`. Grain: original BLGF reporting entity × fiscal year, uniquely identified by `(fiscal_year, region, province, lgu_name, lgu_type)`. `ldrrmf_fact_key` is deterministic xxhash64 of that key. The source publishes separate 70% LDRRMF and 30% QRF amounts in one report; the fact preserves both and their totals.

String fields: selected `source_period`, source labels, `psgc_code` (nullable), `match_status`, `match_method`, `_source_name`, `_batch_id`, `_source_ref`, `_row_hash`. Fiscal year is INT. Amounts are DECIMAL(18,2) Philippine pesos: `appropriation_70_pct`, `expenditure_70_pct`, `appropriation_30_pct`, `expenditure_30_pct`, `total_appropriation`, `total_expenditure`. Utilization fields are DECIMAL(18,4) ratios (spent / budget), NULL for zero denominator; values above one are retained. `has_dq_issues` is BOOLEAN; ingestion/processing timestamps preserve lineage.

`match_confidence` is nullable DECIMAL(5,4) and deliberately unpopulated: there is no calibrated matching probability. Exact contextual/reviewed identity mapping is a status, not probability 1. Candidate-only name matches remain unassigned; source reporting units and current PSGC boundaries may differ historically.

Candidate counts, unique fact key, matched PSGC/year uniqueness and dimension references are checked before MERGE. No absent row is deleted. A missing amount or inconsistent total blocks the dependent fact, preserving the prior accepted target. Negative, zero-budget and overspent reports carry warnings, not inferred preparedness scores. Do not add provincial funds to municipal funds as if they were one LGU's budget.
