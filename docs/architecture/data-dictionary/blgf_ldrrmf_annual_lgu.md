# BLGF LDRRMF source reports

The Bureau of Local Government Finance workbooks contain reporting units at province, city and municipality level, with no barangay rows. The assessed source has seven FY2018–2024 workbooks. Repository, pinned revision, folder, file pattern, year window and expected row counts are caller inputs; the assessed years are not processing defaults.

## Bronze

`bronze.blgf_ldrrmf_annual_lgu` retains original report observations. Header names select the data sheet and columns across differing workbook layouts; missing or repeated headers and invalid numeric cells stop the load. Every workbook is checked before a single Delta write. No full-table overwrite occurs.

| Columns | Type | Meaning |
| --- | --- | --- |
| `region,province,lgu_name,lgu_type` | string | Original reporting-unit labels, including missing source values |
| `70pct_ldrrmf_budget_appropriation,70pct_ldrrmf_expenditures` | double | Reported appropriation and expenditure for the 70% component, pesos |
| `30pct_quick_response_fund_budget_appropriation,30pct_quick_response_fund_expenditures` | double | Reported appropriation and expenditure for the 30% component, pesos |
| `total_budget_appropriation,total_expenditures` | double | Source total amounts, pesos |
| `_source_name,_source_ref,_source_digest,_row_hash,_observation_id,_batch_id` | string | Source, immutable workbook path/digest, raw values, delivery observation and batch |
| `_ingested_at` | timestamp | First accepted observation ingestion time |

The row hash covers the four source labels and six amounts. Observation identity includes batch, workbook digest and raw hash. Replaying a completed delivery preserves lineage; new deliveries permit corrections and reversions. Repeated identical rows are accounted for in DQ. Completed files are immutable.

## Mapping and Silver

`reference.blgf_ldrrmf_lgu_map` maps the original `(region,province,lgu_name,lgu_type)` using a selected PSGC snapshot and the packaged reviewed override CSV. Only uniquely supported contextual matches can assign a code. Ambiguous, unmatched, old-name-only or name-only candidates remain unassigned or require review. Reference cardinality is checked before merging; no calibrated match confidence is invented.

`silver.blgf_ldrrmf_annual_lgu_clean` has one original reporting entity per fiscal year: `(fiscal_year,region,province,lgu_name,lgu_type)`. The year is parsed from the source filename and checked against the configured range. The six amounts become `DECIMAL(18,2)` columns `ldrrmf_70pct_appropriation,ldrrmf_70pct_expenditure,qrf_30pct_appropriation,qrf_30pct_expenditure,total_appropriation,total_expenditure`. Labels are trimmed; source province names are retained.

Missing amounts, invalid years/types, inconsistent totals, missing reference-map entries and conflicting same-delivery business keys block the accepted Silver write. Invalid rows are retained in `silver.blgf_ldrrmf_annual_lgu_reject` before blocking. Negative values, zero appropriations and expenditure above appropriation remain visible with `has_dq_issues`. `psgc_code` is populated only for an accepted match; `psgc_match_status,psgc_match_method,source_period` preserve geographic disposition. Bronze lineage and `_processed_at` trace accepted outputs.

Silver validates the requested delivery and merges business keys without deleting absent reports. An older first-ingested delivery cannot replace a newer accepted correction for the same reporting entity/year. Unchanged records retain processing timestamps; schema changes require review. Province and municipality reports must not be summed together as independent population or hazard evidence.

## Facts and limits

[gold.fact_ldrrmf](fact_ldrrmf.md) retains each original reporting entity/year with separate fund components. `platinum.funding_components` exposes these amounts and ratios. Utilization is expenditure divided by appropriation; zero denominators return NULL and overspending is not clipped. These figures do not establish preparedness or spending effectiveness.

Actual source ingestion and replay were verified for 11,434 rows. Reference mapping, financial edge cases and Gold transformations were exercised with synthetic fixtures. Live PSA reference ingestion has since passed; this does not by itself certify every financial mapping or real-source Gold result. See the [inventory](../source-to-target.md), [validation evidence](../../operations/validation.md) and [runbook](../../operations/runbook.md).
