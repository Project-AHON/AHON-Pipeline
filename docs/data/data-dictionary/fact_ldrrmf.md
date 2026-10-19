# fact_ldrrmf

- **Table:** `gold.fact_ldrrmf`
- **One row is:** one original BLGF reporting LGU (province, city or municipality) in one fiscal year
- **Key:** `ldrrmf_fact_key`, from `fiscal_year`, `region`, `province`, `lgu_name`, `lgu_type`
- **Built from:** [`silver.blgf_ldrrmf_annual_lgu_clean`](blgf_ldrrmf_annual_lgu_clean.md) by `src/sql/gold/build_fact_ldrrmf.sql`
- **Owner:** Crystal
- **Joins to:** `gold.dim_lgu` on `psgc_code`

The source publishes separate 70% LDRRMF and 30% QRF amounts in one report; the fact preserves both and their totals.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `ldrrmf_fact_key` | bigint | Stable row key | `xxhash64` of the five silver key columns |
| `psgc_code` | string | PSGC code | NULL when unmatched |
| `fiscal_year` | int | Fiscal year | |
| `source_period` | string | PSGC snapshot | Must exist in `dim_lgu` |
| `region`, `province`, `lgu_name`, `lgu_type` | string | BLGF labels as published | The correct region comes from `dim_lgu` through `psgc_code` |
| `appropriation_70_pct` | decimal(18,2) | 70% LDRRMF appropriation | Pesos |
| `expenditure_70_pct` | decimal(18,2) | 70% LDRRMF expenditure | Pesos |
| `appropriation_30_pct` | decimal(18,2) | 30% QRF appropriation | Pesos |
| `expenditure_30_pct` | decimal(18,2) | 30% QRF expenditure | Pesos |
| `total_appropriation` | decimal(18,2) | Total appropriation | Pesos |
| `total_expenditure` | decimal(18,2) | Total expenditure | Pesos |
| `utilization_rate_70_pct` | decimal(18,4) | 70% expenditure / appropriation | NULL when the budget is 0; values above 1 kept |
| `utilization_rate_30_pct` | decimal(18,4) | 30% expenditure / appropriation | Same |
| `utilization_rate_total` | decimal(18,4) | Total expenditure / appropriation | Same |
| `match_status` | string | `exact` or `unmatched` | Silver `psgc_match_status` |
| `match_method` | string | How the map found the LGU | Silver `psgc_match_method` |
| `match_confidence` | decimal(5,4) | Always NULL | No calibrated matching probability exists |
| `has_dq_issues` | boolean | Carried from silver | |
| `_source_name`, `_batch_id`, `_source_ref`, `_row_hash` | string | Source lineage carried from silver | |
| `_ingested_at` | timestamp (UTC) | Source ingestion time | |
| `_processed_at` | timestamp (UTC) | When the row was last changed | |

## Rules

All are stop rules, checked before the write. If one fails, the existing gold table is left as it was.

- Same row count as silver
- Unique `ldrrmf_fact_key`
- One row per `psgc_code` and fiscal year among matched rows
- Every `psgc_code` exists in `gold.dim_lgu`, and `source_period` matches the dimension
- `match_confidence` between 0 and 1 when present

## Notes

- No absent row is deleted. Negative, zero-budget and overspent reports carry `has_dq_issues`, not inferred preparedness scores.
- Do not add provincial funds to municipal funds as if they were one LGU's budget.
- 11,434 rows.
