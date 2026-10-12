# fact_ldrrmf

- **Source:** `ahon.silver.blgf_ldrrmf_annual_lgu_clean` (BLGF LDRRMF Annual by LGU, see [blgf_ldrrmf_annual_lgu](blgf_ldrrmf_annual_lgu.md))
- **Coverage:** fiscal years 2018 to 2024, provinces, cities and municipalities
- **Owner:** Crystal

## Gold: `ahon.gold.fact_ldrrmf`

- **One row is:** one LGU (province, city or municipality) in one fiscal year, as BLGF reported it. Same rows as silver (11,434 on ahon_dev, 2026-10-12).
- **Key:** `ldrrmf_fact_key` (a hash of fiscal_year + region + province + lgu_name + lgu_type, stable across rebuilds)
- **Joins to:** `gold.dim_lgu` on `psgc_code` (NULL for unmatched rows)
- **Built by:** `src/sql/datasets/blgf_ldrrmf_annual_lgu/gold/build_fact_ldrrmf.sql`. A run rebuilds the whole table and stops before writing if a stop rule fails.
- **Follows:** "PROJECT AHON: Schema v1" (JC), with the differences listed under Known issues.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `ldrrmf_fact_key` | bigint | Row key | `xxhash64` of the silver key. Unique (stop rule) |
| `psgc_code` | string | PSGC code of the LGU | From silver. NULL when `match_status = 'unmatched'`. Every non-NULL code exists in `dim_lgu` (stop rule) |
| `fiscal_year` | int | Fiscal year of the figures | |
| `region` | string | Region, as BLGF wrote it | Not corrected. The correct region comes from `dim_lgu` |
| `province` | string | Province, as BLGF wrote it | |
| `lgu_name` | string | LGU name, as BLGF wrote it | FY2019 names may show a broken ñ (see the source dictionary) |
| `lgu_type` | string | `Province`, `City` or `Municipality` | As BLGF wrote it |
| `appropriation_70_pct` | decimal(18,2) | Budget for the 70% LDRRMF part | Pesos |
| `expenditure_70_pct` | decimal(18,2) | Spent from the 70% LDRRMF part | Pesos |
| `appropriation_30_pct` | decimal(18,2) | Budget for the 30% Quick Response Fund | Pesos |
| `expenditure_30_pct` | decimal(18,2) | Spent from the 30% Quick Response Fund | Pesos |
| `total_appropriation` | decimal(18,2) | Total budget | Pesos; equals the sum of the two parts |
| `total_expenditure` | decimal(18,2) | Total spent | Pesos; equals the sum of the two parts |
| `utilization_rate_70_pct` | decimal(18,4) | Spent / budget, 70% part | Ratio (1.0000 = all spent). NULL when the budget is 0. Above 1 = overspent, kept as is |
| `utilization_rate_30_pct` | decimal(18,4) | Spent / budget, 30% part | Same rules |
| `utilization_rate_total` | decimal(18,4) | Total spent / total budget | Same rules |
| `match_status` | string | `exact` or `unmatched` | From silver `psgc_match_status` |
| `match_method` | string | How the PSGC code was found | `province_name`, `no_province_name`, `override`, or NULL |
| `match_confidence` | decimal(5,4) | 1 for `exact`, 0 for `unmatched` | The matcher only accepts exact or checked matches, so there are no in-between values |
| `has_dq_issues` | boolean | Row breaks a silver flag rule | Negative amount, zero budget or overspent (see the source dictionary) |
| `_source_ref` | string | Excel file the row came from | From silver; with `_row_hash` it links back to bronze |
| `_row_hash` | string | Bronze hash of the raw source columns | From silver |
| `_processed_at` | timestamp (UTC) | When the gold table was built | |

Match result (ahon_dev, 2026-10-12): 11,429 rows `exact` (10,602 by province and name, 218 with no province, 60 overrides, 549 provinces), 5 `unmatched`.

## Known issues

- **Province rows are separate funds.** A province's own LDRRMF is not the sum of its towns. Filter `lgu_type` before adding amounts up.
- **Old Maguindanao province (FY2018 to FY2022, 5 rows) has no PSGC code.** PSGC Q2_2024 lists only Maguindanao del Norte and del Sur. Kept with `psgc_code` NULL (Crystal, 2026-10-12).
- **Rates are computed, not reported.** BLGF gives amounts only. Rates are NULL for the 187 zero-budget rows and above 1 for overspent rows.
- **Differences from Schema v1:** one row per LGU and year (the 70% and 30% funds are columns, not rows); a hashed key instead of auto-increment (an IDENTITY column renumbers on every rebuild); three utilization rates as `decimal(18,4)` ratios instead of one `decimal(7,2)`; added `match_method`, `has_dq_issues` and the lineage columns.

## Open

- Whether the later assessment table uses city and municipality rows only (likely) and how it treats overspent and zero-budget rows.