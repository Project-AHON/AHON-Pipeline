# blgf_ldrrmf_annual_lgu_clean

- **Table:** `silver.blgf_ldrrmf_annual_lgu_clean`
- **One row is:** one original reporting LGU (province, city or municipality) in one fiscal year
- **Key:** `fiscal_year`, `region`, `province`, `lgu_name`, `lgu_type`
- **Built from:** [`bronze.blgf_ldrrmf_annual_lgu`](blgf_ldrrmf_annual_lgu.md) and [`reference.blgf_ldrrmf_lgu_map`](blgf_ldrrmf_lgu_map.md) by `src/sql/datasets/blgf_ldrrmf_annual_lgu/silver/clean_blgf_ldrrmf_annual_lgu.sql`
- **Checked by:** `check_blgf_ldrrmf_annual_lgu_clean.sql` (finder query lists which flag rule each row fails)
- **Owner:** Crystal
- **Rejects:** [`silver.blgf_ldrrmf_annual_lgu_reject`](blgf_ldrrmf_annual_lgu_reject.md)

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `fiscal_year` | int | Fiscal year | Parsed from the file name (digits after `FY`) |
| `region` | string | Region label, trimmed | Kept as received; use PSGC for the correct region |
| `province` | string | Province label, trimmed | Kept as received |
| `lgu_name` | string | LGU name, trimmed | |
| `lgu_type` | string | `Province`, `City` or `Municipality` | |
| `ldrrmf_70pct_appropriation` | decimal(18,2) | 70% LDRRMF appropriation | Pesos |
| `ldrrmf_70pct_expenditure` | decimal(18,2) | 70% LDRRMF expenditure | Pesos |
| `qrf_30pct_appropriation` | decimal(18,2) | 30% QRF appropriation | Pesos |
| `qrf_30pct_expenditure` | decimal(18,2) | 30% QRF expenditure | Pesos |
| `total_appropriation` | decimal(18,2) | Total appropriation | Pesos |
| `total_expenditure` | decimal(18,2) | Total expenditure | Pesos |
| `has_dq_issues` | boolean | True when an amount is negative, the total budget is 0, or total spent is above total budget | Never NULL; the row is kept |
| `psgc_code` | string | PSGC code | Only for a `matched` map row; otherwise NULL |
| `psgc_match_status` | string | `exact` or `unmatched` | `exact` = map status `matched` |
| `psgc_match_method` | string | Map step, such as `province_name` or `override` | Carried from the map even when unmatched |
| `source_period` | string | PSGC snapshot used by the map | |
| `_source_name`, `_source_ref`, `_ingested_at`, `_batch_id`, `_row_hash` | | Source lineage carried from bronze | |
| `_processed_at` | timestamp (UTC) | When the row was last changed in silver | |

## Rules

| Rule | Action |
| --- | --- |
| Fiscal year missing or outside the configured window | Row goes to the reject table; stop |
| `lgu_type` not Province, City or Municipality | Row goes to the reject table; stop |
| An amount is missing | Row goes to the reject table; stop |
| Total is not the 70% part plus the 30% part | Row goes to the reject table; stop |
| LGU not in `reference.blgf_ldrrmf_lgu_map` | Row goes to the reject table; stop (rerun the map build) |
| Key repeats, or silver rows differ from distinct bronze rows | Stop |
| Two different rows for the same key in one delivery | Stop |
| Text has outer spaces | Fixed by trimming |
| Negative amount, zero total budget, or spent above budget | Flagged (`has_dq_issues`); warning |
| No PSGC code | Warning |

If a stop rule fails, the existing silver table is left as it was.

## Notes

- Silver merges business keys without deleting absent reports. An older first-ingested delivery cannot replace a newer accepted correction for the same reporting entity/year.
- Province and municipality reports must not be summed together as independent population or hazard evidence.
- 11,434 rows; 407 flagged. The five old Maguindanao province rows stay unmatched on purpose.
