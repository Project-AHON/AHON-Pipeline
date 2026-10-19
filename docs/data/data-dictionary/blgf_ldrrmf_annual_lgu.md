# blgf_ldrrmf_annual_lgu

- **Table:** `bronze.blgf_ldrrmf_annual_lgu`
- **Source:** Bureau of Local Government Finance, LDRRMF Annual Utilization by LGU. See the [BLGF LDRRMF data profile](../data-profile/blgf-ldrrmf-profile.md).
- **One row is:** one line of one BLGF workbook (one reporting LGU in one fiscal year), as received, from one delivery
- **Loaded by:** `src/sql/datasets/blgf_ldrrmf_annual_lgu/extract/extract_blgf_ldrrmf_annual_lgu.py` (pinned Hugging Face commit to `/Volumes/<catalog>/reference/<source_volume>/blgf_ldrrmf_annual_lgu/<batch_id>/`), then `src/sql/datasets/blgf_ldrrmf_annual_lgu/bronze/load_blgf_ldrrmf_annual_lgu.py`
- **Key:** `_observation_id`
- **Row hash covers:** the four source labels and six amounts
- **Owner:** Crystal

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `region` | string | Region label as published | Differs by year for BARMM LGUs |
| `province` | string | Province label as published | |
| `lgu_name` | string | LGU name as published | |
| `lgu_type` | string | `Province`, `City` or `Municipality` | Header is `LGU CODE` in FY2021 |
| `70pct_ldrrmf_budget_appropriation` | double | 70% LDRRMF appropriation | Pesos |
| `70pct_ldrrmf_expenditures` | double | 70% LDRRMF expenditure | Pesos |
| `30pct_quick_response_fund_budget_appropriation` | double | 30% QRF appropriation | Pesos |
| `30pct_quick_response_fund_expenditures` | double | 30% QRF expenditure | Pesos |
| `total_budget_appropriation` | double | Total appropriation | Pesos |
| `total_expenditures` | double | Total expenditure | Pesos |
| `_source_name` | string | The dataset the row came from | Always `blgf_ldrrmf_annual_lgu` |
| `_source_ref` | string | Where the row was loaded from: the landed workbook path | The fiscal year is read from this file name in silver |
| `_source_digest` | string | SHA-256 of the landed workbook | |
| `_ingested_at` | timestamp (UTC) | When the row was loaded into bronze | |
| `_batch_id` | string | The load run that wrote the row | |
| `_row_hash` | string | SHA-256 of the ten source columns | Null-aware JSON |
| `_observation_id` | string | Identity of this observation | Hash of `_batch_id`, `_source_digest` and `_row_hash` |

## Notes

- Header names select the data sheet and columns across differing workbook layouts; missing or repeated headers and invalid numeric cells stop the load. Each workbook must match its expected row count (1,513, 1,566, 1,612, 1,637, 1,683, 1,707, 1,716 for FY2018 to FY2024). Every workbook is checked before a single Delta write.
- Replaying a completed delivery preserves lineage; new deliveries permit corrections and reversions. Repeated identical rows are accounted for in DQ. Completed files are immutable.
- 11,434 rows were loaded and verified.
