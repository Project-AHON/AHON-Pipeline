# blgf_ldrrmf_annual_lgu

- **Source:** Bureau of Local Government Finance (BLGF), LGU time series data (https://blgf.gov.ph/lgu-timeseries-data/). One Excel file per fiscal year, `FY2018-LDRRMF-by-LGU.xlsx` to `FY2024-LDRRMF-by-LGU.xlsx`, landed in the `source` volume (`ahon.reference.source`) through Hugging Face (see [0004](../../decisions/0004-source-file-landing-hugging-face.md)).
- **Coverage:** fiscal years 2018 to 2024, one file per year. One line per province, city and municipality. No barangay level.
- **Owner:** to be filled in (see [ownership](../../governance/ownership.md)).

What it measures: each LGU's yearly budget (appropriation) and actual spending (expenditure) of its Local Disaster Risk Reduction and Management Fund (LDRRMF), split into the 70% LDRRMF part and the 30% Quick Response Fund, plus totals. Amounts are in pesos.

## Bronze: `ahon.bronze.blgf_ldrrmf_annual_lgu`

- **One row is:** one line of a BLGF Excel file: one LGU (province, city or municipality) in one fiscal year, as received. All seven files are stacked in one table, so the same LGU appears once per year.
- **Loaded by:** `src/sql/datasets/blgf_ldrrmf_annual_lgu/extract/` copies the files into the volume, and `src/sql/datasets/blgf_ldrrmf_annual_lgu/bronze/load_blgf_ldrrmf_annual_lgu.py` reads them into this table. A run rebuilds the whole table from the files (overwrite), so rerunning does not duplicate rows.
- **Row hash covers:** the ten source columns below, in the order listed, joined with `||` (an empty cell counts as an empty string).
- **No year column:** the fiscal year is only in the file name, which `_source_ref` holds (the digits after `FY`). Silver derives it from there.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `region` | string | Region the LGU belongs to | Source header `REGION`, kept as received |
| `province` | string | Province the LGU belongs to | Source header `PROVINCE` |
| `lgu_name` | string | Name of the province, city or municipality | Source header `LGU NAME`. The same LGU may be spelled differently between years (not yet checked) |
| `lgu_type` | string | Whether the line is a province, a city or a municipality | Source header `LGU TYPE` (labelled `LGU CODE` in FY2021, but it holds the type). Values: `Province`, `City`, `Municipality` |
| `70pct_ldrrmf_budget_appropriation` | double | Budget appropriation for the 70% LDRRMF part | Pesos. Source header `70% LDRRMF` / `Budget Appropriation` |
| `70pct_ldrrmf_expenditures` | double | Amount spent from the 70% LDRRMF part | Pesos. `70% LDRRMF` / `Expenditures` |
| `30pct_quick_response_fund_budget_appropriation` | double | Budget appropriation for the 30% Quick Response Fund | Pesos. `30% QUICK RESPONSE FUND` / `Budget Appropriation` |
| `30pct_quick_response_fund_expenditures` | double | Amount spent from the 30% Quick Response Fund | Pesos. `30% QUICK RESPONSE FUND` / `Expenditures` |
| `total_budget_appropriation` | double | Total budget appropriation of the fund | Pesos. `TOTAL` / `Budget Appropriation`. Equals the sum of the two parts in every row |
| `total_expenditures` | double | Total amount spent | Pesos. `TOTAL` / `Expenditures`. Equals the sum of the two parts in every row |
| `_source_name` | string | The dataset the row came from | Always `blgf_ldrrmf_annual_lgu` |
| `_source_ref` | string | Where the row was loaded from: the path of the Excel file in the volume | For example `/Volumes/ahon_dev/reference/source/blgf_ldrrmf_annual_lgu/FY2018-LDRRMF-by-LGU.xlsx` |
| `_ingested_at` | timestamp (UTC) | When the row was loaded into bronze | |
| `_batch_id` | string | The load run that wrote the row | A random id for each run, the same on every row of that run |
| `_row_hash` | string | SHA-256 of the raw source columns as received | Covers the ten source columns above, not the provenance columns |

Rows per file: 1,513 (FY2018), 1,566, 1,612, 1,637, 1,683, 1,707, 1,716 (FY2024), 11,434 in total. The load stops without writing if a file's count differs.

## Silver: `ahon.silver.blgf_ldrrmf_annual_lgu_clean`

- **One row is:** one LGU (province, city or municipality) in one fiscal year. Same rows as bronze (11,434); no row is dropped and no value is corrected except trimming spaces.
- **Key:** `fiscal_year` + `region` + `province` + `lgu_name` + `lgu_type`.
- **Built from bronze by:** `src/sql/datasets/blgf_ldrrmf_annual_lgu/silver/clean_blgf_ldrrmf_annual_lgu.sql`. A run rebuilds the whole table.
- **Not in silver:** PSGC codes, utilization rates and the PSGC-based region. Reconciling with PSGC is done in gold.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `fiscal_year` | int | Fiscal year of the figures | Digits after `FY` in bronze `_source_ref` |
| `region` | string | Region, as BLGF wrote it | Bronze `region`, trimmed. Kept as received: the BARMM provinces are under Region IX or XII in FY2018 and FY2021 and under BARMM in other years (see Known issues) |
| `province` | string | Province, as BLGF wrote it | Bronze `province`, trimmed |
| `lgu_name` | string | Name of the province, city or municipality | Bronze `lgu_name`, trimmed (removes the trailing space in "Sasmuan ") |
| `lgu_type` | string | `Province`, `City` or `Municipality` | Bronze `lgu_type`, trimmed |
| `ldrrmf_70pct_appropriation` | decimal(18,2) | Budget for the 70% LDRRMF part | Pesos. Bronze `70pct_ldrrmf_budget_appropriation` |
| `ldrrmf_70pct_expenditure` | decimal(18,2) | Spent from the 70% LDRRMF part | Pesos. Bronze `70pct_ldrrmf_expenditures` |
| `qrf_30pct_appropriation` | decimal(18,2) | Budget for the 30% Quick Response Fund | Pesos. Bronze `30pct_quick_response_fund_budget_appropriation` |
| `qrf_30pct_expenditure` | decimal(18,2) | Spent from the 30% Quick Response Fund | Pesos. Bronze `30pct_quick_response_fund_expenditures` |
| `total_appropriation` | decimal(18,2) | Total budget | Pesos. Bronze `total_budget_appropriation`; equals the sum of the two parts |
| `total_expenditure` | decimal(18,2) | Total spent | Pesos. Bronze `total_expenditures`; equals the sum of the two parts |
| `has_dq_issues` | boolean | The row breaks at least one flag rule | True when any amount is negative, the total budget is 0, or total spent is above total budget. Only marks the row. See the rules below |
| `_source_name` | string | The dataset the row came from | Carried from bronze |
| `_source_ref` | string | The Excel file the row was loaded from | Carried from bronze |
| `_ingested_at` | timestamp (UTC) | When the row was loaded into bronze | Carried from bronze |
| `_batch_id` | string | The bronze load run that wrote the row | Carried from bronze |
| `_row_hash` | string | Bronze hash of the raw source columns | Carried from bronze. With `_source_ref` it links each silver row to its bronze row |
| `_processed_at` | timestamp (UTC) | When the silver table was built | Added in silver |

### Pilot data quality rules (bronze to silver)

Draft rules for this dataset. The shared DQ tables (`dq_ruleset`, `dq_results`, ...) are not defined yet, so for now the rules live here and in the check file. Actions: **stop** (the check fails), **fix** (corrected, counted), **flag** (kept, row marked `has_dq_issues`), **report** (documented only).

| # | Check name | Rule | Action | Seen in bronze |
| --- | --- | --- | --- | --- |
| 1 | `fiscal_year_valid` | `fiscal_year` can be read from the file name and is 2018 to 2024 | stop | 0 rows |
| 2 | `lgu_type_valid` | `lgu_type` is Province, City or Municipality | stop | 0 rows |
| 3 | `row_count_matches_bronze`, `unique_lgu_year` | One row per LGU per year; silver rows = bronze rows | stop | 0 rows |
| 4 | `total_is_sum_of_parts` | Total equals the 70% part plus the 30% part | stop | 0 rows |
| 5 | `no_outer_spaces` | Text columns have no outer spaces | fix (trim) | 3 rows ("Sasmuan ", FY2018 to FY2020) |
| 6 | `negative_amount` | No amount below 0 | flag | 3 rows (Romblon FY2020, Albay FY2021, Leon FY2022) |
| 7 | `zero_appropriation` | Total budget is not 0 | flag | 187 rows |
| 8 | `overspent` | Total spent is not above total budget | flag | 388 rows |
| 9 | `region_consistent_across_years` | The same LGU has the same region in every year | report | BARMM provinces in FY2018 and FY2021 |
| 10 | `part_overspent` | A part (70% or 30%) is not spent above its own budget while the total is fine | report | 295 rows (54, 70, 82, 46, 26, 13, 4 for FY2018 to FY2024) |
| 11 | `qrf_share_of_budget` | The 30% part is about 30% of the total budget (29 to 31%) | report | 2,574 rows outside it: 1,371 below 29%, 871 above 31%, 332 with a 30% part of 0 |
| 12 | `year_over_year_jump` | Total budget does not jump 10 times up, fall to a tenth, or fall to 0 against the previous year | report | 13 jumps up, 4 falls to a tenth, 67 falls to 0 (mostly zero-budget rows) |
| 13 | `lgu_present_every_year` | An LGU is present in every year between its first and last year | report | 92 LGUs with a gap, many of them renamed (see Known issues) |
| 14 | `lgu_type_consistent` | An LGU keeps the same type across years | report | 1 LGU: Pateros (Municipality and City) |
| 15 | `amounts_two_decimals` | Amounts have at most 2 decimals (so `DECIMAL(18,2)` rounds nothing) | report | 0 rows (checked once on bronze) |
| 16 | `repeated_amounts` | An LGU's total budget and total spent are not both identical to the previous year | report | 3 rows (checked once, no action) |
| 17 | `province_row_name` | A Province row has the same name as its province | report | 0 rows (checked once) |

The check names are the same as in the data quality checks doc. To see which check a flagged row breaks, run the finder query in `check_blgf_ldrrmf_annual_lgu_clean.sql`. It joins back to silver on `_source_ref` + `_row_hash`.

## Known issues

- **The Excel files do not share one layout.** The data sheet name and position change by year, the header sits on a different row in FY2024, FY2023 puts `LGU TYPE` first, and FY2021 labels it `LGU CODE`. The load finds the header row and maps columns by name, and stops with an error if a file does not fit.
- **Zero may mean "not reported".** 187 rows have a zero total budget (62, 50, 26, 28, 9, 5, 7 for FY2018 to FY2024). Bronze has no nulls, so a blank and a real zero look the same.
- **Spending above budget.** 388 rows (3.4%) have expenditure above appropriation (98, 85, 107, 56, 23, 10, 9). It may be legitimate (carry-over or supplemental funds) or an entry error. Kept as received.
- **Province rows are separate funds** from city and municipality rows. Do not add them together.
- **Coverage grows over time**, from 1,513 lines in FY2018 to 1,716 in FY2024.
- **No PSGC code.** Silver does not add one. Matching to other datasets (and to the correct region) is done in gold; municipality names repeat across provinces, so match on province and name together.
- **Region differs by year.** In FY2018 and FY2021 the BARMM provinces (Basilan, Maguindanao, Sulu, Tawi-Tawi) are listed under Region IX or XII, in other years under BARMM. Isabela City is under Region IX in every year. Silver keeps the region as received.
- **LGU names change between years.** 92 LGUs are missing in some years, and many of those are renames: "Naga City (Cebu)" until FY2021 and "Naga City" from FY2020, "Talisay City (Cebu)" likewise, "Santo Niño (Faire)", "Mendez (Mendez-Nuñez)" and "Mapun (Cagayan De Tawi-Tawi)" in FY2018 to FY2020, and "Western Samar (Samar)" in FY2018 to FY2020. The same LGU can therefore look like two LGUs, which also affects year-over-year comparisons. Silver keeps the names as received; gold matches them through PSGC.
- **Pateros is labelled Municipality in some years and City in others.** PSGC lists it as a municipality (checked in the PSGC 2Q 2026 file).
- **Part-level overspending is common.** 295 rows have a 70% or 30% part spent above its own budget while the total is within budget. Not flagged; it may be money moved between the two parts (not checked with BLGF).
- **The 30% share varies.** About 76% of rows (8,673) have a 30% part of 29 to 31% of the total budget; 2,574 do not. Not flagged.
- **Negative expenditures.** 3 rows have a negative amount (Romblon FY2020, Albay FY2021, Leon FY2022), possibly refunds or reversals. Kept and flagged.
- **Maguindanao** is one province in FY2018 to FY2022 and Maguindanao Del Norte and Del Sur from FY2023 (the 2022 split).
- **Only the data sheet is loaded.** Each Excel file also has a `Metadata` sheet (originator, extraction date, disclaimer), which is not loaded.

## Open

- What the 70% and 30% parts mean in BLGF's own notes (by law the 70% covers mitigation and preparedness and the 30% is the Quick Response Fund, not yet checked against BLGF), and whether "expenditures" means cash paid out or obligations.
- Final catalog, schema and volume names. The dev workspace uses `ahon_dev`.
- Column naming rules are still open in the [naming standard](../../standards/naming.md), so these are the names as they exist in the table.
- Shared DQ tables (`dq_ruleset`, `dq_results`, `dq_audit`) are not defined yet; the pilot rules above move there once they are.
