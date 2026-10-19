# BLGF LDRRMF

## Purpose

The Local Disaster Risk Reduction and Management Fund (LDRRMF) data gives each LGU's yearly LDRRMF budget (appropriation) and actual expenditure, split into the 70% LDRRMF part and the 30% Quick Response Fund (QRF), plus totals.

The 30% is the Quick Response Fund. The 70% is for disaster prevention and mitigation, preparedness, response, rehabilitation and recovery (BLGF Metadata sheet, FY2024).

Project AHON uses it for the preparedness side of the analysis: how much an LGU budgets for and spends on DRRM, per person and against its budget.

> LDRRMF figures show money budgeted and spent. They do not establish preparedness or spending effectiveness on their own.

## Source

| Property | Value |
|---|---|
| Dataset | LDRRMF Annual Utilization by LGU (BLGF LGU Time Series Data) |
| Provider | Bureau of Local Government Finance (BLGF) |
| Original source | [BLGF LGU Time Series Data](https://blgf.gov.ph/lgu-timeseries-data/) |
| Underlying reports | Statement of Receipts and Expenditures (SRE) reports the LGUs submit through the BLGF LIFT system |
| Source format | Excel, one workbook per fiscal year: `FY2018-LDRRMF-by-LGU.xlsx` to `FY2024-LDRRMF-by-LGU.xlsx` |
| Access | Downloaded from BLGF and uploaded to the project's Hugging Face dataset; the pipeline reads a pinned commit |
| Years | FY2018 to FY2024 (7 workbooks) |
| Geographic unit | Reporting LGU: province, city or municipality. No barangay rows. |
| Units | Philippine pesos |
| Extraction date | FY2024 file extracted by BLGF on 2025-11-27 |

Each workbook must match its expected row count before it is loaded.

## Coverage

| Year | City | Municipality | Province | Total |
| --- | ---: | ---: | ---: | ---: |
| 2018 | 120 | 1,316 | 77 | 1,513 |
| 2019 | 126 | 1,364 | 76 | 1,566 |
| 2020 | 132 | 1,403 | 77 | 1,612 |
| 2021 | 135 | 1,423 | 79 | 1,637 |
| 2022 | 144 | 1,458 | 81 | 1,683 |
| 2023 | 149 | 1,476 | 82 | 1,707 |
| 2024 | 149 | 1,485 | 82 | 1,716 |
| **Total** | | | | **11,434** |

LGU rows grow from 1,513 in 2018 to 1,716 in 2024. Part may be real changes in LGUs and part is LGUs that did not report.

## Fields as received

Each workbook has a `Metadata` sheet and one data sheet with one row per reporting LGU.

| Source header | Loaded as | Description |
|---|---|---|
| `REGION` | `region` | Region label as published |
| `PROVINCE` | `province` | Province label as published |
| `LGU NAME` | `lgu_name` | LGU name as published |
| `LGU TYPE` | `lgu_type` | `Province`, `City` or `Municipality` |
| `70% LDRRMF` > `Budget Appropriation` | `70pct_ldrrmf_budget_appropriation` | Pesos |
| `70% LDRRMF` > `Expenditures` | `70pct_ldrrmf_expenditures` | Pesos |
| `30% QUICK RESPONSE FUND` > `Budget Appropriation` | `30pct_quick_response_fund_budget_appropriation` | Pesos |
| `30% QUICK RESPONSE FUND` > `Expenditures` | `30pct_quick_response_fund_expenditures` | Pesos |
| `TOTAL` > `Budget Appropriation` | `total_budget_appropriation` | Pesos |
| `TOTAL` > `Expenditures` | `total_expenditures` | Pesos |

Things to know about the values as received:

- The amount columns have a two-level header: a group name over a sub-header.
- The layout differs by year. The data sheet name and position vary (the `Metadata` sheet may come first), FY2023 has `LGU TYPE` as the first column, the FY2024 header is one row lower, and FY2021 labels `LGU TYPE` as `LGU CODE` (the values are types). Columns are mapped by header name.
- There is no year column. The fiscal year comes from the file name.
- There is no PSGC code. LGUs are identified by region, province, name and type.
- There are no nulls or blanks; a missing value may have been loaded as 0.
- The total always equals the 70% part plus the 30% part (0 mismatches in every year).
- Three `lgu_name` values have stray spaces (`Sasmuan `, FY2018 to FY2020).

## Matching to PSGC

LGUs are matched to PSGC in silver (see [decision 0008](../../decisions/0008-ldrrmf-psgc-matching-in-silver.md)). The match uses province plus LGU name against PSGC cities, municipalities and provinces in the selected snapshot, with a short list of reviewed overrides in `blgf_ldrrmf_lgu_map_overrides.csv`. The result is stored in `reference.blgf_ldrrmf_lgu_map`.

Only a uniquely supported match in the same province (`matched`) assigns a code. Ambiguous, unmatched, old-name-only and name-only candidates stay unassigned (`ambiguous`, `unmatched` or `needs_review`).

Known unmatched rows: the five old Maguindanao province rows. The province was split into Maguindanao del Norte and del Sur in 2022, and the old province has no current PSGC code.

## Known source issues

These are kept as received and documented, not corrected.

| Issue | Evidence |
|---|---|
| Region labels change by year | BARMM has no rows in FY2018 and FY2021; those LGUs are listed under Region IX or XII. Use the region from PSGC for comparisons across years. |
| Zero budget may mean not reported | 187 rows have a zero total budget (62, 50, 26, 28, 9, 5, 7 for FY2018 to FY2024). |
| Spending above appropriation | 388 rows (3.4%), peak 107 in FY2020. May be carry-over or supplemental funds, or entry errors. |
| Negative expenditure | 3 rows: Romblon FY2020, Albay FY2021, Leon FY2022. |
| Cityhood changes | Calaca, Santo Tomas, Baliuag and Carmona are listed as Municipality in early years but are cities in PSGC (16 rows). |
| Pateros type | Pateros is labelled City in FY2018, FY2019, FY2021 and FY2022; PSGC lists it as a municipality. |
| Renamed LGUs | 92 LGUs have a gap between their first and last year, many because they were renamed (for example "Naga City (Cebu)" and "Naga City"). |
| New BARMM towns | The 8 Special Geographic Area municipalities created in 2024 have no FY2024 rows, so FY2024 covers 1,716 of 1,724 PSGC provinces, cities and municipalities. |
| Identical rows in consecutive years | 4 pairs of rows repeat the same values in two years, which may mean figures were carried forward. |

## Limitations

- Annual totals only. No quarterly or project-level detail, and nothing on what the money was spent on.
- Only LDRRMF. Other DRRM money is not included.
- FY2018 to FY2024 only.
- Province rows are separate funds from city and municipality rows. Do not add them together as if they were one LGU's budget.
- Whether "expenditures" means cash paid out or obligations is not stated. The Metadata sheet says the data comes from the SRE reports but does not say which.
- FY2018 to FY2021 are the least complete years.
- BLGF use constraints: the data may be cited or reproduced if BLGF is credited as the source and informed. Reselling, redistributing, or creating plagiarized works from it without BLGF's permission is not allowed. BLGF accepts no responsibility for the accuracy or use of the data.
