# Data quality checks: blgf_ldrrmf_annual_lgu (draft, 2026-10-09)

Checks for the BLGF LDRRMF Annual by LGU dataset so far, by layer. Silver rows are never dropped. A failed check does one of these, shown at the start of each description in brackets:

- **stop**: the build or load fails.
- **fix**: the value is corrected (only trimming spaces).
- **flag**: the row is kept and marked `has_dq_issues = true` in silver.
- **report**: the check is documented only; the rows are not marked.

Check names are the rule codes used in the finder query and the data dictionary. Counts are what was found on `ahon_dev` on 2026-10-09. The shared DQ tables (`dq_ruleset`, `dq_results`) are not defined yet, so for now this page is the list of checks.

| Layer | Check name | Check description | File that runs it |
| --- | --- | --- | --- |
| Bronze | `file_row_count` | [stop] Each Excel file has the expected number of rows (1,513, 1,566, 1,612, 1,637, 1,683, 1,707, 1,716 for FY2018 to FY2024). The load stops before writing if a file differs. | `load_blgf_ldrrmf_annual_lgu.py` |
| Bronze | `one_data_sheet` | [stop] Each file has exactly one data sheet besides `Metadata`. | `load_blgf_ldrrmf_annual_lgu.py` |
| Bronze | `header_row_found` | [stop] Exactly one header row (the row holding `REGION`) is found in each file. | `load_blgf_ldrrmf_annual_lgu.py` |
| Bronze | `expected_columns` | [stop] The columns found match the four text and six amount columns, mapped by header name. | `load_blgf_ldrrmf_annual_lgu.py` |
| Bronze | `amounts_numeric` | [stop] Every amount is a number; a value that is not a number raises an error. | `load_blgf_ldrrmf_annual_lgu.py` |
| Silver | `fiscal_year_valid` | [stop] `fiscal_year` can be read from the file name and is 2018 to 2024. Found: 0 rows. | `clean_blgf_ldrrmf_annual_lgu.sql` (stop check at the end), `check_blgf_ldrrmf_annual_lgu_clean.sql` |
| Silver | `lgu_type_valid` | [stop] `lgu_type` is Province, City or Municipality. Found: 0 rows. | `clean_blgf_ldrrmf_annual_lgu.sql` (stop check at the end), `check_blgf_ldrrmf_annual_lgu_clean.sql` |
| Silver | `row_count_matches_bronze` | [stop] Silver has the same number of rows as bronze (11,434). | `clean_blgf_ldrrmf_annual_lgu.sql` (stop check at the end), `check_blgf_ldrrmf_annual_lgu_clean.sql` |
| Silver | `unique_lgu_year` | [stop] One row per region + province + LGU name + LGU type per fiscal year. Found: 0 duplicates. | `clean_blgf_ldrrmf_annual_lgu.sql` (stop check at the end), `check_blgf_ldrrmf_annual_lgu_clean.sql` |
| Silver | `total_is_sum_of_parts` | [stop] Total appropriation and total expenditure equal the 70% part plus the 30% part. Found: 0 rows. | `clean_blgf_ldrrmf_annual_lgu.sql` (stop check at the end), `check_blgf_ldrrmf_annual_lgu_clean.sql` |
| Silver | `no_outer_spaces` | [fix] Text columns are trimmed. Found: 3 rows ("Sasmuan ", FY2018 to FY2020). | `clean_blgf_ldrrmf_annual_lgu.sql`, `check_blgf_ldrrmf_annual_lgu_clean.sql` |
| Silver | `negative_amount` | [flag] No amount is below 0. Found: 3 rows (Romblon FY2020, Albay FY2021, Leon FY2022). | `clean_blgf_ldrrmf_annual_lgu.sql` (sets `has_dq_issues`), `check_blgf_ldrrmf_annual_lgu_clean.sql` (finder query) |
| Silver | `zero_appropriation` | [flag] Total budget is not 0. Found: 187 rows. | `clean_blgf_ldrrmf_annual_lgu.sql` (sets `has_dq_issues`), `check_blgf_ldrrmf_annual_lgu_clean.sql` (finder query) |
| Silver | `overspent` | [flag] Total spent is not above the total budget. Found: 388 rows. | `clean_blgf_ldrrmf_annual_lgu.sql` (sets `has_dq_issues`), `check_blgf_ldrrmf_annual_lgu_clean.sql` (finder query) |
| Silver | `region_consistent_across_years` | [report] The same LGU has the same region in every year. Found: the BARMM provinces are under Region IX or XII in FY2018 and FY2021, and under BARMM in other years. | None yet (profiling query run by hand) |
| Silver | `part_overspent` | [report] Neither the 70% part nor the 30% part is spent above its own budget while the total is within budget. Found: 295 rows. | None yet (profiling query run by hand) |
| Silver | `qrf_share_of_budget` | [report] The 30% part is about 30% of the total budget (29 to 31%). Found: 2,574 rows outside it (1,371 below, 871 above, 332 with a 30% part of 0). | None yet (profiling query run by hand) |
| Silver | `year_over_year_jump` | [report] The total budget does not grow 10 times, fall to a tenth, or fall to 0 against the LGU's previous year. Found: 13 growths, 4 falls to a tenth, 67 falls to 0. | None yet (profiling query run by hand) |
| Silver | `lgu_present_every_year` | [report] An LGU is present in every year between its first and last year. Found: 92 LGUs with a gap, many of them renamed (for example "Naga City (Cebu)" and "Naga City"). | None yet (profiling query run by hand) |
| Silver | `lgu_type_consistent` | [report] An LGU keeps the same type in every year. Found: 1 LGU, Pateros (Municipality and City). | None yet (profiling query run by hand) |
| Silver | `amounts_two_decimals` | [report] Amounts have at most 2 decimals, so `DECIMAL(18,2)` rounds nothing. Checked once on bronze: 0 rows. Not part of the silver build. | None yet (profiling query run by hand) |
| Silver | `repeated_amounts` | [report] An LGU's total budget and total spent are not both identical to the previous year. Checked once: 3 rows, no action. | None yet (profiling query run by hand) |
| Silver | `province_row_name` | [report] A Province row has the same name as its province. Checked once: 0 rows. | None yet (profiling query run by hand) |


## Where the checks run

- **Bronze:** in the load code (`load_blgf_ldrrmf_annual_lgu.py`).
- **Silver stop rules and flags:** in `clean_blgf_ldrrmf_annual_lgu.sql` and the check file `check_blgf_ldrrmf_annual_lgu_clean.sql`. The finder query in the check file lists which flag rule each flagged row fails.
- **Silver report-only checks:** profiling queries, run by hand so far.