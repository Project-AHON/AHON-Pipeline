> Historical prior implementation. For the supported runtime contract see [psa_population.md](psa_population.md) and [decision 0010](../../decisions/0010-reconciled-source-contracts.md). The details below are retained for historical context and are not executable deployment instructions.

# PSA Population Data Quality Checks

Two read-only notebooks check the PSA population tables after each load: one for bronze (`ahon.bronze.psa_population_raw`) and one for silver (`ahon.silver.psa_population`). Each prints one line per check with its failing row count and a status.

## How they work

- **Run after** the load notebook (bronze checks) or the silver notebook (silver checks). They only read tables and never change data.
- **Output:** a line such as `null_total_population  0  PASS` for every check.
- **Status:** `PASS` when the count is 0, `FAIL` when it is above 0, and `INFO` for numbers that are reported only.
- **They do not stop a job.** A `FAIL` is printed but the notebook still succeeds, so read the output after each run.
- **Row-level checks** count the rows that break a rule. A count of 0 means no row broke it.

## Bronze DQ notebook

Bronze keeps values as published (as text), so these checks look for missing, blank and non-numeric values.

| Check | Flags a row when |
|---|---|
| `total_rows` | Always reported (`INFO`), for reference. |
| `empty_<column>` | The value is empty, whitespace only, or only dots. PXWeb marks missing values with dots. Applies to `geographic_location`, `total_population`, `urban_population` and `percent_urban`. |
| `null_<column>` | The value is NULL. Applies to the four columns above and `census_year`. |
| `non_numeric_<column>` | A value is present but cannot be read as a number. Applies to `total_population`, `urban_population` and `percent_urban`. |
| `duplicate_merge_keys` | Counts `geographic_location` + `census_year` pairs that appear more than once. |

## Silver DQ notebook

Silver has typed numbers and derived columns, so these checks also cover ranges, the rural population arithmetic and the geography columns.

| Group | Check | Flags a row when |
|---|---|---|
| Reference | `total_rows` | Always reported (`INFO`). |
| Missing values | `null_geographic_location`, `null_total_population`, `null_urban_population`, `null_percent_urban`, `null_census_year` | The value is NULL. |
| | `empty_geographic_location` | The name is empty after trimming. |
| Valid ranges | `negative_total_population`, `negative_urban_population` | The value is below 0. |
| | `percent_urban_out_of_range` | `percent_urban` is below 0 or above 100. |
| | `urban_gt_total_population` | Urban population is larger than total population. |
| Rural population | `null_rural_population` | Total and urban are present but `rural_population` is NULL. |
| | `negative_rural_population` | `rural_population` is below 0. |
| | `rural_population_mismatch` | `rural_population` is not total minus urban. |
| Names | `footnote_marker_in_name` | The name still contains a PSA footnote marker such as `1/`. |
| Geography | `null_geo_level`, `geo_level_out_of_range` | `geo_level` is NULL, or outside 0 to 3. |
| | `missing_geo_part` | A level needs a path part that is NULL, for example a province-level row with no `province`. |
| | `extra_geo_part` | A path part is filled in that the level should not have, for example a region row with a `province`. |
| Row counts | `duplicate_merge_keys` | Counts `geographic_location` + `census_year` pairs that appear more than once. |
| | `bronze_silver_row_gap` | Bronze rows minus silver rows. `PASS` only at 0. Above 0 means rows were rejected, and below 0 means silver holds rows bronze no longer has. |
| | `rejects_table_keys` | Distinct path and year pairs in `ahon.silver.psa_population_rejects` (`INFO`, 0 if the table does not exist). The table keeps every run, so this is a history count. |

`geo_level` means 0 national, 1 region, 2 province and 3 city/municipality. The geography checks use it to confirm that each row has exactly the path parts its level should have.

## Expected results

| Notebook | Expected output |
|---|---|
| Bronze | `total_rows` 1,744 and every other check 0 (`PASS`). |
| Silver | `total_rows` 1,744, `bronze_silver_row_gap` 0, `rejects_table_keys` 0, and every other check 0 (`PASS`). |

The row count comes from the first bronze load. The silver figures are expected from the bronze results, so confirm them after your first silver run.