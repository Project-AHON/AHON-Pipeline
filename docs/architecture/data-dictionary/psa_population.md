# PSA population: extract and load

Downloads the PSA 2024 census population table (total population, urban population and percent urban by geographic location) and loads it into the Bronze Delta table.

| Step | Notebook | Input | Output |
|---|---|---|---|
| Extract | `extract_psa_population.py` | PSA PXWeb API | `/Volumes/ahon/reference/source/psa_population/2024_population_urban.csv` |
| Load | `load_psa_population.py` | the CSV above | `ahon.bronze.psa_population_raw` |

Both files run as Databricks notebooks or as plain `.py` files.

## How it works

**Extract**
1. Sends one query to the PXWeb API and requests all locations and parameters as CSV.
2. Converts the response from Latin-1 to UTF-8 if needed.
3. Validates it: valid UTF-8, not empty, has the four required columns, every row well-formed, no empty required values, at least one data row.
4. Saves the file only if validation passes. On failure it raises an error and the existing file is untouched.

**Load**
1. Reads the CSV and checks every row for empty required values. If any row is empty, it stops before writing anything.
2. Builds the full geographic path for each row from the dot indentation in the source (for example `PHILIPPINES 1/ > REGION > PROVINCE > CITY`), because names like "San Isidro" appear in many provinces.
3. Adds provenance columns and a row hash.
4. Merges into Bronze on `geographic_location + census_year`. Existing rows are updated only when the row hash changes, so unchanged rows keep their original `ingested_at` and `batch_id`.

## Columns

Source CSV (required, must be non-empty in every row):

| Column | Meaning |
|---|---|
| `Geographic Location` | Place name, indented with dots (2 dots per level) |
| `Total Population` | Total population |
| `Urban Population` | Urban population |
| `Percent Urban` | Percent of the population that is urban |

Bronze table `ahon.bronze.psa_population_raw`:

| Column | Type | Description |
|---|---|---|
| `geographic_location` | STRING | Full path joined with ` > `, as published (footnote markers such as `1/` are kept) |
| `total_population` | STRING | As published |
| `urban_population` | STRING | As published |
| `percent_urban` | STRING | As published |
| `census_year` | INT | Census year (2024) |
| `source_name` | STRING | `PSA PXWeb API` |
| `source_ref` | STRING | Path of the source CSV |
| `ingested_at` | TIMESTAMP | When the row was first loaded or last changed |
| `batch_id` | STRING | Unique ID (UUID) of the load run |
| `row_hash` | STRING | SHA-256 of the five raw columns, used to detect changes |

Merge keys: `geographic_location` and `census_year`.

## How to rerun

Rerunning is safe: the merge does not create duplicate rows.

1. Run `extract_psa_population` and wait for it to finish. It prints the bytes saved, the number of data rows and the header.
2. Run `load_psa_population`. It prints the batch ID, the number of rows read, the merge keys and the columns.
3. Run `bronze_dq_checks` and confirm every check shows `PASS`.
4. Run `clean_psa_population` and `silver_dq_checks` to refresh Silver.

Always run extract before load. To reload from the file already saved, run only step 2.

If a step fails:
- **Extract validation error:** the PSA response was not usable. Nothing was saved. Retry later, or check whether the table structure changed.
- **Load error about empty required values:** a row in the CSV has an empty or dots-only value. The message gives the first data row. Nothing was written to Bronze.
- **Bad load:** roll back with Delta history, for example `RESTORE TABLE ahon.bronze.psa_population_raw TO VERSION AS OF <n>`. Use `DESCRIBE HISTORY` to find the version.

## Known issues

- **Footnote markers stay in Bronze.** The national name keeps its marker (for example `1/`). Silver removes it, so join on Silver names, not Bronze.
- **One bad row stops the whole load.** Any row with an empty or dots-only value fails the load instead of being set aside. This also covers legitimately missing PSA values.
- **Rows removed at the source are not removed from Bronze.** The merge only inserts and updates, so a place that disappears from a later extract stays in the table.
- **Only the 2024 table is supported.** The API address, file name and `census_year` are fixed in the scripts. Another census year needs code changes.
- **The raw file is overwritten.** Each extract replaces the previous CSV, so older extracts are kept only through Delta history of Bronze.
- **The geographic path depends on dot indentation.** If PXWeb changes the indentation, paths are built wrongly without an error.
- **The Latin-1 fallback is a guess.** If the response is not UTF-8, it is read as Latin-1. Accented names should be spot-checked after the first run.
- **Population values are text.** Bronze keeps them as published. Non-numeric values are caught by `bronze_dq_checks`, not by the load.
- **The load rebuilds the table if its columns differ** from the expected ones (for example, an older table with `census_year` as text). The rebuild keeps table history, but check the table after the first run following a schema change.
