# PSA population: extract and load

How to run, check and rerun the two notebooks that bring the 2024 PSA
population table into bronze.

| Notebook | What it does |
| --- | --- |
| `extract_psa_population.py` | Downloads the table from the PSA PXWeb API, checks it, and saves it as a CSV in a Unity Catalog volume |
| `load_psa_population.py` | Reads that CSV, builds the full geographic path, and merges it into `ahon.bronze.psa_population_raw` |

Both notebooks are set to run on serverless, environment version 6.

## Rerun steps

Before you start, make sure:
- you can write to `/Volumes/ahon/reference/source`
- the compute can reach `openstat.psa.gov.ph`

1. **First time only:** run `src/sql/00_setup/bronze_psa.sql`. It creates
   `ahon.bronze.psa_population_raw`. The load stops if the table is missing.
2. Open `extract_psa_population.py`, attach compute, and click **Run all**.
3. Check the output (see below). The CSV is now at
   `/Volumes/ahon/reference/source/psa_population/2024_population_urban.csv`.
4. Open `load_psa_population.py` and click **Run all**.
5. Check the output, then run `bronze_dq_checks`. Every line should say
   PASS (or INFO for the row total).
6. Continue with `clean_psa_population` and `silver_dq_checks` for silver.

Rerunning is safe:
- The extract only replaces the CSV when the new download passes every
  check. A bad download leaves the old file untouched.
- The load merges on `geographic_location` + `census_year`, so a rerun does
  not create duplicates. Rows whose data has not changed are left alone.

### What a good run prints

| Notebook | Output |
| --- | --- |
| Extract | `Saved N bytes to ...`, `Data rows: N`, `Header: [...]`. A line saying the response was converted from Latin-1 appears only when that happened. |
| Load | `Batch ID : <uuid>`, `Read N rows from ...`, `Merge keys : [...]` |

## Columns

### Source CSV

| Column | Meaning |
| --- | --- |
| Geographic Location | Place name. The hierarchy is shown by leading dots, 2 per level. |
| Total Population | Total population of the place |
| Urban Population | Urban population of the place |
| Percent Urban | Share of the population that is urban, as published |

All four are required and must have a value in every row. A value that is
blank, whitespace only, or only dots counts as empty. Any other columns in
the file are ignored by the load.

### Bronze table `ahon.bronze.psa_population_raw`

1 row = 1 geographic location in 1 census year.

| Column | Type | Meaning |
| --- | --- | --- |
| `geographic_location` | STRING | Full path, levels joined with ` > `, for example `Region I > Ilocos Norte`. The national name may end in a footnote marker such as `1/`. |
| `total_population` | STRING | As published |
| `urban_population` | STRING | As published |
| `percent_urban` | STRING | As published |
| `census_year` | INT | 2024. Set by the load, not read from the CSV. |
| `source_name` | STRING | `PSA PXWeb API` |
| `source_ref` | STRING | Path of the CSV that was loaded |
| `ingested_at` | TIMESTAMP | When the row was last inserted or changed |
| `batch_id` | STRING | UUID of the run that inserted or changed the row |
| `row_hash` | STRING | SHA-256 of the five data columns, used to spot changes |

The merge key is `geographic_location` + `census_year`. The load checks that
the table has exactly these columns and types before it writes anything.

## Known issues

- **All or nothing.** One empty required value fails the whole extract or
  load. Nothing is partly saved.
- **No retry on extract.** It makes one request with a 60 second timeout. A
  temporary network error fails the run, so just rerun it.
- **Encoding assumption.** If the download is not valid UTF-8, it is treated
  as Latin-1 and converted. If PXWeb ever sends Windows-1252, some
  punctuation could come out wrong.
- **Hierarchy comes from dots.** If PXWeb changes the indentation, paths will
  be wrong. Place names repeat (for example San Isidro), so the full path is
  the key.
- **Footnote marker stays in bronze.** `1/` is removed in silver. Join on
  silver, not bronze.
- **Numbers are text in bronze.** Nothing is cast or recomputed here.
- **No PSGC codes.** Rows have names only. The dataset profile mentions
  barangay level and PSGC codes, but this extract has neither.
- **One year only.** The API URL, `CENSUS_YEAR` and the file name are fixed to
  2024. For another year, change them in both notebooks.
- **Settings are copied, not shared.** The volume path and file name appear
  in both notebooks. If one changes, change the other.
- **Bronze never deletes.** A row removed upstream stays in bronze.
- **Load counts are rows read.** The printed row count is how many rows were
  read from the CSV, not how many were inserted or updated.
- **Run times are not run dates.** `ingested_at` and `batch_id` only change
  for rows that are new or changed, so they do not show when a run happened.

## If a run fails

| Message | Meaning | What to do |
| --- | --- | --- |
| `HTTPError` or a timeout in the extract | PXWeb did not answer correctly | Wait and rerun. Check the compute can reach `openstat.psa.gov.ph`. |
| `Response is empty` | The API returned nothing | Rerun. If it repeats, check the table in the PXWeb site. |
| `Response is missing expected columns: [...]` | PSA changed the column names | Update `REQUIRED_COLUMNS` in the extract and `REQUIRED_SOURCE_COLS` in the load. |
| `Malformed CSV row N` | Row N has too few or too many fields | Look at row N of the download. Rerun, or report it to PSA. |
| `Data row N has an empty required value` (extract) | Row N has a blank or dots-only value | The old CSV is still in place. Check the row in PXWeb. |
| `Response has a header but no data rows` | The API returned only a header | Rerun. |
| `FileNotFoundError` in the load | The CSV is not in the volume | Run the extract first. |
| `Data row N in <csv> has an empty required value` (load) | The saved CSV has an empty value | Rerun the extract, which refuses to save such a file. |
| `... does not exist; run src/sql/00_setup/bronze_psa.sql first` | The bronze table has not been created | Run that SQL file, then rerun the load. |
| `... differs from src/sql/00_setup/bronze_psa.sql; found {...}` | The table's columns or types do not match what the load expects | Fix the table or the SQL file so they agree. |
