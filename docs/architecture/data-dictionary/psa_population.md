# PSA Population

Raw 2024 Philippine Statistics Authority (PSA) census population data, loaded into the bronze layer of the `ahon` catalog.

## Summary

- 1,744 rows from the 2024 Census of Population (reference date 1 July 2024), loaded into `ahon.bronze.psa_population_raw`.
- Four geographic levels: 1 national, 18 regions, 118 provinces, 1,607 cities/municipalities.
- Source: PSA OpenSTAT PXWeb table `0241A6DPUP1.px`, saved as a CSV in `/Volumes/ahon/reference/source/psa_population/`.
- The extract validates the API response (non-empty, expected columns, at least one data row) before it replaces the CSV.
- Loaded with a Delta `MERGE` on `geographic_location` + `census_year`; matched rows update only when `row_hash` changes.
- All quality checks passed on the first load, and the regions sum to the national total (112,727,776).
- To do in silver: remove the `1/` footnote marker from the national row and cast the population and percent columns.

## How to reproduce

### Prerequisites

- Databricks workspace with Unity Catalog and a compute cluster or serverless compute that can run Python notebooks.
- Catalog `ahon` containing:
  - schema `reference` with a volume named `source` (path `/Volumes/ahon/reference/source`)
  - schema `bronze`
- Permissions: write access on the `source` volume, and `CREATE TABLE` plus `MODIFY` on `ahon.bronze`.
- Outbound network access from compute to `openstat.psa.gov.ph`.
- Python packages: `requests` (extract step). `pyspark` and `delta` are provided by the Databricks runtime. 

### Run

1. Import both notebooks into the workspace (or sync the repo through Databricks Repos / Git folders).
2. Run the **extract notebook**. It POSTs a query selecting all geographic locations and all parameters from the PXWeb table, and requests CSV. It validates the response (see Extract validation), then writes it to the volume path above, unchanged unless it had to be converted to UTF-8. If validation fails, the notebook stops with an error and the existing CSV is left as it was. On success it prints the byte count, data row count and CSV header.
3. Run the **load notebook**. It reads the CSV, builds the full geographic path for each row, adds provenance columns and a row hash, creates the bronze table if it does not exist, rebuilds the table in place if its columns do not match, for example when `census_year` is still `STRING` (see Migrating an existing table), and merges into it. Matched rows are updated only when `row_hash` differs. It prints the batch ID, rows read, merge keys and column list.
4. Check the results against the expected values below.

### Configuration

Settings are constants at the top of each notebook. Change them there if the team renames anything.

| Setting | Value | Notes |
|---|---|---|
| `API_URL` | `https://openstat.psa.gov.ph/PXWeb/api/v1/en/DB/1A/PO_2024/0241A6DPUP1.px` | Extract notebook |
| `REQUIRED_COLUMNS` | `Geographic Location`, `Total Population`, `Urban Population`, `Percent Urban` | Extract notebook; the CSV header must contain all four |
| `VOLUME_ROOT` | `/Volumes/ahon/reference/source` | Both notebooks |
| `DATASET_NAME` | `psa_population` | One folder per dataset in the volume (team naming standard) |
| `SOURCE_PATH` | `<VOLUME_ROOT>/<DATASET_NAME>/2024_population_urban.csv` | Load notebook |
| `TABLE_NAME` | `ahon.bronze.psa_population_raw` | Load notebook |
| `CENSUS_YEAR` | `2024` (integer) | Load notebook; not present in the file |
| `MERGE_KEYS` | `geographic_location`, `census_year` | Load notebook |
| `UPDATE_CONDITION` | `NOT (target.row_hash <=> source.row_hash)` | Load notebook; matched rows are updated only when this is true |

### Extract validation

The extract notebook checks the API response before it touches the volume. It stops with an error if any of these fail:

- The response is not empty. If it is not valid UTF-8, it is assumed to be Latin-1 and converted to UTF-8 first, so the saved CSV is always UTF-8. The load notebook reads UTF-8 only.
- The header contains all four required columns.
- Every data row has all four fields.
- There is at least one data row.

The check covers structure only. A well-formed but truncated response would still pass, so compare the load results with the expected values below.

### Re-running and idempotency

- Re-running with unchanged source data is safe: the merge matches every row and updates none, because `row_hash` is identical. Row count stays the same.
- If PSA revises a value, only the affected rows are updated, and their provenance columns (`ingested_at`, `batch_id`, `source_ref`) are refreshed.
- The extract step overwrites the CSV each run with the latest pull, but only if the response passes validation. Keep a copy first if you need the previous version.
- To load another census year, update `API_URL` and `CENSUS_YEAR` and re-run both steps. Rows for different years do not overwrite each other, since `census_year` is part of the merge key.

### Migrating an existing table

`census_year` is now an `INT`, and the table must have exactly the 10 columns listed under Columns, in that order. Tables from earlier versions (with `census_year` as `STRING`, or with leftover columns from a manual migration) are fixed automatically on the next load. No manual step is needed.

- **How:** the load notebook replaces the table in one atomic create-or-replace that selects only the expected columns and casts `census_year` to `INT`. Table history is kept.
- **Re-running:** if the table already matches, nothing is rewritten. If a run fails part-way, the table is left as it was, and the next run tries again.
- **Safety check:** the notebook stops with an error, without changing the table, if any `census_year` value is not an integer or the column has an unexpected type.
- **Row hashes** stay the same, because `census_year` is hashed as text (`2024`) either way.
- **After a rebuild:** create-or-replace resets table properties to their defaults, and this pipeline sets none. If you manage permissions or custom properties on this table, confirm they are still as expected.

### Expected results after a first load

| Check | Expected |
|---|---|
| Total rows | 1,744 |
| Distinct `census_year` | 1 (`2024`) |
| `census_year` column type | `INT` |
| Rows by depth (1 / 2 / 3 / 4) | 1 / 18 / 118 / 1,607 |
| Duplicate `geographic_location` | 0 |
| Null or empty values | 0 in every column |
| Sum of region `total_population` | 112,727,776, equal to the national row |

The batch ID and `ingested_at` differ on every full load.

## Files ingested

| File | Location | Origin |
|---|---|---|
| `2024_population_urban.csv` | `/Volumes/ahon/reference/source/psa_population/` | PSA OpenSTAT PXWeb table `0241A6DPUP1.px` (2024 census: total population, urban population and percent urban by geographic location), pulled via the PXWeb API |

This is the only file ingested.

## Columns

All columns are `STRING` except `census_year` (`INT`) and `ingested_at` (`TIMESTAMP`). PSA values are stored exactly as published; casting happens downstream.

### Source columns

| Column | Distinct | Description |
|---|---|---|
| `geographic_location` | 1,744 | Full hierarchical path joined with ` > ` (for example, region > province > city/municipality). Built from the dot-indentation in the source CSV (2 dots per level). The full path is needed because leaf names such as "San Isidro" repeat across provinces. |
| `total_population` | 1,726 | Total population of the area, as published by PSA. Values end in `.00`. |
| `urban_population` | 1,179 | Population living in urban areas within the area. Values end in `.00`. |
| `percent_urban` | 1,086 | Percentage of the area's population that is urban, on a 0 to 100 scale, with decimals. |
| `census_year` | 1 | Census year as an integer (`2024`), so it can be filtered and compared numerically. Set by the load notebook, not read from the file. |

### Provenance columns

| Column | Type | Description |
|---|---|---|
| `source_name` | STRING | Source system. Always `PSA PXWeb API`. |
| `source_ref` | STRING | Path of the file the row was read from. |
| `ingested_at` | TIMESTAMP | When the batch that last wrote this row ran. |
| `batch_id` | STRING | UUID of the load run that last wrote this row. |
| `row_hash` | STRING | SHA-256 of the five source columns joined with `\|`. `census_year` is hashed as text. Used to detect changed rows. Unique per row (1,744 distinct). |

Because updates are gated on `row_hash`, unchanged rows keep their original `ingested_at`, `batch_id` and `source_ref`. These columns mean "last changed by", not "last seen by".

## Data profile (first load)

### Geographic hierarchy

| Depth | Level | Rows |
|---|---|---|
| 1 | National (PHILIPPINES) | 1 |
| 2 | Region | 18 |
| 3 | Province | 118 |
| 4 | City/Municipality | 1,607 |

### Numeric statistics 

| Statistic | total_population | urban_population | percent_urban |
|---|---|---|---|
| Count | 1,744 | 1,744 | 1,744 |
| Mean | 244,710 | 129,477 | 25.6 |
| Std dev | 2,814,404 | 1,594,528 | 28.5 |
| Min | 406 | 0 | 0.0 |
| Max | 112,727,776 | 62,221,580 | 100.0 |

These statistics span all four levels, including the national row, so the mean and standard deviation are for orientation only. Do not use them as population measures.

### `percent_urban` distribution

| Bucket | Rows |
|---|---|
| 0% (fully rural) | 552 |
| 1 to 49% | 853 |
| 50 to 99% | 314 |
| 100% (fully urban) | 25 |

## Known issues and caveats

1. **Footnote marker in the national row.** The top-level name is `PHILIPPINES  1/`. The `1/` is a PSA footnote marker, and every path inherits it (for example, `PHILIPPINES  1/ > Region X (Northern Mindanao)`). Clean it in the silver layer.
2. **Repeated leaf names.** 122 leaf names appear under more than one province (`San Jose` and `San Isidro` 9 times each, `Santa Maria` and `Quezon` 7 times each). Never join or deduplicate on the leaf name alone. The full path is the unique key.
3. **Population and percent columns are strings.** `total_population` and `urban_population` always end in `.00`, and 1,148 of 1,744 `percent_urban` values have decimals. Cast to numeric types in silver.
4. **Mixed hierarchy levels in one table.** Summing `total_population` across all rows multiple-counts the population. Filter by depth first.
5. **Long, redundant paths.** `geographic_location` runs 15 to 122 characters (average about 70). Split it into `region`, `province` and `lgu_name` columns in silver.
6. **Indentation dependency.** The hierarchy relies on 2 leading dots per level in the PXWeb CSV. If PSA changes that format, paths will be wrong without raising an error. The depth counts in the expected-results table are the quickest way to notice this.
7. **Merge key uniqueness.** Two source rows with the same path would make the Delta `MERGE` fail with a "multiple source rows matched" error. None exist today.
8. **Boundary and naming changes across censuses.** Administrative areas change between censuses (for example, the new Negros Island Region and the split of Maguindanao). Joining to 2020 or earlier data by name may not line up. Verify before comparing years.
9. **Official status.** The 2024 counts were declared official through Proclamation No. 973 (11 July 2025). The reference date is 1 July 2024.
10. **Extract validation checks structure, not completeness.** A well-formed response with fewer rows than expected would be saved. Check the row count after each load.
