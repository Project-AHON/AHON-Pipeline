# PSA Population

2024 Philippine Statistics Authority (PSA) census population data, loaded into the `ahon` catalog as a bronze (raw) table and a silver (clean, typed) table.

## Summary

- **Source:** PSA OpenSTAT PXWeb table `0241A6DPUP1.px` (reference date 1 July 2024), saved as a CSV in `/Volumes/ahon/reference/source/psa_population/`.
- **Bronze** `ahon.bronze.psa_population_raw` keeps values as published: 1,744 rows (1 national, 18 regions, 118 provinces, 1,607 cities/municipalities). The regions add up to the national total of 112,727,776.
- **Silver** `ahon.silver.psa_population` removes the `1/` footnote marker, casts the numbers, derives `rural_population` and the geography columns, and sends bad rows to `ahon.silver.psa_population_rejects`.
- **Re-running:** bronze and silver are idempotent, so unchanged data changes nothing. The rejects table is append-only.
- **Data quality notebooks** print PASS or FAIL for each check. They do not stop a job.

## Notebooks (run in this order)

| # | Notebook | Writes | What it does |
|---|---|---|---|
| 1 | `extract_psa_population.py` | CSV in the volume | Pulls the PXWeb table. Saves it only if it is valid UTF-8 (Latin-1 is converted), has the four required columns and at least one row, and every row has a non-blank value in all four columns. Otherwise it stops and keeps the previous CSV. |
| 2 | `load_psa_population.py` | bronze | Re-checks for blank required values, builds the full path (`region > province > city`), adds provenance and a `row_hash`, and merges on `geographic_location` + `census_year`. A matched row is updated only when its `row_hash` changes. If the table's columns do not match (for example `census_year` still `STRING`), it rebuilds the table in place first. |
| 3 | `bronze_dq_checks_psa_population.py` | none | Nulls, empty strings, non-numeric values and duplicate keys. |
| 4 | `silver_psa_population.py` | silver, rejects | See below. |
| 5 | `silver_dq_checks_psa_population.py` | none | Nulls, negative values, `percent_urban` outside 0 to 100, urban above total, `rural_population` null, negative or not equal to total minus urban, leftover footnote marker, duplicate keys. |

### What silver does

- **Names:** removes the footnote marker from every level of the path.
- **Types:** `total_population`, `urban_population` and `rural_population` become BIGINT, `percent_urban` DOUBLE and `census_year` INT. A value that cannot be parsed becomes NULL and is rejected.
- **Derived columns:** `rural_population` (total minus urban), `geo_level` (0 national, 1 region, 2 province, 3 city/municipality), and `region`, `province` and `city_municipality` from the path.
- **Rejects:** a NULL required field, an empty name or a negative `rural_population` goes to the rejects table with a `reject_reason`.
- **Sync with bronze:** silver holds exactly the valid bronze rows. Rows with no match in the source are deleted, including old rows that still carry the marker. The job stops if there is nothing to load, if two rows share a key, or if silver's row count differs from the rows merged.

## How to reproduce

- **Prerequisites:** a Databricks workspace with Unity Catalog and Runtime 12.2 or later. Catalog `ahon` with schemas `reference` (volume `source`), `bronze` and `silver`. Permission to write the volume and create and modify tables in bronze and silver. Outbound access to `openstat.psa.gov.ph`.
- **Run:** import the notebooks (or sync the repo), then run them in the order above. Settings such as table names, paths and `CENSUS_YEAR = 2024` are constants at the top of each notebook.


## Re-running

- **Unchanged data:** nothing is updated, and provenance columns keep their "last changed" meaning.
- **Revised value:** only the affected rows are updated.
- **Another census year:** change `API_URL` and `CENSUS_YEAR`, then re-run. Years do not overwrite each other.
- **Rejects table:** each run appends the same bad rows again with a new `batch_id`, so filter on `batch_id`.

## Expected results

| Check | Bronze | Silver |
|---|---|---|
| Rows | 1,744 | 1,744 |
| Rows by depth / `geo_level` (1 / 18 / 118 / 1,607) | yes | yes |
| Rejected rows | n/a | 0 |
| Data quality notebook | all PASS | all PASS |

The bronze figures come from the first load. The silver figures are expected from the bronze checks, so confirm them after your first silver run.

## Columns

- **Bronze (10):** `geographic_location`, `total_population`, `urban_population` and `percent_urban` as published (STRING), `census_year` (INT), then `source_name`, `source_ref`, `ingested_at` (TIMESTAMP), `batch_id` and `row_hash`. `geographic_location` is the full path joined with ` > `. `row_hash` is a SHA-256 of the five data columns, with `census_year` hashed as text.
- **Silver (15):** `geographic_location` (cleaned), the typed numbers, `rural_population`, `census_year`, `geo_level`, `region`, `province`, `city_municipality`, and the same five provenance columns. `source_ref` is the bronze table name.

## Known issues

1. **Repeated names:** 122 leaf names such as `San Jose` appear in more than one province. Always use the full path as the key.
4. **Indentation:** the hierarchy relies on 2 leading dots per level in the PXWeb CSV. If PSA changes that, paths break without an error. The rows-by-depth check is the quickest way to notice.
5. **Boundary changes:** areas change between censuses (for example the Negros Island Region and the Maguindanao split). Check before comparing years by name.
7. **Official status:** the 2024 counts were declared official through Proclamation No. 973 (11 July 2025).
