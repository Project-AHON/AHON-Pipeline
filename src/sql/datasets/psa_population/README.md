# PSA Population Ingestion

Loads PSA census population CSVs (2020 and 2024) from a Unity Catalog volume into a bronze Delta table.

## Source

| Item | Value |
|------|-------|
| Volume path | `/Volumes/ahon/bronze/population_raw` |
| Files | `2020 population.csv`, `2024 population.csv` |
| Format | `;` delimited, 2 title rows before the header |

Columns: Geographic Location, Total Population, Household Population, Number of Households. The 2024 file also has Average Household Size; for 2020 it is set to `NULL`.

## Pipeline

1. **Read**: `read_files` with `skipRows => 2` and `inferSchema => false`, so every column stays a string.
2. **Combine**: `unionByName` of the 2020 and 2024 frames, tagged with `census_year` and `source_file`.
3. **Add provenance**: `source_name`, `source_ref`, `ingested_at`, `batch_id`, and `row_hash` (SHA-256 of the raw columns).
4. **Load**: `MERGE` into the bronze table on `geographic_location` + `census_year` (null-safe), updating matches and inserting new rows.

## Target Table

`ahon.bronze.psa_population_raw`

All data columns are `STRING` (raw, no casting). Cleaning and typing belong in the silver layer.

## Usage

1. Place both CSVs in the volume path above.
2. Run the notebook.
3. Check the printed batch ID, merged row count, and merge keys.

## Notes

- `geographic_location` must be unique per `census_year` in the source. Duplicate names (for example, the same barangay name in different municipalities) will make the merge fail.
- `row_hash` is stored but not used as a merge key.
