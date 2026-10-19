# psgc_city_population_clean

- **Table:** `silver.psgc_city_population_clean`
- **Source:** PSA Population embedded in the PSGC API. See the [PSA population data profile](../data-profile/psa-population-profile.md).
- **One row is:** one reported population entry for one city or municipality, census year and PSGC snapshot
- **Key:** `psgc_code`, `population_year`, `_api_period`
- **Built from:** [`silver.psa_psgc_clean`](psa_psgc_clean.md) (`CITY` and `MUN` rows in the selected snapshot) by `src/sql/datasets/psa_psgc/silver/clean_city_population.py`
- **Bronze:** no separate bronze table. The census entries arrive in the `populations_json` column of [`bronze.psa_psgc`](psa_psgc.md) and pass through [`silver.psa_psgc_clean`](psa_psgc_clean.md) unchanged.
- **Owner:** Crizza

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `psgc_code` | string | City or municipality PSGC code | |
| `area_name` | string | City or municipality name | From PSGC silver |
| `geographic_level` | string | `CITY` or `MUN` | |
| `population_raw` | string | Population as reported | Text with commas |
| `year_raw` | string | Census year as reported | |
| `population_psgc_raw` | string | PSGC code the entry was reported for | |
| `population_year` | int | Census year | NULL when `year_raw` is not a four-digit year |
| `population` | bigint | Reported residents | Commas removed; NULL when missing or invalid, never zero |
| `disposition` | string | Outcome of typing the entry | `accepted`, `missing_population`, `invalid_population`, `invalid_year`, `different_population_psgc` |
| `_api_period` | string | PSGC snapshot | |
| `_source_name`, `_source_ref`, `_ingested_at`, `_batch_id`, `_row_hash` | | Source lineage carried from PSGC silver | `_row_hash` identifies the PSGC record the entry came from |
| `_processed_at` | timestamp (UTC) | When the row was last changed in silver | |

## Rules

| Rule | Action |
| --- | --- |
| One row per city or municipality in the snapshot | Stop if not |
| `populations_json` cannot be parsed | Stop |
| City or municipality has no population entries | Warning; no row is invented |
| Entry has an invalid year, invalid value or a different reported PSGC code | Kept with its `disposition`; warning |

## Notes

- All parsed entries, including missing and invalid values, remain in silver. Gold blocks invalid values in its selected year; historical dispositions remain visible.
- The census year is independent of the API snapshot.
- An older first-ingested observation cannot replace a newer one.
