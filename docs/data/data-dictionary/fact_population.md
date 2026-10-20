# fact_population

- **Table:** `gold.fact_population`
- **One row is:** one city or municipality for the configured census year
- **Key:** `psgc_code`, `population_year`
- **Built from:** [`gold.dim_lgu`](dim_lgu.md) and [`silver.psgc_city_population_clean`](psgc_city_population_clean.md) by `src/sql/gold/fact_population.py` (query in `fact_population_candidate.sql`)
- **Owner:** Crizza
- **Joins to:** `gold.dim_lgu` on `psgc_code`

Only `CITY`/`MUN` records are analytical population rows. See [decision 0017](../../decisions/0017-city-municipality-analysis-grain.md).

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `psgc_code` | string | City or municipality PSGC code | |
| `area_name` | string | City or municipality name | From `dim_lgu` |
| `geographic_level` | string | `CITY` or `MUN` | |
| `region_code` | string | Region code | From `dim_lgu` |
| `province_code` | string | Province code | From `dim_lgu`; may be NULL |
| `population_year` | int | Census year | Configured `census_year` |
| `population` | bigint | Reported residents | Nullable; never zero for a missing value |
| `source_period` | string | PSGC snapshot | Must equal the selected snapshot |
| `disposition` | string | `accepted`, `missing_population` or `missing_census_observation` | `missing_census_observation` means no source entry for that city and year |
| `coverage_status` | string | `AVAILABLE` or `MISSING` | `AVAILABLE` only when accepted and not NULL |
| `_source_ref`, `_batch_id` | string | Source observation lineage | NULL if no observation exists |
| `_ingested_at` | timestamp (UTC) | Source observation ingestion time | NULL if no observation exists |
| `population_key` | bigint | Stable city/year surrogate key | `xxhash64(psgc_code, population_year)` |
| `_row_hash` | string | SHA-256 of the row's values and lineage | Used to detect changes |
| `_processed_at` | timestamp (UTC) | When the row was last changed | |

## Rules

| Rule | Action |
| --- | --- |
| One row per city or municipality in `dim_lgu` | Stop if not |
| `dim_lgu` and the target carry the selected snapshot | Stop if not |
| Every source city exists in `dim_lgu` | Stop if not |
| Source dispositions are `accepted` or `missing_population` in the selected year | Stop if not |
| Required values present; population not negative | Stop if not |
| Population missing | Warning |
| Source entry no longer matches the current PSGC record | Warning; the older entry is not used |

## Notes

- Gold starts from every city/municipality in the selected `gold.dim_lgu` snapshot and left joins the configured census year. It uses the city's reported measurement directly. It does not sum barangays or combine administrative levels.
- A typed measurement is eligible only while its source row hash matches the current PSGC City/MUN observation. If a newer source array removes that census year, the older typed record stays in silver for audit but cannot appear as a current gold value.
- The table holds cities and municipalities only. Province totals are the sum of their cities and municipalities.
- For 2024 in `Q2_2024`, 19 Sulu municipalities are `MISSING`.
- Resident counts are not earthquake-exposed population.
