# fact_population

Target: `<catalog>.gold.fact_population`. Grain: one city/municipality per census year; unique business key `(psgc_code, population_year)`. `population_key` is deterministic `xxhash64` of that key. Only `CITY`/`MUN` records are analytical population rows. This explicitly replaces the earlier PR #91 barangay-grain contract following the user's correction; see [decision 0017](../decisions/0017-city-municipality-analysis-grain.md).

Source: direct city/municipality population arrays in PSGC Silver, typed by `datasets/psa_psgc/silver/clean_city_population.py` into `silver.psgc_city_population_clean`. Its key is `(psgc_code, population_year, _api_period)`. The census year is independent of the API snapshot. Source codes, raw values, years, reported population PSGC, missing/invalid dispositions and provenance remain in Silver. Historical invalid rows are warned; invalid values in the selected Gold year block publication.

Gold starts from every city/municipality in the selected `gold.dim_lgu` snapshot and left joins the configured census year. It uses the city's reported measurement directly. It does not sum barangays or combine administrative levels. A reported missing value yields `missing_population`; absence of a city/year source record yields `missing_census_observation`. Both retain NULL population, never zero.

A typed measurement is eligible only while its source row hash matches the current PSGC City/MUN observation. If a newer source array removes that census year, the older typed record stays in Silver for audit but cannot appear as a current Gold value. A stale-observation warning records that disposition without deleting history.

| Columns | Type / meaning |
| --- | --- |
| `psgc_code,area_name,geographic_level` | STRING; city/municipality identity and label |
| `region_code,province_code` | STRING; selected reference hierarchy; province may be NULL |
| `population_year` | INT; configured census reference year |
| `population` | BIGINT; reported residents, nullable |
| `source_period` | STRING; selected PSGC snapshot |
| `disposition` | STRING; accepted, missing_population or missing_census_observation |
| `coverage_status` | STRING; AVAILABLE or MISSING for the city-level measurement |
| `_source_ref,_batch_id` | STRING; source observation lineage; NULL if no observation exists |
| `_ingested_at` | TIMESTAMP; source observation ingestion time, nullable for absent records |
| `population_key` | BIGINT; stable city/year surrogate key |
| `_row_hash,_processed_at` | Deterministic value/lineage digest and time of an accepted change |

GX validates source and target uniqueness, selected snapshot, valid dispositions, nonnegative measurements, reference coverage and exactly one row per expected city/year. Missing measurements produce warnings. MERGE updates changed values and preserves same-batch replay timestamps; no absent key is deleted.

Existing barangay-grain targets trigger `city_population_schema_requires_migration` before a write. Use a fresh catalog or reviewed rebuild/cutover; no automatic schema migration or table deletion occurs. `platinum.population_components` exposes the same city/year grain. PXWeb hierarchy rows remain a separate source contract in `fact_population_total` and are not added to these values. Resident counts are not earthquake-exposed population.
