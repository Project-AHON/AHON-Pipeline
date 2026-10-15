# 0012: City/municipality analytical grain

- Status: Implemented following the user's explicit grain correction; pending team review
- Date: 2026-10-15
- Supersedes: the barangay population fact contract in 0010 and the earlier retention of PR #91's grain

The analytical geographic unit is a city or municipality. `gold.fact_population` has one row per `(psgc_code, population_year)` for `CITY`/`MUN` entities in the selected PSGC snapshot. The PSA source supplies population arrays for those entities directly, so those reported totals are used without adding barangay, province or regional observations together.

`silver.psgc_city_population_clean` types the direct source records and retains missing/invalid dispositions. Gold includes every same-snapshot city/municipality for the configured census year, keeping missing measurements or absent records as NULL. `AVAILABLE` and `MISSING` describe the presence of that city-level source measurement, not completeness of barangay data or earthquake exposure. GX gates key uniqueness, snapshot/reference alignment, invalid selected-year values and output cardinality. Source and census reference periods remain distinct inputs.

All native PSGC geographic levels and raw population arrays remain available for reference/provenance. The old barangay Silver loader is not a deployed task. PXWeb's separate hierarchical fact is source context, not the canonical LGU analysis fact. Funding analysis excludes provincial reports; the original financial reports remain retained. CMCI keeps city/municipality × year × indicator detail. Earthquake observations and OSM features retain their native grains until a defensible geographic association exists.

This changes the Gold population schema and business key. Existing barangay facts are not overwritten or automatically migrated: the new writer blocks the old schema. Verification uses a separate development catalog. Production needs a reviewed rebuild/cutover, with dependent views and consumers updated together. No production deployment or GitHub publication is part of this change.
