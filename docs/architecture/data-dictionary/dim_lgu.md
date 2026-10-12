# Shared LGU dimension

gold.dim_lgu has one row per ten-digit STRING psgc_code, with area_name and geographic_level REG/PROV/CITY/MUN/BGY/SUBMUN. region_code, province_code, municipality_code and barangay_code are nullable STRING hierarchy identifiers. source_period STRING identifies the selected PSGC snapshot. Existing processing/audit fields remain as projected by src/sql/gold/dim_lgu.py.

A snapshot change is blocked when existing source_period differs. Duplicate selected reference codes fail. Names alone are not keys. Restrict CITY/MUN for the combined evidence output; never aggregate parent and child facts together. Missing source rows do not delete existing dimension rows; same-period removals need a separately reviewed complete-snapshot migration.
