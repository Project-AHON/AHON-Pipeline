# dim_lgu

`<catalog>.gold.dim_lgu` has one current geographic entity per ten-digit string `psgc_code`. It is the existing dimension, built from `silver.psa_psgc_clean` for the configured API `source_period`.

Columns: `source_period`, `psgc_code`, `area_name`, `geographic_level`, `region_code`, `province_code`, `municipality_code`, `barangay_code` (all strings). Levels are `REG`, `PROV`, `CITY`, `MUN`, `BGY`, `SGU`, `SUBMUN`. Hierarchy codes are standardized PSGC prefixes from Silver; they describe the selected snapshot and must not be assumed to be an independent current province or city record without checking the dimension. Barangay code is populated only for BGY.

The key is `psgc_code`; a snapshot tag does not introduce historical dimension grain. The job checks source/target uniqueness and types, rejects a target carrying a different snapshot, and upserts changed attributes. It does not delete absent entities. A new snapshot requires separate review or a fresh development catalog. Geographic joins must check period, level and cardinality.
