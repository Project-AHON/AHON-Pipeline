# fact_population

Target: `<catalog>.gold.fact_population`. Grain: one barangay per census year; unique key `(barangay_code, population_year)`. This retains PR #91's source/grain, with corrected runtime contracts. `population_key` is deterministic `xxhash64` of the business key and is not a publisher identifier.

Source: `silver.psgc_population_clean`, parsed from embedded PSGC `populations_json` for a selected API snapshot. `population_year` is the census reference year, separate from `source_period`. Region/municipality/province codes come from the same-snapshot `gold.dim_lgu`; codes are strings with leading zeros.

Columns: `population_key` BIGINT; `barangay_code`, `barangay_name`, `municipality_code`, `province_code`, `region_code` STRING; `population` BIGINT (people, nullable); `population_year` INT; `disposition`, `source_period`, `_source_ref`, `_row_hash`, `_batch_id` STRING; `_ingested_at` TIMESTAMP. `province_code` may be NULL. Missing source values remain NULL with `missing_population`; invalid values block Gold and remain visible in Silver.

The dimension must be unique before joining, and joined count must equal the source count. MERGE upserts changed business values without deleting absent observations; target schema/key mismatch stops. Existing NOT NULL populations or different schemas need reviewed migration. This count is not exposed population. Do not load PXWeb administrative totals into this table; those use `fact_population_total`.
