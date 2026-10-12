# PSA PSGC

bronze.psa_psgc retains source fields/labels plus shared observation columns. Source code is preserved raw; Silver psgc_code STRING is normalized to ten digits according to reviewed 8/9/10-digit rules. Extraction profiles four explicit snapshots and rejects missing/count-changed source structure.

silver.psa_psgc_clean: psgc_code, area_name, geographic_level, region_psgc, province_psgc, citymun_psgc, province_name, province_city and _api_period are STRING; source/status/old-name/classification fields follow the preserved source schema. _processed_at and _ingested_at are TIMESTAMP, _source_name/_source_ref/_batch_id/_row_hash are STRING. Key code/level/period; latest ingestion, batch and hash deterministically wins. Invalid missing geographic fields go to silver.psa_psgc_reject with disposition and preserved cleaned fields; malformed code is retained in psa_psgc_code_reject before a persisted blocking failure. No automatic deletion or schema migration. Inspect actual schema before migrating existing data.

Q2_2024 is the default downstream reference. Encoding/aliases remain auditable; conflicting names cannot establish geography without reference context or a reviewed override. Older-period mappings are not historical-boundary proofs.

silver.psa_psgc_code_reject preserves the full raw Bronze schema plus disposition STRING invalid_psgc_code, keyed observation ID (legacy fallback API period+row hash). It is separate from the cleaned-field geography reject schema. Allowed geographic levels are REG/PROV/CITY/MUN/BGY/SUBMUN; unknown levels are quarantined.

All optional descriptive attributes are STRING: correspondence_code,old_name,status,income_classification,city_class,urban_rural,island_region,populations_json,source_version. Source hierarchy pieces region_code,province_code,municipality_code,barangay_code and derived full region_psgc,province_psgc,citymun_psgc are STRING. Canonical DDL: src/sql/00_setup/02_psa_psgc_clean.sql (select the catalog via run_sql.py). SGU is retained as a distinct special geographic level.
