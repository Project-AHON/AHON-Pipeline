# Population

bronze.psa_population preserves geographic_location, total_population, urban_population, percent_urban as source STRING values; census_year INT = 2024 plus shared lineage. Source indentation constructs a separate path during Silver, without changing raw observations.

silver.psa_population_clean: geographic_location STRING full source path; census_year INT; total_population/urban_population/rural_population BIGINT persons; percent_urban DOUBLE source percent, range 0–100; geo_level INT (0 national,1 region,2 provincial/independent-city group,3 locality); region/province/city_municipality nullable STRING; has_dq_issues BOOLEAN and shared lineage/_processed_at. Key path/year. Counts must be integral and urban ≤ total. Fractional/nonfinite/missing/bad arithmetic rows go to psa_population_reject, keyed by observation, disposition STRING. Rural = total−urban; source percent is not silently recomputed.

gold.fact_population retains Silver fields and adds population_fact_key STRING SHA256(path/year), normalized region_key/province_key/locality_key STRING, candidate_codes ARRAY<STRING>, psgc_code nullable STRING, mapping_status STRING and source_period STRING. Exact unique region+province+locality at geo_level3 only. Ambiguous/unmatched/aggregate rows stay. Source aggregates must not be summed with child rows.

2024 profile: 1744 rows across levels [1,18,118,1607]. Accepted+rejected reconciles current landed scope; old corrections remain in Bronze/rejects. Latest valid corrections merge without deleting absent source keys.
