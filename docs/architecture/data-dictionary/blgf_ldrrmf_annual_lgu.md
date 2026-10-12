# LDRRMF source and Silver

bronze.blgf_ldrrmf_annual_lgu retains workbook labels region/province/lgu_name/lgu_type STRING, source numeric appropriation/expenditure cells and generic lineage. Original XLSX files preserve exact source fidelity; numeric workbook cells can be DOUBLE before checked Silver casts.

silver.blgf_ldrrmf_annual_lgu_clean is keyed by fiscal_year INT and contextual region/province/lgu_name/lgu_type STRING. ldrrmf_70pct_appropriation/expenditure, qrf_30pct_appropriation/expenditure and total_appropriation/expenditure are DECIMAL(18,2) pesos. Ratios use checked expenditure/appropriation, zero denominator → NULL. Negative/zero/overspent conditions are flags, never silently clipped. Totals are checked. _source_ref/_row_hash/_source_name/_batch_id STRING, _ingested_at/_processed_at TIMESTAMP. source_period STRING records mapping snapshot; psgc_code STRING nullable; psgc_match_method/status preserve disposition.

reference.blgf_ldrrmf_lgu_map has contextual STRING keys, first_year/last_year INT, psgc_code/psgc_name/match_method/match_status STRING, candidate_count INT, source_period/mapping_version STRING, _processed_at TIMESTAMP. Exact contextual matches and reviewed overrides are accepted; name-only, ambiguous, different-level and historical split cases are withheld. Schema/snapshot changes require review. No overwrite or missing-row deletion.

Extraction covers FY2018–2024 pinned source files. Latest ingested observation per fiscal/context key feeds Silver; cardinality assertions block duplicated mapping joins.

silver.blgf_ldrrmf_annual_lgu_reject retains the typed candidate schema (excluding join-helper _in_lgu_map), provenance and disposition STRING invalid_financial_contract_or_missing_reference. Invalid year/type, null/unparseable amounts, inconsistent totals or missing reference-map rows are saved before a blocking assertion. Key source_ref+row_hash makes retry quarantine stable; raw values remain in the referenced Bronze observation/workbook. Negative, zero and overspent values remain explicit warning flags in accepted Silver.
