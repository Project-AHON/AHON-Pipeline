# Funding fact

gold.fact_ldrrmf: one contextual source LGU per fiscal year, ldrrmf_fact_key BIGINT = xxhash64(fiscal_year,region,province,lgu_name,lgu_type). Collisions/duplicate PSGC-year matches block. psgc_code nullable STRING, fiscal_year INT, source_period STRING; source region/province/lgu_name/lgu_type STRING retained.

appropriation_70_pct/expenditure_70_pct, appropriation_30_pct/expenditure_30_pct, total_appropriation/expenditure DECIMAL(18,2) pesos. utilization_rate_70_pct/30_pct/total DECIMAL(18,4): spent/budget, NULL at zero budget, values above one retained. match_status/method STRING; match_confidence DECIMAL(5,4) always NULL because no calibrated probability exists. has_dq_issues BOOLEAN. Source/ingestion/hash/processed lineage retained from Silver.

Checks precede merge: source accounting, fact-key uniqueness, no duplicate nonnull PSGC/year and LGU membership. Province facts remain separate from City/Municipality. Ratios/funding are proxies, not validated preparedness. See [metrics](../metrics.md).
