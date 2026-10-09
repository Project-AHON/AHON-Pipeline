-- Build the LDRRMF silver table
-- Cleans ahon.bronze.blgf_ldrrmf_annual_lgu into ahon.silver.blgf_ldrrmf_annual_lgu_clean: one row per LGU per fiscal year.
-- What it does:
--   1. Adds fiscal_year, read from the file name in _source_ref (the digits after "FY").
--   2. Trims the text columns. Region is kept as received (it differs by year for the BARMM provinces).
--   3. Renames the amount columns (bronze names start with digits) and stores them as DECIMAL(18,2) pesos.
--   4. Adds has_dq_issues: true when an amount is negative, the total budget is 0, or total spent is above total budget.
--      It only marks the row. No row is dropped and no value is corrected.
--   5. Carries the five bronze provenance columns forward and adds _processed_at.
-- No PSGC codes here: matching to PSGC is done in gold.
-- Each run rebuilds the whole table (CREATE OR REPLACE), so rerunning does not duplicate rows.

-- Settings: the only line to change between ahon_dev and ahon
USE CATALOG ahon_dev;

CREATE OR REPLACE TABLE silver.blgf_ldrrmf_annual_lgu_clean
COMMENT 'BLGF LDRRMF budget and spending, one row per LGU (province, city or municipality) per fiscal year. Cleaned from bronze: trimmed text, fiscal_year added, amounts as exact pesos. has_dq_issues marks rows that fail a data quality rule. Rows are never dropped.'
AS
WITH cleaned AS (
  SELECT
    CAST(regexp_extract(_source_ref, 'FY(\\d{4})', 1) AS INT) AS fiscal_year,
    trim(region) AS region,
    trim(province) AS province,
    trim(lgu_name) AS lgu_name,
    trim(lgu_type) AS lgu_type,
    CAST(`70pct_ldrrmf_budget_appropriation` AS DECIMAL(18, 2)) AS ldrrmf_70pct_appropriation,
    CAST(`70pct_ldrrmf_expenditures` AS DECIMAL(18, 2)) AS ldrrmf_70pct_expenditure,
    CAST(`30pct_quick_response_fund_budget_appropriation` AS DECIMAL(18, 2)) AS qrf_30pct_appropriation,
    CAST(`30pct_quick_response_fund_expenditures` AS DECIMAL(18, 2)) AS qrf_30pct_expenditure,
    CAST(total_budget_appropriation AS DECIMAL(18, 2)) AS total_appropriation,
    CAST(total_expenditures AS DECIMAL(18, 2)) AS total_expenditure,
    _source_name, _source_ref, _ingested_at, _batch_id, _row_hash
  FROM bronze.blgf_ldrrmf_annual_lgu
)
SELECT
  fiscal_year, region, province, lgu_name, lgu_type,
  ldrrmf_70pct_appropriation, ldrrmf_70pct_expenditure,
  qrf_30pct_appropriation, qrf_30pct_expenditure,
  total_appropriation, total_expenditure,
  -- Flag rules negative_amount, zero_appropriation, overspent (see the data dictionary)
  (ldrrmf_70pct_appropriation < 0 OR ldrrmf_70pct_expenditure < 0 OR qrf_30pct_appropriation < 0
   OR qrf_30pct_expenditure < 0 OR total_appropriation < 0 OR total_expenditure < 0
   OR total_appropriation = 0 OR total_expenditure > total_appropriation) AS has_dq_issues,
  _source_name, _source_ref, _ingested_at, _batch_id, _row_hash,
  current_timestamp() AS _processed_at
FROM cleaned;

-- Stop with an error if the new table breaks a stop rule (row count, key, year, LGU type, totals).
-- The table is already written at this point, so fix the cause and rerun the file.
SELECT
  assert_true(count(*) = (SELECT count(*) FROM bronze.blgf_ldrrmf_annual_lgu), 'Silver row count differs from bronze'),
  assert_true(count(*) = count(DISTINCT fiscal_year, region, province, lgu_name, lgu_type), 'Silver has duplicate LGU-year keys'),
  assert_true(count_if(fiscal_year IS NULL OR fiscal_year NOT BETWEEN 2018 AND 2024) = 0, 'fiscal_year is missing or outside 2018 to 2024'),
  assert_true(count_if(lgu_type IS NULL OR lgu_type NOT IN ('Province', 'City', 'Municipality')) = 0, 'lgu_type is not Province, City or Municipality'),
  assert_true(
    count_if(total_appropriation <> ldrrmf_70pct_appropriation + qrf_30pct_appropriation
             OR total_expenditure <> ldrrmf_70pct_expenditure + qrf_30pct_expenditure) = 0,
    'A total is not the sum of its two parts')
FROM silver.blgf_ldrrmf_annual_lgu_clean;