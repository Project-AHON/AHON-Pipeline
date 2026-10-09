-- Build the LDRRMF silver table
-- Cleans ahon.bronze.blgf_ldrrmf_annual_lgu into ahon.silver.blgf_ldrrmf_annual_lgu_clean: one row per LGU per fiscal year.
-- Suggested repo path: src/sql/datasets/blgf_ldrrmf_annual_lgu/silver/clean_blgf_ldrrmf_annual_lgu.sql
-- What it does:
--   1. Builds the cleaned rows as a temporary view, silver_candidate (nothing is written yet).
--      a. Adds fiscal_year, read from the file name in _source_ref (the digits after "FY").
--      b. Trims the text columns. Region is kept as received (it differs by year for the BARMM provinces).
--      c. Renames the amount columns (bronze names start with digits) and stores them as DECIMAL(18,2) pesos.
--      d. Adds has_dq_issues: true when an amount is negative, the total budget is 0, or total spent is above total budget.
--         It only marks the row. No row is dropped and no value is corrected.
--      e. Carries the five bronze provenance columns forward and adds _processed_at.
--   2. Checks the stop rules on the view. If one fails, the run stops with an error and the existing silver table is left as it was.
--   3. Only when every stop rule passes, replaces the silver table with the view.
-- No PSGC codes here: matching to PSGC is done in gold.
-- Each run rebuilds the whole table (CREATE OR REPLACE), so rerunning does not duplicate rows.
-- Run all statements in one session (one SQL task or one notebook run), because a temporary view only lasts for the session.

-- Settings: one line to change between ahon_dev and ahon (check_blgf_ldrrmf_annual_lgu_clean.sql has its own)
USE CATALOG ahon_dev;

-- COMMAND ----------

-- Build the cleaned rows (not written to a table yet)
CREATE OR REPLACE TEMPORARY VIEW silver_candidate AS
WITH cleaned AS (
  SELECT
    -- try_cast gives NULL for a file name without FY####, so the stop rule below reports it
    try_cast(regexp_extract(_source_ref, 'FY(\\d{4})', 1) AS INT) AS fiscal_year,
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
  -- Flag rules negative_amount, zero_appropriation, overspent (see the data dictionary).
  -- coalesce makes sure the column is true or false, never NULL.
  coalesce(
    ldrrmf_70pct_appropriation < 0 OR ldrrmf_70pct_expenditure < 0 OR qrf_30pct_appropriation < 0
    OR qrf_30pct_expenditure < 0 OR total_appropriation < 0 OR total_expenditure < 0
    OR total_appropriation = 0 OR total_expenditure > total_appropriation,
    false) AS has_dq_issues,
  _source_name, _source_ref, _ingested_at, _batch_id, _row_hash,
  current_timestamp() AS _processed_at
FROM cleaned;

-- COMMAND ----------

-- Check the stop rules on the cleaned rows
-- What it does: stops with an error if a stop rule fails, before the silver table is touched.
--   Rules: row count, amounts not NULL, unique key, valid year, valid LGU type, total = sum of parts.
-- What to expect: one line of true values. Any error here means silver was not replaced: fix the cause and rerun the file.
SELECT
  assert_true(count(*) = (SELECT count(*) FROM bronze.blgf_ldrrmf_annual_lgu), 'Silver row count differs from bronze'),
  assert_true(
    count_if(ldrrmf_70pct_appropriation IS NULL OR ldrrmf_70pct_expenditure IS NULL
             OR qrf_30pct_appropriation IS NULL OR qrf_30pct_expenditure IS NULL
             OR total_appropriation IS NULL OR total_expenditure IS NULL) = 0,
    'An amount is NULL'),
  -- DISTINCT over whole rows treats NULL as a value (count(DISTINCT a, b) would skip rows with a NULL)
  assert_true(
    count(*) = (SELECT count(*) FROM (SELECT DISTINCT fiscal_year, region, province, lgu_name, lgu_type FROM silver_candidate)),
    'Silver has duplicate LGU-year keys'),
  -- Update the year range here and in the check file when a new fiscal year is loaded
  assert_true(count_if(fiscal_year IS NULL OR fiscal_year NOT BETWEEN 2018 AND 2024) = 0, 'fiscal_year is missing or outside 2018 to 2024'),
  assert_true(count_if(lgu_type IS NULL OR lgu_type NOT IN ('Province', 'City', 'Municipality')) = 0, 'lgu_type is not Province, City or Municipality'),
  assert_true(
    count_if(total_appropriation <> ldrrmf_70pct_appropriation + qrf_30pct_appropriation
             OR total_expenditure <> ldrrmf_70pct_expenditure + qrf_30pct_expenditure) = 0,
    'A total is not the sum of its two parts')
FROM silver_candidate;

-- COMMAND ----------

-- Write the silver table
-- What it does: replaces the silver table with the checked rows. This only runs if the stop rules above passed.
-- What to expect: the table has the same rows as bronze. Run check_blgf_ldrrmf_annual_lgu_clean.sql next.
CREATE OR REPLACE TABLE silver.blgf_ldrrmf_annual_lgu_clean
COMMENT 'BLGF LDRRMF budget and spending, one row per LGU (province, city or municipality) per fiscal year. Cleaned from bronze: trimmed text, fiscal_year added, amounts as exact pesos. has_dq_issues marks rows that fail a data quality rule. Rows are never dropped.'
AS SELECT * FROM silver_candidate;