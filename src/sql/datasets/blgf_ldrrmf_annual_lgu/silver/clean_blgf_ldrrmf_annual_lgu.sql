-- Build the LDRRMF silver table
-- Cleans ahon.bronze.blgf_ldrrmf_annual_lgu into ahon.silver.blgf_ldrrmf_annual_lgu_clean: one row per LGU per fiscal year.
-- What it does:
--   1. Builds the cleaned rows as a temporary view, silver_candidate (nothing is written yet).
--      a. Adds fiscal_year, read from the file name in _source_ref (the digits after "FY").
--      b. Trims the text columns. Region is kept as received (it differs by year for the BARMM provinces).
--      c. Renames the amount columns (bronze names start with digits) and stores them as DECIMAL(18,2) pesos.
--      d. Adds has_dq_issues: true when an amount is negative, the total budget is 0, or total spent is above total budget.
--         It only marks the row. No row is dropped and no value is corrected.
--      e. Adds psgc_code from reference.blgf_ldrrmf_lgu_map (built by build_blgf_ldrrmf_lgu_map.py, run that first),
--         joined on region + province + lgu_name + lgu_type. Only a "matched" map row gives a code; every other row
--         keeps psgc_code NULL with psgc_match_status 'unmatched'. No row is dropped.
--      f. Carries the five bronze provenance columns forward and adds _processed_at.
--   2. Checks the stop rules on the view. If one fails, the run stops with an error and the existing silver table is left as it was.
--   3. Only when every stop rule passes, replaces the silver table with the view.
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
  -- c. = the cleaned bronze row, m. = the LGU map row (both have region, province, lgu_name and lgu_type)
  c.fiscal_year, c.region, c.province, c.lgu_name, c.lgu_type,
  c.ldrrmf_70pct_appropriation, c.ldrrmf_70pct_expenditure,
  c.qrf_30pct_appropriation, c.qrf_30pct_expenditure,
  c.total_appropriation, c.total_expenditure,
  -- Flag rules negative_amount, zero_appropriation, overspent (see the data dictionary).
  -- coalesce makes sure the column is true or false, never NULL.
  coalesce(
    ldrrmf_70pct_appropriation < 0 OR ldrrmf_70pct_expenditure < 0 OR qrf_30pct_appropriation < 0
    OR qrf_30pct_expenditure < 0 OR total_appropriation < 0 OR total_expenditure < 0
    OR total_appropriation = 0 OR total_expenditure > total_appropriation,
    false) AS has_dq_issues,
  -- PSGC code from the LGU map. psgc_match_status uses the team schema values: 'exact' or 'unmatched'
  CASE WHEN m.match_status = 'matched' THEN m.psgc_code END AS psgc_code,
  CASE WHEN m.match_status = 'matched' THEN 'exact' ELSE 'unmatched' END AS psgc_match_status,
  -- How the map found it (province_name, no_province_name, override), or NULL when it did not
  m.match_method AS psgc_match_method,
  -- Map row found at all? Used by the stop rule below; not written to the table
  m.lgu_name IS NOT NULL AS _in_lgu_map,
  c._source_name, c._source_ref, c._ingested_at, c._batch_id, c._row_hash,
  current_timestamp() AS _processed_at
FROM cleaned AS c
LEFT JOIN reference.blgf_ldrrmf_lgu_map AS m
  ON  m.region   <=> c.region
  AND m.province <=> c.province
  AND m.lgu_name <=> c.lgu_name
  AND m.lgu_type <=> c.lgu_type;

-- COMMAND ----------

-- Check the stop rules on the cleaned rows
-- What it does: stops with an error if a stop rule fails, before the silver table is touched.
--   Rules: row count, amounts not NULL, unique key, valid year, valid LGU type, total = sum of parts,
--   every LGU is in the map (if not, rerun build_blgf_ldrrmf_lgu_map.py).
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
    'A total is not the sum of its two parts'),
  assert_true(count_if(NOT _in_lgu_map) = 0, 'An LGU is missing from reference.blgf_ldrrmf_lgu_map: rerun build_blgf_ldrrmf_lgu_map.py')
FROM silver_candidate;

-- COMMAND ----------

-- Write the silver table
-- What it does: replaces the silver table with the checked rows. This only runs if the stop rules above passed.
-- What to expect: the table has the same rows as bronze. Run check_blgf_ldrrmf_annual_lgu_clean.sql next.
CREATE OR REPLACE TABLE silver.blgf_ldrrmf_annual_lgu_clean
COMMENT 'BLGF LDRRMF budget and spending, one row per LGU (province, city or municipality) per fiscal year. Cleaned from bronze: trimmed text, fiscal_year added, amounts as exact pesos. has_dq_issues marks rows that fail a data quality rule. psgc_code links to PSGC (NULL when not matched). Rows are never dropped.'
AS SELECT * EXCEPT (_in_lgu_map) FROM silver_candidate;