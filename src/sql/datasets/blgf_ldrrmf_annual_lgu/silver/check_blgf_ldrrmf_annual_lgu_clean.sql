-- Databricks notebook source
-- Check the LDRRMF silver table
-- Run after clean_blgf_ldrrmf_annual_lgu.sql. Import as a notebook: each query becomes its own cell.
-- Check names in the comments are the ones in the data quality checks doc for this dataset.

USE CATALOG ahon_dev;

-- COMMAND ----------

-- Check the silver table against the pilot rules
-- What it does: one line with a result for each rule.
--   stop_rules_pass     = true when all five stop checks pass:
--                         row_count_matches_bronze, unique_lgu_year, fiscal_year_valid, lgu_type_valid, total_is_sum_of_parts.
--   text_with_outer_spaces = rows that still have outer spaces (check no_outer_spaces).
--   rows_with_dq_issues = rows where has_dq_issues is true (the union of the checks negative_amount,
--                         zero_appropriation and overspent).
-- What to expect: rows_in_silver equals the bronze row count, stop_rules_pass = true, text_with_outer_spaces = 0.
--   rows_with_dq_issues counts each flagged row once, even if it fails more than one flag check.
SELECT
  count(*) AS rows_in_silver,
  (
    -- row_count_matches_bronze
    count(*) = (SELECT count(*) FROM bronze.blgf_ldrrmf_annual_lgu)
    -- unique_lgu_year
    AND count(*) = count(DISTINCT fiscal_year, region, province, lgu_name, lgu_type)
    -- fiscal_year_valid
    AND count_if(fiscal_year NOT BETWEEN 2018 AND 2024 OR fiscal_year IS NULL) = 0
    -- lgu_type_valid
    AND count_if(lgu_type NOT IN ('Province', 'City', 'Municipality') OR lgu_type IS NULL) = 0
    -- total_is_sum_of_parts
    AND count_if(total_appropriation <> ldrrmf_70pct_appropriation + qrf_30pct_appropriation
                 OR total_expenditure <> ldrrmf_70pct_expenditure + qrf_30pct_expenditure) = 0
  ) AS stop_rules_pass,
  -- no_outer_spaces
  count_if(region <> trim(region) OR province <> trim(province) OR lgu_name <> trim(lgu_name)) AS text_with_outer_spaces,
  -- negative_amount, zero_appropriation, overspent (the three flag checks)
  count_if(has_dq_issues) AS rows_with_dq_issues
FROM silver.blgf_ldrrmf_annual_lgu_clean;

-- COMMAND ----------

-- Check the rows by fiscal year and LGU type (row_count_matches_bronze, lgu_type_valid)
-- What it does: counts rows by year and LGU type.
-- What to expect: the same counts per year and type as in bronze, and only the types Province, City and Municipality.
SELECT fiscal_year,
       count_if(lgu_type = 'City') AS city,
       count_if(lgu_type = 'Municipality') AS municipality,
       count_if(lgu_type = 'Province') AS province,
       count(*) AS total
FROM silver.blgf_ldrrmf_annual_lgu_clean
GROUP BY fiscal_year ORDER BY fiscal_year;

-- COMMAND ----------

-- Check every silver row traces back to exactly one bronze row (row_count_matches_bronze, unique_lgu_year)
-- What it does: counts silver rows with no matching bronze row, and rows whose file + hash appears more than once.
-- What to expect: 0 and 0.
SELECT
  (SELECT count(*) FROM silver.blgf_ldrrmf_annual_lgu_clean s
    LEFT ANTI JOIN bronze.blgf_ldrrmf_annual_lgu b ON s._row_hash = b._row_hash AND s._source_ref = b._source_ref) AS silver_rows_not_in_bronze,
  (SELECT count(*) - count(DISTINCT _source_ref, _row_hash) FROM silver.blgf_ldrrmf_annual_lgu_clean) AS repeated_row_hashes;

-- COMMAND ----------

-- Find the specific data quality issue for each flagged row
-- What it does: re-applies each flag check to the silver table and returns one line per row per check it fails.
--   rule_code is the check name from the data quality checks doc.
--   Join key back to silver: _source_ref + _row_hash.
-- What to expect: one line for each flagged row and check. A row appears more than once if it fails more than one check.
SELECT fiscal_year, region, province, lgu_name, lgu_type, _source_ref, _row_hash, rule_code
FROM (
  -- negative_amount
  SELECT *, 'negative_amount' AS rule_code FROM silver.blgf_ldrrmf_annual_lgu_clean
  WHERE ldrrmf_70pct_appropriation < 0 OR ldrrmf_70pct_expenditure < 0 OR qrf_30pct_appropriation < 0
     OR qrf_30pct_expenditure < 0 OR total_appropriation < 0 OR total_expenditure < 0
  UNION ALL
  -- zero_appropriation
  SELECT *, 'zero_appropriation' FROM silver.blgf_ldrrmf_annual_lgu_clean
  WHERE total_appropriation = 0
  UNION ALL
  -- overspent
  SELECT *, 'overspent' FROM silver.blgf_ldrrmf_annual_lgu_clean
  WHERE total_expenditure > total_appropriation
)
ORDER BY rule_code, fiscal_year, region, province, lgu_name;