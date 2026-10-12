-- Build the LDRRMF gold fact table
-- Builds gold.fact_ldrrmf from silver.blgf_ldrrmf_annual_lgu_clean: one row per LGU (province, city or municipality) per fiscal year.
-- Needs first: the silver table (which already carries psgc_code from the LGU map) and gold.dim_lgu.
-- What it does:
--   1. Builds the fact rows as a temporary view, fact_candidate (nothing is written yet).
--      a. Copies psgc_code, the match status and the match method from silver. Unmatched rows stay, with psgc_code NULL.
--      b. Makes ldrrmf_fact_key, a number computed from the silver key, so the same row gets the same key on every rebuild.
--      c. Renames the amounts to the schema names and adds the three utilization rates (spent / budget).
--         A rate is NULL when the budget is 0, so there is no divide-by-zero. Rates above 1 (overspent) are kept as they are.
--      d. Carries has_dq_issues and the lineage columns (_source_ref, _row_hash) from silver.
--   2. Checks the stop rules on the view. If one fails, the run stops and the existing gold table is left as it was.
--   3. Only when every stop rule passes, replaces the gold table with the view.
-- Source labels (region, province, lgu_name, lgu_type) stay exactly as BLGF wrote them. The correct region comes from dim_lgu through psgc_code.
-- Each run rebuilds the whole table (CREATE OR REPLACE), so rerunning does not duplicate rows.
-- Run all statements in one session, because a temporary view only lasts for the session.

-- Settings: one line to change between ahon_dev and ahon
USE CATALOG ahon_dev;

-- COMMAND ----------

-- Build the fact rows (not written to a table yet)
CREATE OR REPLACE TEMPORARY VIEW fact_candidate AS
SELECT
  -- Same five columns as the silver key, so the key is stable across rebuilds
  xxhash64(s.fiscal_year, s.region, s.province, s.lgu_name, s.lgu_type) AS ldrrmf_fact_key,
  s.psgc_code,
  s.fiscal_year,
  s.region,
  s.province,
  s.lgu_name,
  s.lgu_type,
  s.ldrrmf_70pct_appropriation AS appropriation_70_pct,
  s.ldrrmf_70pct_expenditure AS expenditure_70_pct,
  s.qrf_30pct_appropriation AS appropriation_30_pct,
  s.qrf_30pct_expenditure AS expenditure_30_pct,
  s.total_appropriation,
  s.total_expenditure,
  -- Rates as a ratio: 1.0000 means everything budgeted was spent. try_divide gives NULL when the budget is 0
  CAST(try_divide(s.ldrrmf_70pct_expenditure, nullif(s.ldrrmf_70pct_appropriation, 0)) AS DECIMAL(18, 4)) AS utilization_rate_70_pct,
  CAST(try_divide(s.qrf_30pct_expenditure, nullif(s.qrf_30pct_appropriation, 0)) AS DECIMAL(18, 4)) AS utilization_rate_30_pct,
  CAST(try_divide(s.total_expenditure, nullif(s.total_appropriation, 0)) AS DECIMAL(18, 4)) AS utilization_rate_total,
  -- Schema names: match_status is 'exact' or 'unmatched'. The matcher only accepts exact name matches
  -- (or a checked override), so the confidence is 1 or 0
  s.psgc_match_status AS match_status,
  s.psgc_match_method AS match_method,
  CAST(CASE WHEN s.psgc_match_status = 'exact' THEN 1 ELSE 0 END AS DECIMAL(5, 4)) AS match_confidence,
  s.has_dq_issues,
  s._source_ref,
  s._row_hash,
  current_timestamp() AS _processed_at
FROM silver.blgf_ldrrmf_annual_lgu_clean AS s;

-- COMMAND ----------

-- Check the stop rules on the fact rows
-- What it does: stops with an error if a rule fails, before the gold table is touched.
--   Rules: same row count as silver, unique key, every psgc_code exists in dim_lgu,
--   match_confidence between 0 and 1.
-- What to expect: one line of true values.
SELECT
  assert_true(count(*) = (SELECT count(*) FROM silver.blgf_ldrrmf_annual_lgu_clean), 'Gold row count differs from silver'),
  assert_true(count(DISTINCT ldrrmf_fact_key) = count(*), 'ldrrmf_fact_key is not unique'),
  assert_true(
    count_if(psgc_code IS NOT NULL AND psgc_code NOT IN (SELECT psgc_code FROM gold.dim_lgu)) = 0,
    'A psgc_code is not in dim_lgu'),
  assert_true(count_if(match_confidence NOT BETWEEN 0 AND 1) = 0, 'match_confidence is outside 0 to 1')
FROM fact_candidate;

-- COMMAND ----------

-- Write the gold table
-- What it does: replaces gold.fact_ldrrmf with the checked rows. Only runs if the stop rules passed.
-- What to expect: same rows as silver. Run the match summary below next.
CREATE OR REPLACE TABLE gold.fact_ldrrmf
COMMENT 'BLGF LDRRMF budget and spending, one row per LGU (province, city or municipality) per fiscal year, as reported. psgc_code links to dim_lgu and is NULL for rows not matched yet. Utilization rates are spent / budget (NULL when the budget is 0). Province rows are separate funds: do not add them to their cities and municipalities.'
AS SELECT * FROM fact_candidate;

-- COMMAND ----------

-- Check the PSGC match rate
-- What it does: counts rows by LGU type and match status, so you can see how many still need a PSGC code.
-- What to expect: every row 'exact' except the Maguindanao province rows (FY2018 to FY2022), which have no current PSGC code.
SELECT lgu_type, match_status, match_method, count(*) AS rows
FROM gold.fact_ldrrmf
GROUP BY ALL
ORDER BY lgu_type, match_status, match_method;