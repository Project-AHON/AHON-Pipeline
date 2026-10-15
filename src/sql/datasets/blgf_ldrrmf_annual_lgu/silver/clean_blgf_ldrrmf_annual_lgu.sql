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
--   3. Only when every stop rule passes, merges checked rows without absence-based deletion.
-- Each run merges checked business keys; unchanged observations retain first lineage.
-- Run all statements in one session (one SQL task or one notebook run), because a temporary view only lasts for the session.

-- Catalog selected by runner.

-- Corrections across distinct ingestions are versioned; contradictory rows in one batch have no winner.
SELECT 'conflicting_delivery_reports' AS check_id,
  'bronze.blgf_ldrrmf_annual_lgu' AS table_name, count(*) AS failed_count, 'ERROR' AS severity
FROM (
  SELECT regexp_extract(_source_ref, 'FY([0-9]{4})', 1), trim(region), trim(province),
    trim(lgu_name), trim(lgu_type), _ingested_at, _batch_id
  FROM bronze.blgf_ldrrmf_annual_lgu
  WHERE _batch_id = :batch_id
  GROUP BY ALL HAVING count(DISTINCT _row_hash) > 1
);

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
    TRY_CAST(`70pct_ldrrmf_budget_appropriation` AS DECIMAL(18, 2)) AS ldrrmf_70pct_appropriation,
    TRY_CAST(`70pct_ldrrmf_expenditures` AS DECIMAL(18, 2)) AS ldrrmf_70pct_expenditure,
    TRY_CAST(`30pct_quick_response_fund_budget_appropriation` AS DECIMAL(18, 2)) AS qrf_30pct_appropriation,
    TRY_CAST(`30pct_quick_response_fund_expenditures` AS DECIMAL(18, 2)) AS qrf_30pct_expenditure,
    TRY_CAST(total_budget_appropriation AS DECIMAL(18, 2)) AS total_appropriation,
    TRY_CAST(total_expenditures AS DECIMAL(18, 2)) AS total_expenditure,
    _source_name, _source_ref, _ingested_at, _batch_id, _row_hash
  FROM (SELECT * FROM bronze.blgf_ldrrmf_annual_lgu WHERE _batch_id = :batch_id
    QUALIFY row_number() OVER (PARTITION BY regexp_extract(_source_ref, 'FY([0-9]{4})', 1), region, province, lgu_name, lgu_type
      ORDER BY _ingested_at DESC, _observation_id DESC) = 1)
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
  m.source_period,
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

-- Invalid contract rows are retained before blocking downstream materialization.
CREATE TABLE IF NOT EXISTS silver.blgf_ldrrmf_annual_lgu_reject USING DELTA AS
SELECT * EXCEPT (_in_lgu_map), 'invalid_financial_contract_or_missing_reference' AS disposition
FROM silver_candidate WHERE false;
-- COMMAND ----------
MERGE INTO silver.blgf_ldrrmf_annual_lgu_reject t USING (
 SELECT * EXCEPT (_in_lgu_map), 'invalid_financial_contract_or_missing_reference' AS disposition
 FROM silver_candidate WHERE fiscal_year IS NULL OR fiscal_year NOT BETWEEN :min_year AND :max_year
 OR lgu_type IS NULL OR lgu_type NOT IN ('Province','City','Municipality')
 OR ldrrmf_70pct_appropriation IS NULL OR ldrrmf_70pct_expenditure IS NULL
 OR qrf_30pct_appropriation IS NULL OR qrf_30pct_expenditure IS NULL
 OR total_appropriation IS NULL OR total_expenditure IS NULL
 OR total_appropriation <> ldrrmf_70pct_appropriation + qrf_30pct_appropriation
 OR total_expenditure <> ldrrmf_70pct_expenditure + qrf_30pct_expenditure
 OR NOT _in_lgu_map
) s ON t._source_ref <=> s._source_ref AND t._row_hash <=> s._row_hash
WHEN NOT MATCHED THEN INSERT *;
-- COMMAND ----------
-- Check the stop rules on the cleaned rows
-- What it does: stops with an error if a stop rule fails, before the silver table is touched.
--   Rules: row count, amounts not NULL, unique key, valid year, valid LGU type, total = sum of parts,
--   every LGU is in the map (if not, rerun build_blgf_ldrrmf_lgu_map.py).
-- What to expect: one line of true values. Any error here means silver was not replaced: fix the cause and rerun the file.
WITH metrics AS (
  SELECT count(*) AS rows,
    count_if(ldrrmf_70pct_appropriation IS NULL OR ldrrmf_70pct_expenditure IS NULL
      OR qrf_30pct_appropriation IS NULL OR qrf_30pct_expenditure IS NULL
      OR total_appropriation IS NULL OR total_expenditure IS NULL) AS missing_amounts,
    count_if(fiscal_year IS NULL OR fiscal_year NOT BETWEEN :min_year AND :max_year) AS invalid_year,
    count_if(lgu_type IS NULL OR lgu_type NOT IN ('Province','City','Municipality')) AS invalid_type,
    count_if(total_appropriation <> ldrrmf_70pct_appropriation + qrf_30pct_appropriation
      OR total_expenditure <> ldrrmf_70pct_expenditure + qrf_30pct_expenditure) AS inconsistent_totals,
    count_if(NOT _in_lgu_map) AS missing_map
  FROM silver_candidate
), rules AS (
  SELECT 'delivery_row_accounting' AS check_id,
    abs(rows - (SELECT count(*) FROM (SELECT DISTINCT regexp_extract(_source_ref, 'FY([0-9]{4})', 1),
      region, province, lgu_name, lgu_type FROM bronze.blgf_ldrrmf_annual_lgu WHERE _batch_id = :batch_id))) AS failed_count FROM metrics
  UNION ALL SELECT 'missing_amounts', missing_amounts FROM metrics
  UNION ALL SELECT 'nonempty_delivery', CASE WHEN rows = 0 THEN 1L ELSE 0L END FROM metrics
  UNION ALL SELECT 'unique_report_year', rows - (SELECT count(*) FROM
    (SELECT DISTINCT fiscal_year,region,province,lgu_name,lgu_type FROM silver_candidate)) FROM metrics
  UNION ALL SELECT 'configured_fiscal_window', invalid_year FROM metrics
  UNION ALL SELECT 'valid_reporting_type', invalid_type FROM metrics
  UNION ALL SELECT 'component_totals', inconsistent_totals FROM metrics
  UNION ALL SELECT 'reference_map_coverage', missing_map FROM metrics
)
SELECT check_id, 'silver.blgf_ldrrmf_annual_lgu_clean' AS table_name, failed_count, 'ERROR' AS severity FROM rules;

-- COMMAND ----------

-- Write the silver table
-- What it does: merges checked rows into the silver table. This only runs if the stop rules above passed.
-- What to expect: the table has the same rows as bronze. Run check_blgf_ldrrmf_annual_lgu_clean.sql next.
CREATE TABLE IF NOT EXISTS silver.blgf_ldrrmf_annual_lgu_clean USING DELTA AS SELECT * EXCEPT (_in_lgu_map) FROM silver_candidate WHERE false;
-- COMMAND ----------
MERGE INTO silver.blgf_ldrrmf_annual_lgu_clean t USING (SELECT * EXCEPT (_in_lgu_map) FROM silver_candidate) s ON t.fiscal_year <=> s.fiscal_year AND t.region <=> s.region AND t.province <=> s.province AND t.lgu_name <=> s.lgu_name AND t.lgu_type <=> s.lgu_type
WHEN MATCHED AND (NOT(t._row_hash <=> s._row_hash) OR NOT(t.psgc_code <=> s.psgc_code) OR NOT(t.source_period <=> s.source_period)) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------
-- Retained financial warnings and unmatched geography have explicit row counts.
SELECT 'funding_flagged_rows' AS check_id,
  'silver.blgf_ldrrmf_annual_lgu_clean' AS table_name,
  count_if(has_dq_issues) AS failed_count, 'WARN' AS severity
FROM silver.blgf_ldrrmf_annual_lgu_clean
UNION ALL
SELECT 'unmatched_funding_geography', 'silver.blgf_ldrrmf_annual_lgu_clean',
  count_if(psgc_code IS NULL), 'WARN'
FROM silver.blgf_ldrrmf_annual_lgu_clean;
