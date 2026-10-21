-- Data quality dashboard views
-- Two read-only views on monitoring.dq_result for the AHON Data Quality dashboard
-- (resources/dashboards/). Run by the dashboard_views task after setup; the runner sets the catalog.
-- Safe to rerun: CREATE OR REPLACE only changes the view definitions, never dq_result.

-- COMMAND ----------
-- View: every check result with dataset, layer, check type and owner
-- Reads monitoring.dq_result (build checks and DQ gates) and adds the columns the
-- dashboard groups by. Execution markers (python_transformation, sql_execution, sql_statement_N) say
-- whether a task's script ran. They are kept as check_stage 'Task run' (table_name is the script
-- path), so a failed task counts as a critical failure even when no data check could run.
-- check_type comes from the gate rule (validation_result $.meta.rule_type); build
-- checks have no rule type yet, so it is derived from the check name.
CREATE OR REPLACE VIEW monitoring.dq_check_result AS
WITH base AS (
  SELECT
    run_id,
    check_id,
    -- Build checks store catalog.schema.table, gates store layer.table: keep the last two parts.
    -- Some build checks run on an in-memory step (for example isabela_lookup): keep that name.
    -- Execution markers store the script path: keep it whole.
    CASE WHEN check_id IN ('python_transformation', 'sql_execution') OR check_id LIKE 'sql_statement_%'
         THEN table_name
         WHEN size(split(table_name, '[.]')) >= 2
         THEN concat_ws('.', slice(split(table_name, '[.]'), -2, 2))
         ELSE table_name END AS table_name,
    check_id IN ('python_transformation', 'sql_execution') OR check_id LIKE 'sql_statement_%' AS is_task_run,
    size(split(table_name, '[.]')) >= 2 AS is_table,
    failed_count,
    severity,
    disposition AS status,
    disposition IN ('BLOCKED', 'ERROR') AS is_critical,
    checked_at,
    get_json_object(validation_result, '$.meta.dq_run_id') IS NOT NULL AS is_gate,
    get_json_object(validation_result, '$.meta.source_name') AS source_name,
    get_json_object(validation_result, '$.meta.layer') AS gate_layer,
    get_json_object(validation_result, '$.meta.rule_type') AS gate_rule_type,
    get_json_object(validation_result, '$.meta.action') AS action,
    CAST(get_json_object(validation_result, '$.meta.rows_checked') AS BIGINT) AS rows_checked,
    CAST(get_json_object(validation_result, '$.meta.failed_pct') AS DOUBLE) AS failed_pct,
    get_json_object(validation_result, '$.meta.sample_keys') AS sample_keys,
    get_json_object(validation_result, '$.meta.message') AS message
  FROM monitoring.dq_result
  -- OSM was removed from the pipeline (#114); its old results stay in dq_result
  WHERE NOT (table_name RLIKE '(?i)osm_' OR check_id LIKE 'osm%')
),
labelled AS (
  SELECT
    *,
    CASE
      -- Task runs: the dataset comes from the script path
      WHEN is_task_run AND table_name RLIKE 'openstat|/psa_population/(extract|bronze|silver)/|fact_population_total' THEN 'PSA OpenSTAT population'
      WHEN is_task_run AND table_name RLIKE 'population' THEN 'PSA Population (census)'
      WHEN is_task_run AND table_name RLIKE 'psgc|dim_lgu' THEN 'PSGC'
      WHEN is_task_run AND table_name RLIKE 'ldrrmf|funding' THEN 'BLGF LDRRMF'
      WHEN is_task_run AND table_name RLIKE 'earthquake|philvolcs' THEN 'PHIVOLCS earthquakes'
      WHEN is_task_run AND table_name RLIKE 'cmci' THEN 'CMCI'
      WHEN is_task_run THEN 'Pipeline setup'
      WHEN source_name = 'psa_population_openstat'
        OR table_name RLIKE 'psa_population|fact_population_total|population_source_context|population_psgc_snapshot' THEN 'PSA OpenSTAT population'
      WHEN source_name = 'psa_population' OR table_name LIKE '%population%' THEN 'PSA Population (census)'
      WHEN source_name = 'psa_psgc' OR table_name RLIKE '(?i)psgc|dim_lgu|isabela|basilan|parent|silver business keys' THEN 'PSGC'
      WHEN source_name = 'blgf_ldrrmf_annual_lgu' OR table_name LIKE '%ldrrmf%' THEN 'BLGF LDRRMF'
      WHEN source_name = 'philvolcs_earthquake' OR table_name RLIKE 'earthquake|philvolcs' THEN 'PHIVOLCS earthquakes'
      WHEN source_name = 'cmci' OR table_name LIKE '%cmci%' THEN 'CMCI'
      ELSE 'Other'
    END AS dataset,
    coalesce(gate_layer, CASE WHEN is_task_run THEN 'task' WHEN is_table THEN split(table_name, '[.]')[0] ELSE 'in build' END) AS layer,
    CASE
      WHEN is_task_run THEN 'execution'
      WHEN is_gate THEN gate_rule_type
      WHEN check_id RLIKE '^unique|_unique|repeated_identical|conflicting|target_keys' THEN 'unique'
      WHEN check_id RLIKE 'nonempty|accounting|cardinality|grid|landing_rows' THEN 'volume'
      WHEN check_id RLIKE 'bounds|fiscal_window|flagged' THEN 'range'
      WHEN check_id RLIKE 'totals|matches|reconcil' THEN 'reconciliation'
      WHEN check_id RLIKE 'reference|dimension|snapshot|map|join|fanout|alignment|coverage|candidate|unmatched' THEN 'referential_integrity'
      WHEN check_id RLIKE 'requires_review|requires_migration|stale' THEN 'reconciliation'
      WHEN check_id RLIKE 'rejects' THEN 'accepted_values'
      WHEN check_id RLIKE 'required|missing|null|without|no_' THEN 'null'
      WHEN check_id RLIKE 'nonnegative|negative|range|year|amount' THEN 'range'
      WHEN check_id RLIKE 'disposition|accepted|valid|json|code|typed|standardized' THEN 'accepted_values'
      ELSE 'other'
    END AS check_type,
    CASE WHEN is_task_run THEN 'Task run' WHEN is_gate THEN 'DQ gate' ELSE 'Build check' END AS check_stage
  FROM base
)
SELECT
  run_id, dataset, layer, table_name, check_id, check_type, check_stage, status,
  is_critical, severity, action, failed_count, rows_checked,
  failed_pct, sample_keys, message, checked_at,
  CASE dataset
    WHEN 'PSGC' THEN 'Yanna'
    WHEN 'PSA Population (census)' THEN 'Crizza'
    WHEN 'PSA OpenSTAT population' THEN 'Crizza'
    WHEN 'BLGF LDRRMF' THEN 'Crystal'
    WHEN 'PHIVOLCS earthquakes' THEN 'Jessy'
    WHEN 'CMCI' THEN 'Maki'
  END AS owner
FROM labelled;


-- COMMAND ----------
-- View: the current status of every check
-- One row per table and check: its latest result. A batch can be rerun under the same
-- run_id, so "latest" is by checked_at, not by run_id. Checks that were renamed or
-- removed (for example the old invalid_is_flagged) drop out: a check is current only
-- if it ran within 1 hour of the newest check on the same table.
-- Task runs show only while their latest result failed: a failed script stays a critical
-- failure until a later run of the same script succeeds.
CREATE OR REPLACE VIEW monitoring.dq_check_status AS
SELECT *
FROM (
  SELECT
    *,
    max(checked_at) OVER (PARTITION BY table_name, check_id) AS check_last_run,
    max(checked_at) OVER (PARTITION BY table_name) AS table_last_checked
  FROM monitoring.dq_check_result
)
WHERE checked_at = check_last_run
  AND checked_at >= table_last_checked - INTERVAL 1 HOUR
  AND NOT (check_stage = 'Task run' AND status = 'PASS');
