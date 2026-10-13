-- Read-only Gold preparation. Run through run_earthquake.py --stage gold.
-- No approved event contract: no Gold DDL/MERGE and no automatic exclusions.
-- Temporary candidates preserve legacy wall-clock time, never promote PSGC candidates.
CREATE OR REPLACE TEMP VIEW earthquake_gold_signature_review AS
SELECT id, count(*) AS signature_observations,
  count(DISTINCT struct(event_time, latitude, longitude, depth, magnitude)) AS signature_values
FROM earthquake_event_review GROUP BY id;
-- COMMAND ----------
CREATE OR REPLACE TEMP VIEW earthquake_gold_correction_review AS
SELECT event_time, latitude, longitude,
  count(DISTINCT struct(depth, magnitude)) AS reported_versions
FROM earthquake_event_review
WHERE event_time IS NOT NULL AND latitude BETWEEN -90 AND 90
  AND longitude BETWEEN -180 AND 180 AND NOT isnan(latitude) AND NOT isnan(longitude)
GROUP BY event_time, latitude, longitude;
-- COMMAND ----------
CREATE OR REPLACE TEMP VIEW earthquake_gold_accounting AS
SELECT s.*, coalesce(k.signature_observations, 0) AS signature_observations,
  coalesce(k.signature_values, 0) AS signature_values,
  coalesce(c.reported_versions, 0) AS reported_versions,
  CASE WHEN s.has_dq_issues IS NULL OR s.has_dq_issues
      OR s.event_time IS NULL OR s.latitude IS NULL OR isnan(s.latitude)
      OR NOT s.latitude BETWEEN -90 AND 90 OR s.longitude IS NULL OR isnan(s.longitude)
      OR NOT s.longitude BETWEEN -180 AND 180 OR s.depth IS NULL OR isnan(s.depth)
      OR s.depth < 0 OR s.depth = double('Infinity') OR s.magnitude IS NULL
      OR isnan(s.magnitude) OR s.magnitude < 0 OR s.magnitude = double('Infinity')
      OR s.year IS NULL OR s.year <> year(s.event_time)
      OR s.month IS NULL OR s.month <> month(s.event_time) THEN 'invalid_observation_review'
    WHEN k.signature_values > 1 THEN 'signature_collision_review'
    WHEN k.signature_observations > 1 THEN 'duplicate_signature_review'
    WHEN c.reported_versions > 1 THEN 'possible_correction_review'
    ELSE 'eligible_contract_blocked' END AS review_disposition
FROM earthquake_silver_check s
LEFT JOIN earthquake_gold_signature_review k ON s.id <=> k.id
LEFT JOIN earthquake_gold_correction_review c ON s.event_time = c.event_time
  AND s.latitude = c.latitude AND s.longitude = c.longitude;
-- COMMAND ----------
-- Provisional signature candidate, NOT an approved Gold schema/event identity.
-- Every remaining observation is retained in accounting with a review reason.
CREATE OR REPLACE TEMP VIEW earthquake_gold_candidate AS
SELECT * FROM earthquake_gold_accounting WHERE review_disposition = 'eligible_contract_blocked';
-- COMMAND ----------
CREATE OR REPLACE TEMP VIEW earthquake_gold_dimension_comparison AS
SELECT r.psgc_code AS reference_code, d.psgc_code AS dimension_code,
  NOT (r.area_name <=> d.area_name AND r.geographic_level <=> d.geographic_level
    AND r.region_code <=> d.region_code AND r.province_code <=> d.province_code
    AND r.municipality_code <=> d.municipality_code AND r.barangay_code <=> d.barangay_code
  ) AS attributes_differ
FROM earthquake_gold_reference r FULL OUTER JOIN earthquake_gold_dimension d
  ON r.psgc_code = d.psgc_code;
-- COMMAND ----------
-- Bounded summaries only; these are observations, not materialized fact counts.
SELECT review_disposition, count(*) AS scoped_observations,
  count_if(psgc_code IS NULL) AS unassigned_observations,
  'BLOCKED' AS status, 'No event dedup/correction/eligibility policy approved; no Gold writes' AS details
FROM earthquake_gold_accounting GROUP BY review_disposition ORDER BY review_disposition;
-- COMMAND ----------
WITH observation_metrics AS (
  SELECT count(*) AS n, count(*) - count(DISTINCT _row_hash) AS duplicate_observations,
    count_if(_source_name IS NULL OR trim(_source_name) = '' OR _source_ref IS NULL
      OR trim(_source_ref) = '' OR _ingested_at IS NULL OR _batch_id IS NULL
      OR trim(_batch_id) = '' OR _row_hash IS NULL OR NOT _row_hash RLIKE '^[0-9a-f]{64}$'
      OR _processed_at IS NULL) AS missing_lineage,
    count_if(id IS NULL OR NOT (id <=> xxhash64(event_time, latitude, longitude, depth, magnitude)))
      AS changed_signature,
    count_if(psgc_code IS NOT NULL OR NOT (psgc_match_status <=> 'blocked_meaning')
      OR psgc_match_method IS NOT NULL) AS assignment_mismatch,
    count_if(_psgc_snapshot IS NULL OR _psgc_snapshot <>
      (SELECT source_period FROM earthquake_config)) AS wrong_snapshot
  FROM earthquake_gold_accounting
), reference_metrics AS (
  SELECT count(*) AS n, count(*) - count(DISTINCT psgc_code) AS duplicate_codes,
    count_if(psgc_code IS NULL OR NOT psgc_code RLIKE '^[0-9]{10}$'
      OR area_name IS NULL OR trim(area_name) = '' OR geographic_level IS NULL) AS invalid_rows
  FROM earthquake_gold_reference
), dimension_metrics AS (
  SELECT count(*) AS n, count(*) - count(DISTINCT psgc_code) AS duplicate_codes,
    count_if(psgc_code IS NULL OR NOT psgc_code RLIKE '^[0-9]{10}$'
      OR area_name IS NULL OR trim(area_name) = '' OR geographic_level IS NULL) AS invalid_rows
  FROM earthquake_gold_dimension
), context_metrics AS (
  SELECT count(*) AS n, count(*) - count(DISTINCT _row_hash) AS duplicate_observations,
    count_if(id IS NULL OR NOT (id <=> xxhash64(event_time, latitude, longitude, depth, magnitude)))
      AS changed_signature
  FROM earthquake_event_review
), rules AS (
  SELECT 'gold_observation_identity' AS check_name, 'Uniqueness' AS attribute,
    'stop' AS action, duplicate_observations AS failed_rows, n AS denominator,
    'One accounting row per scoped Silver observation; no join fan-out' AS expected_result
  FROM observation_metrics
  UNION ALL SELECT 'gold_scope_nonempty', 'Completeness', 'stop', CASE WHEN n = 0 THEN 1 ELSE 0 END, n,
    'Requested scope is nonempty; original batch provenance is preserved on retries' FROM observation_metrics
  UNION ALL SELECT 'gold_observation_accounting', 'Completeness', 'stop',
    abs(n - (SELECT count(*) FROM earthquake_silver_check)),
    (SELECT count(*) FROM earthquake_silver_check), 'Every scoped observation has exactly one disposition'
    FROM observation_metrics
  UNION ALL SELECT 'gold_lineage', 'Auditability', 'stop', missing_lineage, n,
    'Five source lineage fields plus Silver processing timestamp retained' FROM observation_metrics
  UNION ALL SELECT 'gold_signature_expression', 'Consistency', 'stop', changed_signature, n,
    'Legacy signature unchanged; it is not an approved immutable event key' FROM observation_metrics
  UNION ALL SELECT 'gold_context_identity', 'Uniqueness', 'stop', duplicate_observations, n,
    'Historical conflict context contains each observation once' FROM context_metrics
  UNION ALL SELECT 'gold_context_signature_expression', 'Consistency', 'stop', changed_signature, n,
    'Historical context also uses the existing signature expression' FROM context_metrics
  UNION ALL SELECT 'gold_mapping_assignment', 'Consistency', 'stop', assignment_mismatch, n,
    'Candidate PSGC codes never become analytical assignments; blocked_meaning and null code/method'
    FROM observation_metrics
  UNION ALL SELECT 'gold_scope_snapshot', 'Consistency', 'stop', wrong_snapshot, n,
    'Every scoped observation uses the configured PSGC snapshot' FROM observation_metrics
  UNION ALL SELECT 'gold_reference_prerequisite', 'Auditability', 'stop',
    CASE WHEN NOT reference_supported OR n = 0 THEN NULL ELSE 0 END, n,
    'Configured reference exists with supported string schema and nonempty selected snapshot'
    FROM reference_metrics CROSS JOIN earthquake_gold_prerequisites
  UNION ALL SELECT 'gold_reference_identity', 'Validity', 'stop', duplicate_codes + invalid_rows, n,
    'Selected reference codes unique, 10-digit strings with names and levels' FROM reference_metrics
  UNION ALL SELECT 'gold_dimension_prerequisite', 'Auditability', 'stop',
    CASE WHEN NOT dimension_supported OR n = 0 THEN NULL ELSE 0 END, n,
    'dim_lgu exists with supported seven-column string schema; never rebuilt here'
    FROM dimension_metrics CROSS JOIN earthquake_gold_prerequisites
  UNION ALL SELECT 'gold_dimension_identity', 'Validity', 'stop', duplicate_codes + invalid_rows, n,
    'Dimension codes unique, 10-digit strings with names and levels' FROM dimension_metrics
  UNION ALL SELECT 'gold_dimension_snapshot_equivalence', 'Consistency', 'stop',
    CASE WHEN NOT (SELECT reference_supported AND dimension_supported FROM earthquake_gold_prerequisites)
      OR (SELECT n FROM reference_metrics) = 0 OR (SELECT n FROM dimension_metrics) = 0
      THEN NULL ELSE count_if(reference_code IS NULL OR dimension_code IS NULL OR attributes_differ) END, count(*),
    'Exact selected reference projection, including names, levels, hierarchy and target-only codes'
    FROM earthquake_gold_dimension_comparison
  UNION ALL SELECT 'gold_candidate_signature_unique', 'Uniqueness', 'stop',
    count(*) - count(DISTINCT id), count(*), 'Provisional candidate signatures unique without arbitrary selection'
    FROM earthquake_gold_candidate
  UNION ALL SELECT 'gold_unmatched_retention', 'Completeness', 'stop',
    abs((SELECT count(*) FROM earthquake_gold_candidate WHERE psgc_code IS NULL) -
      (SELECT count(*) FROM earthquake_gold_accounting
       WHERE review_disposition = 'eligible_contract_blocked' AND psgc_code IS NULL)),
    (SELECT count(*) FROM earthquake_gold_accounting WHERE review_disposition = 'eligible_contract_blocked'),
    'Every eligible unmatched observation remains in the provisional candidate'
  UNION ALL SELECT 'gold_signature_collision_review', 'Uniqueness', 'report',
    count_if(signature_values > 1), count(*), 'Review every conflicting signature; no hash winner'
    FROM earthquake_gold_accounting
  UNION ALL SELECT 'gold_duplicate_review', 'Uniqueness', 'report',
    count_if(signature_observations > 1), count(*), 'Includes matching historical signatures across batches'
    FROM earthquake_gold_accounting
  UNION ALL SELECT 'gold_correction_review', 'Consistency', 'report',
    count_if(reported_versions > 1), count(*), 'Same-time/coordinate depth or magnitude differences need policy'
    FROM earthquake_gold_accounting
  UNION ALL SELECT 'gold_fact_contract', 'Auditability', 'stop', cast(NULL AS BIGINT), n,
    'BLOCKED: approve event key/types, duplicates/corrections/eligibility, time and geographic relationship'
    FROM observation_metrics
)
SELECT check_name, attribute, 'gold.fact_earthquake_event (unmaterialized)' AS table_name,
  'landing hashes, or explicit historical reprocess; historical conflict context excluded from totals' AS scope,
  action, failed_rows, denominator, expected_result,
  CASE WHEN failed_rows IS NULL THEN 'BLOCKED' WHEN failed_rows = 0 THEN 'PASS'
    WHEN action = 'stop' THEN 'FAIL' ELSE 'WARN' END AS status,
  (SELECT as_of FROM earthquake_config) AS as_of
FROM rules ORDER BY check_name;
-- COMMAND ----------
-- Standalone execution also stops; the Python stop gate raises on the rules above.
SELECT assert_true(false, 'BLOCKED: earthquake fact contract unapproved; no Gold target writes');
