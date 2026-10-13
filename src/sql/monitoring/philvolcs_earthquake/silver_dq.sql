-- Read-only aggregate checks, usable before and after target MERGE.
WITH paired AS (
  SELECT s.*, b._row_hash AS bronze_hash,
    b.`Date-Time` AS raw_time, b.Month AS source_month, b.Year AS source_year,
    b._raw_period_invalid AS raw_period_invalid,
    NOT (s._source_name <=> b._source_name AND s._source_ref <=> b._source_ref
      AND s._ingested_at <=> b._ingested_at AND s._batch_id <=> b._batch_id
      AND s._row_hash <=> b._row_hash) AS lineage_changed,
    NOT (s.latitude <=> b.Latitude AND s.longitude <=> b.Longitude
      AND s.depth <=> b.Depth AND s.magnitude <=> b.Magnitude
      AND s.location_description <=> nullif(trim(b.Location), '')) AS values_changed
  FROM earthquake_silver_check s FULL OUTER JOIN earthquake_scope b
    ON s._row_hash = b._row_hash
), metrics AS (
  SELECT count(*) AS n,
    count_if(_row_hash IS NULL OR bronze_hash IS NULL) AS lost_rows,
    count_if(lineage_changed OR _source_name IS NULL OR trim(_source_name) = ''
      OR _source_ref IS NULL OR trim(_source_ref) = '' OR _ingested_at IS NULL
      OR _batch_id IS NULL OR trim(_batch_id) = '' OR _row_hash IS NULL
      OR NOT _row_hash RLIKE '^[0-9a-f]{64}$' OR _processed_at IS NULL) AS lineage,
    count_if(values_changed) AS typed_mismatch,
    count_if(has_dq_issues IS NULL OR (NOT has_dq_issues AND (
      event_time IS NULL OR latitude IS NULL OR isnan(latitude) OR NOT latitude BETWEEN -90 AND 90
      OR longitude IS NULL OR isnan(longitude) OR NOT longitude BETWEEN -180 AND 180
      OR depth IS NULL OR isnan(depth) OR depth < 0 OR depth = double('Infinity')
      OR magnitude IS NULL OR isnan(magnitude) OR magnitude < 0 OR magnitude = double('Infinity')
      OR source_year IS NULL OR source_month IS NULL OR source_year <> year(event_time)
      OR coalesce(raw_period_invalid, false)
      OR lower(trim(source_month)) <> lower(date_format(event_time, 'MMMM'))))) AS missing_flags,
    count(*) - count(DISTINCT _row_hash) AS duplicate_hashes,
    count_if(event_time IS NULL OR latitude IS NULL OR isnan(latitude)
      OR NOT latitude BETWEEN -90 AND 90 OR longitude IS NULL OR isnan(longitude)
      OR NOT longitude BETWEEN -180 AND 180 OR depth IS NULL OR isnan(depth)
      OR depth < 0 OR depth = double('Infinity') OR magnitude IS NULL
      OR isnan(magnitude) OR magnitude < 0 OR magnitude = double('Infinity')) AS invalid,
    count_if(coalesce(raw_period_invalid, false) OR source_year IS NULL OR source_month IS NULL OR event_time IS NULL
      OR source_year <> year(event_time)
      OR lower(trim(source_month)) <> lower(date_format(event_time, 'MMMM'))
      OR NOT (month <=> cast(month(event_time) AS INT))
      OR NOT (year <=> cast(year(event_time) AS INT))) AS period_mismatch,
    count_if(psgc_code IS NOT NULL OR psgc_match_status <> 'blocked_meaning'
      OR psgc_match_status IS NULL OR psgc_match_method IS NOT NULL) AS code_status,
    count_if(_psgc_snapshot IS NULL OR _psgc_snapshot <>
      (SELECT source_period FROM earthquake_config)) AS snapshot_mismatch,
    count_if(psgc_candidate_status = 'single_candidate') AS single_candidates,
    count_if(psgc_candidate_status <> 'single_candidate') AS mapping_gaps,
    max(event_time) AS maximum_event_time, max(_ingested_at) AS maximum_ingestion_time
  FROM paired
), rules AS (
  SELECT 'silver_observation_reconciliation' AS check_name, 'Completeness' AS attribute,
    'stop' AS action, lost_rows AS failed_rows, n AS denominator,
    'Every scoped Bronze observation retained, including invalids' AS expected_result FROM metrics
  UNION ALL SELECT 'silver_provenance_unchanged', 'Auditability', 'stop', lineage, n,
    'Five Bronze fields unchanged; SHA-256 format and _processed_at present' FROM metrics
  UNION ALL SELECT 'silver_typed_reconciliation', 'Accuracy', 'stop', typed_mismatch, n,
    'Typed source values preserved; not proof of source accuracy' FROM metrics
  UNION ALL SELECT 'silver_observation_unique_no_fanout', 'Uniqueness', 'stop', duplicate_hashes, n,
    'One Silver row per Bronze hash; enrichment never multiplies observations' FROM metrics
  UNION ALL SELECT 'silver_event_validity', 'Validity', 'flag', invalid, n,
    'Finite values, valid physical ranges and parseable timestamp' FROM metrics
  UNION ALL SELECT 'silver_source_period_consistency', 'Consistency', 'flag', period_mismatch, n,
    'Integer derived month/year agree with source wall time' FROM metrics
  UNION ALL SELECT 'silver_invalid_observation_flag', 'Consistency', 'stop', missing_flags, n,
    'All invalid observations retained with has_dq_issues true; gaps are separate' FROM metrics
  UNION ALL SELECT 'silver_event_signature_duplicates', 'Uniqueness', 'report',
    count(*) - count(DISTINCT struct(event_time, latitude, longitude, depth, magnitude)), count(*),
    'Review duplicate event signatures; never deduplicate observations silently'
    FROM earthquake_silver_check WHERE event_time IS NOT NULL
  UNION ALL SELECT 'silver_requested_period_coverage', 'Completeness', 'report',
    missing_periods, requested_periods,
    'Landing periods missing from requested scope; no event-per-LGU/month requirement'
    FROM earthquake_config
  UNION ALL SELECT 'silver_original_period_evidence', 'Auditability', 'report',
    count_if(_raw_period_invalid IS NULL), count(*),
    'Original Year text checked for landing hashes; history needs retained original files'
    FROM earthquake_scope
  UNION ALL SELECT 'silver_assignment_blocked', 'Consistency', 'stop', code_status, n,
    'No analytical PSGC assignment until meaning is approved' FROM metrics
  UNION ALL SELECT 'silver_reference_snapshot', 'Consistency', 'stop', snapshot_mismatch, n,
    'One configured PSGC snapshot; no 2026 workbook substitution' FROM metrics
  UNION ALL SELECT 'silver_mapping_coverage', 'Completeness', 'report', mapping_gaps, n,
    'Denominator = ALL scoped observations, including unparsed/invalid descriptions' FROM metrics
  UNION ALL SELECT 'silver_candidate_reference_available', 'Auditability', 'report',
    CASE WHEN count_if(candidate_status = 'blocked_reference') > 0 THEN NULL ELSE 0 END, count(*),
    'Configured PSGC snapshot required for candidate reporting; independent cleaning continues'
    FROM earthquake_lgu_candidates
  UNION ALL SELECT 'silver_analytical_mapping', 'Completeness', 'report', cast(NULL AS BIGINT), n,
    'BLOCKED: geographic meaning unapproved; candidates are not impact/containment' FROM metrics
  UNION ALL SELECT 'silver_timeliness', 'Timeliness', 'report', cast(NULL AS BIGINT), n,
    concat('max_event_wall=', coalesce(cast(maximum_event_time AS STRING), 'NULL'),
      '; max_ingested_utc=', coalesce(cast(maximum_ingestion_time AS STRING), 'NULL'),
      '; no approved SLA') FROM metrics
  UNION ALL SELECT 'silver_map_key_unique', 'Uniqueness', 'stop',
    count(*) - count(DISTINCT struct(locality_token, province_token)), count(*),
    'One candidate map row per normalized locality/province' FROM earthquake_lgu_candidates
  UNION ALL SELECT 'silver_candidate_cardinality', 'Validity', 'stop', count_if(
    (candidate_status = 'single_candidate' AND size(candidate_codes) <> 1)
    OR (candidate_status = 'ambiguous' AND size(candidate_codes) < 2)
    OR (candidate_status IN ('unparsed', 'unmatched', 'blocked_reference') AND size(candidate_codes) <> 0)), count(*),
    'Candidate status agrees with exact reference count' FROM earthquake_lgu_candidates
  UNION ALL SELECT 'silver_candidate_method', 'Validity', 'stop', count_if(
    (size(candidate_codes) > 0 AND NOT (candidate_method <=> 'exact_locality_province_candidate'))
    OR (size(candidate_codes) = 0 AND candidate_method IS NOT NULL)), count(*),
    'Method names describe exact candidate matching, not a probability' FROM earthquake_lgu_candidates
)
SELECT check_name, attribute, 'silver.philvolcs_earthquake_data_clean' AS table_name,
  'scoped Bronze observation hashes' AS scope, action, failed_rows, denominator, expected_result,
  CASE WHEN check_name = 'silver_analytical_mapping'
       OR (check_name = 'silver_candidate_reference_available' AND failed_rows IS NULL) THEN 'BLOCKED'
       WHEN check_name = 'silver_original_period_evidence' AND failed_rows > 0 THEN 'NOT_RUN'
       WHEN failed_rows IS NULL THEN 'NOT_RUN' WHEN failed_rows = 0 THEN 'PASS'
       WHEN action = 'stop' THEN 'FAIL' ELSE 'WARN' END AS status,
  (SELECT as_of FROM earthquake_config) AS as_of
FROM rules ORDER BY check_name;
