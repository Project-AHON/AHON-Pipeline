-- Read-only checks. Runner supplies earthquake_bronze_check (candidate or persisted
-- landing hashes), earthquake_raw, earthquake_raw_text and earthquake_config.
-- FAIL stop rules are explicitly raised by the runner before any MERGE.
WITH parsed AS (
  SELECT *, try_to_timestamp(upper(trim(`Date-Time`)), 'dd MMMM yyyy - hh:mm a') AS t
  FROM earthquake_bronze_check
), metrics AS (
  SELECT count(*) AS n, count(*) - count(DISTINCT _row_hash) AS duplicates,
    count_if(_source_name IS NULL OR trim(_source_name) = '' OR _source_ref IS NULL
      OR trim(_source_ref) = '' OR _batch_id IS NULL OR trim(_batch_id) = ''
      OR _ingested_at IS NULL OR _row_hash IS NULL
      OR NOT _row_hash RLIKE '^[0-9a-f]{64}$') AS lineage,
    count_if(t IS NULL OR Latitude IS NULL OR isnan(Latitude)
      OR NOT Latitude BETWEEN -90 AND 90 OR Longitude IS NULL OR isnan(Longitude)
      OR NOT Longitude BETWEEN -180 AND 180 OR Depth IS NULL OR isnan(Depth)
      OR Depth < 0 OR Depth = double('Infinity') OR Magnitude IS NULL
      OR isnan(Magnitude) OR Magnitude < 0 OR Magnitude = double('Infinity')) AS invalid,
    count_if(Year IS NULL OR Month IS NULL OR t IS NULL OR Year <> year(t)
      OR lower(trim(Month)) <> lower(date_format(t, 'MMMM'))) AS period_mismatch,
    count_if(`Date-Time` IS NULL OR trim(`Date-Time`) = '' OR Latitude IS NULL
      OR Longitude IS NULL OR Depth IS NULL OR Magnitude IS NULL
      OR Month IS NULL OR Year IS NULL) AS missing_required,
    max(t) AS maximum_source_time, max(_ingested_at) AS maximum_ingestion_time
  FROM parsed
), hashes AS (
  SELECT count(*) AS hash_ambiguities FROM (
    SELECT _row_hash FROM earthquake_bronze_observations
    GROUP BY _row_hash HAVING count(DISTINCT source_signature) > 1
  )
), reconciliation AS (
  SELECT count(*) AS missing_or_changed FROM earthquake_bronze_candidate r
  LEFT JOIN earthquake_bronze_check b ON r._row_hash = b._row_hash
  WHERE b._row_hash IS NULL OR NOT (
    r.`Date-Time` <=> b.`Date-Time` AND r.Latitude <=> b.Latitude
    AND r.Longitude <=> b.Longitude AND r.Depth <=> b.Depth
    AND r.Magnitude <=> b.Magnitude AND r.Location <=> b.Location
    AND r.Month <=> b.Month AND r.Year <=> b.Year
  )
), rules AS (
  SELECT 'bronze_nonempty' AS check_name, 'Completeness' AS attribute, 'stop' AS action,
    CASE WHEN n = 0 THEN 1 ELSE 0 END AS failed_rows, n AS denominator,
    'At least one landed observation' AS expected_result FROM metrics
  UNION ALL SELECT 'bronze_lineage_hash_format', 'Auditability', 'stop', lineage, n,
    'Five nonblank lineage fields and lowercase 64-character SHA-256' FROM metrics
  UNION ALL SELECT 'bronze_merge_key_unique', 'Uniqueness', 'stop', duplicates, n,
    'Zero duplicate observation hashes' FROM metrics
  UNION ALL SELECT 'bronze_legacy_hash_unambiguous', 'Auditability', 'stop', hash_ambiguities,
    (SELECT count(*) FROM earthquake_bronze_observations),
    'One source signature per legacy hash; ambiguous null/delimiter cases stop' FROM hashes
  UNION ALL SELECT 'bronze_raw_reconciliation', 'Accuracy', 'stop', missing_or_changed,
    (SELECT count(*) FROM earthquake_bronze_candidate),
    'Every distinct landed hash recomputed and typed fields reconciled' FROM reconciliation
  UNION ALL SELECT 'bronze_event_validity', 'Validity', 'flag', invalid, n,
    'Parseable time, finite in-range coordinates, finite nonnegative depth/magnitude' FROM metrics
  UNION ALL SELECT 'bronze_required_values', 'Completeness', 'flag', missing_required, n,
    'Required source fields populated; blank Location allowed' FROM metrics
  UNION ALL SELECT 'bronze_source_period_consistency', 'Consistency', 'flag', period_mismatch, n,
    'Source Month/Year agree with parsed Philippine wall time' FROM metrics
  UNION ALL SELECT 'bronze_requested_period_coverage', 'Completeness', 'report',
    missing_periods, requested_periods,
    'Requested landing periods with zero observations; reported without invented cadence'
    FROM earthquake_config
  UNION ALL SELECT 'bronze_raw_required_values', 'Completeness', 'flag', raw_blank_required, raw_rows,
    'Blank original source values, including numeric text; retained in immutable raw file' FROM earthquake_config
  UNION ALL SELECT 'bronze_raw_parseability', 'Validity', 'flag', raw_invalid_events, raw_rows,
    'Unparseable/nonfinite original numeric or timestamp text remains auditable' FROM earthquake_config
  UNION ALL SELECT 'bronze_raw_integer_period', 'Validity', 'flag', raw_invalid_periods, raw_rows,
    'Source Year must be four integer digits and Month a named month; typed casts cannot prove this' FROM earthquake_config
  UNION ALL SELECT 'bronze_source_schema', 'Consistency', 'stop', 0, 8,
    'Runner verified exact eight-column CSV header, every row width and pinned inference types' FROM earthquake_config
  UNION ALL SELECT 'bronze_batch_identity', 'Auditability', 'stop', count_if(
    NOT (_batch_id <=> (SELECT batch_id FROM earthquake_config))), count(*),
    'One supplied batch ID on all NEW candidate observations; existing rows retain earlier IDs'
    FROM earthquake_bronze_observations
  UNION ALL SELECT 'bronze_landing_row_accounting', 'Completeness', 'stop',
    abs(coalesce(sum(raw_occurrences), 0) - (SELECT raw_rows FROM earthquake_config)),
    (SELECT raw_rows FROM earthquake_config),
    'raw_rows = distinct Bronze observations + repeated raw rows'
    FROM earthquake_bronze_candidate
  UNION ALL SELECT 'bronze_event_signature_duplicates', 'Uniqueness', 'report',
    count(*) - count(DISTINCT struct(t, Latitude, Longitude, Depth, Magnitude)), count(*),
    'Event signatures are not observations; duplicate/correction policy remains unapproved'
    FROM parsed WHERE t IS NOT NULL
  UNION ALL SELECT 'bronze_plausibility', 'Accuracy', 'report', invalid, n,
    'Same physical bounds as validity; not proof of source accuracy' FROM metrics
  UNION ALL SELECT 'bronze_raw_repeated_rows', 'Uniqueness', 'report',
    coalesce(sum(raw_occurrences - 1), 0), coalesce(sum(raw_occurrences), 0),
    'Explicit repeated raw observations, retained in immutable file' FROM earthquake_bronze_candidate
  UNION ALL SELECT 'bronze_timeliness', 'Timeliness', 'report', cast(NULL AS BIGINT), n,
    concat('max_source_wall=', coalesce(cast(maximum_source_time AS STRING), 'NULL'),
      '; max_ingested_utc=', coalesce(cast(maximum_ingestion_time AS STRING), 'NULL'),
      '; no approved cadence/SLA; backfills allowed') FROM metrics
)
SELECT check_name, attribute, 'bronze.philvolcs_earthquake_data' AS table_name,
  'immutable landing hashes' AS scope, action, failed_rows, denominator, expected_result,
  CASE WHEN failed_rows IS NULL THEN 'NOT_RUN' WHEN failed_rows = 0 THEN 'PASS'
       WHEN action = 'stop' THEN 'FAIL' ELSE 'WARN' END AS status,
  (SELECT as_of FROM earthquake_config) AS as_of
FROM rules ORDER BY check_name;
