-- Candidate only. Every distinct observation survives; id is not the merge key.
CREATE OR REPLACE TEMP VIEW earthquake_parsed AS
SELECT b.*, try_to_timestamp(upper(trim(`Date-Time`)),
  'dd MMMM yyyy - hh:mm a') AS event_time,
  nullif(trim(Location), '') AS location_description
FROM earthquake_scope b;
-- COMMAND ----------
CREATE OR REPLACE TEMP VIEW earthquake_silver_candidate AS
SELECT xxhash64(p.event_time, p.Latitude, p.Longitude, p.Depth, p.Magnitude) AS id,
  p.event_time, p.Latitude AS latitude, p.Longitude AS longitude,
  p.Depth AS depth, p.Magnitude AS magnitude, p.location_description,
  cast(month(p.event_time) AS INT) AS month, cast(year(p.event_time) AS INT) AS year,
  coalesce(p.event_time IS NULL OR p.Latitude IS NULL OR isnan(p.Latitude)
    OR NOT p.Latitude BETWEEN -90 AND 90 OR p.Longitude IS NULL
    OR isnan(p.Longitude) OR NOT p.Longitude BETWEEN -180 AND 180
    OR p.Depth IS NULL OR isnan(p.Depth) OR p.Depth < 0
    OR p.Depth = double('Infinity') OR p.Magnitude IS NULL
    OR isnan(p.Magnitude) OR p.Magnitude < 0 OR p.Magnitude = double('Infinity')
    OR coalesce(p._raw_period_invalid, false) OR p.Year IS NULL OR p.Year <> year(p.event_time)
    OR p.Month IS NULL OR lower(trim(p.Month)) <> lower(date_format(p.event_time, 'MMMM')),
    true) AS has_dq_issues,
  p._source_name, p._source_ref, p._ingested_at, p._batch_id, p._row_hash,
  c.as_of AS _processed_at,
  m.locality_token AS reported_locality, m.province_token AS reported_province,
  coalesce(m.candidate_codes, cast(array() AS ARRAY<STRING>)) AS psgc_candidate_codes,
  coalesce(m.candidate_status, 'unparsed') AS psgc_candidate_status,
  m.candidate_method AS psgc_candidate_method,
  c.source_period AS _psgc_snapshot,
  cast(NULL AS STRING) AS psgc_code, 'blocked_meaning' AS psgc_match_status,
  cast(NULL AS STRING) AS psgc_match_method
FROM earthquake_parsed p
LEFT JOIN earthquake_description_keys k ON p._row_hash = k._row_hash
LEFT JOIN earthquake_lgu_candidates m
  ON k.locality_token = m.locality_token AND k.province_token = m.province_token
CROSS JOIN earthquake_config c;
