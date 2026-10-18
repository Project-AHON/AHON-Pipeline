-- Observations can repeat one event; these counts are not deduplicated events or LGU risk.
CREATE OR REPLACE VIEW platinum.earthquake_observation_summary AS
SELECT source_year, source_month, _psgc_snapshot AS source_period,
  count(*) AS source_observations,
  count_if(has_dq_issues) AS invalid_observations,
  count_if(candidate_status = 'single_candidate') AS single_textual_candidates,
  count_if(candidate_status = 'ambiguous') AS ambiguous_textual_candidates,
  count_if(candidate_status IN ('unmatched','unparsed')) AS unresolved_textual_candidates,
  max(magnitude) FILTER (WHERE NOT has_dq_issues) AS maximum_reported_magnitude,
  'Observation summary only; no geographic exposure or risk assignment' AS interpretation
FROM silver.philvolcs_earthquake_observation_clean
GROUP BY source_year, source_month, _psgc_snapshot;
