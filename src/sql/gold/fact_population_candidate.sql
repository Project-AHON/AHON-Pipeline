-- One city/municipality per selected census year, including absent source observations.
-- Use reported city totals directly; never add administrative levels together.
WITH source AS (
  SELECT s.* FROM silver.psgc_city_population_clean s
  JOIN silver.psa_psgc_clean p ON s.psgc_code = p.psgc_code
    AND s._api_period = p._api_period AND s._row_hash = p._row_hash
  WHERE s._api_period = :source_period AND s.population_year = :census_year
    AND p.geographic_level IN ('CITY', 'MUN')
)
SELECT d.psgc_code, d.area_name, d.geographic_level, d.region_code, d.province_code,
  :census_year AS population_year, s.population, d.source_period,
  CASE WHEN s.psgc_code IS NULL THEN 'missing_census_observation' ELSE s.disposition END AS disposition,
  CASE WHEN s.disposition = 'accepted' AND s.population IS NOT NULL
    THEN 'AVAILABLE' ELSE 'MISSING' END AS coverage_status,
  s._source_ref, s._ingested_at, s._batch_id
FROM gold.dim_lgu d
LEFT JOIN source s ON d.psgc_code = s.psgc_code
WHERE d.source_period = :source_period AND d.geographic_level IN ('CITY', 'MUN');
