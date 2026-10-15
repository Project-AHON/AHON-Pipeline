-- Grain: current snapshot city/municipality × census year. Units: people and barangays.
-- Partial measured subtotal remains separate from a complete population total.
CREATE OR REPLACE VIEW platinum.population_components AS
WITH expected AS (
  SELECT municipality_code, source_period, count(*) AS expected_barangay_count
  FROM gold.dim_lgu WHERE geographic_level = 'BGY'
  GROUP BY municipality_code, source_period
), periods AS (
  SELECT DISTINCT source_period, population_year FROM gold.fact_population
), measured AS (
  SELECT municipality_code, source_period, population_year,
    count(*) AS observed_barangay_count, count(population) AS measured_barangay_count,
    sum(population) AS available_population_subtotal
  FROM gold.fact_population GROUP BY municipality_code, source_period, population_year
)
SELECT d.psgc_code, d.area_name, d.geographic_level, p.source_period, p.population_year,
  e.expected_barangay_count,
  coalesce(m.observed_barangay_count, 0) AS observed_barangay_count,
  coalesce(m.measured_barangay_count, 0) AS measured_barangay_count,
  m.available_population_subtotal,
  CASE WHEN m.measured_barangay_count = e.expected_barangay_count
      AND m.observed_barangay_count = e.expected_barangay_count
    THEN m.available_population_subtotal END AS complete_population_total,
  CASE WHEN m.measured_barangay_count = e.expected_barangay_count
      AND m.observed_barangay_count = e.expected_barangay_count
    THEN 'COMPLETE' ELSE 'PARTIAL_OR_MISSING' END AS coverage_status
FROM gold.dim_lgu d JOIN periods p ON d.source_period = p.source_period
LEFT JOIN measured m ON d.psgc_code = m.municipality_code
  AND p.source_period = m.source_period AND p.population_year = m.population_year
LEFT JOIN expected e ON d.psgc_code = e.municipality_code AND p.source_period = e.source_period
WHERE d.geographic_level IN ('CITY', 'MUN');
