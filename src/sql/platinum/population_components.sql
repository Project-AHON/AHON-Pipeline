-- Canonical analysis grain: city/municipality × census year. Units: people.
CREATE OR REPLACE VIEW platinum.population_components AS
SELECT psgc_code, area_name, geographic_level, region_code, province_code,
  population_year, population, source_period, coverage_status, disposition
FROM gold.fact_population;
