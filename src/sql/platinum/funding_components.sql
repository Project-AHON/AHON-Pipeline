-- Grain: original BLGF LGU × fiscal year; pesos and spent/budget ratios.
-- Province funds and city/municipality funds remain separate rows.
CREATE OR REPLACE VIEW platinum.funding_components AS
SELECT f.*, d.area_name AS reference_area_name, d.geographic_level,
  'Funding utilization is not measured preparedness' AS interpretation
FROM gold.fact_ldrrmf f LEFT JOIN gold.dim_lgu d
  ON f.psgc_code = d.psgc_code AND f.source_period = d.source_period;
