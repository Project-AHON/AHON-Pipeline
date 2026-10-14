CREATE SCHEMA IF NOT EXISTS ahon.platinum;

CREATE TABLE IF NOT EXISTS ahon.platinum.cmci_pillar_score (
    psgc_code STRING NOT NULL,
    year INT NOT NULL,
    pillar_name STRING NOT NULL,

    pillar_score_raw DOUBLE,

    indicators_expected INT NOT NULL,
    indicators_available INT NOT NULL,
    indicators_missing INT NOT NULL,
    coverage_pct DOUBLE NOT NULL,
    score_status STRING NOT NULL,

    platinum_processed_timestamp TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'COMMENT ON TABLE ahon.platinum.cmci_pillar_score IS
Interim CMCI pillar-level aggregation based on AHON''s selected 35-indicator scope; one row per PSGC LGU, reporting year, and CMCI pillar. COMPLETE means all indicators selected for that pillar are available. 
Missing values are not imputed. pillar_score_raw is an unnormalized sum and is not the official CMCI 0-20 pillar score.';

COMMENT ON COLUMN ahon.platinum.cmci_pillar_score.pillar_score_raw IS
'Interim unnormalized sum of available CMCI indicator scores. Populated only when all expected indicators for the LGU-year-pillar are available. 
This is not the official normalized 0–20 CMCI pillar score and must not be treated as a final preparedness score until the applicable CMCI weighting methodology is confirmed.';