CREATE SCHEMA IF NOT EXISTS ahon.gold;

-- CMCI INDICATOR FACT

CREATE TABLE IF NOT EXISTS ahon.gold.fact_cmci_indicator (
    psgc_code STRING NOT NULL,
    year INT NOT NULL,
    indicator_code STRING NOT NULL,
    raw_score DOUBLE
)
USING DELTA
COMMENT 'CMCI indicator fact table; one row per PSGC LGU, reporting year, and CMCI indicator';  

-- CMCI INDICATOR DIMENSION

CREATE TABLE IF NOT EXISTS ahon.gold.dim_cmci_indicator (
    indicator_code STRING NOT NULL,
    indicator_name STRING NOT NULL,
    pillar_name STRING NOT NULL
)
USING DELTA
COMMENT 'CMCI indicator dimension containing the 35 approved indicators and their corresponding pillars';