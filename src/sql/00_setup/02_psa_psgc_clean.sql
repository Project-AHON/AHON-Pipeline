-- Create the PSA PSGC silver table
-- Defines ahon_dev.silver.psa_psgc_clean so its columns and types are reviewed in the repo.
-- Safe to rerun: IF NOT EXISTS leaves an existing table unchanged.
-- Written by silver/psa_psgc_clean.py. Change ahon_dev to ahon for production.

CREATE SCHEMA IF NOT EXISTS ahon_dev.silver;

CREATE TABLE IF NOT EXISTS ahon_dev.silver.psa_psgc_clean (
    -- Geographic identity
    psgc_code STRING NOT NULL,
    area_name STRING NOT NULL,
    geographic_level STRING NOT NULL,

    -- Hierarchy codes: full 10-digit PSGC codes derived from psgc_code
    region_psgc STRING,
    province_psgc STRING,
    citymun_psgc STRING,

    -- Hierarchy pieces as given by the PSA API
    region_code STRING,
    province_code STRING,
    municipality_code STRING,
    barangay_code STRING,

    -- Display fields (use the PSGC codes for joins)
    province_name STRING,
    province_city STRING,

    -- PSGC attributes
    correspondence_code STRING,
    old_name STRING,
    status STRING,
    income_classification STRING,
    city_class STRING,
    urban_rural STRING,
    island_region STRING,
    populations_json STRING,

    -- Source snapshot
    source_version STRING,
    _api_period STRING NOT NULL,

    -- Bronze provenance (carried forward unchanged)
    _source_name STRING,
    _source_ref STRING,
    _ingested_at TIMESTAMP,
    _batch_id STRING,
    _row_hash STRING,

    -- Silver audit
    _processed_at TIMESTAMP
)
USING DELTA
COMMENT 'Cleaned PSA PSGC records, one row per psgc_code, geographic_level and _api_period snapshot';