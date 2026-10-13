CREATE SCHEMA IF NOT EXISTS ahon.reference;

CREATE TABLE IF NOT EXISTS ahon.reference.lgu_master (
    psgc_code STRING NOT NULL,
    correspondence_code STRING,
    lgu_name STRING NOT NULL,
    province_code STRING,
    province_name STRING,
    geographic_level STRING NOT NULL,
    old_name STRING,
    city_class STRING,
    income_classification STRING,
    population_2024 BIGINT,
    source_file STRING NOT NULL,
    source_publication_date DATE NOT NULL,
    is_active BOOLEAN NOT NULL,
    created_timestamp TIMESTAMP NOT NULL,
    updated_timestamp TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Canonical city and municipality reference table sourced from the Philippine Standard Geographic Code';

CREATE TABLE IF NOT EXISTS ahon.reference.cmci_lgu_map (
    psgc_code STRING NOT NULL,
    psgc_name STRING NOT NULL,
    cmci_name STRING,
    match_status STRING NOT NULL,
    match_method STRING,
    reviewed BOOLEAN NOT NULL,
    is_active BOOLEAN NOT NULL,
    created_timestamp TIMESTAMP NOT NULL,
    updated_timestamp TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Mapping between official PSGC LGUs and exact CMCI Data Portal locality names';
