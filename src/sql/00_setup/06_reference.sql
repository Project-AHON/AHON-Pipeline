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
