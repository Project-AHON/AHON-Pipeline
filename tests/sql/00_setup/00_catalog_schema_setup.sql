-- Create the nyc_mobility catalog
CREATE CATALOG IF NOT EXISTS ahon;

-- Create Bronze, Silver, Gold, and Quality  Schemas for NYC Mobility
CREATE SCHEMA IF NOT EXISTS ahon.01_bronze;

CREATE SCHEMA IF NOT EXISTS ahon.02_silver;

CREATE SCHEMA IF NOT EXISTS ahon.03_gold;

CREATE SCHEMA IF NOT EXISTS ahon.04_platinum;

CREATE SCHEMA IF NOT EXISTS ahon.05_quality;

CREATE SCHEMA IF NOT EXISTS ahon.06_reference;

SHOW SCHEMAS IN ahon;