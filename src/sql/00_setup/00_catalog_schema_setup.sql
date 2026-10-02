-- Create the nyc_mobility catalog
CREATE CATALOG IF NOT EXISTS ahon;

-- Create Bronze, Silver, Gold, and Quality  Schemas for NYC Mobility
CREATE SCHEMA IF NOT EXISTS ahon.bronze;

CREATE SCHEMA IF NOT EXISTS ahon.silver;

CREATE SCHEMA IF NOT EXISTS ahon.gold;

CREATE SCHEMA IF NOT EXISTS ahon.platinum;

CREATE SCHEMA IF NOT EXISTS ahon.quality;

CREATE SCHEMA IF NOT EXISTS ahon.reference;

SHOW SCHEMAS IN ahon;