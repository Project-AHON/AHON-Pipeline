CREATE SCHEMA IF NOT EXISTS ahon.silver;

-- =====================================================
-- CMCI ECONOMIC DYNAMISM
-- =====================================================

CREATE TABLE IF NOT EXISTS ahon.silver.cmci_economic_dynamism (
    psgc_code STRING NOT NULL,
    lgu STRING NOT NULL,
    cmci_name STRING NOT NULL,
    year STRING NOT NULL,

    local_economy_size DOUBLE,
    local_economy_growth DOUBLE,
    active_establishments DOUBLE,
    employment_generation DOUBLE,

    source_response_hash STRING NOT NULL,
    source_ingestion_timestamp TIMESTAMP NOT NULL,
    silver_processed_timestamp TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Cleaned CMCI Economic Dynamism indicators; one row per approved PSGC LGU and reporting year';


-- =====================================================
-- CMCI GOVERNMENT EFFICIENCY
-- =====================================================

CREATE TABLE IF NOT EXISTS ahon.silver.cmci_government_efficiency (
    psgc_code STRING NOT NULL,
    lgu STRING NOT NULL,
    cmci_name STRING NOT NULL,
    year STRING NOT NULL,

    compliance_national_directives DOUBLE,
    investment_promotion_unit DOUBLE,
    arta_citizens_charter DOUBLE,
    local_resource_generation DOUBLE,
    health_services_capacity DOUBLE,
    school_services_capacity DOUBLE,
    performance_recognition DOUBLE,
    business_permits DOUBLE,
    peace_and_order DOUBLE,
    social_protection DOUBLE,

    source_response_hash STRING NOT NULL,
    source_ingestion_timestamp TIMESTAMP NOT NULL,
    silver_processed_timestamp TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Cleaned CMCI Government Efficiency indicators; one row per approved PSGC LGU and reporting year';


-- =====================================================
-- CMCI INFRASTRUCTURE
-- =====================================================

CREATE TABLE IF NOT EXISTS ahon.silver.cmci_infrastructure (
    psgc_code STRING NOT NULL,
    lgu STRING NOT NULL,
    cmci_name STRING NOT NULL,
    year STRING NOT NULL,

    road_network DOUBLE,
    distance_to_ports DOUBLE,
    basic_utilities DOUBLE,
    transportation_vehicles DOUBLE,
    education DOUBLE,
    health DOUBLE,
    lgu_investment DOUBLE,
    accommodation_capacity DOUBLE,
    information_technology_capacity DOUBLE,
    financial_technology_capacity DOUBLE,

    source_response_hash STRING NOT NULL,
    source_ingestion_timestamp TIMESTAMP NOT NULL,
    silver_processed_timestamp TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Cleaned CMCI Infrastructure indicators; one row per approved PSGC LGU and reporting year';


-- =====================================================
-- CMCI RESILIENCY
-- =====================================================

CREATE TABLE IF NOT EXISTS ahon.silver.cmci_resiliency (
    psgc_code STRING NOT NULL,
    lgu STRING NOT NULL,
    cmci_name STRING NOT NULL,
    year STRING NOT NULL,

    land_use_plan DOUBLE,
    disaster_risk_reduction_plan DOUBLE,
    annual_disaster_drill DOUBLE,
    early_warning_system DOUBLE,
    budget_for_drrmp DOUBLE,
    local_risk_assessments DOUBLE,
    emergency_infrastructure DOUBLE,
    utilities DOUBLE,
    employed_population DOUBLE,
    sanitary_system DOUBLE,

    source_response_hash STRING NOT NULL,
    source_ingestion_timestamp TIMESTAMP NOT NULL,
    silver_processed_timestamp TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Cleaned CMCI Resiliency indicators; one row per approved PSGC LGU and reporting year';


-- =====================================================
-- CMCI INNOVATION
-- =====================================================

CREATE TABLE IF NOT EXISTS ahon.silver.cmci_innovation (
    psgc_code STRING NOT NULL,
    lgu STRING NOT NULL,
    cmci_name STRING NOT NULL,
    year STRING NOT NULL,

    internet_capability DOUBLE,

    source_response_hash STRING NOT NULL,
    source_ingestion_timestamp TIMESTAMP NOT NULL,
    silver_processed_timestamp TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Cleaned CMCI Innovation indicators; one row per approved PSGC LGU and reporting year';