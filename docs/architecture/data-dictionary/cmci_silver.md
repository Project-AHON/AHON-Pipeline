# CMCI Silver Tables

- **Source:** Department of Trade and Industry, Cities and Municipalities Competitiveness Index Data Portal
- **Coverage:** 1,634 approved LGUs, 2014 to 2024
- **Owner:** Project AHON data team

## Silver CMCI Pillar Tables

- **Built from Bronze by:** `src/sql/02_silver_clean/cmci_indicator_parse.py`
- **One row is:** one approved CMCI LGU and reporting year for one pillar
- **Key:** `psgc_code`, `year`
- **Expected coverage:** 1,634 LGUs × 11 years = 17,974 rows per table

The parser selects the latest valid version of each Bronze batch, validates all 35 indicators, and merges records using `psgc_code + year`.

### Common Columns

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `psgc_code` | string | Canonical PSGC identifier for the LGU | Part of the logical key |
| `lgu` | string | Canonical PSGC LGU name | From `ahon.reference.cmci_lgu_map` |
| `cmci_name` | string | Locality name used by the CMCI portal | Preserved for source traceability |
| `year` | string | CMCI reporting year | Part of the logical key; 2014 to 2024 |
| `source_response_hash` | string | Hash of the selected Bronze response | A changed hash triggers a Silver update |
| `source_ingestion_timestamp` | timestamp (UTC) | Timestamp of the selected Bronze response | Used to choose the latest source record |
| `silver_processed_timestamp` | timestamp (UTC) | Time the Silver record was processed | Set by the Silver parser |

## Silver: `ahon.silver.cmci_economic_dynamism`

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `local_economy_size` | double | CMCI Local Economy Size value | Blank or `-` source values become `NULL` |
| `local_economy_growth` | double | CMCI Local Economy Growth value | |
| `active_establishments` | double | CMCI Active Establishments in the Locality value | |
| `employment_generation` | double | CMCI Employment Generation value | |

## Silver: `ahon.silver.cmci_government_efficiency`

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `compliance_national_directives` | double | Compliance to National Directives value | |
| `investment_promotion_unit` | double | Presence of Investment Promotion Unit value | |
| `arta_citizens_charter` | double | Compliance to ARTA Citizens Charter value | |
| `local_resource_generation` | double | Capacity to Generate Local Resource value | |
| `health_services_capacity` | double | Capacity of Health Services value | |
| `school_services_capacity` | double | Capacity of School Services value | |
| `performance_recognition` | double | Recognition of Performance value | |
| `business_permits` | double | Getting Business Permits value | |
| `peace_and_order` | double | Peace and Order value | |
| `social_protection` | double | Social Protection value | |

## Silver: `ahon.silver.cmci_infrastructure`

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `road_network` | double | Road Network value | |
| `distance_to_ports` | double | Distance to Ports value | |
| `basic_utilities` | double | Availability of Basic Utilities value | |
| `transportation_vehicles` | double | Transportation Vehicles value | |
| `education` | double | Education infrastructure value | |
| `health` | double | Health infrastructure value | |
| `lgu_investment` | double | LGU Investment value | |
| `accommodation_capacity` | double | Accommodation Capacity value | |
| `information_technology_capacity` | double | Information Technology Capacity value | |
| `financial_technology_capacity` | double | Financial Technology Capacity value | |

## Silver: `ahon.silver.cmci_resiliency`

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `land_use_plan` | double | Land Use Plan value | |
| `disaster_risk_reduction_plan` | double | Disaster Risk Reduction Plan value | |
| `annual_disaster_drill` | double | Annual Disaster Drill value | |
| `early_warning_system` | double | Early Warning System value | |
| `budget_for_drrmp` | double | Budget for DRRMP value | |
| `local_risk_assessments` | double | Local Risk Assessments value | |
| `emergency_infrastructure` | double | Emergency Infrastructure value | |
| `utilities` | double | Utilities value | |
| `employed_population` | double | Employed Population value | |
| `sanitary_system` | double | Sanitary System value | |

## Silver: `ahon.silver.cmci_innovation`

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `internet_capability` | double | Internet Capability value | |

## Known Issues

- CMCI provides separate locality records for 1,634 of the 1,642 PSGC cities and municipalities.
- Eight newer BARMM municipalities do not receive placeholder Silver rows.
- Missing CMCI values remain `NULL`, not zero.
- The parser expects exactly 35 configured indicator tables.
