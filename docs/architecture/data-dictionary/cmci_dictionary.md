# CMCI

- **Source:** Department of Trade and Industry, Cities and Municipalities Competitiveness Index Data Portal
- **Coverage:** Philippine cities and municipalities, 2014–2024; loaded on demand from the public portal
- **Owner:** Project AHON Data Team

Source portal: [CMCI Data Portal](https://cmci.dti.gov.ph/data-portal.php)

## Scope

| Measure | Count |
| --- | ---: |
| PSGC cities and municipalities | 1,642 |
| Live CMCI locality values | 1,634 |
| Approved CMCI mappings | 1,634 |
| Known PSGC-only source gaps | 8 |
| Reporting years | 11 |
| Selected indicators | 35 |
| CMCI pillars | 5 |

Project AHON uses a deliberate analytical subset of 35 CMCI indicators:

| Pillar | Selected indicators |
| --- | ---: |
| Economic Dynamism | 4 |
| Government Efficiency | 10 |
| Infrastructure | 10 |
| Resiliency | 10 |
| Innovation | 1 |
| **Total** | **35** |

Completeness and coverage in this pipeline refer to Project AHON's selected 35-indicator scope, not every indicator in the complete CMCI framework.

CMCI complements hazard datasets by describing local competitiveness, capacity, infrastructure, resiliency, and innovation. CMCI does not measure hazard occurrence, intensity, frequency, or geographic exposure.

## Bronze: `ahon.bronze.cmci_raw_indicator_batch_html`

- **One row is:** one CMCI source request containing one or more LGUs, all configured years, and all configured indicators
- **Loaded by:** `src/sql/01_bronze_ingest/cmci_batch_ingest.py`
- **Row hash covers:** the raw request arrays, expected and returned counts, raw response HTML, and response hash; excludes the five standard provenance columns

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `batch_id` | string | Deterministic identifier for the CMCI request batch | Derived from PSGC codes, years, and indicator codes |
| `requested_psgc_codes` | array\<string\> | Canonical PSGC codes included in the request | Used to determine completed LGU ingestion scope |
| `requested_cmci_names` | array\<string\> | Exact CMCI locality values submitted to the portal | Order corresponds to `requested_psgc_codes` |
| `requested_years` | array\<string\> | CMCI years requested | Currently 2014–2024 |
| `requested_indicator_codes` | array\<string\> | Selected CMCI indicator codes requested | Currently 35 indicators |
| `expected_lgu_count` | integer | Number of LGUs expected in the response | Usually 10; the final batch may be smaller |
| `expected_year_count` | integer | Number of years expected in the response | Currently 11 |
| `expected_indicator_count` | integer | Number of indicator tables expected | Currently 35 |
| `returned_value_count` | integer | Number of validated values parsed from the response | Expected LGUs × years × indicators |
| `response_html` | string | Complete raw HTML returned by CMCI | Retained for audit and reprocessing |
| `response_hash` | string | Stable SHA-256 hash of parsed source values | Used to detect changed responses |
| `ingestion_timestamp` | timestamp (UTC) | Original pipeline ingestion timestamp | Retained for compatibility and source ordering |
| `_source_name` | string | Dataset or feed that produced the row | Always `cmci_data_portal` |
| `_source_ref` | string | Sanitized CMCI endpoint used for the request | Must not contain keys, tokens, or credentials |
| `_ingested_at` | timestamp (UTC) | When the row was written to Bronze | Standard Bronze provenance timestamp |
| `_batch_id` | string | Ingestion run identifier | May match `batch_id` when one request is one ingestion unit |
| `_row_hash` | string | SHA-256 of the raw source columns | Excludes the provenance columns |

## Bronze: `ahon.bronze.cmci_raw_indicator`

- **One row is:** one raw CMCI indicator value for one PSGC LGU and reporting year within a source batch
- **Loaded by:** `src/sql/01_bronze_ingest/cmci_batch_ingest.py`
- **Row hash covers:** `batch_id`, `psgc_code`, `psgc_name`, `cmci_name`, `indicator_label`, `year`, `raw_value`, `response_hash`, and `ingestion_timestamp`; excludes the five standard provenance columns

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `batch_id` | string | Batch containing the source value | Links to `cmci_raw_indicator_batch_html` |
| `psgc_code` | string | Canonical PSGC LGU identifier | Approved through `ahon.reference.cmci_lgu_map` |
| `psgc_name` | string | Canonical PSGC LGU name | Retained for traceability |
| `cmci_name` | string | Exact CMCI locality name | Source identity submitted to CMCI |
| `indicator_label` | string | CMCI indicator represented by the row | Must belong to the selected 35 indicators |
| `year` | string | CMCI reporting year | Currently 2014–2024 |
| `raw_value` | string | Raw value returned by CMCI | Blank and `-` become `NULL` in Silver |
| `response_hash` | string | Hash of the validated CMCI response | Links the row to its source response |
| `ingestion_timestamp` | timestamp (UTC) | Original source ingestion timestamp | Used for latest-record selection |
| `_source_name` | string | Dataset or feed that produced the row | Always `cmci_data_portal` |
| `_source_ref` | string | Sanitized CMCI endpoint used for the request | Must not contain keys, tokens, or credentials |
| `_ingested_at` | timestamp (UTC) | When the row was written to Bronze | Standard Bronze provenance timestamp |
| `_batch_id` | string | Ingestion run identifier | Links related rows from the same load run |
| `_row_hash` | string | SHA-256 of the raw source columns | Excludes the provenance columns |

## Reference: `ahon.reference.cmci_lgu_map`

- **One row is:** one PSGC LGU and its reviewed CMCI mapping status
- **Key:** `psgc_code`
- **Maintained by:** `src/sql/01_bronze_ingest/cmci_mapping_update.py`
- **Profiled by:** `src/sql/01_bronze_ingest/cmci_source_profile.py`

Only mappings meeting all three conditions are eligible for ingestion:

```text
match_status = MATCHED
reviewed = true
is_active = true
```

Approved mapping methods:

| Method | Count |
| --- | ---: |
| `EXACT` | 1,113 |
| `NORMALIZED` | 136 |
| `COMPACT` | 7 |
| `MANUAL` | 7 |
| `PROVINCE_SUFFIX` | 347 |
| `PROVINCE_SUFFIX_EXCEPTION` | 4 |
| `MANUAL_ALIAS` | 20 |
| **Total** | **1,634** |

## Silver Tables

The CMCI Silver layer contains one table per pillar:

```text
ahon.silver.cmci_economic_dynamism
ahon.silver.cmci_government_efficiency
ahon.silver.cmci_infrastructure
ahon.silver.cmci_resiliency
ahon.silver.cmci_innovation
```

- **One row is:** one approved PSGC LGU and reporting year for one pillar
- **Key:** `psgc_code`, `year`
- **Built from Bronze by:** `src/sql/02_silver_clean/cmci_indicator_parse.py`
- **Expected rows per table:** 17,974

The parser selects the latest valid version of each Bronze batch, validates the selected 35 indicators, and merges records using `psgc_code + year`.

### Common Silver Columns

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `psgc_code` | string | Canonical PSGC identifier for the LGU | Part of the logical key |
| `lgu` | string | Canonical PSGC LGU name | From `ahon.reference.cmci_lgu_map` |
| `cmci_name` | string | Locality name used by the CMCI portal | Preserved for source traceability |
| `year` | string | CMCI reporting year | Part of the logical key; 2014–2024 |
| Pillar indicator columns | double | Typed CMCI indicator values | Blank or unavailable source values remain `NULL` |
| `source_response_hash` | string | Hash of the selected Bronze response | A changed hash triggers a Silver update |
| `source_ingestion_timestamp` | timestamp (UTC) | Timestamp of the selected Bronze response | Used to select the latest source record |
| `silver_processed_timestamp` | timestamp (UTC) | Time the Silver record was processed | Set by the Silver parser |

### Silver: `ahon.silver.cmci_economic_dynamism`

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `local_economy_size` | double | CMCI Local Economy Size value | Blank or `-` source values become `NULL` |
| `local_economy_growth` | double | CMCI Local Economy Growth value | |
| `active_establishments` | double | CMCI Active Establishments in the Locality value | |
| `employment_generation` | double | CMCI Employment Generation value | |

### Silver: `ahon.silver.cmci_government_efficiency`

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

### Silver: `ahon.silver.cmci_infrastructure`

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

### Silver: `ahon.silver.cmci_resiliency`

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

### Silver: `ahon.silver.cmci_innovation`

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `internet_capability` | double | Internet Capability value | |

### Expected Silver Coverage

Each Silver table must have:

```text
Rows: 17,974
Unique PSGC-year keys: 17,974
LGUs: 1,634
Years: 11
Duplicate PSGC-year keys: 0
Unauthorized PSGC codes: 0
```

## Gold: `ahon.gold.fact_cmci_indicator`

- **One row is:** one selected CMCI indicator score for one PSGC LGU and reporting year
- **Grain:** PSGC LGU × reporting year × selected CMCI indicator
- **Key:** `psgc_code`, `year`, `indicator_code`
- **Built from Silver by:** `src/sql/gold/fact_cmci.py`
- **Setup:** `src/sql/00_setup/gold_cmci_setup.sql`
- **Expected rows:** 629,090

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `psgc_code` | string | Canonical PSGC identifier for the LGU | Use this field for LGU joins |
| `year` | integer | CMCI reporting year | Expected values are 2014–2024 |
| `indicator_code` | string | Code identifying the selected CMCI indicator | Resolves to `ahon.gold.dim_cmci_indicator` |
| `raw_score` | double | Final indicator score extracted from the CMCI portal | May be `NULL` when the source value is unavailable |

The expected fact size is:

```text
1,634 LGUs × 11 years × 35 selected indicators
= 629,090 rows
```

### Gold Score Rules

Across all reporting years:

- `NULL` is allowed when the source score is unavailable.
- Non-null scores must not be negative.

For 2022–2024:

- Non-null scores must be between `0` and `2.0001`.
- The `0.0001` allowance reflects observed source precision.

Three indicators have fixed recent-year domains:

| Indicator code | Indicator | Allowed non-null values |
| --- | --- | --- |
| `bre` | Compliance to ARTA Citizens Charter | `0`, `1`, `2` |
| `bpls` | Getting Business Permits | `0`, `0.5`, `1`, `1.5`, `2` |
| `lra` | Local Risk Assessments | `0`, `1`, `2` |

The remaining 32 selected indicators contain continuous decimal scores and are not restricted to fixed value lists.

The 2022–2024 ceiling is not applied to earlier years because historical CMCI scoring ranges differ.

## Gold: `ahon.gold.dim_cmci_indicator`

- **One row is:** one CMCI indicator included in Project AHON's selected scope
- **Key:** `indicator_code`
- **Built by:** `src/sql/gold/dim_cmci_indicator.py`
- **Setup:** `src/sql/00_setup/gold_cmci_setup.sql`
- **Expected rows:** 35

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `indicator_code` | string | Short code identifying the selected CMCI indicator | Joins to `fact_cmci_indicator` |
| `indicator_name` | string | Human-readable CMCI indicator name | Used for reporting and analysis |
| `pillar_name` | string | CMCI pillar containing the indicator | One of the five selected pillars |

Expected indicator distribution:

| Pillar | Indicators |
| --- | ---: |
| Economic Dynamism | 4 |
| Government Efficiency | 10 |
| Infrastructure | 10 |
| Resiliency | 10 |
| Innovation | 1 |
| **Total** | **35** |

## Platinum: `ahon.platinum.cmci_pillar_score`

- **One row is:** one selected CMCI pillar aggregation for one PSGC LGU and reporting year
- **Grain:** PSGC LGU × reporting year × selected CMCI pillar
- **Key:** `psgc_code`, `year`, `pillar_name`
- **Built from Gold by:** `src/sql/platinum/cmci_pillar_score.py`
- **Setup:** `src/sql/00_setup/platinum_cmci_setup.sql`
- **Expected rows:** 89,870

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `psgc_code` | string | Canonical PSGC identifier for the LGU | Use this field for LGU joins |
| `year` | integer | CMCI reporting year | Expected values are 2014–2024 |
| `pillar_name` | string | CMCI pillar represented by the row | One of the five selected pillars |
| `pillar_score_raw` | double | Unnormalized sum of the selected indicator scores | Populated only when all selected pillar indicators are available |
| `indicators_expected` | integer | Number of indicators selected for the pillar | Expected values by pillar are 4, 10, 10, 10, and 1 |
| `indicators_available` | integer | Number of selected indicators with non-null scores | Must be between zero and `indicators_expected` |
| `indicators_missing` | integer | Number of selected indicators without an available score | Derived as expected minus available |
| `coverage_pct` | double | Percentage of selected pillar indicators available | Range is 0–100 |
| `score_status` | string | Whether the raw pillar sum is suitable for use | `COMPLETE` or `INSUFFICIENT_DATA` |
| `platinum_processed_timestamp` | timestamp (UTC) | When the Platinum record was last calculated | Updated when the record changes |

The expected Platinum size is:

```text
1,634 LGUs × 11 years × 5 pillars
= 89,870 rows
```

### Platinum Score Status

`COMPLETE` means:

- all indicators selected by Project AHON for the pillar are available;
- `indicators_missing = 0`;
- `coverage_pct = 100`;
- `pillar_score_raw` is populated.

`INSUFFICIENT_DATA` means:

- at least one selected pillar indicator is unavailable;
- `indicators_missing > 0`;
- `coverage_pct < 100`;
- `pillar_score_raw` remains `NULL`.

Current validated distribution:

| Status | Rows |
| --- | ---: |
| `COMPLETE` | 59,367 |
| `INSUFFICIENT_DATA` | 30,503 |
| **Total** | **89,870** |

`pillar_score_raw` is an interim, unnormalized sum of Project AHON's selected indicator scores. It is not the official CMCI 0–20 pillar score and must not be treated as the final Project AHON preparedness score.

## Data Quality

CMCI post-write validation uses Great Expectations through:

`tests/sql/monitoring/cmci_quality.py`

The validation flow is:

```text
Spark quality metrics
→ Great Expectations evaluation
→ Delta DQ results
→ release gate
```

Quality results are stored in:

- `ahon.quality.dq_results`
- `ahon.quality.dq_run_log`

Current validated baseline:

| Layer | Checks | Result | Blocking failures |
| --- | ---: | --- | ---: |
| Bronze | 14 | PASS | 0 |
| Silver | 70 | PASS | 0 |
| Gold | 21 | WARN | 0 |
| Platinum | 17 | PASS | 0 |

The Gold warning is expected:

```text
132,797 of 629,090 fact rows have raw_score = NULL
```

Missing scores are advisory rather than blocking because blank and `-` source values are intentionally preserved as `NULL`.

Platinum rows marked `INSUFFICIENT_DATA` remain valid when their coverage counts, status, and `NULL` raw pillar score are internally consistent.

## Known Issues

- Eight valid PSGC municipalities do not have separate locality values in the current CMCI portal:

  ```text
  1999901000 | Kapalawan
  1999902000 | Old Kaabakan
  1999903000 | Kadayangan
  1999904000 | Nabalawag
  1999905000 | Pahamuddin
  1999906000 | Malidegao
  1999907000 | Ligawasan
  1999908000 | Tugunan
  ```

- These eight LGUs remain unmatched and must not inherit values from former parent municipalities.
- Missing CMCI values remain `NULL`, not zero.
- Historical missingness is concentrated in earlier reporting years, especially 2014–2016.
- Some indicators are unavailable for particular LGUs or reporting years.
- The Gold fact contains 132,797 rows with unavailable scores.
- Historical CMCI scoring ranges differ from the 2022–2024 scoring pattern.
- The CMCI portal is a public website rather than a documented high-volume API.
- Project AHON uses a selected 35-indicator scope.
- `COMPLETE` in Platinum means complete within this selected scope.
- `pillar_score_raw` is not the official normalized CMCI pillar score.
- LGU names are not stored in the Gold fact or Platinum table. Use the shared LGU dimension when available.
- The existing Delta Bronze tables require a one-time schema migration before writers populate all five standard provenance columns.
- A reproducible baseline CMCI mapping bootstrap still requires validation for fresh-environment deployment.

## Open

- Complete the one-time Delta schema migration for both CMCI Bronze tables.
- Populate and validate the five standard Bronze provenance fields in `cmci_batch_ingest.py`.
- Validate `_row_hash` as 64 hexadecimal characters.
- Confirm the official methodology required to convert selected CMCI indicators into the final Project AHON preparedness measure.
- Create the human-readable CMCI analytical view after the shared LGU dimension is available.

## Related Documentation

- [PSGC Dataset](psgc.md)
- [CMCI Gold Analytical Model](../decisions/0008-cmci-gold-analytical-model.md)
- [CMCI Great Expectations Data Quality Framework](../decisions/0009-cmci-great-expectations-data-quality.md)
- [Data Ingestion](ingestion.md)
- [Source Profiles](source-profiles.md)
- [Data Validation](validation.md)
- [Operations Runbook](../operations/runbook.md)
