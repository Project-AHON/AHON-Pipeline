# CMCI Dataset

## Purpose

The Cities and Municipalities Competitiveness Index (CMCI) provides indicators describing local economic conditions, government capacity, infrastructure, resiliency, and innovation.

Project AHON uses CMCI to provide context on:

- socioeconomic vulnerability;
- LGU preparedness and response capacity;
- critical infrastructure and essential services;
- disaster risk reduction planning; and
- communication and digital capability.

> CMCI does not measure hazard occurrence, intensity, frequency, or geographic exposure. Hazard datasets remain the primary sources for hazard analysis. CMCI complements those datasets by describing local capacity and preparedness.

## Source Profile

| Property | Value |
| --- | --- |
| Source | CMCI Data Portal |
| Provider | Department of Trade and Industry |
| Source format | Dynamically generated HTML |
| Geographic unit | Cities and municipalities |
| Reporting years | 2014–2024 |
| Years included | 11 |
| Selected indicators | 35 |
| PSGC LGUs profiled | 1,642 |
| Live CMCI locality values | 1,634 |
| Approved CMCI mappings | 1,634 |
| Known PSGC-only LGUs | 8 |
| Approved mapping coverage | 99.51% |

Source portal: [CMCI Data Portal](https://cmci.dti.gov.ph/data-portal.php)

## Current Pipeline Status

The implemented CMCI pipeline is:

```text
CMCI Data Portal
→ reviewed PSGC-CMCI mapping
→ Bronze batch HTML and raw indicators
→ five Silver pillar tables
→ Gold indicator fact and dimension
→ Platinum pillar aggregation
→ Great Expectations quality gates
```

Current validated coverage:

| Layer | Table coverage |
| --- | --- |
| Bronze | 1,634 approved CMCI LGUs represented in the completed batch scope |
| Silver | 17,974 LGU-year rows in each of five pillar tables |
| Gold fact | 629,090 LGU-year-indicator rows |
| Gold dimension | 35 selected indicators across five pillars |
| Platinum | 89,870 LGU-year-pillar rows |

Detailed table schemas, grains, keys, score rules, and known data limitations are documented in [CMCI Data Dictionary](cmci.md).

## LGU Identity and Mapping

Project AHON uses the Philippine Standard Geographic Code (PSGC) as the canonical LGU identifier.

The reviewed mapping table is:

```text
ahon.reference.cmci_lgu_map
```

The mapping keeps canonical and source identities separate:

| Column | Purpose |
| --- | --- |
| `psgc_code` | Canonical geographic identifier |
| `psgc_name` | Official PSGC locality name |
| `cmci_name` | Exact locality value submitted to CMCI |
| `match_status` | Mapping result |
| `match_method` | Method used to identify the mapping |
| `reviewed` | Indicates whether the mapping was reviewed |
| `is_active` | Indicates whether ingestion may use the mapping |
| `created_timestamp` | Initial mapping-record creation time |
| `updated_timestamp` | Most recent mapping-record update time |

Only mappings meeting all three conditions are eligible for ingestion:

```text
match_status = MATCHED
reviewed = true
is_active = true
```

The ingestion workflow does not perform fuzzy matching. Locality reconciliation is completed and reviewed before ingestion.

## Mapping Outcome

All 1,642 PSGC cities and municipalities were considered during CMCI locality reconciliation. The live CMCI portal exposes 1,634 locality values, each assigned to one approved PSGC record.

```text
Approved active mappings: 1,634
Known PSGC-only LGUs: 8
Ambiguous active mappings: 0
Duplicate active CMCI names: 0
Unused CMCI locality names: 0
Approved CMCI names missing from portal: 0
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

## Known PSGC-Only Source Gaps

The eight LGUs without separate CMCI locality records are:

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

These are valid PSGC municipalities. The current CMCI portal does not expose separate locality values for them.

This is treated as a known source-coverage gap, not a mapping failure or malformed-data condition. The pipeline does not:

- map these LGUs to former parent municipalities;
- copy historical values from former parent municipalities;
- assign zero values for unavailable CMCI data; or
- remove the LGUs from the canonical PSGC reference.

Analyses requiring all 1,642 PSGC LGUs must start from `ahon.reference.lgu_master` and left-join CMCI data using `psgc_code`.

## Indicator Scope

Project AHON retains a deliberate subset of 35 CMCI indicators:

| Pillar | Selected indicators |
| --- | ---: |
| Economic Dynamism | 4 |
| Government Efficiency | 10 |
| Infrastructure | 10 |
| Resiliency | 10 |
| Innovation | 1 |
| **Total** | **35** |

Completeness in the CMCI pipeline refers to this selected scope, not every indicator in the complete CMCI framework.

The selected indicators are preserved at the lowest useful analytical grain in Gold. Platinum aggregates them by pillar while retaining coverage metadata.

## Source Profiling and Mapping Updates

Read-only profiling is performed by:

```text
src/sql/01_bronze_ingest/cmci_source_profile.py
```

The profiler validates source and mapping coverage without modifying reference, Bronze, Silver, Gold, or Platinum data.

Reviewed mapping changes are applied through:

```text
src/sql/01_bronze_ingest/cmci_mapping_update.py
```

Profiling and mapping updates remain separate so routine validation cannot modify reviewed identity relationships.

## Bronze Ingestion

Batch ingestion is performed by:

```text
src/sql/01_bronze_ingest/cmci_batch_ingest.py
```

CMCI Bronze uses two complementary tables:

```text
ahon.bronze.cmci_raw_indicator_batch_html
ahon.bronze.cmci_raw_indicator
```

The batch HTML table preserves one row per source request, including the complete HTML response. The raw indicator table stores one row per batch, PSGC LGU, selected indicator, and reporting year.

The ingestion workflow:

1. Loads approved and active mappings.
2. Determines completed LGUs from `requested_psgc_codes` in batch Bronze.
3. Selects only approved PSGC codes not yet represented in batch Bronze.
4. Sorts pending LGUs by PSGC code and creates deterministic batches.
5. Requests 11 years and 35 selected indicators.
6. Validates returned titles, tables, LGUs, years, indicators, and values.
7. Stores batch HTML and raw indicator rows with a shared batch ID and response hash.
8. Stops after the first failed batch so completed batches remain restartable.

The committed safe default remains:

```text
--run-mode test
```

Full ingestion must be requested explicitly:

```text
--run-mode full
--batch-size 10
```

## Silver Layer

Silver parsing is performed by:

```text
src/sql/02_silver_clean/cmci_indicator_parse.py
```

Silver contains one table per selected CMCI pillar:

```text
ahon.silver.cmci_economic_dynamism
ahon.silver.cmci_government_efficiency
ahon.silver.cmci_infrastructure
ahon.silver.cmci_resiliency
ahon.silver.cmci_innovation
```

Each table has a grain of one approved PSGC LGU and reporting year, with a logical key of:

```text
psgc_code + year
```

Silver parsing is complete for the approved source scope:

```text
Rows per pillar table: 17,974
Unique PSGC-year keys: 17,974
LGUs: 1,634
Years: 11
Duplicate PSGC-year keys: 0
Unauthorized PSGC codes: 0
```

Unavailable source values remain `NULL`, not zero.

## Gold Layer

Gold uses:

```text
ahon.gold.fact_cmci_indicator
ahon.gold.dim_cmci_indicator
```

The fact table has one row per PSGC LGU, reporting year, and selected CMCI indicator:

```text
1,634 LGUs × 11 years × 35 indicators
= 629,090 rows
```

The indicator dimension contains 35 rows and supplies indicator names and pillar assignments.

LGU names are not duplicated in the Gold fact. `psgc_code` remains the authoritative LGU key. A human-readable analytical view can be created after the shared LGU dimension is available.

Missing scores remain `NULL`. They are not converted to zero or imputed.

## Platinum Layer

Platinum uses:

```text
ahon.platinum.cmci_pillar_score
```

The table has one row per PSGC LGU, reporting year, and selected pillar:

```text
1,634 LGUs × 11 years × 5 pillars
= 89,870 rows
```

Current validated status distribution:

| Status | Rows |
| --- | ---: |
| `COMPLETE` | 59,367 |
| `INSUFFICIENT_DATA` | 30,503 |
| **Total** | **89,870** |

`COMPLETE` means all indicators selected by Project AHON for the pillar are available. `INSUFFICIENT_DATA` means at least one selected indicator is unavailable, so `pillar_score_raw` remains `NULL`.

`pillar_score_raw` is an interim, unnormalized sum. It is not the official CMCI 0–20 pillar score and must not be treated as the final Project AHON preparedness score.

## Data Quality Status

Great Expectations post-write validation is implemented across all four CMCI layers.

The standalone quality workflow runs in this order:

```text
Bronze GX
→ Silver GX
→ Gold GX
→ Platinum GX
```

Current validated baseline:

| Layer | Checks | Result | Blocking failures |
| --- | ---: | --- | ---: |
| Bronze | 14 | PASS | 0 |
| Silver | 70 | PASS | 0 |
| Gold | 21 | WARN | 0 |
| Platinum | 17 | PASS | 0 |

The Gold warning is expected because 132,797 of 629,090 fact rows contain unavailable source scores represented as `NULL`.

For 2022–2024, Gold also validates:

- non-null scores between `0` and `2.0001`;
- `bre` values in `0`, `1`, or `2`;
- `bpls` values in `0`, `0.5`, `1`, `1.5`, or `2`; and
- `lra` values in `0`, `1`, or `2`.

The recent-year range is not applied before 2022 because historical scoring ranges differ.

Detailed quality rules and release-gate behavior are documented in [CMCI Great Expectations Data Quality Framework](../decisions/0009-cmci-great-expectations-data-quality.md).

## Limitations

- Eight of the 1,642 PSGC LGUs do not have separate locality records in the current CMCI portal.
- Some indicators are unavailable for particular LGUs or historical years.
- Historical missingness is concentrated in earlier years, especially 2014–2016.
- CMCI describes competitiveness and capacity, not hazard occurrence or geographic exposure.
- CMCI scores must not automatically be interpreted as raw counts or amounts.
- Historical scoring ranges differ from the 2022–2024 scoring pattern.
- The CMCI portal is a public website rather than a documented high-volume API.
- Project AHON uses a selected 35-indicator scope.
- `pillar_score_raw` is not the official normalized CMCI pillar score.

## Next Steps

1. Confirm the methodology for converting selected CMCI indicator scores into the final Project AHON preparedness measure.
2. Create a human-readable CMCI analytical view after the shared LGU dimension is available.
3. Complete the standard Bronze provenance migration and `_row_hash` validation.
4. Validate a reproducible baseline CMCI mapping bootstrap for fresh-environment deployment and disaster recovery.
5. Integrate Bronze, Silver, Gold, Platinum, and their GX gates into the final production workflow.

## Related Documentation

- [CMCI Data Dictionary](cmci.md)
- [PSGC Dataset](psgc.md)
- [CMCI Gold Analytical Model](../decisions/0008-cmci-gold-analytical-model.md)
- [CMCI Great Expectations Data Quality Framework](../decisions/0009-cmci-great-expectations-data-quality.md)
- [Operations Runbook](../operations/runbook.md)
