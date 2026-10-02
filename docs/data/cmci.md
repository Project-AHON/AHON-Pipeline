# CMCI Dataset

## Purpose

The Cities and Municipalities Competitiveness Index (CMCI) provides indicators describing local economic conditions, government capacity, infrastructure, resiliency, and innovation.

Project AHON uses CMCI to provide context on:

- Socioeconomic vulnerability
- LGU preparedness and response capacity
- Critical infrastructure and essential services
- Disaster risk reduction planning
- Communication and digital capability

> CMCI does not measure hazard occurrence, intensity, frequency, or geographic exposure. Hazard datasets remain the primary sources for hazard analysis. CMCI complements those datasets by describing local capacity and preparedness.

## Source Profile

| Property | Value |
|---|---|
| Source | CMCI Data Portal |
| Provider | Department of Trade and Industry |
| Source format | Dynamically generated HTML |
| Geographic unit | Cities and municipalities |
| Years ingested | 2014 to 2024 |
| Selected indicators | 35 |
| PSGC LGUs profiled | 1,642 |
| Approved CMCI mappings | 1,262 |
| Unmatched PSGC LGUs | 380 |

Source portal:

[CMCI Data Portal](https://cmci.dti.gov.ph/data-portal.php)

## Source Profiling

Source profiling and production ingestion are separate stages.

The official PSGC reference contains:

```text
Cities: 149
Municipalities: 1,493
Total PSGC LGUs: 1,642
```

All 1,642 PSGC cities and municipalities were considered during CMCI locality mapping.

The mapping process compared the official PSGC identities with the locality values available from the CMCI portal.

Current mapping outcome:

```text
Approved active mappings: 1,262
Unmatched PSGC LGUs: 380
Ambiguous active mappings: 0
```

The 380 unmatched LGUs were profiled but were not included in CMCI ingestion.

These LGUs remain in:

```text
ahon.reference.lgu_master
```

Their mapping outcomes remain visible through:

```text
ahon.reference.cmci_lgu_map
```

They are not deleted from the reference layer.

### Why 380 LGUs Were Not Ingested

PSGC and CMCI sometimes use different locality names. Some city and municipality names also occur in multiple provinces.

Assigning a CMCI locality through uncertain text matching could attach valid CMCI values to the wrong PSGC LGU.

The pipeline therefore excludes unmapped or unapproved LGUs rather than guessing their CMCI identities.

The remaining LGUs may require:

- Province-qualified matching
- Alternate-name review
- Abbreviation handling
- Review of renamed or reorganized LGUs
- Identification of LGUs absent from the CMCI source
- Manual review of source-specific naming differences

## LGU Identity and Mapping

Project AHON uses the Philippine Standard Geographic Code (PSGC) as the canonical LGU identifier.

The pipeline uses this reviewed mapping table:

```text
ahon.reference.cmci_lgu_map
```

The mapping keeps the source identities separate:

| Column | Purpose |
|---|---|
| `psgc_code` | Canonical geographic identifier |
| `psgc_name` | Official PSGC locality name |
| `cmci_name` | Exact locality value submitted to CMCI |
| `match_status` | Mapping result |
| `match_method` | Method used to identify the mapping |
| `reviewed` | Indicates whether the mapping was reviewed |
| `is_active` | Indicates whether ingestion may use the mapping |

Only mappings meeting all three conditions are eligible for ingestion:

```text
match_status = MATCHED
reviewed = true
is_active = true
```

The ingestion script does not perform fuzzy matching.

## Indicator Scope

The pipeline retains 35 CMCI indicators:

| Pillar | Indicator count |
|---|---:|
| Economic Dynamism | 4 |
| Government Efficiency | 10 |
| Infrastructure | 10 |
| Resiliency | 10 |
| Innovation | 1 |
| **Total** | **35** |

All 35 indicators are retained because requesting additional indicators adds little processing overhead once LGUs and years are batched.

The wider dataset may also support future analysis. Gold models and dashboards can use a smaller business-relevant subset without removing the other indicators from Bronze or Silver.

## Production Ingestion Scope

Production CMCI ingestion includes only the 1,262 approved mappings.

The final ingestion scope is:

```text
Approved LGUs: 1,262
Years: 2014 to 2024
Years included: 11
Indicators: 35
```

The 380 unmatched PSGC LGUs are not ingested because no approved CMCI source identity is available.

This is an intentional coverage rule, not an ingestion failure.

## Bronze Layer

Bronze stores the complete raw CMCI response in:

```text
ahon.bronze.cmci_raw_indicator_batch_html
```

### Grain

```text
One row per CMCI request batch
```

### Production Batch Scope

```text
Up to 10 LGUs
11 years
35 indicators
```

Each Bronze row stores:

- Requested PSGC codes
- Exact CMCI locality names
- Requested years
- Requested indicator codes
- Expected and returned counts
- Complete raw HTML response
- Stable response hash
- UTC ingestion timestamp

Raw HTML is retained for:

- Source traceability
- Auditing
- Reprocessing
- Parser improvements
- Recovery from transformation errors

## Silver Layer

Silver contains one table per CMCI pillar:

```text
ahon.silver.cmci_economic_dynamism
ahon.silver.cmci_government_efficiency
ahon.silver.cmci_infrastructure
ahon.silver.cmci_resiliency
ahon.silver.cmci_innovation

```text
ahon.silver.cmci_economic_dynamism
ahon.silver.cmci_government_efficiency
ahon.silver.cmci_infrastructure
ahon.silver.cmci_resiliency
ahon.silver.cmci_innovation
```

### Grain

```text
One row per approved PSGC LGU and year
```

### Logical Key

```text
psgc_code + year
```

Each Silver table contains:

- PSGC code
- Official PSGC locality name
- Exact CMCI locality name
- Year
- Pillar-specific indicator columns
- Source response hash
- Source ingestion timestamp
- Silver processing timestamp

Each pillar's indicators are stored as typed columns in the corresponding Silver table.

Unavailable CMCI values are stored as `NULL`, not zero.

## Silver Coverage

Silver represents only the approved and successfully ingested CMCI population.

Expected coverage per pillar table:

```text
1,262 approved LGUs × 11 years
= 13,882 LGU-year rows
```

Silver does not create placeholder CMCI rows for the 380 unmatched LGUs because no trustworthy CMCI source record exists for those localities.

When complete PSGC coverage is needed, analysts should begin with:

```text
ahon.reference.lgu_master
```

and left-join the appropriate CMCI Silver table using:

```text
psgc_code
```

Unmatched LGUs will then remain visible with `NULL` CMCI values.

This preserves the distinction between:

```text
LGU exists in PSGC
```

and:

```text
LGU has approved CMCI data
```

## Verified Coverage

### Bronze

The historical Bronze ingestion completed successfully:

| Measure | Result |
|---|---:|
| Approved LGUs | 1,262 |
| Years | 11 |
| Year range | 2014 to 2024 |
| Indicators | 35 |
| Production batches | 127 |
| Failed production batches | 0 |
| Validated values | 485,870 |

Validated value calculation:

```text
1,262 LGUs × 11 years × 35 indicators
= 485,870 values
```

### Silver

The full Silver load also completed successfully:

| Measure | Result |
|---|---:|
| Silver pillar tables | 5 |
| LGU-year rows per table | 13,882 |
| Rows per year | 1,262 |
| Years per table | 11 |

Silver row calculation:

```text
1,262 LGUs × 11 years
= 13,882 rows per pillar table
```

All five Silver tables passed:

- Coverage validation
- LGU-year uniqueness validation
- Yearly row-count validation
- Required-column validation
- Source-hash validation

## Data Quality Scope

Great Expectations checks are planned for a later implementation.

CMCI quality monitoring will distinguish between:

- Reference coverage
- Approved ingestion coverage
- Invalid pipeline records

The 380 unmatched PSGC LGUs are a known mapping-coverage gap. They remain in the reference and mapping layers for future reconciliation, but they are intentionally excluded from CMCI Bronze and Silver processing.

They should not be silently dropped, treated as malformed CMCI records, or placed in quarantine solely because no approved mapping exists.

Future quality checks will use two denominators:

```text
Reference coverage: 1,642 PSGC LGUs
CMCI pipeline coverage: 1,262 approved mappings
```

### Reference Completeness

```text
1,642 PSGC LGUs
```

Use this denominator when measuring:

- PSGC master completeness
- Mapping coverage
- Unmatched LGUs
- Overall geographic reference coverage

### CMCI Pipeline Completeness

```text
1,262 approved CMCI mappings
```

Use this denominator when measuring:

- Bronze ingestion coverage
- Silver coverage
- Yearly CMCI row counts
- Duplicate prevention
- Pipeline completeness

This distinction prevents the CMCI pipeline from failing because it does not contain LGUs that were never approved for CMCI ingestion.

## Repository Files

```text
src/
├── 01_bronze_ingest/
│   ├── cmci_source_profile.py
│   ├── cmci_common.py
│   └── cmci_batch_ingest.py
├── 02_silver_clean/
│   └── cmci_indicator_parse.py
└── 06_reference/
    ├── load_lgu_master.py
    └── load_cmci_lgu_map.py
```

## Limitations

- Only 1,262 of 1,642 PSGC LGUs currently have approved CMCI mappings.
- The remaining 380 LGUs are excluded from ingestion rather than assigned through uncertain matching.
- Some indicators are unavailable for particular LGUs or historical years.
- CMCI describes competitiveness and capacity, not hazard occurrence or geographic exposure.
- CMCI values are normalized scores and must not automatically be interpreted as raw counts or amounts.
- The CMCI portal is a public website rather than a documented high-volume API.

## Next Steps

1. Configure the Bronze and Silver Databricks jobs.
2. Add Great Expectations after job execution is stable.
3. Add critical quality gates for unauthorized mappings, duplicates, and incomplete approved coverage.
4. Add warning-level checks for mapping coverage and missing values.
5. Create a review or quarantine process for actual rule violations.
6. Continue reconciling the 380 unmatched PSGC LGUs.
7. Recalculate expected Bronze and Silver coverage whenever the approved mapping count changes.
8. Document the Gold-layer CMCI indicator selection and scoring rules.

## Related Documentation

- [Data Ingestion](ingestion.md)
- [Source Profiles](source-profiles.md)
- [Data Validation](validation.md)
- [Governance Decisions](../governance/decisions.md)
- [Operations Runbook](../operations/runbook.md)
