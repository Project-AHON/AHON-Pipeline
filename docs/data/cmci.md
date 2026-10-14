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
| Years requested | 2014 to 2024 |
| Years included | 11 |
| Selected indicators | 35 |
| PSGC LGUs profiled | 1,642 |
| Live CMCI locality values | 1,634 |
| Approved CMCI mappings | 1,634 |
| Known PSGC-only LGUs | 8 |
| Approved mapping coverage | 99.51% |

Source portal:

[CMCI Data Portal](https://cmci.dti.gov.ph/data-portal.php)

## Source Profiling

Source profiling, mapping updates, production ingestion, and Silver parsing are separate stages.

The official PSGC reference contains:

```text
Cities: 149
Municipalities: 1,493
Total PSGC LGUs: 1,642
```

The live CMCI portal contains:

```text
Distinct CMCI locality values: 1,634
```

All 1,642 PSGC cities and municipalities were considered during CMCI locality reconciliation. Every live CMCI locality is now assigned to exactly one approved PSGC record.

Current mapping outcome:

```text
Approved active mappings: 1,634
Known PSGC-only LGUs: 8
Ambiguous active mappings: 0
Duplicate active CMCI names: 0
Unused CMCI locality names: 0
Approved CMCI names missing from portal: 0
```

The eight PSGC-only LGUs remain in:

```text
ahon.reference.lgu_master
```

Their mapping status remains visible through:

```text
ahon.reference.cmci_lgu_map
```

They are not deleted from the reference layer or assigned to former parent municipalities.

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

These are valid PSGC municipalities in the BARMM Special Geographic Area. The current CMCI portal does not expose separate locality values for them.

This is treated as a known source-coverage gap, not a matching failure or malformed-data condition.

The pipeline does not:

- Map these LGUs to former parent municipalities
- Copy historical values from former parent municipalities
- Assign zero values for unavailable CMCI data
- Remove the LGUs from the canonical PSGC reference

If CMCI later adds separate locality records, the mapping can be reviewed and activated through the controlled mapping workflow.

## LGU Identity and Mapping

Project AHON uses the Philippine Standard Geographic Code (PSGC) as the canonical LGU identifier.

The reviewed mapping table is:

```text
ahon.reference.cmci_lgu_map
```

The mapping keeps canonical and source identities separate:

| Column | Purpose |
|---|---|
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

The ingestion workflow does not perform fuzzy matching. Name reconciliation is completed and reviewed before ingestion.

## Mapping Methods

Approved mapping-method counts:

| Method | Count | Description |
|---|---:|---|
| `EXACT` | 1,113 | PSGC and CMCI names matched directly. |
| `NORMALIZED` | 136 | Names matched after safe cleanup such as removing city prefixes, accents, punctuation, or spacing differences. |
| `COMPACT` | 7 | Names matched after removing spaces and punctuation, only when unique in both sources. |
| `MANUAL` | 7 | Earlier reviewed exceptions were resolved using contextual evidence. |
| `PROVINCE_SUFFIX` | 347 | Duplicate locality names were resolved using reviewed CMCI suffixes and PSGC province context. |
| `PROVINCE_SUFFIX_EXCEPTION` | 4 | The conflicting `DS` suffix was resolved using reviewed locality and province combinations. |
| `MANUAL_ALIAS` | 20 | Alternate names, abbreviations, spelling differences, and special-city names were verified manually against CMCI profiles. |
| **Total approved** | **1,634** | One approved mapping for every live CMCI locality. |

Mapping reconciliation checks confirm:

```text
Portal suffixes found: 77
Reviewed suffixes configured: 77
Unknown portal suffixes: 0
Configured suffixes not currently used: 0
Duplicate PSGC codes: 0
Duplicate active CMCI names: 0
```

## Mapping Update Workflow

Reviewed mapping changes are applied through:

```text
cmci_mapping_update.py
```

The update workflow is separate from source profiling because it modifies the reference mapping table. It validates the proposed PSGC codes and CMCI names before performing a guarded Delta merge.

The update process validates that:

- Each PSGC code exists in the target mapping table
- Each target row is currently unmatched before the update
- Each proposed CMCI name exists in the live portal
- No proposed PSGC code appears more than once
- No proposed CMCI name appears more than once
- No proposed CMCI name is already assigned
- Expected mapping counts reconcile before and after the merge

The completed reconciliation added:

```text
Province-suffix mappings: 347
Conflicting-suffix exceptions: 4
Reviewed manual aliases: 20
Total newly approved mappings: 371
```

The update produced:

```text
Final approved mappings: 1,634
Final unmatched PSGC records: 8
```

## Source Profiling Workflow

Read-only profiling is performed by:

```text
cmci_source_profile.py
```

The profiler:

- Validates required reference, Bronze, and Silver tables
- Reads the live CMCI locality selector
- Profiles the active PSGC reference
- Reports mapping status and method counts
- Compares live CMCI names with approved mappings
- Detects unknown or unused CMCI suffixes
- Lists known PSGC-only source gaps
- Profiles Bronze and Silver coverage
- Performs no reference, Bronze, or Silver writes

Current reconciliation checks should report:

```text
Live CMCI locality names: 1,634
Approved unique CMCI names: 1,634
Unused CMCI locality names: 0
Approved CMCI names missing from portal: 0
Known PSGC-only LGUs: 8
```

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

All 35 indicators are retained because requesting additional indicators adds little processing overhead after LGUs and years are batched.

Gold models and dashboards may use a smaller business-relevant subset without removing other indicators from Bronze or Silver.

## Production Ingestion Scope

Production ingestion now covers all 1,634 approved CMCI mappings.

```text
Approved CMCI LGUs: 1,634
Years requested: 2014 to 2024
Years included: 11
Indicators: 35
```

The eight PSGC-only LGUs are excluded because no CMCI source identity exists for them. This is an intentional source-coverage rule, not an ingestion failure.

## Shared CMCI Configuration

Shared source configuration and request helpers remain in:

```text
cmci_common.py
```

The batch-ingestion workflow imports the following shared settings and helpers:

```text
EXPECTED_INDICATOR_COUNT
EXPECTED_INDICATOR_LABELS
PORTAL_URL
PROCESS_URL
REQUESTED_INDICATOR_CODES
REQUEST_TIMEOUT_SECONDS
create_http_session
```

This module remains a runtime dependency and should not be removed while those imports are in use.

## Bronze Layer

CMCI Bronze uses two complementary tables.

### Batch HTML Table

```text
ahon.bronze.cmci_raw_indicator_batch_html
```

#### Grain

```text
One row per CMCI request batch
```

Each row stores:

- Deterministic batch ID
- Requested PSGC codes
- Exact CMCI locality names
- Requested years
- Requested indicator codes
- Expected and returned counts
- Complete raw HTML response
- Stable response hash
- UTC ingestion timestamp

Raw HTML is retained for source traceability, auditing, reprocessing, parser improvements, and recovery from transformation errors.

### Raw Indicator Table

```text
ahon.bronze.cmci_raw_indicator
```

#### Grain

```text
One row per batch, PSGC LGU, indicator, and year
```

Important columns include:

| Column | Purpose |
|---|---|
| `batch_id` | Links the indicator row to the original request batch |
| `psgc_code` | Canonical LGU identifier |
| `psgc_name` | Canonical PSGC locality name |
| `cmci_name` | Exact CMCI source locality name |
| `indicator_label` | CMCI indicator represented by the row |
| `year` | Requested CMCI year |
| `raw_value` | Source value before Silver typing |
| `response_hash` | Links the row to the validated source response |
| `ingestion_timestamp` | UTC source-ingestion time |

The raw indicator table was added to support direct, traceable parsing of newly ingested batches. Earlier historical coverage remains preserved in the raw batch HTML and existing Silver tables.

## Batch Ingestion Behavior

Batch ingestion is performed by:

```text
cmci_batch_ingest.py
```

The ingestion workflow:

1. Loads approved and active CMCI mappings.
2. Explodes `requested_psgc_codes` from the batch HTML table to determine completed LGUs.
3. Selects only approved PSGC codes not already represented in batch Bronze.
4. Applies the test limit after the pending-LGU filter.
5. Sorts pending LGUs by PSGC code.
6. Groups pending LGUs into deterministic batches.
7. Requests 11 years and 35 indicators from CMCI.
8. Validates returned titles, tables, LGUs, years, indicators, and values.
9. Stores raw HTML and raw indicator rows with a shared batch ID and response hash.
10. Stops after the first failed batch so completed batches remain restartable.

### Run Modes

The committed safe default is:

```text
--run-mode test
```

Recommended test parameters:

```text
--run-mode test
--batch-size 10
--test-lgu-limit 10
```

Full ingestion must be requested explicitly:

```text
--run-mode full
--batch-size 10
```

Existing batches should not be refreshed during normal catch-up ingestion.

### Restartability

The workflow preserves successful batches if a later request fails. The next run recalculates the pending PSGC set from batch Bronze and excludes LGUs already represented there.

This prevents successful batches from being repeated and keeps pending batch boundaries stable when the workflow stops after the first failure.

## Verified Bronze Coverage

The historical batch ingestion originally covered 1,262 LGUs. The completed catch-up added the remaining 372 approved LGUs.

| Measure | Result |
|---|---:|
| Approved CMCI LGUs | 1,634 |
| Historical LGUs represented before catch-up | 1,262 |
| Newly ingested LGUs | 372 |
| New catch-up batches | 38 |
| New full-size batches | 37 |
| New final partial batch | 1 |
| Years | 11 |
| Indicators | 35 |
| Catch-up indicator rows | 143,220 |
| Failed catch-up batches | 0 |

Catch-up calculation:

```text
372 LGUs × 11 years × 35 indicators
= 143,220 raw indicator rows
```

Final batch-Bronze locality scope:

```text
1,262 historical LGUs + 372 catch-up LGUs
= 1,634 approved CMCI LGUs
```

## Silver Layer

Silver contains one table per CMCI pillar:

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
- Pillar-specific typed indicator columns
- Source response hash
- Source ingestion timestamp
- Silver processing timestamp

Unavailable CMCI values are stored as `NULL`, not zero.

## Silver Coverage Status

Before the 372-LGU catch-up is parsed, each Silver table contains:

```text
1,262 LGUs × 11 years
= 13,882 LGU-year rows
```

The approved final Silver scope is:

```text
1,634 LGUs × 11 years
= 17,974 LGU-year rows per pillar table
```

The remaining parser workload is:

```text
372 LGUs × 11 years
= 4,092 additional LGU-year rows per pillar table
```

The current Silver coverage warnings therefore represent pending parsing of successfully ingested Bronze data, not corrupted Silver records.

After the incremental parser completes, each pillar table must contain:

```text
Rows: 17,974
Unique PSGC-year keys: 17,974
LGUs: 1,634
Years: 11
Duplicate PSGC-year keys: 0
Unauthorized PSGC codes: 0
```

Silver parsing must merge incrementally using:

```text
psgc_code + year
```

It must not overwrite or duplicate the existing 1,262-LGU Silver scope.

## Complete PSGC Analysis

When analysis requires all PSGC LGUs, begin with:

```text
ahon.reference.lgu_master
```

Then left-join the appropriate CMCI Silver table using:

```text
psgc_code
```

The eight PSGC-only LGUs will remain visible with `NULL` CMCI values. This preserves the distinction between:

```text
LGU exists in PSGC
```

and:

```text
LGU has an approved CMCI source record
```

## Data Quality Scope

Great Expectations checks are planned for a later implementation.

CMCI quality monitoring must distinguish among:

- PSGC reference coverage
- CMCI source coverage
- Approved mapping coverage
- Bronze ingestion coverage
- Silver parsing coverage
- Invalid pipeline records

The eight PSGC-only LGUs are known source gaps. They should not be silently dropped, treated as malformed CMCI records, quarantined solely because no CMCI record exists, or assigned a zero score.

Future quality checks will use separate denominators:

```text
PSGC reference coverage: 1,642 LGUs
CMCI source and mapping coverage: 1,634 LGUs
```

### PSGC Reference Completeness

Use the 1,642-LGU denominator when measuring:

- PSGC master completeness
- National LGU reference coverage
- CMCI mapping coverage against PSGC
- Known source gaps

### CMCI Pipeline Completeness

Use the 1,634-LGU denominator when measuring:

- Live CMCI locality reconciliation
- Approved mapping coverage
- Bronze ingestion coverage
- Silver parsing coverage
- Yearly CMCI row counts
- Duplicate prevention

This distinction prevents the CMCI pipeline from failing because the CMCI source does not contain the eight newer PSGC municipalities.

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
    ├── load_cmci_lgu_map.py
    └── cmci_mapping_update.py
```

Bronze table creation is defined in the repository SQL setup, including:

```text
ahon.bronze.cmci_raw_indicator_batch_html
ahon.bronze.cmci_raw_indicator
```

Temporary diagnostic and cleanup files used during controlled recovery are not part of the production pipeline and should not be committed.

## Limitations

- Eight of the 1,642 PSGC LGUs do not have separate locality records in the current CMCI portal.
- Some indicators are unavailable for particular LGUs or historical years.
- CMCI describes competitiveness and capacity, not hazard occurrence or geographic exposure.
- CMCI values are normalized scores and must not automatically be interpreted as raw counts or amounts.
- The CMCI portal is a public website rather than a documented high-volume API.
- The raw indicator Bronze table currently represents the 372-LGU catch-up, while earlier source responses remain preserved in batch HTML and existing Silver tables.
- Final 1,634-LGU Silver coverage depends on completing the incremental indicator parsing step.

## Next Steps

1. Run `cmci_indicator_parse.py` incrementally for the 372 newly ingested LGUs.
2. Validate 17,974 unique LGU-year rows in each Silver pillar table.
3. Configure the Bronze and Silver Databricks jobs with explicit run parameters.
4. Keep `test` as the safe committed default for batch ingestion.
5. Add Great Expectations after job execution is stable.
6. Add critical quality gates for unauthorized mappings, duplicates, and incomplete approved coverage.
7. Add warning-level checks for known source gaps and missing values.
8. Document Gold-layer indicator selection and scoring rules.
