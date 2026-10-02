## CMCI Pipeline Runbook

### Purpose

This runbook covers the operational steps for:

- Profiling the CMCI source and pipeline coverage
- Loading PSGC and CMCI reference data
- Running restartable CMCI Bronze ingestion
- Processing the five CMCI Silver pillar tables
- Validating expected coverage
- Recovering from failed or interrupted runs

For the dataset overview and mapping limitations, see ../data/cmci.md.

## Pipeline Order

Run the CMCI pipeline in this order:

```text
Load PSGC LGU master
        ↓
Load reviewed CMCI mappings
        ↓
Run CMCI source profile
        ↓
Ingest CMCI Bronze batches
        ↓
Parse CMCI Silver tables
        ↓
Validate coverage and uniqueness
```

Required scripts:

```text
src/06_reference/load_lgu_master.py
src/06_reference/load_cmci_lgu_map.py
src/01_bronze_ingest/cmci_source_profile.py
src/01_bronze_ingest/cmci_batch_ingest.py
src/02_silver_clean/cmci_indicator_parse.py
```

## Expected Scope

The current approved CMCI pipeline scope is:

| Measure | Expected value |
|---|---:|
| PSGC LGUs profiled | 1,642 |
| Approved CMCI mappings | 1,262 |
| Unmatched PSGC LGUs | 380 |
| Years | 11 |
| Year range | 2014 to 2024 |
| Indicators | 35 |
| Production Bronze batches | 127 |
| Bronze values | 485,870 |
| Silver pillar tables | 5 |
| Rows per Silver table | 13,882 |
| Rows per year per Silver table | 1,262 |

The 380 unmatched PSGC LGUs are outside the approved CMCI ingestion scope. Their absence from CMCI Bronze and Silver is not a pipeline failure.

## Prerequisites

Confirm that these tables exist:

```text
ahon.reference.lgu_master
ahon.reference.cmci_lgu_map
ahon.bronze.cmci_raw_indicator_batch_html
ahon.silver.cmci_economic_dynamism
ahon.silver.cmci_government_efficiency
ahon.silver.cmci_infrastructure
ahon.silver.cmci_resiliency
ahon.silver.cmci_innovation
```

Confirm the approved mapping count:

```sql
SELECT
    COUNT(*) AS approved_mapping_count
FROM *hon.reference.cmci_lgu_map
WHERE m*tch_status = 'MATCHED'
  AND revie*ed = true
  AND is_active = true;
*``

Expected result:

*``text
1,262
```

If the approved *apping count changes:

1. Review the mapping changes.
2. Recalculate expected Bronze coverage.
3. Recalculate expected Silver coverage.
4. Update the documented expected counts.
5. Run source profiling before production ingestion.

## Source Profiling

Run:

```text
src/00_source_profile/cmci_source_profile.py
```

The profiling script is read-only. It does not modify reference, Bronze, or Silver tables.

The profile reports:

- Live CMCI portal availability
- CMCI locality count
- PSGC city and municipality counts
- Mapping status and method counts
- Approved and unmatched mapping counts
- Duplicate active CMCI names
- Bronze production coverage
- Silver row and key coverage
- Unauthorized PSGC codes in Silver

### Expected Profile Results

```text
Active PSGC LGUs: 1,642
Duplicate PSGC codes: 0

Approved active mappings: 1,262
Unmatched mappings: 380
Ambiguous mappings: 0
Duplicate active CMCI names: 0
Approved PSGC codes missing from master: 0

Production batches: 127
Production LGU slots: 1,262
Production values: 485,870

Expected LGU-year rows per Silver table: 13,882
Silver coverage matches the approved CMCI scope
```

The expected final line is:

```text
Read-only profile complete. No reference, Bronze, or Silver data was changed.
```

### Profiling Warning Interpretation

An empty list of years discoverable from the portal HTML does not automatically mean the historical years are unavailable.

The production ingestion and Silver tables already validated coverage from 2014 through 2024.

Treat the live year-discovery warning as informational unless CMCI requests for the configured years begin to fail.

## Bronze Run Mode

The current default run mode in `cmci_batch_ingest.py` is:

```text
full
```

Existing deterministic production batches are skipped automatically unless refresh is explicitly requested.

Use test mode when validating changes against a limited LGU scope:

```text
--run-mode test
```

> Do not use `--refresh-existing` unless a complete source refresh is intentionally required.

## Bronze Full Run

Run:

```text
src/01_bronze_ingest/cmci_batch_ingest.py
```

Expected startup:

```text
Run mode: full
LGUs selected: 1,262
Years selected: 2014 to 2024
Indicators selected: 35
Batches expected: 127
```

Expected value count:

```text
1,262 LGUs × 11 years × 35 indicators
= 485,870 values
```

### First Full Run

A new historical load should finish with:

```text
Total batches: 127
Successful batches: 127
Existing or unchanged batches: 0
New or changed batches written: 127
Failed batches: 0
```

### Repeat Full Run

If all deterministic batches already exist, the run should skip them:

```text
SKIPPED | Batch 1 of 127 | Already saved
SKIPPED | Batch 2 of 127 | Already saved
...
SKIPPED | Batch 127 of 127 | Already saved
```

Expected final summary:

```text
Total batches: 127
Successful batches: 127
Existing or unchanged batches: 127
New or changed batches written: 0
Failed batches: 0
```

This confirms that restartability and duplicate prevention are working.

## Bronze Failure Recovery

Successful batches are written immediately using deterministic batch IDs.

If ingestion stops because of:

- Compute interruption
- Browser disconnection
- HTTP timeout
- Temporary CMCI source failure
- Batch response validation failure

rerun the same command with the same:

- LGU scope
- Year list
- Indicator list
- Batch size

Previously completed batches will be skipped automatically.

Expected recovery behavior:

```text
SKIPPED | Batch 1 of 127 | Already saved
SKIPPED | Batch 2 of 127 | Already saved
WRITTEN | Next incomplete batch
```

Do not delete successful Bronze batches before retrying.

## Bronze Failure Conditions

Stop and investigate if the output includes:

```text
FAILED | Batch
```

| Condition | Action |
|---|---|
| HTTP 429 | Wait before retrying and keep request throttling enabled |
| HTTP 403 | Stop and confirm that the CMCI portal is accessible manually |
| HTTP 5xx | Retry later because the source may be temporarily unavailable |
| Empty response | Retry the failed batch |
| Unexpected table count | Inspect the CMCI response structure |
| Missing LGU row | Check the approved mapping and returned response |
| Missing year column | Confirm that the requested years remain available |
| Unexpected nonnumeric value | Inspect the affected indicator table |
| Mapping mismatch | Compare the Bronze request identity with `cmci_lgu_map` |

Do not proceed to Silver while failed production batches remain.

## Bronze Validation

Validate production batch coverage:

```sql
SELECT
    COUNT(*) AS production_batch_rows,
    SUM(expected_lgu_count) AS lgu_slots,
    SUM(returned_value_count) AS returned_values
FROM ahon.bronze.cmci_raw_indicator_batch_html
WHERE expected_year_count = 11
  AND expected_indicator_count = 35
  AND expected_lgu_count IN (10, 2);
```

Expected result:

```text
production_batch_rows: 127
lgu_slots: 1,262
returned_values: 485,870
```

Earlier five-LGU test batches may remain in Bronze. The query excludes those test batches.

### Bronze Batch Summary

```sql
SELECT
    expected_lgu_count,
    expected_year_count,
    expected_indicator_count,
    COUNT(*) AS batch_count,
    SUM(returned_value_count) AS returned_values
FROM ahon.bronze.cmci_raw_indicator_batch_html
GROUP BY
    expected_lgu_count,
    expected_year_count,
    expected_indicator_count
ORDER BY
    expected_year_count,
    expected_lgu_count;
```

Use this query to distinguish test batches from production batches.

## Silver Run

After Bronze completes with zero failed production batches, run:

```text
src/02_silver_clean/cmci_indicator_parse.py
```

The Silver process:

1. Selects the latest version of each Bronze batch.
2. Parses all 35 indicator tables.
3. Expands LGU rows and year columns.
4. Validates the CMCI-to-PSGC mapping.
5. Resolves overlapping test and production batches.
6. Keeps the latest source record per `psgc_code + year`.
7. Creates five pillar-specific DataFrames.
8. Merges the records into the Silver tables.
9. Verifies coverage, uniqueness, hashes, and yearly counts.

### Expected Silver Output

```text
Bronze batches parsed: 129
Unique LGU-year records: 13,882
```

The 129 Bronze batches consist of:

```text
127 production batches
+ 2 earlier test batches
= 129 total batches
```

Expected successful ending:

```text
CMCI BATCH SILVER LOAD COMPLETE
Bronze batches processed: 129
Approved LGUs represented: 1,262
Years represented: 11
LGU-year rows per pillar: 13,882
Pillar tables updated: 5
```

Each Silver table should report:

```text
Rows processed: 13,882
```

## Silver Failure Recovery

Silver uses Delta `MERGE` with:

```text
psgc_code + year
```

If Silver fails:

1. Capture the first reported error.
2. Identify whether the issue is in Bronze data, mapping data, table schema, or parser logic.
3. Fix the affected source or code.
4. Rerun the complete Silver script.
5. Do not manually delete valid Silver rows.

The merge will:

- Insert missing LGU-year records
- Update rows when the Bronze source hash changes
- Leave unchanged rows untouched

## Silver Row Validation

Run the following query for each Silver pillar table:

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(
        DISTINCT CONCAT(
            psgc_code,
            '|',
            year
        )
    ) AS unique_lgu_year_keys
FROM ahon.silver.cmci_resiliency;
```

Expected result:

```text
total_rows: 13,882
unique_lgu_year_keys: 13,882
```

Required Silver tables:

```text
ahon.silver.cmci_economic_dynamism
ahon.silver.cmci_government_efficiency
ahon.silver.cmci_infrastructure
ahon.silver.cmci_resiliency
ahon.silver.cmci_innovation
```

## Year Coverage Validation

```sql
SELECT
    year,
    COUNT(*) AS row_count
FROM ahon.silver.cmci_resiliency
GROUP BY year
ORDER BY year;
```

Expected result:

```text
2014: 1,262
2015: 1,262
2016: 1,262
2017: 1,262
2018: 1,262
2019: 1,262
2020: 1,262
2021: 1,262
2022: 1,262
2023: 1,262
2024: 1,262
```

Each of the five Silver pillar tables must have the same yearly coverage.

## Duplicate-Key Validation

```sql
SELECT
    psgc_code,
    year,
    COUNT(*) AS row_count
FROM ahon.silver.cmci_resiliency
GROUP BY
    psgc_code,
    year
HAVING COUNT(*) > 1;
```

Expected result:

```text
0 rows
```

## Unauthorized PSGC Validation

```sql
SELECT DISTINCT
    silver.psgc_code
FROM ahon.silver.cmci_resiliency AS silver
LEFT ANTI JOIN (
    SELECT DISTINCT
        psgc_code
    FROM ahon.reference.cmci_lgu_map
    WHERE match_status = 'MATCHED'
      AND reviewed = true
      AND is_active = true
) AS approved
ON silver.psgc_code = approved.psgc_code;
```

Expected result:

```text
0 rows
```

## Missing-Value Handling

CMCI may return unavailable values for some LGUs or historical years.

Silver stores unavailable values as:

```text
NULL
```

Do not replace `NULL` with zero unless a documented Gold-layer rule explicitly requires it.

A zero is a valid CMCI value and must remain distinct from unavailable data.

## Coverage Interpretation

CMCI pipeline coverage is based on:

```text
1,262 approved CMCI mappings
```

The 380 unmatched PSGC LGUs are outside the approved CMCI ingestion scope.

Do not treat their absence from Bronze or Silver as a pipeline failure.

When an output requires all 1,642 PSGC LGUs:

1. Begin with `ahon.reference.lgu_master`.
2. Left-join the required CMCI Silver table using `psgc_code`.
3. Preserve unmatched LGUs with `NULL` CMCI values.

## Safe Operational Controls

Keep these safeguards enabled:

- Sequential CMCI requests
- Request delay
- Retry and backoff behavior
- Deterministic batch IDs
- Mapping validation
- Response-structure validation
- Immediate persistence of successful batches
- Silver Delta merges
- Post-write coverage validation

Use `--refresh-existing` only when a deliberate source refresh is required.

## Future Great Expectations Flow

Great Expectations is not yet implemented.

The planned task order is:

```text
Load references
        ↓
GX reference validation
        ↓
Bronze ingestion
        ↓
GX Bronze validation
        ↓
Silver processing
        ↓
GX Silver validation
        ↓
Gold processing
```

The 380 unmatched LGUs should be reported as a mapping-coverage warning, not automatically quarantined or treated as invalid CMCI records.

Detailed planned expectations are documented in ../data/validation.md.

## Escalation Conditions

Stop the workflow and investigate when:

- The approved mapping count changes unexpectedly.
- Duplicate active CMCI names are detected.
- CMCI returns fewer than 35 indicator tables.
- Returned LGUs do not match the request.
- Requested years are missing from a batch response.
- Multiple batches repeatedly fail.
- Bronze production coverage is below 127 batches.
- Bronze represents fewer than 485,870 values.
- Silver produces fewer than 13,882 LGU-year rows.
- A Silver table contains duplicate keys.
- A Silver table contains unauthorized PSGC codes.
- Source-hash validation fails.
- Any year contains fewer or more than 1,262 rows.

## Related Documentation

- [CMCI](../data/cmci.md)
- [Source Profiles](../data/source-profiles.md)
- [Data Ingestion](../data/ingestion.md)
- [Data Validation](../data/validation.md)
- [Governance Decisions](../governance/decisions.md)