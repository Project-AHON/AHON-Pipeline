## CMCI Validation

### Validation Scope

CMCI validation separates three concerns:

1. PSGC reference coverage
2. Approved CMCI ingestion coverage
3. Bronze and Silver data validity

These concerns use different expected populations.

```text
PSGC reference population: 1,642 LGUs
Approved CMCI population:  1,262 LGUs
Unmatched PSGC population:   380 LGUs
```

The 380 unmatched LGUs were included in source profiling but intentionally excluded from CMCI ingestion because no approved CMCI identity is available.

They are a known mapping-coverage gap, not failed CMCI records.

## Expected Production Coverage

| Measure | Expected result |
|---|---:|
| PSGC LGUs profiled | 1,642 |
| Approved CMCI mappings | 1,262 |
| Unmatched PSGC LGUs | 380 |
| Years ingested | 11 |
| Year range | 2014 to 2024 |
| Indicators | 35 |
| Production Bronze batches | 127 |
| Bronze values represented | 485,870 |
| Silver pillar tables | 5 |
| Rows per Silver table | 13,882 |
| Rows per year per Silver table | 1,262 |

Expected Bronze values:

```text
1,262 approved LGUs × 11 years × 35 indicators
= 485,870 values
```

Expected Silver rows per pillar:

```text
1,262 approved LGUs × 11 years
= 13,882 LGU-year rows
```

## Reference Mapping Validation

The CMCI pipeline uses:

```text
ahon.reference.cmci_lgu_map
```

An LGU is eligible for CMCI ingestion only when:

```text
match_status = MATCHED
reviewed = true
is_active = true
```

The mapping process validates that:

- `psgc_code` is populated.
- `psgc_name` is populated.
- `cmci_name` is populated.
- Each active PSGC code appears once.
- Each active CMCI name maps to only one PSGC code.
- Every eligible PSGC code exists in `ahon.reference.lgu_master`.
- Unmatched and unreviewed mappings remain inactive.

Expected approved mapping count:

```text
1,262
```

The unmatched count should be monitored separately:

```text
380
```

A change in either count requires review because expected Bronze and Silver coverage may also need to change.

## Bronze Validation

Bronze target:

```text
ahon.bronze.cmci_raw_indicator_batch_html
```

### Required Columns

The Bronze table must contain:

```text
batch_id
requested_psgc_codes
requested_cmci_names
requested_years
requested_indicator_codes
expected_lgu_count
expected_year_count
expected_indicator_count
returned_value_count
response_html
response_hash
ingestion_timestamp
```

### Request Validation

Each batch must satisfy:

- PSGC and CMCI name arrays have the same length.
- Every PSGC code belongs to the approved mapping set.
- Every CMCI name agrees with the approved mapping.
- Requested years are populated.
- Requested indicator codes are populated.
- Batch size is greater than zero.
- Expected indicator count is 35.

### Response Structure Validation

Each CMCI response must contain:

```text
35 indicator tables
```

Each indicator table must contain:

- One valid indicator heading
- One `Province / LGU` column
- One column per requested year
- One row per requested LGU

The parser verifies that:

1. The page is a valid CMCI result page.
2. Every expected indicator appears exactly once.
3. No unexpected indicator appears.
4. Returned LGUs match the request.
5. Returned years match the request.
6. Each table has the expected row count.
7. Each row has the expected column count.
8. Each LGU appears once per indicator table.
9. The total parsed value count matches the batch scope.

### Value Validation

Accepted CMCI values are:

- Numeric values
- Blank values
- A dash representing unavailable data

Unexpected nonnumeric values cause the batch to fail.

Bronze preserves the original response. Silver converts blank and dash values to:

```text
NULL
```

A valid zero remains:

```text
0
```

Zero and unavailable data must remain distinct.

### Batch Value Counts

A standard 10-LGU production batch must contain:

```text
10 LGUs × 11 years × 35 indicators
= 3,850 values
```

The final two-LGU production batch must contain:

```text
2 LGUs × 11 years × 35 indicators
= 770 values
```

### Batch Identity

Each batch receives a deterministic ID based on:

```text
Requested PSGC codes
+ requested years
+ requested indicator codes
```

The same logical request should produce the same `batch_id`.

### Response Hash

The stable response hash is based on:

```text
Indicator label
+ CMCI locality name
+ year
+ returned value
```

The complete HTML document is not used for change detection because formatting, scripts, metadata, and session content may change without changing the CMCI results.

## Bronze Production Result

The completed historical ingestion produced:

```text
Production batches: 127
Successful batches: 127
Failed production batches: 0
Validated values: 485,870
```

Two earlier test batches remain in Bronze for traceability.

These test batches overlap with production coverage and are resolved during Silver processing.

## Restartability Validation

Successful batches are written immediately.

When the same deterministic batch already exists:

```text
Saved batch ID
→ skip batch
```

The restartability test produced:

```text
Existing or unchanged batches: 1
New or changed batches written: 0
Failed batches: 0
```

This confirms that identical completed batches are not stored again.

## Silver Validation

Silver targets:

```text
ahon.silver.cmci_economic_dynamism
ahon.silver.cmci_government_efficiency
ahon.silver.cmci_infrastructure
ahon.silver.cmci_resiliency
ahon.silver.cmci_innovation
```

### Batch Expansion

Silver expands each Bronze response by:

```text
Indicator table
→ CMCI locality
→ year
→ indicator value
→ PSGC LGU-year record
```

The parser validates every CMCI locality against the approved mapping before creating a Silver record.

### Overlap Resolution

Bronze may contain overlapping test, production, or refreshed batches.

Silver retains the latest valid source record for:

```text
psgc_code + year
```

The latest record is selected using the Bronze ingestion timestamp.

### Expected Silver Grain

```text
One row per approved PSGC LGU and year
```

Logical key:

```text
psgc_code + year
```

### Expected Silver Coverage

```text
1,262 approved LGUs × 11 years
= 13,882 rows per pillar table
```

Validated results:

| Silver table | Validated rows |
|---|---:|
| `ahon.silver.cmci_economic_dynamism` | 13,882 |
| `ahon.silver.cmci_government_efficiency` | 13,882 |
| `ahon.silver.cmci_infrastructure` | 13,882 |
| `ahon.silver.cmci_resiliency` | 13,882 |
| `ahon.silver.cmci_innovation` | 13,882 |

### Required Silver Checks

Each Silver table must satisfy:

- Exactly 13,882 LGU-year rows
- Exactly 1,262 rows per year
- Complete coverage from 2014 through 2024
- No duplicate `psgc_code + year` keys
- No PSGC codes outside the approved mapping set
- Populated PSGC code
- Populated official LGU name
- Populated CMCI name
- Populated year
- Populated source response hash
- Populated source ingestion timestamp
- Populated Silver processing timestamp
- Source hash matches the selected Bronze response

### Delta Merge Behavior

Silver uses Delta `MERGE` with:

```text
psgc_code + year
```

The merge:

- Inserts missing LGU-year records
- Updates records when the Bronze response hash changes
- Leaves unchanged records untouched

## Complete PSGC Coverage

CMCI Silver contains only the 1,262 approved LGUs.

When an output needs all 1,642 PSGC LGUs, begin with:

```text
ahon.reference.lgu_master
```

Then left-join CMCI Silver using:

```text
psgc_code
```

The 380 unmatched LGUs will remain visible with `NULL` CMCI values.

This is expected behavior and should not fail CMCI pipeline validation.

## Future Great Expectations Checks

Great Expectations implementation is planned after the Bronze and Silver jobs are configured.

### Critical Expectations

Critical failures should stop the affected pipeline stage.

Planned critical checks include:

- Every ingested PSGC code exists in `lgu_master`.
- Every ingested record uses a reviewed, active, matched mapping.
- No active CMCI name maps to multiple PSGC codes.
- Bronze covers all 1,262 approved LGUs.
- Bronze responses contain all 35 indicator tables.
- Returned LGUs and years match the request.
- Silver contains exactly 13,882 unique LGU-year rows per pillar.
- Every year contains exactly 1,262 approved LGUs.
- Silver contains no duplicate `psgc_code + year` keys.
- Silver contains no PSGC codes outside the approved mapping set.
- Silver source hashes match the selected Bronze responses.

### Warning-Level Expectations

Warnings should be reported without automatically failing a healthy CMCI ingestion.

Planned warning checks include:

- Current unmatched PSGC LGU count
- Mapping coverage percentage
- Changes in approved or unmatched counts
- Newly available CMCI locality names
- Renamed or reorganized LGUs
- Missing indicator values by pillar and year
- Material changes in historical source coverage

### Quarantine Policy

Quarantine is appropriate for returned or transformed records that violate a defined quality rule.

Examples include:

- A returned CMCI locality that was not requested
- A PSGC code that conflicts with the approved mapping
- Duplicate active CMCI mappings
- Unexpected response structures
- Invalid nonnumeric values
- Duplicate Silver keys

The 380 known unmatched PSGC LGUs should not be quarantined solely because they are outside the approved CMCI ingestion scope.

They should remain in the reference and mapping layers for future review.

## Quality Denominators

Use the correct denominator for each quality question.

### Reference Coverage

```text
1,642 PSGC LGUs
``

Use this denominator for:

- PSGC source completeness
- Mapping coverage
- Unmatched LGU reporting
- Geographic reference coverage

### CMCI Pipeline Coverage

```text
1,262 approved CMCI mappings
```

Use this denominator for:

- Bronze ingestion completeness
- Silver coverage
- Yearly CMCI row counts
- Duplicate checks
- Pipeline pass-or-fail decisions

This distinction prevents the CMCI pipeline from failing because it does not contain LGUs that were never approved for CMCI ingestion.

## Current Validation Status

```text
PSGC LGUs profiled: 1,642
Approved CMCI mappings: 1,262
Unmatched PSGC LGUs: 380
Production Bronze batches: 127
Successful production batches: 127
Failed production batches: 0
Validated Bronze values: 485,870
Silver pillar tables: 5
Rows per Silver table: 13,882
Rows per year: 1,262
Duplicate Silver keys: 0
Source-hash validation: passed
Year coverage validation: passed
Great Expectations implementation: pending
```

## Related Documentation

- [CMCI Dataset](cmci.md)
- [Data Ingestion](ingestion.md)
- [Source Profiles](source-profiles.md)
- [Governance Decisions](../governance/decisions.md)