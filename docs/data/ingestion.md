## CMCI Ingestion

### Purpose

The CMCI ingestion workflow retrieves historical indicator data for approved LGUs and preserves the raw source responses in Bronze.

Detailed source profiling, mapping coverage, and analytical use are documented in [CMCI Dataset](cmci.md).

## Source Endpoints

| Endpoint | Purpose |
|---|---|
| `https://cmci.dti.gov.ph/data-portal.php` | Initializes the HTTP session and checks portal availability |
| `https://cmci.dti.gov.ph/data-portal-process.php` | Processes the requested LGUs, years, and indicators |

CMCI returns dynamically generated HTML containing the selected indicator results.

## Eligible LGUs

The ingestion reads reviewed mappings from:

```text
ahon.reference.cmci_lgu_map
```

Only records meeting all three conditions are eligible:

```text
match_status = MATCHED
reviewed = true
is_active = true
```

Current ingestion scope:

```text
Approved LGUs: 1,262
Unmatched PSGC LGUs excluded: 380
```

The 380 unmatched LGUs remain in the reference and mapping layers. They are excluded from ingestion because no approved CMCI source identity is available.

## Request Scope

The production ingestion requests:

```text
Approved LGUs: 1,262
Years: 2014 to 2024
Indicators: 35
```

The 35 indicators are distributed across five pillars:

| Pillar | Indicator count |
|---|---:|
| Economic Dynamism | 4 |
| Government Efficiency | 10 |
| Infrastructure | 10 |
| Resiliency | 10 |
| Innovation | 1 |
| **Total** | **35** |

The indicator labels and internal CMCI codes are maintained in:

```text
src/01_bronze_ingest/cmci_common.py
```

## Batch Strategy

The production request design is:

```text
Up to 10 LGUs
11 years
35 indicators
1 HTTP request
```

A complete batch represents:

```text
10 LGUs × 11 years × 35 indicators
= 3,850 values
```

The final production batch contains two LGUs:

```text
2 LGUs × 11 years × 35 indicators
= 770 values
```

The complete production scope requires:

```text
127 batch requests
```

Without batching, the same historical scope would require:

```text
1,262 LGUs × 11 years
= 13,882 separate LGU-year requests
```

## Response Structure

Each batch response contains 35 HTML tables:

```text
One table per indicator
```

Each indicator table contains:

- One `Province / LGU` column
- One column per requested year
- One row per requested LGU

The logical value grain within the response is:

```text
CMCI indicator + LGU + year
```

## Bronze Target

Validated responses are stored in:

```text
ahon.bronze.cmci_raw_indicator_batch_html
```

Bronze grain:

```text
One row per CMCI request batch
```

Each Bronze row stores:

- Deterministic batch ID
- Requested PSGC codes
- Requested CMCI locality names
- Requested years
- Requested indicator codes
- Expected LGU, year, and indicator counts
- Returned value count
- Complete raw HTML response
- Stable response hash
- UTC ingestion timestamp

## Batch Identity

The deterministic `batch_id` is generated from:

```text
Requested PSGC codes
+ requested years
+ requested indicator codes
```

The same logical request produces the same batch identifier.

This allows completed batches to be recognized and skipped during reruns.

## Response Hash

The response hash is calculated from the parsed CMCI results using:

```text
Indicator label
+ CMCI locality name
+ year
+ returned value
```

The hash is not calculated from the complete HTML document.

This prevents harmless source-page changes from creating false data versions, including changes to:

- Whitespace
- Formatting
- Page scripts
- Metadata
- Session-specific content

## Pre-Write Validation

Each batch must pass validation before it is stored.

The ingestion verifies:

1. The response is a valid CMCI result page.
2. Exactly 35 indicator tables are returned.
3. Every expected indicator appears exactly once.
4. Returned LGUs match the requested CMCI names.
5. Returned year columns match the requested years.
6. Each table contains the expected number of rows.
7. Each row contains the expected number of values.
8. Values are numeric, blank, or CMCI's unavailable marker.
9. The parsed value count matches the expected batch scope.
10. The PSGC and CMCI identities agree with the approved mapping.

A failed batch is not written to Bronze.

Detailed validation rules belong in [Data Validation](validation.md).

## Missing Values

CMCI may return a blank value or dash when an indicator is unavailable.

Bronze preserves the original source response.

Silver converts unavailable values to:

```text
NULL
```

An unavailable value must not automatically be interpreted as zero.

## Throttling and Retries

CMCI is a public web portal rather than a documented high-volume API.

Requests are submitted sequentially with:

- A configurable request delay
- Connection retries
- Read retries
- Retry backoff
- Handling for selected temporary HTTP errors

Retryable status codes include:

```text
429
500
502
503
504
```

These controls reduce the risk of temporary blocking and incomplete requests.

## Restartability

Successful batches are written immediately.

When the same deterministic batch already exists:

```text
Saved batch ID
→ batch is skipped
```

If a later batch fails, previously completed batches remain available. Rerunning the ingestion continues from incomplete batches instead of restarting the full historical load.

Validated rerun result:

```text
Existing or unchanged batches: 1
New or changed batches written: 0
Failed batches: 0
```

## Run Modes

The ingestion supports:

| Mode | Behavior |
|---|---|
| `test` | Limits the number of approved LGUs |
| `full` | Processes all approved and active mappings |

The safe default is:

```text
test
```

Full mode must be selected explicitly by the production job.

## Implementation Files

```text
src/01_bronze_ingest/
├── cmci_source_profile.py
├── cmci_common.py
└── cmci_batch_ingest.py
```

- `cmci_source_profile.py`: Read-only profiling of the live CMCI source, reference mappings, Bronze coverage, and Silver coverage.
- `cmci_common.py`: Shared CMCI endpoints, indicator configuration, and HTTP session logic.
- `cmci_batch_ingest.py`: Restartable batch ingestion into `ahon.bronze.cmci_raw_indicator_batch_html`.

## Verified Production Result

The full historical ingestion completed successfully:

| Measure | Result |
|---|---:|
| Approved LGUs | 1,262 |
| Years | 11 |
| Year range | 2014 to 2024 |
| Indicators | 35 |
| Production batches | 127 |
| Failed production batches | 0 |
| Validated values | 485,870 |

Calculation:

```text
1,262 LGUs × 11 years × 35 indicators
= 485,870 values
```

## Downstream Processing

Silver reads the raw Bronze batches and expands them into one LGU-year record per pillar.

```text
ahon.bronze.cmci_raw_indicator_batch_html
        ↓
ahon.silver.cmci_economic_dynamism
ahon.silver.cmci_government_efficiency
ahon.silver.cmci_infrastructure
ahon.silver.cmci_resiliency
ahon.silver.cmci_innovation
```

The expected Silver scope is:

```text
1,262 approved LGUs × 11 years
= 13,882 rows per pillar table
```

## Related Documentation

- [CMCI Dataset](cmci.md)
- [Source Profiles](source-profiles.md)
- [Data Validation](validation.md)
- [Governance Decisions](../governance/decisions.md)
- [Operations Runbook](../operations/runbook.md)