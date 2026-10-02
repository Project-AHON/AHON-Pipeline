## CMCI Pipeline Decisions

### Use PSGC as the Canonical LGU Identifier

**Decision:** Use `psgc_code` as the stable identifier for CMCI records and downstream joins.

**Reason:**

- PSGC and CMCI use different locality names.
- Multiple LGUs may share the same name.
- Locality names may change over time.
- PSGC codes provide a stable join key across Project AHON datasets.

The pipeline keeps these identities separate:

| Field | Purpose |
|---|---|
| `psgc_code` | Canonical LGU identifier |
| `psgc_name` | Official PSGC locality name |
| `cmci_name` | Exact locality value expected by CMCI |

### Require Reviewed CMCI Mappings

**Decision:** Prepare and review PSGC-to-CMCI mappings before ingestion.

The ingestion source is:

```text
ahon.reference.cmci_lgu_map
```

A mapping is eligible only when:

```text
match_status = MATCHED
reviewed = true
is_active = true
```

**Reason:**

A valid CMCI response could still belong to the wrong LGU if matching relies only on locality names. Mapping review must therefore remain separate from source ingestion.

The ingestion script does not perform fuzzy matching.

### Exclude Uncertain Mappings

**Decision:** Exclude unmatched or unapproved LGUs instead of guessing their CMCI identities.

Current coverage:

```text
PSGC LGUs profiled: 1,642
Approved CMCI mappings: 1,262
Unmatched PSGC LGUs: 380
```

**Reason:**

Incorrectly assigning CMCI values to an LGU would be more harmful than temporarily excluding that LGU.

The 380 unmatched LGUs:

- Remain in `ahon.reference.lgu_master`
- Remain represented in the mapping workflow
- Are excluded from CMCI Bronze and Silver processing
- Require separate reconciliation and review

They are a known mapping-coverage gap, not failed ingestion records.

### Use Separate Quality Denominators

**Decision:** Measure reference completeness and CMCI pipeline completeness separately.

Reference denominator:

```text
1,642 PSGC LGUs
```

CMCI pipeline denominator:

```text
1,262 approved CMCI mappings
```

**Reason:**

The CMCI pipeline should not fail because it excludes LGUs that were never approved for CMCI ingestion.

Use the reference denominator for:

- PSGC completeness
- Mapping coverage
- Unmatched-LGU reporting

Use the CMCI pipeline denominator for:

- Bronze ingestion coverage
- Silver coverage
- Yearly row counts
- Pipeline pass-or-fail decisions

### Do Not Quarantine Known Unmatched LGUs

**Decision:** Do not place the 380 known unmatched PSGC LGUs in a quarantine table solely because no approved CMCI mapping exists.

**Reason:**

These LGUs are valid PSGC reference records. They are not malformed CMCI records because they never enter CMCI ingestion.

Future quarantine handling should be reserved for actual rule violations, such as:

- A returned CMCI locality that was not requested
- A PSGC code that conflicts with the approved mapping
- Duplicate active CMCI mappings
- Invalid response structures
- Unexpected nonnumeric values
- Duplicate Silver keys

Mapping gaps should be monitored and reviewed separately.

### Retain All 35 Selected Indicators

**Decision:** Ingest and retain all 35 configured CMCI indicators.

| Pillar | Indicator count |
|---|---:|
| Economic Dynamism | 4 |
| Government Efficiency | 10 |
| Infrastructure | 10 |
| Resiliency | 10 |
| Innovation | 1 |
| **Total** | **35** |

**Reason:**

- CMCI accepts multiple indicators in one request.
- Retaining 35 indicators adds little processing overhead after batching.
- Additional indicators may support future analysis.
- Bronze and Silver should preserve reusable source data.
- Gold models can select a smaller business-relevant subset later.

Keeping an indicator in Silver does not mean that the indicator must be included in a composite risk or priority score.

### Batch LGUs and Years

**Decision:** Request multiple LGUs and all historical years in each CMCI request.

Production batch shape:

```text
Up to 10 LGUs
11 years
35 indicators
```

Historical coverage:

```text
2014 to 2024
```

**Reason:**

The unbatched design would require:

```text
1,262 LGUs × 11 years
= 13,882 requests
```

The batch design requires:

```text
127 production requests
```

A batch size of 10 balances:

- Request reduction
- Response size
- Validation complexity
- Timeout risk
- Retry cost
- Failure isolation

Batching improves ingestion performance more than reducing the indicator count.

### Store One Bronze Row per Request Batch

**Decision:** Store one raw Bronze record for each CMCI request batch.

Bronze table:

```text
ahon.bronze.cmci_raw_indicator_batch_html
```

Bronze grain:

```text
One row per CMCI request batch
```

**Reason:**

- Bronze should preserve the source response.
- The same HTML should not be duplicated by LGU, year, pillar, or indicator.
- Raw responses can be reprocessed when Silver logic changes.
- Batch metadata supports auditing and restartability.
- Ingestion remains separate from analytical transformation.

### Preserve Raw HTML in Bronze

**Decision:** Store the complete CMCI result page rather than only extracted values.

**Reason:**

Raw HTML supports:

- Source traceability
- Auditing
- Parser improvements
- Recovery from transformation errors
- Rebuilding Silver without requesting CMCI again
- Investigation of source-format changes

Typed indicator values belong in Silver.

### Use Deterministic Batch IDs

**Decision:** Generate `batch_id` from the requested PSGC codes, years, and indicator codes.

Conceptual batch identity:

```text
Requested PSGC codes
+ requested years
+ requested indicator codes
```

**Reason:**

- The same logical request produces the same identifier.
- Completed batches can be recognized during reruns.
- Failed runs can resume without repeating successful batches.
- Duplicate raw requests are avoided.

### Hash Parsed Results Instead of Full HTML

**Decision:** Calculate the response hash from validated CMCI results.

The canonical hash content uses:

```text
Indicator label
+ CMCI locality name
+ year
+ returned value
```

**Reason:**

Full HTML may change because of:

- Whitespace
- Formatting
- Scripts
- Metadata
- Session-specific content

Hashing parsed results prevents harmless page changes from creating false data versions.

### Persist Successful Batches Immediately

**Decision:** Write each successful batch to Bronze before starting the next request.

**Reason:**

- A failure near the end does not invalidate previous work.
- Completed batches remain available after an interruption.
- Reruns skip previously saved deterministic batch IDs.
- Historical ingestion can resume safely.

The workflow is restartable rather than all-or-nothing.

### Keep Test Mode as the Safe Default

**Decision:** Keep `test` as the default ingestion mode.

| Mode | Behavior |
|---|---|
| `test` | Limits the number of approved LGUs |
| `full` | Processes all approved and active mappings |

**Reason:**

- Prevents accidental nationwide ingestion
- Supports controlled validation
- Reduces risk during source development
- Requires deliberate selection of production behavior

Full mode should be provided explicitly by the production job.

### Use One Silver Table per Pillar

**Decision:** Separate CMCI Silver data into five pillar-specific tables.

```text
ahon.silver.cmci_economic_dynamism
ahon.silver.cmci_government_efficiency
ahon.silver.cmci_infrastructure
ahon.silver.cmci_resiliency
ahon.silver.cmci_innovation
```

**Reason:**

- Keeps related indicators together
- Makes pillar-level analysis easier
- Avoids one excessively wide table
- Supports independent use of each pillar
- Preserves understandable business groupings

Each table stores its indicators as columns.

### Use One Silver Row per LGU-Year

**Decision:** Use the following Silver grain:

```text
One row per approved PSGC LGU and year
```

Logical key:

```text
psgc_code + year
```

**Reason:**

- Supports historical comparison
- Provides a consistent grain across all pillar tables
- Simplifies joins to hazard, population, and reference data
- Prevents duplicate records for the same LGU and year

Expected scope per pillar:

```text
1,262 approved LGUs × 11 years
= 13,882 rows
```

### Convert Unavailable Values to NULL

**Decision:** Convert CMCI blank and dash values to `NULL` in Silver.

**Reason:**

- An unavailable value is not necessarily zero.
- Preserving the distinction prevents incorrect scoring and aggregation.
- Analysts can explicitly decide how missing data should be handled.

A valid zero remains distinct from unavailable data.

### Deduplicate Overlapping Bronze Batches in Silver

**Decision:** Retain the latest source record for each:

```text
psgc_code + year
```

**Reason:**

Bronze may contain overlapping:

- Test batches
- Production batches
- Refreshed batches

Bronze preserves ingestion history. Silver presents one current analytical record per approved LGU-year.

### Use Delta MERGE for Silver

**Decision:** Load Silver tables using Delta `MERGE`.

Merge key:

```text
psgc_code + year
```

Merge behavior:

- Insert missing LGU-year records
- Update records when the Bronze response hash changes
- Leave unchanged records untouched

**Reason:**

- Supports repeatable processing
- Prevents duplicate LGU-year rows
- Allows updated Bronze values to refresh Silver
- Makes Silver processing idempotent

### Keep CMCI Separate from Hazard Exposure

**Decision:** Use CMCI as capacity and vulnerability context, not as a hazard measure.

**Reason:**

CMCI does not describe:

- Hazard location
- Hazard frequency
- Hazard intensity
- Hazard magnitude
- Geographic exposure

Hazard datasets remain responsible for hazard characterization and exposure analysis.

CMCI supports interpretation of:

- Preparedness
- Institutional capacity
- Infrastructure
- Essential services
- Economic conditions
- Recovery capability

### Select Gold Indicators Separately

**Decision:** Retain all 35 indicators in Silver, but document and validate the Gold-layer subset independently.

**Reason:**

- Data availability and model selection are separate concerns.
- Not every available indicator should affect a risk score.
- Gold scoring requires business justification.
- Normalization, weighting, directionality, and missing-data rules must be documented.
- Future analyses may require indicators excluded from the first scoring model.

No CMCI indicator should enter a composite score solely because the indicator is available.

### Implement Great Expectations Later

**Decision:** Add Great Expectations after the Bronze and Silver job workflow is stable.

**Reason:**

- Existing Python checks already validate the current historical load.
- GX expectations should reflect finalized task boundaries.
- Quality gates must distinguish critical failures from coverage warnings.
- The 380 unmatched LGUs require monitoring, not automatic pipeline failure.

Future GX rules are documented in:

```text
docs/data/validation.md
```

## Verified Outcome

The implemented decisions produced:

```text
PSGC LGUs profiled: 1,642
Approved CMCI mappings: 1,262
Unmatched PSGC LGUs: 380
Years ingested: 11
Year range: 2014 to 2024
Indicators retained: 35
Production Bronze batches: 127
Failed production batches: 0
Validated Bronze values: 485,870
Silver pillar tables: 5
Rows per Silver table: 13,882
Rows per year: 1,262
Duplicate Silver LGU-year keys: 0
```

## Related Documentation

- [CMCI](../data/cmci.md)
- [Source Profiles](../data/source-profiles.md)
- [Data Ingestion](../data/ingestion.md)
- [Data Validation](../data/validation.md)
- [Operations Runbook](../operations/runbook.md)
