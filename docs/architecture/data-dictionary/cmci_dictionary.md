# CMCI Executable Contracts

The supported CMCI implementation is:

```text
src/sql/datasets/cmci/pipeline.py
```

The task sequence is defined in:

```text
resources/jobs/ahon_job.yml
```

The active sequence is:

```text
cmci_extract
→ cmci_bronze
→ cmci_silver
→ cmci_gold
→ cmci_platinum
```

CMCI depends on the PSGC reference sequence:

```text
psgc_extract
→ psgc_bronze
→ psgc_silver
→ dim_lgu
```

Historical wide Silver tables and `cmci_pillar_score` are not part of the supported executable path. Existing incompatible schemas require a reviewed migration or a fresh catalog.

## Source Scope

Project AHON currently uses the 35 selected indicators defined in:

```text
resources/cmci_indicators.csv
```

These indicators cover:

| Pillar | Indicator count |
|---|---:|
| Economic Dynamism | 4 |
| Government Efficiency | 10 |
| Infrastructure | 10 |
| Resiliency | 10 |
| Innovation | 1 |
| **Total** | **35** |

This is an AHON-selected analytical scope, not the complete official CMCI indicator universe.

## Q2 2024 Mapping Scope

The reviewed CMCI mapping is aligned with the Q2 2024 PSGC snapshot.

```text
Q2 2024 PSGC cities and municipalities: 1,642
Live CMCI portal locality values: 1,634
Reviewed CMCI mappings: 1,634
Unchanged mappings: 1,615
Reviewed Sulu PSGC code replacements: 19
Unresolved mappings: 0
PSGC LGUs without CMCI portal records: 8
```

The 19 replacements reconcile prior Sulu codes beginning with `09066` to the Q2 2024 codes beginning with `19066`.

Reconciliation requires both:

1. The expected PSGC code transformation.
2. Exact locality-name agreement.

Name-only matching is not sufficient because locality names such as `Parang` are not nationally unique.

The reviewed mapping asset follows this path:

```text
/Volumes/<catalog>/reference/<source_volume>/cmci_reviewed_mapping_Q2_2024.csv
```

Its required columns are:

```text
psgc_code
cmci_name
source_period
reviewed
reviewer
evidence
```

The mapping must contain unique ten-digit PSGC codes and unique CMCI source names. Every PSGC code must resolve to a city or municipality in `gold.dim_lgu` for the selected `source_period`.

## Known Source Gaps

The following Q2 2024 PSGC municipalities have no exact or partial locality-name match in the live CMCI portal:

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

Their CMCI disposition is:

```text
NO_CMCI_SOURCE_RECORD
```

These LGUs remain valid PSGC records in `gold.dim_lgu`. They are not assigned to former parent LGUs, given synthetic values, converted to zero, or removed from the PSGC reference.

## Layer Outputs

The active pipeline produces:

```text
bronze.cmci_raw_indicator_batch_html

reference.cmci_lgu_map

silver.cmci_indicator_clean
silver.cmci_reject

gold.dim_cmci_indicator
gold.fact_cmci_indicator

platinum.cmci_pillar_coverage
```

Detailed Bronze and Silver contracts are documented separately in:

```text
cmci_bronze.md
cmci_silver.md
```

## Gold Contract

`gold.dim_cmci_indicator` contains:

```text
indicator_code
indicator_name
pillar_name
metric_version
```

`gold.fact_cmci_indicator` has the logical key:

```text
psgc_code + year + indicator_code
```

Gold preserves the source value and observation lineage. Missing or invalid values remain `NULL`; absence is never converted to zero.

Changes to the indicator definition or `metric_version` require review. The pipeline does not silently reinterpret existing facts under a changed definition.

## Platinum Contract

The current Platinum table is:

```text
platinum.cmci_pillar_coverage
```

Its logical key is:

```text
psgc_code + year + pillar_name
```

The table reports:

```text
expected_indicator_count
available_indicator_count
coverage_status
metric_version
```

Coverage statuses are:

```text
COMPLETE
INSUFFICIENT_DATA
```

`COMPLETE` means all selected indicators for the pillar are available. The output does not calculate:

- An official CMCI pillar score
- A composite competitiveness score
- An AHON preparedness score

## Missing-Value Contract

The following CMCI source values are treated as missing:

```text
blank
-
```

Missing values remain `NULL`.

Nonnumeric, negative, `NaN`, or infinite nonblank values are retained in:

```text
silver.cmci_reject
```

Rejected source observations remain auditable and do not become zero.

## Quality and Lineage

CMCI uses the shared runtime and quality modules:

```text
src/sql/common/runtime.py
src/sql/common/quality.py
```

The pipeline validates:

- Completed landing-batch integrity
- Source-response file hashes
- Observation accounting
- Mapping uniqueness
- Mapping alignment with the selected PSGC snapshot
- Silver and Gold logical-key uniqueness
- Invalid numeric values
- Indicator-definition consistency
- Source-period alignment
- Platinum logical-key uniqueness

Blocking failures prevent dependent writes. Quality results are persisted in:

```text
monitoring.dq_result
monitoring.run_log
```

## Limitations

- Eight Q2 2024 PSGC municipalities have no separate locality record in the live CMCI portal.
- Historical availability varies by LGU, year, indicator, and pillar.
- CMCI describes competitiveness and institutional capacity, not hazard occurrence or exposure.
- The source is a public HTML portal rather than a documented high-volume API.
- The selected 35 indicators are an AHON analytical scope.
- `cmci_pillar_coverage` reports completeness only and is not a preparedness score.