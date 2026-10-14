# 0009: CMCI Great Expectations data quality framework

- **Status:** Accepted
- **Date:** 2026-10-12

## Context

The CMCI pipeline performs defensive validation during ingestion and transformation. Project AHON also requires independent post-write validation of persisted Bronze, Silver, Gold, and Platinum data.

The source contains legitimate missing values and historical scoring patterns that differ from recent years. Quality rules must distinguish source limitations from pipeline defects.

## Decision

Great Expectations is used as the post-write CMCI data-quality and release-gate framework.

```text
Spark quality metrics
→ Great Expectations evaluation
→ Delta quality results
→ release gate
```

The shared runner supports:

- `--layer bronze`
- `--layer silver`
- `--layer gold`
- `--layer platinum`

Spark performs table-level computations. Only aggregate metrics are passed to pandas and Great Expectations.

Results are stored in:

- `ahon.quality.dq_results`
- `ahon.quality.dq_run_log`

Blocking failures stop an enforced quality task after results are saved. Advisory checks report accepted source characteristics without blocking release.

## Scope

Quality checks validate Project AHON's selected 35-indicator scope:

| Pillar | Selected indicators |
| --- | ---: |
| Economic Dynamism | 4 |
| Government Efficiency | 10 |
| Infrastructure | 10 |
| Resiliency | 10 |
| Innovation | 1 |
| **Total** | **35** |

Completeness refers to this selected scope.

## Why

- Defensive transformation checks still prevent unsafe writes.
- Great Expectations independently verifies persisted outputs.
- Large-table processing remains in Spark.
- One runner provides consistent PASS, WARN, and FAIL behavior.
- Persisted results create an audit trail, including for failed gates.
- Blocking and advisory rules separate defects from legitimate missing source values.

## Layer checks

### Bronze

Bronze performs 14 checks covering required values, uniqueness, request-array consistency, expected counts, approved PSGC scope, and PSGC duplication across batches.

### Silver

Silver performs 70 checks across the five pillar tables, including required fields, lineage, one row per PSGC-year, approved mapping, expected years, key coverage, and Bronze response-hash consistency.

### Gold

Gold performs 21 checks across `fact_cmci_indicator` and `dim_cmci_indicator`, including grain uniqueness, approved PSGC values, year and indicator domains, expected row counts, non-negative scores, recent score rules, and missing-score monitoring.

### Platinum

Platinum performs 17 checks on `cmci_pillar_score`, including grain uniqueness, pillar scope, selected indicator counts, coverage calculations, status consistency, expected rows, and non-negative raw pillar sums.

`INSUFFICIENT_DATA` rows are valid when coverage metadata, status, and the `NULL` pillar score are consistent.

## Score validity rules

### All years

Non-null `raw_score` values must not be negative. Missing values are allowed and monitored as an advisory warning.

### 2022–2024

All non-null selected indicator scores must be between `0` and `2.0001`. The `0.0001` allowance reflects observed source precision.

Three indicators have stable discrete domains:

- `bre`: `0`, `1`, `2`
- `bpls`: `0`, `0.5`, `1`, `1.5`, `2`
- `lra`: `0`, `1`, `2`

The other 32 selected indicators are continuous calculated scores and are not restricted to fixed value lists.

### Historical years

The 2022–2024 ceiling is not applied before 2022. Historical valid scores include values around `2.5`, `3.3333`, and `4.1667`.

### Missing values

Blank and `-` source values remain `NULL`. They are not converted to zero, averaged, forward-filled, backfilled, or otherwise imputed.

### Pillar scores

No universal maximum is enforced on `pillar_score_raw`. It is an unnormalized sum of selected indicators, not the official CMCI 0–20 pillar score.

The framework does not enforce these global rules:

- `raw_score <= 2` across all years
- `raw_score IN (0, 1, 2)` across all indicators
- `raw_score IN (0, 2.5)` across all indicators
- `pillar_score_raw <= 20`

## Alternatives considered

- **Rely only on transformation validation:** not chosen because persisted outputs would lack independent assessment and centralized history.
- **Move all checks into Great Expectations:** not chosen because transformations still require defensive validation before writing.
- **Run full datasets through pandas:** not chosen because Spark is the appropriate execution layer.
- **Use separate runners per layer:** not chosen because a shared runner standardizes logging and gate behavior.
- **Treat all NULL scores as failures:** not chosen because unavailable values are a legitimate source characteristic.
- **Use one score ceiling for 2014–2024:** not chosen because observed scoring regimes differ.

## Consequences

- `tests/sql/monitoring/cmci_quality.py` validates all four layers.
- `great-expectations==1.23.1` is required by the job environment.
- The job runs Bronze, Silver, Gold, and Platinum checks in dependency order.
- Results are stored in the shared `ahon.quality` schema.
- Recent score rules apply only to 2022–2024.
- Platinum completeness is evaluated against the selected 35-indicator scope.

## Validated baseline

| Layer | Checks | Result | Blocking failures |
| --- | ---: | --- | ---: |
| Bronze | 14 | PASS | 0 |
| Silver | 70 | PASS | 0 |
| Gold | 21 | WARN | 0 |
| Platinum | 17 | PASS | 0 |

The expected Gold warning is caused by 132,797 of 629,090 fact rows having `raw_score = NULL`. All blocking Gold checks passed.
