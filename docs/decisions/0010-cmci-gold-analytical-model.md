# 0010: CMCI Gold analytical model

- **Status:** Superseded
- **Date:** 2026-10-12

## Context

CMCI Silver data is stored across five pillar-specific tables:

- Economic Dynamism
- Government Efficiency
- Infrastructure
- Resiliency
- Innovation

Project AHON needs an analytical structure that supports comparisons across LGUs, years, pillars, and individual indicators while preserving missing source values and a stable LGU key.

Project AHON uses a deliberate subset of 35 CMCI indicators:

| Pillar | Selected indicators |
| --- | ---: |
| Economic Dynamism | 4 |
| Government Efficiency | 10 |
| Infrastructure | 10 |
| Resiliency | 10 |
| Innovation | 1 |
| **Total** | **35** |

This selected scope is not intended to represent every indicator in the complete CMCI framework.

## Decision

CMCI Gold uses:

- `ahon.gold.fact_cmci_indicator`
- `ahon.gold.dim_cmci_indicator`

### Fact table

`ahon.gold.fact_cmci_indicator` has one row per PSGC LGU, reporting year, and selected CMCI indicator.

Columns:

- `psgc_code`
- `year`
- `indicator_code`
- `raw_score`

The five Silver pillar tables are consolidated into this single long-form fact table.

### Indicator dimension

`ahon.gold.dim_cmci_indicator` contains one row for each of the 35 selected indicators.

Columns:

- `indicator_code`
- `indicator_name`
- `pillar_name`

### Missing values

Blank or unavailable source values are stored as `NULL`, not zero. `NULL` means that no usable score was provided, while zero is a valid measured score.

### LGU attributes

LGU names and geographic descriptions are not stored in the fact. `psgc_code` remains the authoritative LGU key. Human-readable LGU attributes will come from the shared LGU dimension or a later analytical view.

## Why

- A single fact provides a consistent grain across all 35 selected indicators.
- The long format supports analysis by indicator and pillar.
- `psgc_code` connects CMCI to shared geographic dimensions without duplicating LGU attributes.
- A single indicator dimension avoids unnecessary pillar-specific dimensions.
- Missing source scores remain distinguishable from valid zero scores.
- Gold preserves the lowest useful analytical grain instead of embedding an unconfirmed preparedness methodology.

## Alternatives considered

- **Keep the five Silver tables as Gold:** not chosen because analysis would repeatedly combine separate tables for one business process.
- **Create one fact per pillar:** not chosen because all selected indicators share the LGU-year-indicator grain.
- **Create one indicator dimension per pillar:** not chosen because pillar is an attribute of an indicator.
- **Store descriptive names in the fact:** not chosen because those values belong in dimensions and would be duplicated.
- **Replace missing values with zero:** not chosen because unavailable data and measured zero have different meanings.
- **Aggregate directly in Gold:** not chosen because Gold should preserve detailed analytical records.

## Consequences

- `gold_cmci_setup.sql` creates the Gold fact and indicator dimension.
- `fact_cmci.py` consolidates the five Silver tables.
- `dim_cmci_indicator.py` creates the 35-row selected-indicator dimension.
- The complete fact contains 629,090 rows: 1,634 LGUs × 11 years × 35 indicators.
- The dimension contains 35 rows across five pillars.
- Bronze and Silver remain unchanged.
- Human-readable LGU output requires the shared LGU dimension when available.

## Platinum consumption

Gold feeds `ahon.platinum.cmci_pillar_score`, which has one row per PSGC LGU, reporting year, and selected pillar.

`pillar_score_raw` is an interim, unnormalized sum of selected indicator scores. It is not the official CMCI 0–20 pillar score or the final Project AHON preparedness score.

A Platinum row is `COMPLETE` only when every Project AHON-selected indicator for the pillar is available. Otherwise, it is `INSUFFICIENT_DATA`, and `pillar_score_raw` remains `NULL`.
