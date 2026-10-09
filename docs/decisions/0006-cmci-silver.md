# 0005: Store CMCI Silver data by pillar

- **Status:** Proposed
- **Date:** 2026-10-09

## Context

CMCI returns 35 indicators grouped into five pillars. Bronze preserves the original batch HTML, while analytics needs typed values with one stable LGU-year key.

## Decision

Parse the latest valid CMCI Bronze responses into five Silver tables:

- `ahon.silver.cmci_economic_dynamism`
- `ahon.silver.cmci_government_efficiency`
- `ahon.silver.cmci_infrastructure`
- `ahon.silver.cmci_resiliency`
- `ahon.silver.cmci_innovation`

Each table contains one row per approved `psgc_code` and `year`. Silver records are merged using `psgc_code + year` and updated when the source response hash changes.

## Why

- Preserves the five-pillar structure published by CMCI.
- Keeps each table focused and easier to use.
- Prevents duplicate LGU-year records.
- Supports repeatable updates when Bronze responses change.
- Preserves traceability through the CMCI name, response hash, and source timestamps.

## Alternatives considered

- **One wide Silver table containing all 35 indicators:** Not chosen because it combines separate CMCI pillars into a less focused table.
- **One long table with one row per indicator:** Not chosen because common pillar-level analysis would require repeated pivots.
- **Append-only Silver writes:** Not chosen because reruns could create duplicate LGU-year records.

## Consequences

- Five Silver tables must exist before running the parser.
- `cmci_indicator_parse.py` reads the latest valid version of every Bronze batch.
- Each Silver table contains 17,974 rows when coverage is complete.
- The expected scope is 1,634 approved LGUs across 11 years.
- The eight PSGC-only CMCI source gaps do not receive placeholder Silver rows.
- Changes to the 35 indicator labels or pillar assignments require updates to the parser, table setup, and data dictionary.
