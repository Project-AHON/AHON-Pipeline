# 0012: One PSGC snapshot, with PSGC codes attached in silver

- **Status:** Accepted
- **Date:** 2026-10-12

## Context

Every dataset is joined on `psgc_code`, but PSGC codes change between snapshots: between Q4_2023 and Q2_2024 alone, the Negros Island Region renumbered 3 provinces, 19 cities and 44 municipalities. The first plans attached codes in gold, and CMCI work used a different PSGC list (2Q 2026) than the PSGC API tables. Without one snapshot and one place to attach codes, the same town could end up with two different codes in two facts.

## Decision

- All datasets use one PSGC snapshot: **`Q2_2024`** from the PSA PSGC API, set by the bundle variable `source_period`.
- `silver.psa_psgc_clean` is built first and keeps every snapshot the API returns. `gold.dim_lgu` holds only the selected snapshot.
- PSGC codes are attached in **silver**, not gold. Each dataset's silver table carries `psgc_code` and how it was matched (for example `psgc_match_status`, `psgc_match_method`), using `silver.psa_psgc_clean` for the selected snapshot.
- Every fact carries the `source_period` it was matched with, and it must equal `dim_lgu.source_period`.
- Data keyed on another snapshot's codes is matched to `Q2_2024` before it is used, never joined directly.

## Why

- One snapshot means one code per town in every table, so facts can be joined safely.
- Attaching codes in silver means every consumer reads the same match instead of repeating it in each gold table.
- `Q2_2024` is the latest snapshot available from the API when the pipeline was built, and matches the 2024 census year used for population.

## Alternatives considered

- **Match in gold:** works, but silver would hold no codes and each gold table would repeat the join.
- **Let each dataset pick its snapshot:** rejected. Codes would not line up across facts.
- **Use the 2Q 2026 PSGC publication workbook:** rejected for now. It is a different source from the API tables the pipeline builds, and it would not match the 2024 census.

## Consequences

- Build order: PSGC silver, then each dataset's silver, then gold.
- Changing the snapshot means rerunning every matcher and rebuilding silver and gold, or using a fresh catalog.
- Areas that no longer exist in `Q2_2024` (for example the old Maguindanao province) stay unmatched, with `psgc_code` NULL, and are documented.
- LDRRMF matching is in [0013](0013-ldrrmf-psgc-matching-in-silver.md); earthquake locality candidates are in [0014](0014-earthquake-observations-and-locality-candidates.md).

## Open

- `Q2_2021` codes in bronze use PSA's older format (8 or 9 digits). Silver pads them to 10 digits, which gives wrong region codes for that snapshot. `Q2_2021` is not used downstream; fix or drop it before any cross-snapshot use.
- The broken ñ (`Ã±`) in `silver.psa_psgc_clean` names; matchers repair it for now.
