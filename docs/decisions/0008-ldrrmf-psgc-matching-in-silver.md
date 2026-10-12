# 0008: Match LDRRMF to PSGC codes in silver through a reference map

- **Status:** Proposed
- **Date:** 2026-10-12

## Context

BLGF LDRRMF has no PSGC codes, only names (region, province, LGU name and LGU type), and those names do not always match PSGC: typos ("Pililia"), renames ("Bumbaran", now Amai Manabilang), a broken ñ in both sources, and a province that no longer exists (Maguindanao, split in 2022). The gold fact `fact_ldrrmf` must join `dim_lgu` on `psgc_code`, so every LDRRMF row needs a code, and the way it got that code must be reviewable. The silver decision for this dataset first kept PSGC matching out of silver ("basic cleaning only"). This record changes that.

## Decision

- PSGC codes are added to LDRRMF in **silver**. `silver.blgf_ldrrmf_annual_lgu_clean` gets `psgc_code`, `psgc_match_status` (`exact` / `unmatched`, the Schema v1 values) and `psgc_match_method`. The gold fact copies them.
- The matching runs once per distinct LGU, not per row, in a Python script that writes a reference table, `reference.blgf_ldrrmf_lgu_map` (one row per region + province + LGU name + LGU type as BLGF writes it). Silver joins this map with plain SQL.
- The PSGC side is `silver.psa_psgc_clean`, one snapshot (`Q2_2024`), the same snapshot `gold.dim_lgu` is built from.
- Only exact matches give a code: the same name inside the same province, the same name after normalising (case, accents, ‘City of’, abbreviations). Anything else stays `unmatched` with `psgc_code` NULL, and the row is kept.
- Person-checked fixes live in `blgf_ldrrmf_lgu_map_overrides.csv` in the repository, one row per LGU with a reason. The map table is rebuilt in full on every run and is never edited by hand.

## Why

- **Silver needs the PSGC table, not gold.** `psa_psgc_clean` is built before LDRRMF silver, so silver can match without depending on gold.
- **Every consumer gets the same code.** Gold, and any later table that reads silver, uses one match instead of repeating it.
- **Reviewable and repeatable.** The map shows every match and how it was made, and the overrides csv keeps the human decisions in version control. A rebuild always gives the same result.
- **No wrong guesses.** A wrong code silently attaches one town's budget to another; an unmatched row is visible and can be fixed with one csv row.

## Alternatives considered

- **Match in gold (the earlier plan):** works, but silver would hold no codes, and each gold table would have to repeat the join.
- **Match directly in the silver SQL:** name matching needs accent removal, broken-ñ repair, "City of" handling and province aliases, which is hard to read and test in SQL, and it would run on 11,434 rows instead of about 2,000 LGUs.
- **Fuzzy matching with a score threshold:** finds more matches automatically, but the doubtful ones would still need review. With overrides, every LGU matched (except the dissolved Maguindanao province), so it was not needed.
- **MERGE into the map instead of overwrite:** only needed if the table held decisions that cannot be rebuilt; here they live in the csv.

## Consequences

- Build order: `silver.psa_psgc_clean`, then `build_blgf_ldrrmf_lgu_map.py`, then the LDRRMF silver SQL, then `build_fact_ldrrmf.sql`. The silver build stops if an LGU is missing from the map (for example a new fiscal year with a new LGU): rerun the matcher, review the unmatched ones and add overrides.
- On `ahon_dev` (2026-10-12): 2,053 of 2,055 BLGF LGUs matched; in silver, 11,429 of 11,434 rows have a code.
- The old Maguindanao province rows (FY2018 to FY2022, 5 rows) have no code, because PSGC Q2_2024 lists only Maguindanao del Norte and del Sur. They are kept and documented.
- The silver table and its data dictionary page gain three columns and a section for the LGU map.
- Downside accepted: silver now depends on the PSGC table and on one PSGC snapshot. Changing the snapshot means rerunning the matcher and the silver build.

## Open

- **Which PSGC snapshot every dataset uses.** PSGC codes change between snapshots (for example the Negros Island Region renumbered 3 provinces, 19 cities and 44 municipalities between Q4_2023 and Q2_2024), and `reference.lgu_master` (CMCI) uses 2Q 2026. The team should pick one snapshot for all datasets, and a crosswalk for data keyed on another snapshot's codes, in a separate record.
- The broken ñ in `psa_psgc_clean` (`Ã±`) should be fixed there. The matcher repairs it in the meantime.
- Whether the old Maguindanao province gets a code later (for example from PSGC Q2_2021, added to `dim_lgu` as a retired area).
