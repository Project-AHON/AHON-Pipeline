# blgf_ldrrmf_lgu_map

- **Table:** `reference.blgf_ldrrmf_lgu_map`
- **One row is:** one distinct BLGF reporting LGU (region, province, name, type), with its PSGC match
- **Key:** `region`, `province`, `lgu_name`, `lgu_type`
- **Built from:** [`bronze.blgf_ldrrmf_annual_lgu`](blgf_ldrrmf_annual_lgu.md), [`silver.psa_psgc_clean`](psa_psgc_clean.md) (selected snapshot) and the reviewed overrides in `src/sql/datasets/blgf_ldrrmf_annual_lgu/silver/blgf_ldrrmf_lgu_map_overrides.csv`, by `src/sql/datasets/blgf_ldrrmf_annual_lgu/silver/build_blgf_ldrrmf_lgu_map.py`
- **Owner:** Crystal
- **Decision:** [0013: LDRRMF PSGC matching in silver](../../decisions/0013-ldrrmf-psgc-matching-in-silver.md)

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `region`, `province`, `lgu_name`, `lgu_type` | string | BLGF labels, trimmed | Same as silver |
| `first_year` | int | First fiscal year the LGU appears | Helps review |
| `last_year` | int | Last fiscal year the LGU appears | Helps review |
| `psgc_code` | string | Matched PSGC code | NULL unless one candidate was found |
| `psgc_name` | string | PSGC name of the match | |
| `match_method` | string | Step that found the candidate | `override`, `province_name`, `no_province_name`, `name_only`, `old_name` |
| `match_status` | string | Outcome | `matched`, `needs_review`, `ambiguous`, `unmatched` |
| `candidate_count` | int | PSGC candidates found at that step | |
| `source_period` | string | PSGC snapshot used | Must equal the selected snapshot |
| `mapping_version` | string | Version of the matching rules | `blgf_context_v1` |
| `_processed_at` | timestamp (UTC) | When the row was last changed | |

## How a match is found

Names are compared after normalization. Steps run in order and stop at the first one that finds candidates:

| Step | Rule | Status if one candidate |
| --- | --- | --- |
| `override` | A reviewed row in the overrides CSV; an empty code means "checked, no PSGC code" | `matched` (or `unmatched` with no code) |
| `province_name` | Province rows: same province name. Cities and municipalities: same name in the same province | `matched` |
| `no_province_name` | Same name among cities with no province (HUCs, NCR cities) | `needs_review` |
| `name_only` | Same name anywhere | `needs_review` |
| `old_name` | Same former name in the same province | `needs_review` |

Several candidates at a step give `ambiguous`. A candidate at a different level (for example a BLGF Municipality that is a PSGC City) gives `needs_review`. Only `matched` rows pass a code to silver; no calibrated match confidence is invented.

## Notes

- Each override needs a written reason; the build stops if a reason is blank, an override code is not in PSGC, or overrides conflict.
- The build stops if a BLGF LGU appears twice in the map or the existing map uses a different snapshot.
- The `old_name` step currently finds nothing, because the PSGC read does not pass old names.
- Rerun this before the silver build whenever bronze has a new LGU name; silver stops if an LGU is missing from the map.
