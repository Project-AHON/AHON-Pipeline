# earthquake_observation_summary

- **Table:** `platinum.earthquake_observation_summary` (view)
- **One row is:** one listing year, month and PSGC snapshot
- **Key:** `source_year`, `source_month`, `source_period`
- **Built from:** [`silver.philvolcs_earthquake_observation_clean`](philvolcs_earthquake_observation_clean.md) by `src/sql/platinum/earthquake_components.sql`
- **Owner:** Jessy

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `source_year` | int | Listing year | |
| `source_month` | int | Listing month, 1 to 12 | |
| `source_period` | string | PSGC snapshot used for candidates | |
| `source_observations` | bigint | Observations listed | Not deduplicated events |
| `invalid_observations` | bigint | Observations with `has_dq_issues` | |
| `single_textual_candidates` | bigint | Observations with one PSGC candidate | |
| `ambiguous_textual_candidates` | bigint | Observations with several PSGC candidates | |
| `unresolved_textual_candidates` | bigint | Observations that are unmatched or unparsed | |
| `maximum_reported_magnitude` | double | Largest magnitude among valid observations | |
| `interpretation` | string | Fixed note | `Observation summary only; no geographic exposure or risk assignment` |

## Notes

- Observations can repeat one event; these counts are not deduplicated events, LGU event counts, exposed population or validated risk.
