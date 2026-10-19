# philvolcs_earthquake_observation_clean

- **Table:** `silver.philvolcs_earthquake_observation_clean`
- **One row is:** one earthquake observation (one bronze row), typed, with PSGC locality candidates
- **Key:** `_observation_id`
- **Built from:** [`bronze.philvolcs_earthquake_observation`](philvolcs_earthquake_observation.md) and [`silver.psa_psgc_clean`](psa_psgc_clean.md) (`CITY` and `MUN` in the selected snapshot) by `src/sql/datasets/philvolcs_earthquake/observation_pipeline.py --stage silver`
- **Owner:** Jessy
- **Rejects:** [`silver.philvolcs_earthquake_reject`](philvolcs_earthquake_reject.md)

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `event_time` | timestamp_ntz | Listed date and time | Listing wall-clock time; not converted to UTC |
| `latitude` | double | Latitude in degrees | |
| `longitude` | double | Longitude in degrees | |
| `depth_km` | double | Depth in kilometres | |
| `magnitude` | double | Magnitude as published | Publisher's unspecified magnitude scale |
| `source_year` | int | Year of the listing page | |
| `source_month` | int | Month of the listing page, 1 to 12 | |
| `Date-Time`, `Location`, `Month`, `Year` | string | Source values as received | Kept for audit |
| `_raw_latitude`, `_raw_longitude`, `_raw_depth_km`, `_raw_magnitude` | string | Numeric source values as received | Bronze `Latitude`, `Longitude`, `Depth`, `Magnitude` |
| `locality_token` | string | Town parsed from `Location` | Lowercased; empty when unparsed |
| `province_token` | string | Province parsed from `Location` | Lowercased; empty when unparsed |
| `candidate_codes` | array of string | PSGC city/municipality codes with that town and province name | Sorted; empty when none |
| `candidate_status` | string | `single_candidate`, `ambiguous`, `unmatched` or `unparsed` | |
| `psgc_code` | string | Assigned PSGC code | Always NULL until the location meaning is approved |
| `geographic_assignment_status` | string | Why no code is assigned | Always `WITHHELD_LOCALITY_IS_NOT_EXPOSURE` |
| `_psgc_snapshot` | string | PSGC snapshot used for candidates | For example `Q2_2024` |
| `has_dq_issues` | boolean | True when the row fails a rule below | Flagged rows stay in this table |
| `_source_row`, `_source_name`, `_source_ref`, `_source_digest`, `_ingested_at`, `_batch_id`, `_row_hash`, `_observation_id` | | Source lineage carried from bronze unchanged | |
| `_processed_at` | timestamp (UTC) | When the row was last changed in silver | |

## Rules

A row is flagged (`has_dq_issues`) and copied to the reject table when any of these hold:

- `event_time`, latitude, longitude, depth or magnitude is missing or cannot be parsed
- Latitude is outside -90 to 90, or longitude outside -180 to 180
- Depth or magnitude is negative
- Any number is NaN or infinite
- The listing month or year is missing, or differs from the month and year of `event_time`

The build stops if the PSGC snapshot is empty, PSGC codes repeat, the candidate join adds rows, or an observation repeats.

## Notes

- Every observation remains; there is no event deduplication. One earthquake can appear in several rows.
- Candidates use the town referenced by the location text, compared with PSGC names by case and whitespace only. No aliases, overrides or fuzzy matching. A candidate is not epicenter containment or impact.
- An existing row is updated only when its candidates or PSGC snapshot change.
