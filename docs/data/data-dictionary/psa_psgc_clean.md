# psa_psgc_clean

- **Table:** `silver.psa_psgc_clean`
- **One row is:** one geographic entity in one API snapshot
- **Key:** `psgc_code`, `geographic_level`, `_api_period`
- **Built from:** [`bronze.psa_psgc`](psa_psgc.md) by `src/sql/datasets/psa_psgc/silver/psa_psgc_clean.py`
- **Owner:** Yanna
- **Rejects:** [`silver.psa_psgc_code_reject`](psa_psgc_code_reject.md), [`silver.psa_psgc_reject`](psa_psgc_reject.md)

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `psgc_code` | string | PSGC code | Ten-digit string, leading zeros retained |
| `area_name` | string | Name of the geographic entity | PSA footnote markers, non-breaking spaces and surplus whitespace removed |
| `geographic_level` | string | Geographic level | Uppercase: `REG`, `PROV`, `CITY`, `MUN`, `SUBMUN`, `SGU`, `BGY` |
| `region_psgc` | string | Region code derived from `psgc_code` | First 2 digits + `00000000` |
| `province_psgc` | string | Province code derived from `psgc_code` | First 5 digits + `00000`; NULL for `REG` |
| `citymun_psgc` | string | City or municipality code derived from `psgc_code` | First 7 digits + `000`; NULL for `REG` and `PROV` |
| `region_code`, `province_code`, `municipality_code`, `barangay_code` | string | Parent codes as published | Carried from bronze, not standardized |
| `province_name` | string | Province name, looked up by code in the same snapshot | NULL for regions, Pateros and the Special Geographic Area; City of Isabela uses Basilan |
| `province_city` | string | Display label, such as `Area, Province` | Display only; not a join key |
| `correspondence_code` | string | Correspondence code | |
| `old_name` | string | Former or previous name | |
| `status` | string | Status as published | |
| `income_classification` | string | Income classification | |
| `city_class` | string | City classification | Used to identify independent cities (HUC, ICC) |
| `urban_rural` | string | Urban or rural tag | |
| `island_region` | string | Island group | |
| `populations_json` | string | Source population array as JSON text | Parsed downstream into city and municipality population |
| `source_version` | string | Publisher's version | Bronze `version`; separate from `_api_period` |
| `_api_period` | string | API snapshot | |
| `_source_name`, `_source_ref`, `_ingested_at`, `_batch_id`, `_row_hash` | | Source lineage carried from bronze | `_row_hash` is unchanged from bronze |
| `_processed_at` | timestamp (UTC) | When the row was last changed in silver | Replay leaves it unchanged |

## Rules

| Rule | Action |
| --- | --- |
| Code is blank, nonnumeric or not 8 to 10 digits | Row goes to `psa_psgc_code_reject`; the delivery stops |
| Name, level or snapshot missing, or level not in the list above | Row goes to `psa_psgc_reject`; warning |
| Two different rows for the same key in one delivery | Stop; no arbitrary winner |
| Same key in several deliveries | Latest delivery kept; superseded rows stay in bronze and are counted (warning) |
| Key, province lookup, parent lookup, Isabela and Basilan lookups unique | Stop if not |
| Every written code is ten digits | Stop if not |

## Notes

- Silver merges business keys without absence-based deletion or schema evolution. An older first-ingested observation cannot replace a newer accepted row. Incompatible existing schemas require a fresh catalog or reviewed migration.
- The four configured snapshots produced 175,067 silver rows (validated 2026-10-15), with one warning rule and no blocking checks.
- Barangay rows stay in this table for reference; they are not analytical rows. LGU analysis uses `CITY` and `MUN` only.
