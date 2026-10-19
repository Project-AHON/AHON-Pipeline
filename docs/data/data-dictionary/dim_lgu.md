# dim_lgu

- **Table:** `gold.dim_lgu`
- **One row is:** one geographic entity in the selected PSGC snapshot
- **Key:** `psgc_code`
- **Built from:** [`silver.psa_psgc_clean`](psa_psgc_clean.md), filtered to `source_period`, by `src/sql/gold/dim_lgu.py`
- **Owner:** Yanna
- **Joined by:** every gold and platinum table on `psgc_code`

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `source_period` | string | PSGC snapshot the row came from | `Q2_2024` |
| `psgc_code` | string | PSGC code | Ten-digit string |
| `area_name` | string | Name of the geographic entity | Cleaned in silver |
| `geographic_level` | string | Geographic level | `REG`, `PROV`, `CITY`, `MUN`, `SUBMUN`, `SGU`, `BGY` |
| `region_code` | string | Region code | Silver `region_psgc` |
| `province_code` | string | Province code | Silver `province_psgc`; NULL for `REG` |
| `municipality_code` | string | City or municipality code | Silver `citymun_psgc`; NULL for `REG` and `PROV` |
| `barangay_code` | string | Barangay code | Full PSGC code for `BGY`; NULL for other levels |

## Notes

- The key is `psgc_code`; a snapshot tag does not introduce historical dimension grain. The build stops if a target row carries a different snapshot. A new snapshot requires separate review or a fresh development catalog.
- The build checks source and target uniqueness and types, and upserts changed attributes. It does not delete absent entities.
- Hierarchy codes are standardized PSGC prefixes from silver. They describe the selected snapshot and must not be assumed to be an independent current province or city record without checking the dimension. Geographic joins must check period, level and cardinality.
- The dimension retains all native geographic levels for reference and contextual mapping. This does not set the analytical grain: LGU analysis uses only `CITY` and `MUN` rows (1,642 in `Q2_2024`).
- `Q2_2024` has 43,760 rows.
