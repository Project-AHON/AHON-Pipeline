# psa_psgc_reject

- **Table:** `silver.psa_psgc_reject`
- **One row is:** one cleaned PSGC row with missing or unknown geographic fields
- **Key:** `_api_period`, `_row_hash`
- **Built from bronze by:** `src/sql/datasets/psa_psgc/silver/psa_psgc_clean.py`
- **Owner:** Yanna

Same columns as the cleaned rows before hierarchy lookups: the source columns of [`silver.psa_psgc_clean`](psa_psgc_clean.md) without `region_psgc`, `province_psgc`, `citymun_psgc`, `province_name`, `province_city` and `_processed_at`, plus:

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `disposition` | string | Why the row was rejected | Always `invalid_geographic_fields` |

A row is rejected when its code, name or snapshot is missing, or its level is not `REG`, `PROV`, `CITY`, `MUN`, `SUBMUN`, `SGU` or `BGY`.

## Notes

- Rows here raise a warning; they do not stop the build.
