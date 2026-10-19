# fact_cmci_indicator

- **Table:** `gold.fact_cmci_indicator`
- **One row is:** one CMCI indicator value for one city or municipality and year
- **Key:** `psgc_code`, `year`, `indicator_code`
- **Built from:** [`silver.cmci_indicator_clean`](cmci_indicator_clean.md) and [`silver.cmci_reject`](cmci_reject.md) by `src/sql/datasets/cmci/pipeline.py --stage gold`
- **Owner:** Maki
- **Joins to:** `gold.dim_lgu` on `psgc_code`; `gold.dim_cmci_indicator` on `indicator_code`

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `psgc_code` | string | City or municipality PSGC code | Must exist in `gold.dim_lgu` for the selected snapshot |
| `cmci_name` | string | CMCI source locality name | |
| `year` | int | CMCI reporting year | |
| `indicator_code` | string | Indicator code | |
| `raw_value` | string | Value as returned by CMCI | |
| `raw_score` | double | Numeric value | Nullable; never converts absence to zero |
| `source_period` | string | PSGC snapshot | Must equal the selected snapshot |
| `_source_ref`, `_row_hash`, `_observation_id`, `_ingested_at`, `_batch_id`, `_processed_at`, `has_dq_issues` | | Source lineage carried from silver | |

## Notes

- The portal's numeric field is retained as `raw_score`; units, direction and scoring method are not published per indicator. A lower raw value across different indicators does not mean weaker preparedness.
- An invalid latest correction masks a prior value (`raw_score` is `NULL`) until a later accepted observation recovers it.
