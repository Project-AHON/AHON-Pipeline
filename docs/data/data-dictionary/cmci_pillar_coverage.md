# cmci_pillar_coverage

- **Table:** `platinum.cmci_pillar_coverage`
- **One row is:** one city or municipality, year and CMCI pillar
- **Key:** `psgc_code`, `year`, `pillar_name`
- **Built from:** `gold.fact_cmci_indicator` and `gold.dim_cmci_indicator` by `src/sql/datasets/cmci/pipeline.py --stage platinum`
- **Owner:** Maki

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `psgc_code` | string | City or municipality PSGC code | |
| `year` | int | CMCI reporting year | |
| `pillar_name` | string | CMCI pillar | |
| `source_period` | string | PSGC snapshot | |
| `expected_indicator_count` | bigint | Selected indicators in the pillar | |
| `available_indicator_count` | bigint | Indicators with a non-missing value | |
| `source_ingested_at` | timestamp (UTC) | Latest source ingestion time for the pillar | |
| `coverage_status` | string | `COMPLETE` when every selected indicator has a value, else `INSUFFICIENT_DATA` | |
| `metric_version` | string | Version of the selected indicator set | |

## Notes

- Counts completeness of the selected subset only. It publishes no pillar sum, official score or preparedness metric.
- `COMPLETE` means selected-subset completeness only, not official CMCI pillar completeness.
