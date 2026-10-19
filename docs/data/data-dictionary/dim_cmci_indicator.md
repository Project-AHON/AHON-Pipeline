# dim_cmci_indicator

- **Table:** `gold.dim_cmci_indicator`
- **One row is:** one selected CMCI indicator
- **Key:** `indicator_code`
- **Built from:** `resources/cmci_indicators.csv` by `src/sql/datasets/cmci/pipeline.py --stage gold`
- **Owner:** Maki

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `indicator_code` | string | Indicator code | For example `drrp` |
| `indicator_name` | string | Indicator label as shown on the CMCI portal | For example `Disaster Risk Reduction Plan` |
| `pillar_name` | string | CMCI pillar | Economic Dynamism, Government Efficiency, Infrastructure, Resiliency, Innovation |
| `metric_version` | string | Version of the selected indicator set | `selected_35_v1` |

## Notes

- Covers the 35 selected indicators, not the complete official CMCI indicator list.
- If the CSV changes, the build stops for review instead of silently changing definitions.
