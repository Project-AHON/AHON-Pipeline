# funding_components

- **Table:** `platinum.funding_components` (view)
- **One row is:** one city or municipality report in one fiscal year
- **Key:** `ldrrmf_fact_key`
- **Built from:** [`gold.fact_ldrrmf`](fact_ldrrmf.md) and [`gold.dim_lgu`](dim_lgu.md) by `src/sql/platinum/funding_components.sql`
- **Owner:** Crystal

All columns of [`gold.fact_ldrrmf`](fact_ldrrmf.md), plus:

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `reference_area_name` | string | PSGC name of the matched LGU | From `dim_lgu`; NULL when unmatched |
| `geographic_level` | string | PSGC level, `CITY` or `MUN` | From `dim_lgu` |
| `interpretation` | string | Fixed note | `Funding utilization is not measured preparedness` |

## Notes

- City and municipality reports only (`lgu_type` City or Municipality). Province reports remain in silver and gold.
- Utilization is expenditure divided by appropriation; zero denominators return NULL and overspending is not clipped. These figures do not establish preparedness or spending effectiveness.
