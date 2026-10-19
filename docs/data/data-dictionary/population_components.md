# population_components

- **Table:** `platinum.population_components` (view)
- **One row is:** one city or municipality for the configured census year
- **Key:** `psgc_code`, `population_year`
- **Built from:** [`gold.fact_population`](fact_population.md) by `src/sql/platinum/population_components.sql`
- **Owner:** Crizza

| Column | Type | Description |
| --- | --- | --- |
| `psgc_code` | string | City or municipality PSGC code |
| `area_name` | string | City or municipality name |
| `geographic_level` | string | `CITY` or `MUN` |
| `region_code` | string | Region code |
| `province_code` | string | Province code; may be NULL |
| `population_year` | int | Census year |
| `population` | bigint | Reported residents (people); nullable |
| `source_period` | string | PSGC snapshot |
| `coverage_status` | string | `AVAILABLE` or `MISSING` |
| `disposition` | string | `accepted`, `missing_population` or `missing_census_observation` |

## Notes

- A view over `gold.fact_population` without lineage columns. Same grain and values.
