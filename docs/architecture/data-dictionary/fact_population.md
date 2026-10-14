# fact_population

Gold table `ahondev.gold.fact_population`, built by `fact_population.py`.

**Grain:** one barangay per census year, keyed by `(barangay_code, population_year)`.
Population is a barangay total, not an exposed-population measure.

## Source

- Population: the `Bgy` rows of `ahondev.silver.psa_psgc` for the chosen API period, read from the `populations_json` field for the chosen census year.
- Parent codes: `ahondev.gold.dim_lgu`, joined on `barangay_code = psgc_code`.

## Parameters

| Widget | Default | Description |
| --- | --- | --- |
| `census_year` | `2024` | Census year to extract from `populations_json` |
| `source_version` | `Q2_2024` | API period filter on `psa_psgc._api_period` |

## Columns

| Column | Type | Description |
| --- | --- | --- |
| `population_key` | BIGINT | Hash of `barangay_code` and `population_year` |
| `barangay_code` | STRING | 10-digit PSGC code, references `dim_lgu.psgc_code` |
| `barangay_name` | STRING | Barangay name from the source |
| `municipality_code` | STRING | Parent municipality or city code |
| `province_code` | STRING | Parent province code, null if none |
| `region_code` | STRING | Parent region code |
| `population` | BIGINT | Barangay total population |
| `population_year` | INT | Census year |

## How to rerun

Run `silver.psa_psgc`, then `dim_lgu`, then `fact_population`. Rerunning is safe: it upserts on the key and does not delete other years.

The notebook stops before writing if there are no barangay rows, any source barangay has no `dim_lgu` match, a required value is null, population is negative, or the key is duplicated.

## Known issues

- Barangays with no population value in the source get no row.
- Other years and rows no longer in the source are kept, not removed.
