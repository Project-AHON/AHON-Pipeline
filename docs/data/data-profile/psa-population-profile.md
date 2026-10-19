# PSA Population

## Purpose

Population gives the number of residents in each city and municipality. Project AHON uses it to put earthquake risk and preparedness in per-person terms, for example LDRRMF budget per person.

Population comes from the census figures embedded in the PSGC API. It has no separate bronze table: the figures arrive with each PSGC record in [`bronze.psa_psgc`](../data-dictionary/psa_psgc.md) and are typed in [`silver.psgc_city_population_clean`](../data-dictionary/psgc_city_population_clean.md), then published in [`gold.fact_population`](../data-dictionary/fact_population.md).

> Counts are residents, not earthquake-exposed population.

## Source

| Property | Value |
|---|---|
| Source | [PSA PSGC classification API](https://classification.psa.gov.ph/psgc), `populations` field. See the [PSGC data profile](psgc-profile.md). |
| Provider | Philippine Statistics Authority (PSA) |
| Source format | JSON array per geographic entity, one entry per census year |
| Geographic unit used | Cities and municipalities (`CITY`, `MUN`) |
| Snapshot | `Q2_2024` (`source_period`) |
| Census years in the snapshot | 2015, 2020, 2024 |
| Census year used | Configured as `census_year`; independent of the API snapshot |

## Coverage

| Measure | Count |
|---|---:|
| Cities and municipalities in `Q2_2024` | 1,642 |
| With 2024 population | 1,623 |
| Without 2024 population | 19 |

## Fields as received

Each city or municipality record carries an array of population entries:

| Field | Description |
|---|---|
| `year` | Census year |
| `population` | Reported residents, as text with formatting characters such as commas |
| `psgc` | PSGC code the measurement was reported for |
| `id` | Source entry id |

Things to know about the values as received:

- The city's reported measurement is used directly. Barangays are not summed, and administrative levels are not combined.
- A blank or `-` value means no reported population.
- The reported `psgc` can differ from the record's own code. Such entries are flagged, not used.

## Limitations

- Population is available only for census years (2015, 2020, 2024). Other years are not interpolated.
- Nineteen Sulu municipalities (`province_code = 19066`) have no 2024 population in the PSGC snapshot. Their population is NULL, never zero or an older year's value.
- The PSGC fact holds cities and municipalities only. Province totals are the sum of their cities and municipalities.
- Counts are residents, not earthquake-exposed population.
