# LGU Master

## Overview

The LGU Master table is the canonical geographic reference table for AHON. It provides one standardized record for each active Philippine city and municipality and serves as the common geographic reference for downstream datasets.

The table is built from the PSA PSGC API Bronze table rather than from a separate Excel reference file.

### Source
- **Publisher:** Philippine Statistics Authority (PSA)
- **Dataset:** Philippine Standard Geographic Code (PSGC)
- **Source:** PSA PSGC API
- **Selected snapshot:** `Q2_2024`
- **API reference:** https://classification.psa.gov.ph/psgc

### Table
`ahon.reference.lgu_master`

### Grain
One row represents one active city or municipality. `psgc_code` is the canonical identifier.

### Current coverage

| Measure | Count |
|---|---:|
| Total LGUs | 1,642 |
| Cities | 149 |
| Municipalities | 1,493 |
| Duplicate PSGC codes | 0 |
| Missing required fields | 0 |
| LGUs with 2024 population | 1,623 |
| LGUs without 2024 population | 19 |

## Columns

| Column | Type | Description | Notes |
|---|---|---|---|
| `psgc_code` | STRING | Unique PSGC identifier for the city or municipality. | Canonical LGU identity; must be unique and not NULL. |
| `correspondence_code` | STRING | Correspondence code associated with the LGU. | Sourced from the PSA PSGC API. |
| `lgu_name` | STRING | Name of the city or municipality. | Sourced from `area_name`; must not be NULL. |
| `province_code` | STRING | PSGC code identifying the province associated with the LGU. | Derived from the first five PSGC digits. SGA municipalities use `999`. |
| `province_name` | STRING | Name of the province associated with the LGU. | May be NULL for legitimate PSGC hierarchy cases such as Pateros and Special Geographic Area municipalities. |
| `geographic_level` | STRING | Geographic level of the LGU. | Only `City` and `Mun` are included. |
| `old_name` | STRING | Former or previous name of the LGU. | Sourced from the PSGC API; may be NULL. |
| `city_class` | STRING | Classification of the city. | Primarily applicable to city records. |
| `income_classification` | STRING | Income classification of the LGU. | Sourced from the PSGC API. |
| `population_2024` | BIGINT | 2024 population of the LGU. | Extracted from `populations_json`; NULL when the source does not provide 2024 population. |
| `source_name` | STRING | Name of the source used to build the reference table. | `PSA PSGC API`. |
| `source_period` | STRING | PSGC snapshot/version used to create the table. | `Q2_2024`. |
| `source_reference` | STRING | Reference to the source system or API. | `https://classification.psa.gov.ph/psgc` |
| `is_active` | BOOLEAN | Indicates whether the LGU is included as an active reference record. | Current implementation sets Q2_2024 LGUs to `true`. |
| `created_timestamp` | TIMESTAMP | Timestamp associated with creation of the reference record. | Maintained by the reference-table loader. |
| `updated_timestamp` | TIMESTAMP | Timestamp associated with the latest update. | Maintained by the reference-table loader. |

## Source and Transformation

PSA PSGC API  
↓  
`ahon.bronze.psa_psgc`  
↓  
Filter `Q2_2024`  
↓  
Select Cities and Municipalities  
↓  
Derive province mapping  
↓  
Extract 2024 population  
↓  
Validate  
↓  
`ahon.reference.lgu_master`

### PSGC Identity

`psgc_code` is the canonical identifier for an LGU. Names are not used as the primary identifier because LGU names may not be unique or may change over time.

### Province Mapping

Province information is derived from the PSGC hierarchy. The standard province code is obtained from the first five digits of the LGU PSGC code.

Special Geographic Area municipalities use province code `999` because they belong to the Special Geographic Area rather than a conventional province.

Pateros is preserved according to the PSGC hierarchy. It has province code `13817` but no conventional province name. These are known source/hierarchy cases and are not data-quality failures.

## Population

The PSGC API provides population information for multiple census years. The loader extracts the record where `year = 2024`.

The population value is cleaned by removing formatting characters such as commas before conversion to `BIGINT`.

### Population validation

- LGUs with 2024 population: **1,623**
- LGUs without 2024 population: **19**
- Multiple 2024 population records: **0**
- Invalid population values: **0**

The 19 LGUs without a 2024 population are municipalities in Sulu (`province_code = 19066`). Their missing 2024 population is preserved as NULL rather than replacing it with an older population value.

## Data Quality Rules

The final reference table validates:

1. Total LGUs = 1,642.
2. Cities = 149.
3. Municipalities = 1,493.
4. `psgc_code` is unique.
5. Required fields are not NULL.
6. Unexpected municipalities without province mapping = 0.
7. Multiple 2024 population records = 0.
8. Invalid population values = 0.
9. Expected population coverage = 1,623.
10. Expected missing population = 19.
11. No unexpected LGUs are missing population.

## Known Exceptions

### Special Geographic Area

The following municipalities belong to the Special Geographic Area and use province code `999`:

- Kapalawan
- Old Kaabakan
- Kadayangan
- Nabalawag
- Pahamuddin
- Malidegao
- Ligawasan
- Tugunan

Their province name is NULL because they do not belong to a conventional province.

### Pateros

Pateros is a municipality in the National Capital Region. Its PSGC hierarchy provides province code `13817`, but no conventional province name. This is preserved from the source and is not considered an unexpected missing province mapping.

### Sulu Population Coverage

Nineteen Sulu municipalities do not have a 2024 population value in the selected `Q2_2024` PSGC snapshot. The table keeps these records with `population_2024 = NULL` rather than substituting the 2020 population.

## Downstream Use

`ahon.reference.lgu_master` serves as the canonical LGU reference for downstream AHON datasets.

Other datasets can use `psgc_code` to consistently link geographic information to the same city or municipality.

The table can support joins with hazard, population, infrastructure, investment, and other LGU-level datasets.
