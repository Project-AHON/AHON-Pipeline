# PSA PSGC

## Overview

The Philippine Standard Geographic Code (PSGC) dataset provides the official geographic hierarchy and classification of Philippine administrative areas.

For AHON, the PSGC API is ingested into the Bronze layer to preserve source data and historical API snapshots. The `Q2_2024` snapshot is currently used to build the canonical LGU reference table.

### Source
- **Publisher:** Philippine Statistics Authority (PSA)
- **Dataset:** Philippine Standard Geographic Code (PSGC)
- **Source:** PSA PSGC API
- **API reference:** https://classification.psa.gov.ph/psgc
- **API versions ingested:** `Q2_2024`, `April_2024`, `Q4_2023`, `Q2_2021`

### Bronze table
`ahon.bronze.psa_psgc`

### Grain
One record represents one PSGC geographic record from one API snapshot. The same PSGC code may appear across different API periods; `_api_period` identifies the snapshot.

### Current source counts

| API Period | Records |
|---|---:|
| Q2_2024 | 43,762 |
| April_2024 | 43,761 |
| Q4_2023 | 43,758 |
| Q2_2021 | 43,798 |
| **Total** | **175,079** |

## Bronze Columns

| Column | Type | Description | Notes |
|---|---|---|---|
| `_api_period` | STRING | PSGC API snapshot from which the record was retrieved. | `Q2_2024`, `April_2024`, `Q4_2023`, or `Q2_2021`. |
| `area_name` | STRING | Name of the geographic area. | Source field: `area_name`. |
| `bgy` | BIGINT | PSGC code identifying the barangay. | Source field: `bgy`. May be NULL above barangay level. |
| `city_class` | STRING | Classification of a city. | Source field: `city_class`. |
| `code` | STRING | PSGC code identifying the geographic area. | Source field: `code`; used as `psgc_code` downstream. |
| `correspondence_code` | STRING | Correspondence code associated with the area. | Source field: `correspondence_code`. |
| `geographic_level` | STRING | Geographic level of the record. | Source field: `geographic_level`. |
| `income_classification` | STRING | Income classification of the LGU. | Source field: `income_classification`. |
| `island_region` | STRING | Island group or region classification. | Source field: `island_region`. |
| `mun` | BIGINT | PSGC code identifying the municipality. | Source field: `mun`. |
| `old_name` | STRING | Former or previous name of the area. | Source field: `old_name`; may be NULL. |
| `populations_json` | STRING | Population information returned by the PSGC API in JSON format. | Contains available census years, including 2015, 2020, and 2024. |
| `prv` | BIGINT | PSGC code identifying the province. | Source field: `prv`. |
| `reg` | BIGINT | PSGC code identifying the region. | Source field: `reg`. |
| `status` | STRING | Status associated with the geographic area. | Source field: `status`. |
| `urban_rural` | STRING | Urban or rural classification. | Source field: `urban_rural`. |
| `version` | STRING | PSGC version associated with the source record. | Source field: `version`. |
| `_source_name` | STRING | Name identifying the original source. | Provenance column. |
| `_source_ref` | STRING | Reference identifying the source. | Provenance column. |
| `_ingested_at` | TIMESTAMP | Timestamp when the record was ingested. | Provenance column. |
| `_batch_id` | STRING | Identifier for the ingestion batch. | Provenance column. |
| `_row_hash` | STRING | Hash used to identify a source record for deduplication/change detection. | Hash input columns are defined by the ingestion logic. Changes to those columns change the hash. |

## Ingestion

The PSGC API is called for each configured API period. The API token is retrieved from a Databricks secret and is not hard-coded.

Each API response is stored in the raw source location before being loaded into Bronze. `_api_period` is added to each record so different PSGC snapshots are not treated as the same source record.

### Raw source location
`/Volumes/ahon/reference/source/psa_psgc/`

### API endpoint
`https://classification.psa.gov.ph/psgc/{period}/all`

## Data Quality Rules

The ingestion validates that:
- The API request succeeds.
- The API token is available.
- All expected API periods are processed.
- Raw records are successfully loaded into Bronze.
- `_api_period` is retained.
- PSGC records are not incorrectly deduplicated across API snapshots.

## Downstream Use

The Bronze PSGC table is used to create `ahon.reference.lgu_master`. The `Q2_2024` snapshot is currently the canonical source for the active LGU reference table.
