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
| `area_name` | STRING | Cleaned name of the geographic area. | Derived from Bronze `area_name`; PSA footnote markers and unwanted whitespace are cleaned in Silver.|
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

### Silver table
`ahon_dev.silver.psa_psgc_clean`
Defined in `src/sql/00_setup/02_psa_psgc_clean.sql` and written by `src/sql/datasets/psa_psgc/silver/psa_psgc_clean.py`.

### Grain
One record represents one cleaned PSGC geographic record from one API snapshot. The same PSGC code may appear across different API periods; `_api_period` identifies the snapshot.

### Business key

`psgc_code + geographic_level + _api_period`

### Current row counts

| API Period | Bronze records | Silver records |
|---|---:|---:|
| Q2_2024 | 43,762 | 43,760 |
| April_2024 | 43,761 | 43,759 |
| Q4_2023 | 43,758 | 43,756 |
| Q2_2021 | 43,798 | 43,796 |
| **Total** | **175,079** | **175,071** |

The difference of 8 rows (2 per API period) is explained under Known issues. To recheck.

```sql
SELECT
    _api_period,
    COUNT(*) AS records
FROM ahon_dev.silver.psa_psgc_clean
GROUP BY _api_period
ORDER BY _api_period;
```

## Silver Columns

| Column | Type | Description | Notes |
|---|---|---|---|
| `psgc_code` | STRING | Standardized PSGC code identifying the geographic area. | Business-key field. Stored as a string to preserve leading zeros; standardized to 10 digits in Silver. |
| `geographic_name` | STRING | Cleaned name of the geographic area. | Derived from Bronze `area_name`; PSA footnote markers and unwanted whitespace are cleaned in Silver. |
| `geographic_level` | STRING | Geographic level of the record. | Standardized to uppercase, trimmed values such as `REG`, `PROV`, `CITY`, `MUN`, `SUBMUN`, `BGY`, and `SGU`. |
| `region_psgc` | STRING | PSGC code for the record's region. | Derived from the standardized PSGC code; used for code-based joins. |
| `province_psgc` | STRING | PSGC code for the record's province. | Derived from the standardized PSGC code; may be a hierarchy code even when a conventional province name is not applicable. |
| `citymun_psgc` | STRING | PSGC code for the record's city or municipality parent. | Derived from the standardized PSGC code; not applicable to region and province records. |
| `region_code` | STRING | Source region code. | Retained from the Bronze source for reference and hierarchy-related logic. |
| `province_code` | STRING | Source province code. | Retained from the Bronze source; special values such as `999` require exception-aware handling. |
| `municipality_code` | STRING | Source municipality code. | Retained from the Bronze source. |
| `barangay_code` | STRING | Source barangay code. | Retained from the Bronze source; may be NULL above barangay level. |
| `province_name` | STRING | Resolved province name for the geographic record. | Province records carry their own name; region records are NULL. For other records it is derived through exact PSGC-code joins and special-case rules. Pateros and SGA may remain NULL; City of Isabela and its barangays map to Basilan. |
| `province_city` | STRING | Display label combining the geographic name with relevant parent and province context. | Display field only. Downstream joins should use PSGC code columns, not this label. Region and province records show their own name. Barangays show "barangay, parent place, independent city (only if different from the parent place), province", skipping parts that do not apply (for example `Adams, Adams, Ilocos Norte`; `Barangay 1, Tondo I/II, City of Manila`). |
| `correspondence_code` | STRING | Correspondence code associated with the geographic area. | Retained from the source. |
| `old_name` | STRING | Former or previous name of the area. | Retained from the source; may be NULL. |
| `status` | STRING | Status associated with the geographic area. | Retained from the source. |
| `income_classification` | STRING | Income classification of the LGU. | Retained from the source; may be NULL. |
| `city_class` | STRING | Classification of a city. | Retained from the source; supports HUC/ICC exception logic. |
| `urban_rural` | STRING | Urban or rural classification. | Retained from the source; may be NULL. |
| `island_region` | STRING | Island group or region classification. | Retained from the source; may be NULL. |
| `populations_json` | STRING | Population information returned by the PSGC API in JSON format. | Retained from the source as JSON text; may be NULL. |
| `source_version` | STRING | Source PSGC version associated with the record. | Retained from the source `version` field. This is distinct from `_api_period`, which identifies the ingested snapshot. |
| `_api_period` | STRING | PSGC API snapshot from which the record was retrieved. | Examples include `Q2_2024`, `April_2024`, `Q4_2023`, and `Q2_2021`. Part of the business key. |
| `_source_name` | STRING | Name identifying the original source. | Provenance column retained from Bronze. |
| `_source_ref` | STRING | Reference identifying the source. | Provenance column retained from Bronze. |
| `_ingested_at` | TIMESTAMP | Timestamp when the Bronze record was ingested. | Provenance column retained from Bronze. |
| `_batch_id` | STRING | Identifier for the ingestion batch. | Provenance column retained from Bronze. |
| `_row_hash` | STRING | SHA-256 hash identifying the Bronze source record for change detection. | Provenance column carried forward from Bronze unchanged (decision 0006); expected to be 64 hexadecimal characters. The Silver merge updates a row only when this hash changes. |
| `_processed_at` | TIMESTAMP | Timestamp when the Silver record was processed. | Silver audit column set each time the Silver script builds the row; it is written when a row is inserted or updated. |

## Silver Transformations

The Silver layer:

- Cleans and standardizes source values, including geographic names and geographic levels.
- Standardizes PSGC codes to 10-character strings while preserving leading zeros.
- Deduplicates records within each API snapshot using the business key.
- Retains `_api_period` so records from different snapshots remain distinguishable.
- Derives `region_psgc`, `province_psgc`, and `citymun_psgc` from PSGC code structure.
- Resolves `province_name` and `province_city` using exact code-based lookups and documented special-case rules.
- Preserves Bronze provenance fields, including `_row_hash`, and adds a Silver `_processed_at` timestamp.
- Uses an idempotent Delta `MERGE` keyed by `psgc_code`, `geographic_level`, and `_api_period`. A matched row is updated only when its Bronze `_row_hash` changes.
- Does not delete target-only records by default; snapshot history may remain in Silver even if a record is absent from a later Bronze load.

## Data Quality Rules

The Silver checks validate that:

- Required business-key and geographic-name fields are populated.
- The composite business key is unique.
- PSGC codes contain exactly 10 numeric digits.
- Geographic levels use the expected standardized values.
- PSA footnote markers and unwanted leading/trailing whitespace have been removed.
- Provenance columns are populated and `_row_hash` has the expected SHA-256 format.
- `_processed_at` is populated.
- Derived hierarchy codes are consistent with the PSGC code structure.
- `province_city` is populated and follows the expected display-label pattern.
- Missing province mappings are reviewed with exceptions for HUCs/ICCs and their descendants, Pateros, and SGA.
- Barangay and sub-municipality parent relationships are checked within the same API snapshot.

> **Schema note:** Column types above reflect the intended Silver schema. If the live Delta table has evolved, confirm the exact names and data types with `DESCRIBE TABLE ahon_dev.silver.psa_psgc_clean`.

## Known issues

- Bronze records with no `geographic_level` are not loaded into Silver. These are PSA placeholder entries ("City of Isabela (Not a Province)", "City of Cotabato (Not a Province)", "Special Geographic Area" and the interim BARMM province code), 8 records in total (2 per API period).