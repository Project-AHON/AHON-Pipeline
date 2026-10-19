# psa_psgc

- **Table:** `bronze.psa_psgc`
- **Source:** Philippine Statistics Authority, PSGC classification API. See the [PSGC data profile](../data-profile/psgc-profile.md).
- **One row is:** one geographic entity as received in one API snapshot, from one delivery
- **Loaded by:** `src/sql/datasets/psa_psgc/extract/psa_psgc_ingest.py` (API to JSONL files under `/Volumes/<catalog>/reference/<source_volume>/psa_psgc/<batch_id>/`), then `src/sql/datasets/psa_psgc/bronze/psa_psgc.py`
- **Key:** `_observation_id`
- **Row hash covers:** all source columns below, including `populations_json`
- **Owner:** Yanna

Source codes are strings, never numeric warehouse keys. All source columns are nullable strings, kept as received.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `psgc_code` | string | PSGC code | API field `code`; 8, 9 or 10 digits as received (leading zeros dropped) |
| `area_name` | string | Name of the geographic entity | May carry PSA footnote markers such as `1/` |
| `correspondence_code` | string | Correspondence code | |
| `geographic_level` | string | Geographic level | `Reg`, `Prov`, `City`, `Mun`, `SubMun`, `SGU`, `Bgy` |
| `region_code` | string | Region code as published | API field `reg` |
| `province_code` | string | Province code as published | API field `prv` |
| `municipality_code` | string | City or municipality code as published | API field `mun` |
| `barangay_code` | string | Barangay code as published | API field `bgy` |
| `old_name` | string | Former or previous name | May be NULL |
| `city_class` | string | City classification, such as HUC or ICC | |
| `income_classification` | string | Income classification | |
| `urban_rural` | string | Urban or rural tag | |
| `island_region` | string | Island group | |
| `status` | string | Status as published | |
| `_api_period` | string | API snapshot the row came from | `Q2_2024`, `April_2024`, `Q4_2023` or `Q2_2021` |
| `version` | string | Publisher's version | |
| `populations_json` | string | Source population array as JSON text | One entry per census year |
| `_source_name` | string | The dataset the row came from | Always `psa_psgc` |
| `_source_ref` | string | Where the row was loaded from: the landed JSONL file path | One file per snapshot |
| `_source_digest` | string | SHA-256 of the landed file | |
| `_ingested_at` | timestamp (UTC) | When the row was loaded into bronze | |
| `_batch_id` | string | The load run that wrote the row | |
| `_row_hash` | string | SHA-256 of the raw source columns as received | Null-aware JSON of all source values and populations |
| `_observation_id` | string | Identity of this observation | Hash of `_batch_id`, `_source_digest` and `_row_hash` |

## Notes

- Each snapshot file must match its configured expected row count, and the batch must contain every configured snapshot, before it is read. File checksums gate reads.
- Same-batch replay preserves first lineage; corrected deliveries require new batch IDs. Identical rows in one delivery are counted in DQ before collapsing by observation ID.
- The four configured snapshots landed 175,079 bronze rows (validated 2026-10-15).
