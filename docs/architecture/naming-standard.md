# Naming standard

How catalogs, schemas, tables, columns and files are named. See [decision 0001](../decisions/0001-naming-standard.md) for the reasoning.

## General rules

- Lowercase letters, digits and underscores only.
- No number prefixes on catalogs or schemas (use `bronze`, not `01_bronze`).
- Names describe what something is, not who made it or when.

## Catalogs

| Catalog | Environment |
| --- | --- |
| `ahon` | Production |
| `ahon_dev` | Shared development |
| `ahon_dev_<name>` | A developer's own catalog, for example `ahon_dev_crystal` |

All catalogs have the same schemas, so code moves between environments by changing only the catalog name. The catalog is a job parameter (`catalog`), never written into code.

## Schemas (layers)

| Schema | Holds |
| --- | --- |
| `reference` | The `source` volume with raw files exactly as received (never edited), and reviewed lookup tables |
| `bronze` | Raw data loaded into tables, unchanged apart from provenance columns |
| `silver` | Cleaned, typed, checked, with PSGC codes attached |
| `gold` | Dimensions and facts |
| `platinum` | Analysis tables and views built from gold |
| `monitoring` | Data quality results and run records |

`default` and `information_schema` are created by Databricks and are not part of this standard.

Raw files live in the volume `reference.source` ([decision 0004](../decisions/0004-source-file-landing-hugging-face.md)), one folder per dataset and one subfolder per batch: `/Volumes/<catalog>/reference/source/<dataset>/<batch_id>/`.

## Table names

General rules:

- Lowercase `snake_case` with descriptive nouns; no abbreviations the team would not recognize.
- Do not put the layer name in the name. The schema already says it: `gold.dim_lgu`, not `gold_dim_lgu`.
- No dates, versions, environments or people in names (for example `event_v2`, `event_2026`).
- Singular nouns (`observation`, `indicator`).

By schema:

| Schema | Table naming | Examples |
| --- | --- | --- |
| `bronze` | Named after the source dataset, `<publisher>_<dataset>`; usually equals the `_source_name` value | `psa_psgc`, `blgf_ldrrmf_annual_lgu`, `philvolcs_earthquake_observation` |
| `silver` | The cleaned table ends in `_clean`; rows that fail a stop rule go to a matching `_reject` table | `psa_psgc_clean`, `psa_psgc_reject`, `blgf_ldrrmf_annual_lgu_clean` |
| `reference` | Plain name for the lookup; LGU name-to-PSGC maps end in `_lgu_map` | `cmci_lgu_map`, `blgf_ldrrmf_lgu_map` |
| `gold` | `dim_` prefix for dimensions, `fact_` prefix for facts | `dim_lgu`, `fact_ldrrmf`, `fact_population` |
| `platinum` | Named for what it shows; no prefix | `funding_components`, `cmci_pillar_coverage` |
| `monitoring` | `dq_` prefix for data quality tables | `dq_result` |

Silver may attach PSGC codes from `reference` maps and from the PSGC silver table. Measures from different datasets are combined only in gold and platinum.

## Column names

- Lowercase `snake_case`.
- `psgc_code` is the shared LGU key in every table: a 10-digit string, never a number, leading zeros kept.
- Other codes end in `_code` (`region_code`, `indicator_code`). Surrogate keys end in `_key` (`ldrrmf_fact_key`).
- Booleans start with `is_` or `has_` (`has_dq_issues`).
- Timestamps end in `_at` (`_ingested_at`). Years end in `_year` or are named `year` (`fiscal_year`, `population_year`).
- Units go in the name when the column alone is unclear (`depth_km`). Peso amounts carry no suffix; the dictionary states the unit.
- Status columns end in `_status` and hold lowercase or uppercase codes listed in the dictionary (`psgc_match_status`, `coverage_status`).
- Bronze keeps source values as received. Source headers are renamed to `snake_case` in bronze when they cannot be used as column names; otherwise silver renames them. The [data dictionary](../data/data-dictionary/README.md) records the source header for every bronze column.

## Provenance columns

Added so every row can be traced back to where and when it was loaded. Values in the example column are illustrative.

| Column | Type | Added in | What it holds | Example |
| --- | --- | --- | --- | --- |
| `_source_name` | string | bronze | The dataset the row came from | `blgf_ldrrmf_annual_lgu` |
| `_source_ref` | string | bronze | Where the row was loaded from: the landed file path (never including keys or tokens) | `/Volumes/ahon_dev/reference/source/blgf_ldrrmf_annual_lgu/<batch_id>/FY2024-LDRRMF-by-LGU.xlsx` |
| `_source_digest` | string | bronze | SHA-256 of the landed file | 64 hex characters |
| `_ingested_at` | timestamp (UTC) | bronze | When the row was loaded into bronze | `2026-10-04 03:51:33` |
| `_batch_id` | string | bronze | The load run that wrote the row; by default the job run ID | `735696377477443` |
| `_row_hash` | string | bronze | SHA-256 of the raw source columns as received (provenance columns excluded) | 64 hex characters |
| `_observation_id` | string | bronze | Identity of the row: hash of `_batch_id`, `_source_digest` and `_row_hash` | 64 hex characters |
| `_processed_at` | timestamp (UTC) | silver, gold | When the row was last changed in that layer | `2026-10-09 04:12:00` |

Notes:

- The leading underscore keeps these columns visibly separate from source columns and avoids name clashes.
- `_row_hash` is computed in bronze over the raw source columns, so it shows how data arrived. The column list it covers is documented per table, because changing that list changes every hash.
- Silver does not get a second hash; `_row_hash` stays the single hash. Silver and gold carry the bronze lineage columns forward and add `_processed_at` ([decision 0008](../decisions/0008-silver-provenance-and-dq-flag.md)).
- Some datasets define `_observation_id` differently where the file has no stable row identity (PHIVOLCS uses file digest and row number). The dictionary page says so.

## File and folder names

| What | Pattern | Example |
| --- | --- | --- |
| Dataset code folder | `src/sql/datasets/<dataset>/{extract,bronze,silver}/` | `src/sql/datasets/psa_psgc/silver/` |
| Gold and platinum code | `src/sql/gold/`, `src/sql/platinum/`, named after the table | `src/sql/gold/dim_lgu.py` |
| DQ gate code | `src/sql/monitoring/<dataset>/<layer>_dq_gate.py` | `src/sql/monitoring/psa_psgc/silver_dq_gate.py` |
| Data profile | `docs/data/data-profile/<dataset>-profile.md` | `psgc-profile.md` |
| Dictionary page | `docs/data/data-dictionary/<table>.md` | `psa_psgc_clean.md` |
| Decision record | `docs/decisions/NNNN-short-title.md` | `0004-source-file-landing-hugging-face.md` |

For new datasets, use one name for the code folder, the volume folder and the bronze table, so one name finds the code, the raw files and the table. PSGC and LDRRMF follow this; PHIVOLCS (`philvolcs_earthquake` folder, `philvolcs_earthquake_data` volume folder, `philvolcs_earthquake_observation` table) and CMCI (`cmci` folder) predate it.

Branch names follow [CONTRIBUTING](../../CONTRIBUTING.md).
