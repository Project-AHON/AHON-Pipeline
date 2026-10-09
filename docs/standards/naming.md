# Naming standard

Status: draft. Sections marked "Open" are still to be agreed with the team.

## General rules

- Lowercase letters, digits and underscores only.
- No number prefixes on catalogs or schemas (use `bronze`, not `01_bronze`).
- Names describe what something is, not who made it or when.

## Catalogs

| Catalog | Environment |
| --- | --- |
| `ahon` | Production |
| `ahon_dev` | Development |

Both catalogs have the same schemas, so code moves between environments by
changing only the catalog name.

## Schemas (layers)

| Schema | Holds |
| --- | --- |
| `source` (pending) | Raw files exactly as received (a Volume, not tables); never edited |
| `bronze` | Raw data loaded into tables, unchanged apart from provenance columns |
| `silver` | Cleaned, typed, deduplicated, standardized |
| `gold` | Dimensions and facts |
| `platinum` | Analytics-specific tables built from gold |
| `reference` | Lookup and code tables |
| `monitoring` | Control table, run logs, data quality rules and results |

`default` and `information_schema` are created by Databricks and are not part
of this standard. See decision 0001 for the reasoning.

Open: the final name of the `source` schema.

## Table names

Agreed, except where marked open.

General rules:

- Lowercase `snake_case` with descriptive nouns; no abbreviations the team would not recognize.
- Do not put the layer name in the name. The schema already says it: `ahon.gold.dim_location`, not `gold_dim_location`. The only exception is the `_clean` suffix on silver tables, which pairs each silver table with its bronze table.
- No dates, versions, environments or people in names (for example `event_v2`, `event_2026`).
- Singular nouns everywhere (`event`, `location`).

By schema:

| Schema | Table naming | Example |
| --- | --- | --- |
| `bronze` | One table per source dataset, named after it; equals the `_source_name` value | `publisher_dataset` |
| `silver` | The bronze table name plus the suffix `_clean`; one silver table per source | `publisher_dataset_clean` |
| `gold` | `dim_` prefix for dimensions, `fact_` prefix for facts | `dim_location`, `fact_event` |
| `platinum` | Named for the analysis or question it answers; no prefix | `exposure_by_area` |
| `reference` | Plain name for the lookup; no prefix | `location_codes` |
| `monitoring` | `dq_` prefix for data quality tables; plain names for control and run tables | `dq_result`, `control`, `run_log` |

Data that is not tabular (for example nested or spatial files): the raw files stay
in the `source` volume, in one folder per dataset named after the dataset. If a
file is also loaded into a bronze table, the table keeps the same dataset name.

Datasets are combined only in gold, never in silver, so every silver table maps to one bronze table.

## Column names

Open. To agree with the team:

- Casing and word separators.
- Suffixes for keys, dates and timestamps, and names for boolean columns.
- Units in names (for example a magnitude or depth column).
- Handling of source column names that break the rules above. Where renaming happens is also open: bronze keeps source names as received and silver renames them is the working assumption.

## Provenance columns

Core set, added so every row can be traced back to where and when it was loaded.
Values in the example column are illustrative.

| Column | Type | Added in | What it holds | Example |
| --- | --- | --- | --- | --- |
| `_source_name` | string | bronze | The dataset or feed the row came from, matching its control-table entry | `publisher_dataset` |
| `_source_ref` | string | bronze | Where the data was fetched from: a raw file path, or the request URL (never including keys or tokens) | `/Volumes/ahon/source/raw/publisher_dataset/2026-10-04/export.csv` or `https://example.org/api/events?start=2026-10-01` |
| `_ingested_at` | timestamp (UTC) | bronze | When the row was loaded into bronze | `2026-10-04 03:51:33` |
| `_batch_id` | string | bronze | The load run that wrote the row; links to the run log in `monitoring` | `publisher_dataset_20261004_035133` |
| `_row_hash` | string | bronze | SHA-256 of the raw source columns as received (provenance columns excluded) | `9f86d081884c7d65...` (64 hex characters, shortened here) |
| `_processed_at` | timestamp (UTC) | silver | When the silver table was built | `2026-10-09 04:12:00` |

Notes:

- The leading underscore keeps these columns visibly separate from source
  columns and avoids name clashes.
- `_row_hash` is computed in bronze over the raw source columns, so it shows how
  data arrived: repeated loads, repeated change events, and which bronze rows
  were later removed by silver dedup. The column list it covers must be
  documented per table, because changing that list changes every hash.
- Silver does not get a second hash; `_row_hash` stays the single hash.
- Open: whether silver and later layers carry the bronze provenance columns
  (including `_row_hash`) forward or add their own (for example `_processed_at`).
- Later candidates, not core: `_source_modified_at`, `_load_mode`.
- Data that is not tabular and lands only in `source` has no rows to hold these
  columns; the control table or batch record would hold the same facts.

## File and folder names

Open. Decision records use `NNNN-short-title.md`. Other repository
file and folder names follow the structure agreed in Issue 27.
