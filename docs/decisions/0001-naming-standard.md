# 0001: Naming standard

- **Status:** Accepted, except where marked open (see Open)
- **Date:** 2026-10-04

## Context

The workspace and the draft setup SQL used different names (`ahon` and
`ahondev`; numbered schemas such as `01_bronze`; separate `quality` and
`marts` schemas). We need one set of names before writing the setup SQL
and the architecture overview, because every table, doc and job refers
to them. The full rules are in the naming standard; this record keeps
the decisions and the reasons.

## Decision

### Catalogs and schemas

Two catalogs, one per environment, with identical schemas in each:

```
ahon         prod
ahon_dev     dev

Schemas in each catalog:
  source (pending)   raw files exactly as received (a Volume, not tables); never edited
  bronze             raw data loaded into tables, unchanged apart from provenance columns
  silver             cleaned, typed, deduplicated, standardized
  gold               dimensions and facts
  platinum           analytics-specific tables built from gold
  reference          lookup and code tables
  monitoring         control table, run logs, data quality rules and results
```

`default` and `information_schema` are created by Databricks and are not
part of this design.

### Table names

- Lowercase snake_case, singular nouns, no layer name, dates, versions,
  environments or people in the name.
- bronze: the dataset name, which equals `_source_name` (`publisher_dataset`).
- silver: the bronze name plus `_clean` (`publisher_dataset_clean`), one
  silver table per source. Datasets are combined only in gold.
- gold: `dim_` and `fact_` prefixes (`dim_location`, `fact_event`).
- platinum: named by the analysis it answers, no prefix (`exposure_by_area`).
- reference: plain names (`location_codes`).
- monitoring: `dq_` prefix for quality tables, plain `control` and `run_log`.

### Provenance columns

Leading underscore, so they stay separate from source columns. The core set,
all added in bronze: `_source_name`, `_source_ref`, `_ingested_at`,
`_batch_id`, `_row_hash`. `_row_hash` is a SHA-256 of the raw source columns
as received, computed in bronze; silver does not get a second hash.

## Why

- A catalog per environment is the standard way to separate dev from prod,
  and identical schemas mean code moves between them by changing only
  the catalog name.
- Plain lowercase names with no number prefixes avoid backticks in SQL
  and keep names stable if layers are ever added.
- `gold` holds dimensions and facts; `platinum` holds tables built for
  specific analyses, so the two have different jobs.
- Singular names, no layer in the name and one silver table per source
  keep every table traceable to a single bronze table.
- `_clean` marks silver without putting a layer name in the table name
  of any other layer.
- A leading underscore keeps provenance columns apart from source columns,
  so a source column can never collide with them.
- `_row_hash` in bronze detects duplicate loads at the point of ingestion
  and keeps CDC and lineage traceable from the raw data.
- One `monitoring` schema for the control table, run logs and data
  quality results keeps operational tables together.

## Alternatives considered

- **Numbered schema prefixes (`01_bronze`):** rejected. They need backticks
  and encode an order that may change.
- **`marts` instead of `platinum`:** rejected in favor of `platinum`.
- **Separate `quality` schema:** rejected. Merged into `monitoring`.
- **`ahondev` as the dev catalog:** rejected. `ahon_dev` is clearer and
  needs no backticks, since underscores are allowed.
- **Plural table names, layer prefixes (`bronze_`, `silver_`):** rejected.
  The schema already says the layer.
- **`_row_hash` computed in silver:** rejected. Duplicate loads would
  already be in bronze, and the hash would cover cleaned values, not the
  data as received.
- **One catalog per layer:** rejected as too much for a project this size.

## Consequences

- The naming standard, architecture overview, environments doc and setup
  steps must use these names.
- The existing dev catalog `ahondev` needs renaming to `ahon_dev`.
- The `source` name must be confirmed before the first raw file lands (P2).

## Open

- Final name for the `source` schema.
- Column names (casing, suffixes, units, source names that break the rules).
- Whether silver and later layers carry provenance columns forward.
- File and folder names (decided under the repo structure task).
