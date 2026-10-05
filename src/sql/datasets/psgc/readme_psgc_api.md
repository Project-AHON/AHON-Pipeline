# PSGC API Ingestion

Loads Philippine Standard Geographic Code (PSGC) data from the PSA API into a bronze Delta table.

## Source Choice: API

We pull from the PSGC API rather than downloading publication files.

- **Programmatic and repeatable**: no manual download or upload step.
- **Multi-period in one run**: each publication period is a single URL parameter.
- **Structured JSON**: fields arrive already named; nested `populations` is preserved.
- **Easy to automate**: the job can be scheduled once the token is supplied.

**Endpoint:** `https://classification.psa.gov.ph/psgc/{period}/all?token=<token>`

**Periods:** `Q2_2024`, `April_2024`, `Q4_2023`, `Q2_2021`

## Pipeline

| Step | What happens |
|------|--------------|
| 1. Parameters | Token is read from the `psgc_api_token` Databricks widget (never hardcoded). |
| 2. Extract | Loops over each period, follows `next` pagination, pauses 0.2s between pages, and collects all records into `all_records`. |
| 3. Transform | Renames API fields to the bronze schema (`code` → `psgc_code`, `reg`/`prv`/`mun`/`bgy` → `*_code`) and serializes `populations` to `populations_json`. |
| 4. Load | `MERGE` into the bronze table, matching on `_row_hash`. |

## Target Table

`ahon.bronze.psgc`

Columns: PSGC code, area name, correspondence code, geographic level, region/province/municipality/barangay codes, old name, city class, income classification, urban/rural, island region, status, version, `populations_json`.

Provenance columns:

- `_source_name`: `PSGC API`
- `_source_ref`: API base URL
- `_ingested_at`: ingestion timestamp
- `_batch_id`: UUID per run
- `_row_hash`: SHA-256 of all source columns

## Idempotency

Rows are matched on `_row_hash`, so re-running the notebook with unchanged data does not create duplicates.

## Usage

1. Paste your PSGC API token into the **PSGC API Token** widget.
2. Run the extract cells, then the bronze load cell.
3. Check the printed row count, batch ID, and merge metrics.

## Notes

- The load cell requires `all_records` from the extract step; it raises an error if it is missing or empty.
