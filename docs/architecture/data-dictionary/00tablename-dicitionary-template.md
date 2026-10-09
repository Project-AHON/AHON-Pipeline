# DATASET_NAME

<!--
How to use this template:
1. Copy this file and rename it after the dataset (publisher_dataset.md),
   or after the table for a gold or platinum table (dim_location.md).
2. Replace the text in each section. Keep descriptions short.
3. Delete sections that do not apply, for example the silver section while
   there is no silver table yet.
4. Remove these comments, and add the page to the list in README.md.
Keep the five provenance rows at the end of every bronze table. Their meaning
is defined in the naming standard, so change them there first.
-->

- **Source:** publisher and where the files or data come from
- **Coverage:** period, geography and update frequency
- **Owner:** who to ask about this dataset

## Bronze: `ahon.bronze.DATASET_NAME`

- **One row is:** what a single row represents
- **Loaded by:** path of the load code, for example `src/.../DATASET_NAME/bronze/`
- **Row hash covers:** which source columns go into `_row_hash` (for example "all columns in the table below")

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `column_name` | string | What the value means | Source header, units, allowed values, quirks |
| `_source_name` | string | The dataset the row came from | Always `DATASET_NAME` |
| `_source_ref` | string | Where the row was loaded from: the raw file path | Never includes keys or tokens |
| `_ingested_at` | timestamp (UTC) | When the row was loaded into bronze | |
| `_batch_id` | string | The load run that wrote the row | Links to the run log in `monitoring` |
| `_row_hash` | string | SHA-256 of the raw source columns as received | Covers the source columns above, not the provenance columns |

## Silver: `ahon.silver.DATASET_NAME_clean`

- **One row is:** what a single row represents
- **Key:** the columns that identify one row
- **Built from bronze by:** path of the silver code

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `column_name` | string | What the value means | Bronze column it comes from, or how it is derived |

## Known issues

Data quality problems and limits that someone using the table should know. Remove this section if there are none.

## Open

Questions that remain, if any. Remove this section if there are none.
