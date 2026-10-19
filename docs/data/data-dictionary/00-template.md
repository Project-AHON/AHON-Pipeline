# table_name

<!--
How to use this template:
1. Copy this file and rename it after the table, without the schema
   (psa_psgc_clean.md for silver.psa_psgc_clean).
2. Replace the text in each section. Keep descriptions short.
3. Delete lines and sections that do not apply, for example "Rejects" when
   there is no reject table, or the provenance rows on a gold table.
4. Remove these comments, and add the page to the list in README.md.
Keep the seven provenance rows at the end of every bronze table. Their meaning
is defined in the naming standard, so change them there first.
-->

- **Table:** `schema.table_name`
- **Source:** (bronze only) publisher, and a link to the dataset's data profile
- **One row is:** what a single row represents
- **Key:** the columns that identify one row
- **Built from:** source tables and the code that builds it (bronze: **Loaded by:** the extract and load code)
- **Row hash covers:** (bronze only) which source columns go into `_row_hash`
- **Owner:** who to ask about this table
- **Rejects:** the reject table, if there is one

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `column_name` | string | What the value means | Source field or derivation, units, allowed values, quirks |
| `_source_name` | string | The dataset the row came from | Always `dataset_name` |
| `_source_ref` | string | Where the row was loaded from: the raw file path | Never includes keys or tokens |
| `_source_digest` | string | SHA-256 of the landed file | |
| `_ingested_at` | timestamp (UTC) | When the row was loaded into bronze | |
| `_batch_id` | string | The load run that wrote the row | |
| `_row_hash` | string | SHA-256 of the raw source columns as received | Covers the source columns above, not the provenance columns |
| `_observation_id` | string | Identity of this observation | Hash of `_batch_id`, `_source_digest` and `_row_hash` |

<!-- Silver and gold: replace the provenance rows above with one lineage row and _processed_at:
| `_source_name`, `_source_ref`, `_ingested_at`, `_batch_id`, `_row_hash` | | Source lineage carried from bronze | |
| `_processed_at` | timestamp (UTC) | When the row was last changed | |
-->

## Rules

| Rule | Action |
| --- | --- |
| What is checked | Stop, reject, flag, fix or warning |

## Notes

Things someone using the table should know: grain caveats, known issues, verified row counts. Remove this section if there are none.