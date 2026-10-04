# Data dictionary

What each table and column in the pipeline means. One page per dataset, plus one page per gold or platinum table. This index holds the shared rules and the list of pages.

## Pages

| Page | Tables it covers | Layers |
| --- | --- | --- |
| [blgf_ldrrmf_annual_lgu](blgf_ldrrmf_annual_lgu.md) | `blgf_ldrrmf_annual_lgu` | bronze |

## How to add a page

1. Copy [00tablename-dictionary-template.md](00tablename-dictionary-template.md).
2. Rename it after the dataset, exactly as the dataset is named in the code folder, the volume folder and the bronze table (`publisher_dataset.md`). For a gold or platinum table, use the table name (`dim_location.md`).
3. Fill in each section. Remove the instruction comments.
4. Add a row to the table above.

## Rules for every page

- **One page per dataset.** It covers the bronze table and the silver table together, because every silver table comes from one bronze table.
- **Gold and platinum tables** combine datasets, so each gets its own page, named after the table.
- **Provenance columns are listed on every bronze table.** `_source_name`, `_source_ref`, `_ingested_at`, `_batch_id` and `_row_hash` are added to every bronze table, so each page lists them, after the source columns, with the wording from the template. The [naming standard](../../standards/naming.md#provenance-columns) is where they are defined, so if their meaning changes, change it there first and then update the pages. A page also says which columns `_row_hash` covers, because changing that list changes every hash. Whether silver carries them forward is still open in the naming standard, so silver sections list them only once that is decided.
- **Same column table everywhere:** Column, Type, Description, Notes.
  - Description: what the value means, in plain words.
  - Notes: units, allowed values, known quirks. In bronze, also the header the column came from in the source file. Leave Notes empty when there is nothing to add.
- **Column names** follow the naming standard. Its column naming rules are still open, so pages use the names as they exist in the tables and are updated when the rules are agreed.
- **Keep it true to the table.** Change the page in the same PR that changes the table's columns.
