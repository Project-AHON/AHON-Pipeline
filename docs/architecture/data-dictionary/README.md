# Data dictionary

What each table and column in the pipeline means. One page per dataset, plus one page per gold or platinum table. This index holds the shared rules and the list of pages.

## Pages

| Page | Tables it covers | Layers |
| --- | --- | --- |
| [blgf_ldrrmf_annual_lgu](blgf_ldrrmf_annual_lgu.md) | `blgf_ldrrmf_annual_lgu` | bronze |
| [psa_population](psa_population.md) | `psa_population` | bronze, silver |
| [philvolcs_earthquake_data](philvolcs_earthquake_data.md) | `philvolcs_earthquake_data`, `philvolcs_earthquake_data_clean` | bronze, silver |
| [dq_results](dq_results.md) | `monitoring.dq_results` | monitoring |
| [philvolcs_earthquake_data](philvolcs_earthquake_data.md) | `philvolcs_earthquake_observation`, `philvolcs_earthquake_observation_clean` | bronze, silver |

The supported runtime contracts are now the following pages. The older CMCI and population DQ pages are explicitly marked historical; their prior table names and orchestration must not be used to deploy this branch.

| Current page | Contract |
| --- | --- |
| [PSGC](psa_psgc_data.md) | Raw observations and current snapshot cleaning |
| [LGU dimension](dim_lgu.md) | One selected snapshot and standardized hierarchy |
| [City/municipality population](fact_population.md) | City/municipality PSGC × census year, NULL preservation |
| [Population sources](psa_population.md) | Separate PXWeb administrative totals and dispositions |
| [CMCI](cmci_dictionary.md) | Reviewed identities, selected long-form indicators and coverage |
| [Funding fact](fact_ldrrmf.md) | Original reporting unit × fiscal year, pesos and ratios |
| [Earthquake observations](philvolcs_earthquake_data.md) | Observation grain, invalid retention and withheld locality assignment |
| [Source-to-target inventory](../source-to-target.md) | All table paths, keys, reference periods and OSM feature contract |


## How to add a page

1. Copy [the dictionary template](00tablename-dicitionary-template.md).
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
