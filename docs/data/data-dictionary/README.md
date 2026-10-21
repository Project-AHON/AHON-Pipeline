# Data dictionary

What each table and column in the pipeline means. One page per table, named after the table. This index holds the shared rules and the list of pages.

For the source as received (where it comes from, coverage, quirks), see the [data profiles](../data-profile/README.md). For how tables connect, see the [data model](../../architecture/data-model.md) and [source-to-target](../../architecture/source-to-target.md).

## Pages

### PSGC (owner: Yanna)

| Page | Table | Layer |
| --- | --- | --- |
| [psa_psgc](psa_psgc.md) | `bronze.psa_psgc` | bronze |
| [psa_psgc_clean](psa_psgc_clean.md) | `silver.psa_psgc_clean` | silver |
| [psa_psgc_code_reject](psa_psgc_code_reject.md) | `silver.psa_psgc_code_reject` | silver (reject) |
| [psa_psgc_reject](psa_psgc_reject.md) | `silver.psa_psgc_reject` | silver (reject) |
| [dim_lgu](dim_lgu.md) | `gold.dim_lgu` | gold |

### PSA population (owner: Crizza)

PSA Population comes in `bronze.psa_psgc` (`populations_json`); it has no bronze table of its own.

| Page | Table | Layer |
| --- | --- | --- |
| [psgc_city_population_clean](psgc_city_population_clean.md) | `silver.psgc_city_population_clean` | silver |
| [fact_population](fact_population.md) | `gold.fact_population` | gold |
| [population_components](population_components.md) | `platinum.population_components` | platinum (view) |

### PHIVOLCS earthquake (owner: Jessy)

| Page | Table | Layer |
| --- | --- | --- |
| [philvolcs_earthquake_observation](philvolcs_earthquake_observation.md) | `bronze.philvolcs_earthquake_observation` | bronze |
| [philvolcs_earthquake_observation_clean](philvolcs_earthquake_observation_clean.md) | `silver.philvolcs_earthquake_observation_clean` | silver |
| [philvolcs_earthquake_reject](philvolcs_earthquake_reject.md) | `silver.philvolcs_earthquake_reject` | silver (reject) |
| [earthquake_observation_summary](earthquake_observation_summary.md) | `platinum.earthquake_observation_summary` | platinum (view) |

### CMCI (owner: Maki)

| Page | Table | Layer |
| --- | --- | --- |
| [cmci_raw_indicator_batch_html](cmci_raw_indicator_batch_html.md) | `bronze.cmci_raw_indicator_batch_html` | bronze |
| [cmci_lgu_map](cmci_lgu_map.md) | `reference.cmci_lgu_map` | reference |
| [cmci_indicator_clean](cmci_indicator_clean.md) | `silver.cmci_indicator_clean` | silver |
| [cmci_reject](cmci_reject.md) | `silver.cmci_reject` | silver (reject) |
| [fact_cmci_indicator](fact_cmci_indicator.md) | `gold.fact_cmci_indicator` | gold |
| [dim_cmci_indicator](dim_cmci_indicator.md) | `gold.dim_cmci_indicator` | gold |
| [cmci_pillar_coverage](cmci_pillar_coverage.md) | `platinum.cmci_pillar_coverage` | platinum |

### BLGF LDRRMF (owner: Crystal)

| Page | Table | Layer |
| --- | --- | --- |
| [blgf_ldrrmf_annual_lgu](blgf_ldrrmf_annual_lgu.md) | `bronze.blgf_ldrrmf_annual_lgu` | bronze |
| [blgf_ldrrmf_lgu_map](blgf_ldrrmf_lgu_map.md) | `reference.blgf_ldrrmf_lgu_map` | reference |
| [blgf_ldrrmf_annual_lgu_clean](blgf_ldrrmf_annual_lgu_clean.md) | `silver.blgf_ldrrmf_annual_lgu_clean` | silver |
| [blgf_ldrrmf_annual_lgu_reject](blgf_ldrrmf_annual_lgu_reject.md) | `silver.blgf_ldrrmf_annual_lgu_reject` | silver (reject) |
| [fact_ldrrmf](fact_ldrrmf.md) | `gold.fact_ldrrmf` | gold |
| [funding_components](funding_components.md) | `platinum.funding_components` | platinum (view) |

Monitoring outputs are documented in [DQ results](dq_result.md) and [pipeline runs](../../operations/pipeline-runs.md).

## How to add a page

1. Copy [the dictionary template](00-template.md).
2. Rename it after the table, without the schema: `psa_psgc_clean.md` for `silver.psa_psgc_clean`.
3. Fill in each section. Remove the instruction comments and any section that does not apply.
4. Add a row to the table above, under its dataset.

## Rules for every page

- **One page per table.** Bronze, silver, reject, reference, gold and platinum tables each get their own page, named after the table. Views are documented the same way and marked "(view)".
- **Header block first:** table name, what one row is, key, what it is built from (or loaded by), owner, and the reject table if there is one. Bronze pages also link to the dataset's [data profile](../data-profile/README.md) and say which columns `_row_hash` covers, because changing that list changes every hash.
- **Same column table everywhere:** Column, Type, Description, Notes.
  - Description: what the value means, in plain words.
  - Notes: units, allowed values, known quirks. In bronze, also the source field or header the column came from. Leave Notes empty when there is nothing to add.
- **Provenance columns.** Every bronze table lists `_source_name`, `_source_ref`, `_source_digest`, `_ingested_at`, `_batch_id`, `_row_hash` and `_observation_id` after the source columns, with the wording from the template. Silver and gold pages list the lineage columns they carry in one row, plus `_processed_at`. The [naming standard](../../architecture/naming-standard.md#provenance-columns) is where they are defined; if their meaning changes, change it there first and then update the pages.
- **Rules section** for silver and gold: each check and what it does (stop, reject, flag, fix or warning).
- **Column names** follow the naming standard. Pages use the names as they exist in the tables.
- **Keep it true to the table.** Change the page in the same PR that changes the table's columns.
