# Source to target

Which script builds which table, from what, and in which job task. Paths are relative to `src/sql/`. Tasks are in `resources/jobs/ahon_job.yml` (job `ahon_pipeline`). Catalog and source-volume names are job parameters.

Raw files land under `/Volumes/<catalog>/reference/<source_volume>/<dataset>/<batch_id>/`. `_SUCCESS.json` records file name, SHA-256 and parsed source-row count. Existing completed batches cannot change; source corrections use a new batch ID.

## PSGC

Source: PSA PSGC classification API, snapshots `Q2_2024`, `April_2024`, `Q4_2023`, `Q2_2021`; selected snapshot `Q2_2024`. Needs the PSA token secret.

| Target | Built from | Script | Task |
| --- | --- | --- | --- |
| Raw JSONL, one file per snapshot | PSA API | `datasets/psa_psgc/extract/psa_psgc_ingest.py` | `psgc_extract` |
| `bronze.psa_psgc` | Raw JSONL | `datasets/psa_psgc/bronze/psa_psgc.py` | `psgc_bronze` |
| `silver.psa_psgc_clean`, `silver.psa_psgc_reject`, `silver.psa_psgc_code_reject` | `bronze.psa_psgc` | `datasets/psa_psgc/silver/psa_psgc_clean.py` | `psgc_silver` |
| `gold.dim_lgu` | `silver.psa_psgc_clean` (selected snapshot) | `gold/dim_lgu.py` | `dim_lgu` |

## PSA Population

Source: PSA census population inside the PSGC API records (`populations_json`); census year set by `census_year`.

| Target | Built from | Script | Task |
| --- | --- | --- | --- |
| `silver.psgc_city_population_clean` | `silver.psa_psgc_clean` (`CITY`, `MUN`) | `datasets/psa_psgc/silver/clean_city_population.py` | `psgc_population_silver` |
| `gold.fact_population` | `gold.dim_lgu`, `silver.psgc_city_population_clean` | `gold/fact_population.py` (query in `gold/fact_population_candidate.sql`) | `population_gold` |
| `platinum.population_components` (view) | `gold.fact_population` | `platinum/population_components.sql` | `population_components` |

## PHIVOLCS earthquakes

Source: PHIVOLCS monthly listings, months from `start_month` to `end_month`.

| Target | Built from | Script | Task |
| --- | --- | --- | --- |
| Raw CSV, one file per month | PHIVOLCS website | `datasets/philvolcs_earthquake/extract/philvolcs_earthquake_ingest.py` | `earthquake_extract` |
| `bronze.philvolcs_earthquake_observation` | Raw CSV | `datasets/philvolcs_earthquake/observation_pipeline.py --stage bronze` | `earthquake_bronze` |
| `silver.philvolcs_earthquake_observation_clean`, `silver.philvolcs_earthquake_reject` | `bronze.philvolcs_earthquake_observation`, `silver.psa_psgc_clean` | `datasets/philvolcs_earthquake/observation_pipeline.py --stage silver` | `earthquake_silver` |
| `platinum.earthquake_observation_summary` (view) | `silver.philvolcs_earthquake_observation_clean` | `platinum/earthquake_components.sql` | `earthquake_components` |

No earthquake gold table yet: event identity and LGU assignment are not approved.

## CMCI

Source: DTI CMCI portal, years `cmci_start_year` to `cmci_end_year`, 35 selected indicators (`resources/cmci_indicators.csv`). Needs the reviewed mapping CSV (`cmci_mapping_file`).

| Target | Built from | Script | Task |
| --- | --- | --- | --- |
| Raw HTML responses | CMCI portal | `datasets/cmci/pipeline.py --stage extract` | `cmci_extract` |
| `bronze.cmci_raw_indicator_batch_html` | Raw responses | `datasets/cmci/pipeline.py --stage bronze` | `cmci_bronze` |
| `reference.cmci_lgu_map` | Reviewed mapping CSV, `gold.dim_lgu` | `datasets/cmci/pipeline.py --stage bronze` | `cmci_bronze` |
| `silver.cmci_indicator_clean`, `silver.cmci_reject` | `bronze.cmci_raw_indicator_batch_html` | `datasets/cmci/pipeline.py --stage silver` | `cmci_silver` |
| `gold.fact_cmci_indicator`, `gold.dim_cmci_indicator` | `silver.cmci_indicator_clean`, `silver.cmci_reject`, `resources/cmci_indicators.csv` | `datasets/cmci/pipeline.py --stage gold` | `cmci_gold` |
| `platinum.cmci_pillar_coverage` | `gold.fact_cmci_indicator`, `gold.dim_cmci_indicator` | `datasets/cmci/pipeline.py --stage platinum` | `cmci_platinum` |

## BLGF LDRRMF

Source: seven BLGF workbooks, FY2018 to FY2024, from a pinned Hugging Face commit (`hf_repo`, `hf_revision`).

| Target | Built from | Script | Task |
| --- | --- | --- | --- |
| Raw workbooks | Hugging Face dataset | `datasets/blgf_ldrrmf_annual_lgu/extract/extract_blgf_ldrrmf_annual_lgu.py` | `ldrrmf_extract` |
| `bronze.blgf_ldrrmf_annual_lgu` | Raw workbooks | `datasets/blgf_ldrrmf_annual_lgu/bronze/load_blgf_ldrrmf_annual_lgu.py` | `ldrrmf_bronze` |
| `reference.blgf_ldrrmf_lgu_map` | `bronze.blgf_ldrrmf_annual_lgu`, `silver.psa_psgc_clean`, `blgf_ldrrmf_lgu_map_overrides.csv` | `datasets/blgf_ldrrmf_annual_lgu/silver/build_blgf_ldrrmf_lgu_map.py` | `ldrrmf_mapping` |
| `silver.blgf_ldrrmf_annual_lgu_clean`, `silver.blgf_ldrrmf_annual_lgu_reject` | `bronze.blgf_ldrrmf_annual_lgu`, `reference.blgf_ldrrmf_lgu_map` | `datasets/blgf_ldrrmf_annual_lgu/silver/clean_blgf_ldrrmf_annual_lgu.sql` | `ldrrmf_silver` |
| `gold.fact_ldrrmf` | `silver.blgf_ldrrmf_annual_lgu_clean`, `gold.dim_lgu` | `gold/build_fact_ldrrmf.sql` | `ldrrmf_gold` |
| `platinum.funding_components` (view) | `gold.fact_ldrrmf`, `gold.dim_lgu` | `platinum/funding_components.sql` | `funding_components` |

## Setup

| Target | Script | Task |
| --- | --- | --- |
| Schemas `bronze`, `silver`, `gold`, `platinum`, `reference`, `monitoring` | `00_setup/00_catalog_schema_setup.sql` | `setup` (also job `catalog_setup`) |

## Also in the job, not used downstream

| Dataset | Targets | Script | Tasks |
| --- | --- | --- | --- |
| PSA OpenSTAT population (table `0241A6DPUP1`) | `bronze.psa_population`, `silver.psa_population_clean`, `silver.psa_population_reject`, `gold.fact_population_total` | `datasets/psa_population/...`, `gold/build_fact_population_total.py` | `population_extract`, `population_bronze`, `population_silver`, `population_total_gold` |
| OpenStreetMap critical infrastructure | `bronze.osm_critical_infrastructures`, `silver.osm_critical_infrastructures_clean` | `datasets/osm_critical_infrastructures/pipeline.py` | `osm_extract`, `osm_bronze`, `osm_silver` |
