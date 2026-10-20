# Glossary

Acronyms and terms used in the AHON docs and code.

## Agencies and data sources

| Term | Meaning |
| --- | --- |
| **PSA** | Philippine Statistics Authority. Publishes the PSGC codes and the census population |
| **PSGC** | Philippine Standard Geographic Code. The official 10-digit code for every region, province, city, municipality and barangay. AHON's shared key (`psgc_code`) |
| **PHIVOLCS** | Philippine Institute of Volcanology and Seismology. Publishes the monthly earthquake listings |
| **DTI** | Department of Trade and Industry. Runs the CMCI |
| **CMCI** | Cities and Municipalities Competitiveness Index. Yearly scores for each city and municipality; AHON uses 35 indicators, mainly from the Resiliency pillar |
| **BLGF** | Bureau of Local Government Finance. Publishes the LDRRMF utilization reports |
| **LDRRMF** | Local Disaster Risk Reduction and Management Fund. At least 5% of an LGU's regular income, set aside for disasters: 70% for preparedness and mitigation, 30% for the Quick Response Fund (QRF) |
| **QRF** | Quick Response Fund. The 30% of the LDRRMF kept for response after a disaster |
| **OpenSTAT / PXWeb** | PSA's statistics portal and the software behind it. One population table comes from it; it is not used by the dashboard |


## Places

| Term | Meaning |
| --- | --- |
| **LGU** | Local Government Unit: a province, city, municipality or barangay |
| **CITY / MUN** | City and municipality, the `geographic_level` values AHON analyses |
| **Barangay** | The smallest LGU, inside a city or municipality |
| **DRRM** | Disaster Risk Reduction and Management |
| **BARMM** | Bangsamoro Autonomous Region in Muslim Mindanao |

## Pipeline

| Term | Meaning |
| --- | --- |
| **Databricks** | The cloud platform the pipeline runs on |
| **Unity Catalog** | Databricks' data catalog: catalog > schema > table or volume |
| **Volume** | A folder for files (not tables) inside a schema. Raw files land in `reference.source` |
| **Delta table** | The table format Databricks uses; supports updates (MERGE) and history |
| **DAB** | Databricks Asset Bundle. Jobs and settings defined in `databricks.yml` and `resources/`, deployed with the Databricks CLI |
| **Job / task** | A job (for example `ahon_pipeline`) is a set of tasks; each task runs one script |
| **Serverless** | Databricks compute that starts on demand; no cluster to manage |
| **Batch** | One delivery of source files, in its own folder under the volume, identified by `batch_id` |
| **Medallion layers** | Bronze, silver, gold, platinum: each layer refines the one before |
| **Bronze** | Data as received from the source, in tables, plus provenance columns |
| **Silver** | Cleaned, typed and checked data, with PSGC codes attached |
| **Gold** | Dimensions and facts: the tables analysis is built on |
| **Platinum** | Views built from gold for the dashboard |
| **Dimension / fact** | A dimension describes things (`dim_lgu`: one row per LGU); a fact holds measurements (`fact_ldrrmf`: amounts per LGU and year) |
| **Provenance columns** | Columns starting with `_` that record where and when a row was loaded (`_batch_id`, `_source_ref`, ...) |
| **Reference map** | A reviewed table that links a source's LGU names to PSGC codes (`reference.cmci_lgu_map`, `reference.blgf_ldrrmf_lgu_map`) |
| **ERD** | Entity relationship diagram: how tables link by key |

## Data quality

| Term | Meaning |
| --- | --- |
| **DQ** | Data quality |
| **GX** | Great Expectations, the Python library used for data quality checks |
| **Stop rule** | A check that stops the task before it writes, when the data is wrong |
| **Flag rule** | A check that keeps the rows but marks them with `has_dq_issues` |
| **Reject table** | A silver table ending in `_reject` that holds rows that could not be cleaned |
| **DQ gate** | A set of GX checks run on a table after a layer is built |

## Analysis

| Term | Meaning |
| --- | --- |
| **Risk score** | Earthquake hazard combined with population, ranked 0 to 100 |
| **Preparedness score** | CMCI resiliency and LDRRMF spending, ranked 0 to 100 |
| **Gap** | Risk score minus preparedness score |
| **Percentile rank** | Where a value falls among all cities and municipalities, from 0 to 100 |

See the [analytics method](analytics-method.md) for the full definitions.
