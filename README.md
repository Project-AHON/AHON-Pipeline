# AHON-Pipeline

Project AHON is a data-driven disaster and hazard monitoring platform designed to provide actionable insights that strengthen disaster preparedness and decision-making.

Earthquake data in the Philippines is spread across agencies, formats and place names, so local leaders rarely see the full picture. AHON aims to combine these sources with reviewed city and municipality identities to show where earthquake risk is high, where preparedness falls short, and what to strengthen first. The main product is the AHON dashboard; this repository holds the pipeline that feeds it.

## Implementation status

The current pipeline publishes geographic references, reported census population, CMCI indicator measurements and coverage, disaster-fund components, and earthquake observation summaries.

The analytical questions below describe the project's intended outcomes. Composite LGU risk, preparedness and vulnerability rankings are not yet implemented as approved pipeline outputs. Earthquake locality candidates are reported, but observations are not assigned to LGUs as exposure or impact.

The dashboard remains under development. See [dashboard status](docs/insights/dashboard.md).

## Questions AHON aims to answer

1. What is each LGU's earthquake risk level?
2. How prepared is each LGU relative to its risk?
3. Which LGUs have high risk but low preparedness?
4. Which areas of preparedness should these LGUs strengthen first?

Users, stakeholders and expected impact are in the [problem statement](docs/project/problem-statement.md). How the measures are calculated is in the [analytics method](docs/architecture/analytics-method.md).

## Data sources

| Dataset | Publisher | Used for |
| --- | --- | --- |
| Philippine Standard Geographic Code (PSGC) | Philippine Statistics Authority (PSA) | The shared LGU codes and names (`gold.dim_lgu`) |
| PSA Population | PSA (census, from the PSGC API) | Exposure: residents per city and municipality |
| Earthquake listings | Philippine Institute of Volcanology and Seismology (PHIVOLCS) | Hazard: earthquake counts, magnitude and depth |
| Cities and Municipalities Competitiveness Index (CMCI) | Department of Trade and Industry (DTI) | Preparedness: resiliency indicators |
| Local Disaster Risk Reduction and Management Fund (LDRRMF) utilization | Bureau of Local Government Finance (BLGF) | Preparedness: disaster fund budget and spending |

Details, periods and what is out of scope: [sources](docs/data/sources.md).

## How it works

```mermaid
flowchart LR
  A["Public sources<br/>(PSA, PHIVOLCS,<br/>DTI, BLGF)"] --> B["Raw files<br/>(never edited)"]
  B --> C["Bronze<br/>as received"]
  C --> D["Silver<br/>cleaned and checked"]
  D --> E["Gold<br/>analysis tables"]
  E --> F["Platinum<br/>dashboard views"]
  F --> G[["AHON dashboard"]]
```

AHON is a batch pipeline on Databricks. Each source is downloaded into a folder of raw files, then refined in four layers: **bronze** keeps the data as received, **silver** cleans and types source data, preserves quality findings, and attaches reviewed PSGC identities where supported. Earthquake observations retain locality candidates with geographic assignment withheld; unmatched funding reports retain a NULL PSGC code. **Gold** builds the tables used for analysis, and **platinum** prepares the views the dashboard reads. Build checks record quality results. Earlier successful writes are not rolled back when a later check fails.

New to the acronyms? See the [glossary](docs/architecture/glossary.md).

Layer-by-layer diagram: [architecture overview](docs/architecture/overview.md), [data model](docs/architecture/data-model.md), [data quality](docs/data-quality/README.md).

## Getting started

New to the repository? Read in this order:

1. This README, and the [glossary](docs/architecture/glossary.md) for acronyms.
2. [Architecture overview](docs/architecture/overview.md): how data moves through the layers.
3. [Setup](docs/getting-started/setup.md): tools, Databricks access, settings and your first run.
4. [Runbook](docs/operations/runbook.md): running one dataset, rerunning and fixing a failed run.
5. [CONTRIBUTING](CONTRIBUTING.md): branches, pull requests and CI checks.

Once set up, the commands are:

```text
databricks bundle validate --strict -t dev --profile <profile>
databricks bundle deploy -t dev --profile <profile>
databricks bundle run ahon_pipeline -t dev --profile <profile>
```

Credentials (Databricks login, PSA token) and your bundle settings stay on your machine or in Databricks secrets, never in the repository.

## Documentation

| Folder | What is in it |
| --- | --- |
| [docs/architecture/glossary.md](docs/architecture/glossary.md) | Acronyms and terms |
| [docs/project](docs/project/) | Problem statement, assumptions and limitations, ownership |
| [docs/data](docs/data/) | Sources, a data profile per source, and a data dictionary page per table |
| [docs/architecture](docs/architecture/) | Overview, data model, source to target, analytics method, naming standard |
| [docs/data-quality](docs/data-quality/) | How checks work, the DQ gates and the results table |
| [docs/operations](docs/operations/) | Runbook: running, rerunning and recovering the pipeline |
| [docs/getting-started](docs/getting-started/) | Setup for new team members |
| [docs/decisions](docs/decisions/) | Decision records, one per decision |
| [docs/archive](docs/archive/) | Older pages kept for reference |

## Repository structure

```
AHON-Pipeline/
â”œâ”€â”€ README.md                    # this file
â”œâ”€â”€ CONTRIBUTING.md              # how the team works: branches, PRs, CI checks, review
â”œâ”€â”€ databricks.yml               # Asset Bundle: dev and prod targets, variables, includes resources/
â”œâ”€â”€ config/
â”‚   â””â”€â”€ variable-overrides.example.json   # template for your private bundle settings
â”œâ”€â”€ .github/
â”‚   â”œâ”€â”€ workflows/               # CI checks and the reviewer auto-assign
â”‚   â”œâ”€â”€ ISSUE_TEMPLATE/          # issue templates
â”‚   â””â”€â”€ pull_request_template.md # PR template
â”œâ”€â”€ resources/
â”‚   â”œâ”€â”€ jobs/                    # ahon_pipeline, catalog_setup, data_quality (dev only)
â”‚   â””â”€â”€ cmci_indicators.csv      # the selected CMCI indicators
â”œâ”€â”€ src/
â”‚   â””â”€â”€ sql/                     # all pipeline code, SQL and Python together
â”‚       â”œâ”€â”€ 00_setup/            # catalog, schemas, volume and monitoring tables
â”‚       â”œâ”€â”€ datasets/            # one folder per source dataset
â”‚       â”‚   â””â”€â”€ <dataset>/
â”‚       â”‚       â”œâ”€â”€ extract/     # pulls data from the source and writes raw files
â”‚       â”‚       â”œâ”€â”€ bronze/      # loads raw files into bronze.<dataset>
â”‚       â”‚       â””â”€â”€ silver/      # cleans into silver.<dataset>_clean (+ _reject)
â”‚       â”œâ”€â”€ gold/                # dimensions and facts, combining datasets
â”‚       â”œâ”€â”€ platinum/            # analysis views built from gold
â”‚       â”œâ”€â”€ monitoring/          # DQ gates per dataset
â”‚       â””â”€â”€ common/              # shared Python: runners, quality checks, DQ gate
â”œâ”€â”€ notebooks/
â”‚   â””â”€â”€ exploration/             # experiments only; jobs never run these
â”œâ”€â”€ tests/                       # unit and job contract tests, mirroring src/sql/
â””â”€â”€ docs/                        # see Documentation above
```

### Where things go

- **Code:** each dataset has its own folder under `src/sql/datasets/`, named after the dataset, with `bronze/` and `silver/` folders (and `extract/` when it pulls from the source). Code that is shared or combines datasets goes in the shared folders (`gold/`, `platinum/`, `common/`).
- **Source files:** file-based sources are uploaded to the team's Hugging Face dataset repository and copied unchanged into the `<catalog>.reference.<source_volume>` volume by the dataset's `extract/` code, in one folder per dataset and batch. Bronze code reads from the volume only ([decision 0004](docs/decisions/0004-source-file-landing-hugging-face.md)).
- **Jobs:** tasks are defined in `resources/jobs/`. Jobs run `.sql` and `.py` files from `src/sql/`, never notebooks.
- **Setup scripts:** files in `src/sql/00_setup/` have number prefixes because they run in order.
- **Tests:** in `tests/`, in the same layout as `src/sql/`.
- **Docs:** add or update the table's [data dictionary](docs/data/data-dictionary/README.md) page in the same PR that adds or changes the table. Record design choices in `docs/decisions/`.

Names follow the [naming standard](docs/architecture/naming-standard.md).

## Team

Yanna, Maki, Jessy, Crystal and Crizza. Who owns which dataset: [ownership](docs/project/ownership.md).

How to contribute: [CONTRIBUTING](CONTRIBUTING.md).

## Layer-gate integration

This integration candidate adds 12 layer-gate tasks after their corresponding builds. Census and OpenSTAT population have independent Silver and Gold gates. Blocking results stop dependent tasks; warning results remain visible. Gate and build checks share `monitoring.dq_result`. Earlier writes are not rolled back.
