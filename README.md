# AHON-Pipeline

For a fresh checkout, follow the [start-to-finish configuration guide](docs/operations/configuration.md). Its blank template covers every required bundle input; credentials and target settings stay local.

Project AHON is a data-driven disaster and hazard monitoring platform designed to provide actionable insights that strengthen disaster preparedness and decision-making.

The executable batch pipeline covers PSGC/LGU references, city/municipality census population, separate PXWeb source totals, selected CMCI indicators, BLGF disaster funding, PHIVOLCS observations and OSM infrastructure features. The analytical geographic unit is a city or municipality; the canonical population fact is keyed by `(psgc_code, population_year)`. Native source observations remain available for lineage. Risk, vulnerability, preparedness and priority scores remain withheld until their methodology is supported.

GX Core gates dataset quality through the shared rule-metric path. Production uses an explicit run identity and excludes fixture jobs. Enter the PSA PSGC token through the [local secret setup commands](docs/operations/runbook.md#enter-the-psgc-token-locally); only scope/key names belong in configuration.

The complete job has 31 tasks, ending with `quality_checks`: every branch must finish and provide valid GX evidence for the current delivery, and terminal outputs must be nonempty with unique keys. See the [PR #97 review and validation](docs/operations/pr97-gx-review.md) and [completion-barrier runbook](docs/operations/runbook.md#end-to-end-gx-completion-barrier). STANDARD serverless, no schedules and one extra audit task keep the change small.

The [city/municipality grain correction](docs/decisions/0012-city-municipality-analysis-grain.md) changes the earlier barangay population schema. Use a fresh catalog or reviewed migration; the pipeline never overwrites an existing barangay fact automatically.

Start with the [development runbook](docs/operations/runbook.md), [source-to-target inventory and PR decisions](docs/architecture/source-to-target.md), [verification evidence](docs/operations/validation.md), [analytical limitations](docs/architecture/metrics.md), and [reconciled contract decision](docs/decisions/0010-reconciled-source-contracts.md). Use a fresh development catalog; existing incompatible schemas require review. `resources/jobs/` contains executable jobs with no schedules. Production deployment and execution are outside this task.

## Repository structure

```
ahon-pipeline/
├── README.md                    # this file
├── CONTRIBUTING.md              # how the team works: branches, PRs, review, commits
├── .gitignore                   # keeps secrets, local files and data out of git
├── databricks.yml               # Asset Bundle: dev and prod targets, variables, includes resources/
├── .github/
│   ├── workflows/               # CI checks and the reviewer auto-assign
│   ├── ISSUE_TEMPLATE/          # issue templates
│   └── pull_request_template.md # PR template
├── src/                         # pipeline code; SQL and Python files sit together
│   └── sql/                     # for now, all pipeline code sits under src/sql/ (may move up to src/ later)
│       ├── 00_setup/                               # shared: schemas; catalog and volume created by the runner
│       ├── datasets/                               # one folder per source dataset
│       │   └── publisher_dataset/                  # example: replace with the real dataset name
│       │       ├── extract/                        # optional: pulls data from the source and writes raw files
│       │       │   └── extract.py                  # example
│       │       ├── bronze/                         # raw data loaded into bronze.publisher_dataset
│       │       │   ├── load.sql                    # example
│       │       │   └── load_files.py               # example: Python only if the dataset needs it
│       │       └── silver/                         # cleaned data in silver.publisher_dataset_clean
│       │           ├── clean.sql                   # example
│       │           └── clean_rules.py              # example: Python only if the dataset needs it
│       ├── gold/                                   # shared: combines datasets into dimensions and facts
│       ├── platinum/                               # shared: analytics tables built from gold
│       ├── monitoring/                             # shared: data quality rules
│       └── common/                                 # shared reusable Python
├── resources/                   # job and pipeline definitions (YAML), included by databricks.yml
├── notebooks/                   # experiments only; jobs never run these
│   └── exploration/
├── tests/                       # mirrors src/sql/; data quality checks sit next to the code they check
│   ├── datasets/
│   ├── gold/
│   ├── platinum/
│   └── common/
└── docs/
    ├── architecture/            # data model, source-to-target mapping, data-dictonary
    │   └── data-dictionary/     # what each table and column means: README.md is the index, one page per dataset
    ├── standards/               # naming standard
    ├── decisions/               # decision records, one per decision
    ├── governance/              # ownership
    ├── operations/              # monitoring, runbook, CI documentation check
    ├── getting-started/         # local setup for new team members
    └── stakeholder/             # guide for LGU users
```

### Where things go

- **Code:** under `src/sql/` for now (the `sql/` folder may be dropped later). Each dataset has its own folder under `src/sql/datasets/` with a `bronze/` and a `silver/` folder for its SQL, and Python where the dataset needs it. Code that is shared or combines datasets goes in the shared folders.
- **Dataset subfolders:** `bronze/` and `silver/` are the standard ones. If a dataset needs more, its owner can add a subfolder, such as `extract/` for code that pulls data from the source and writes the raw files. Keep the subfolder inside that dataset's folder, and describe it with a comment in this tree or a short note in the dataset's folder.
- **Source files:** files from file-based sources are uploaded to the team's Hugging Face dataset repository and copied, unchanged, into the `source` volume, which is in the `reference` schema (`<catalog>.reference.<source_volume>`), by the dataset's `extract/` code, in one folder named after the dataset. Bronze code reads from the volume only. See [docs/decisions/0004-source-file-landing-hugging-face.md](docs/decisions/0004-source-file-landing-hugging-face.md).
- **Schemas:** folder names match the schema names in the naming standard, with no number prefixes.
- **Setup scripts:** files in `src/sql/00_setup/` have number prefixes (`01_`, `02_`) because they must run in order.
- **Reusable Python:** in `src/sql/common/`, imported by the layer code.
- **Notebooks:** experiments only. Jobs run `.sql` and `.py` files from `src/sql/`.
- **Tests:** in `tests/`, in the same layout as `src/sql/`.
- **Docs:** in `docs/`. Decisions that shape the design are recorded in `docs/decisions/`.
- **Data dictionary:** each dataset has one page in `docs/architecture/data-dictionary/`, named after the dataset, describing its tables and columns. Add or update the page in the same PR that adds or changes the table.

The naming standard is in [docs/standards/naming.md](docs/standards/naming.md), and the team's way of working is in [CONTRIBUTING.md](CONTRIBUTING.md).
