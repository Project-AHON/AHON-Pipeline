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
│   ├── setup/                   # shared: catalogs, schemas, volumes, control table
│   ├── datasets/                # one folder per source dataset: its bronze and silver code
│   │   └── publisher_dataset/   # example: replace with the real dataset name
│   │       ├── bronze/          # raw data loaded into bronze.publisher_dataset
│   │       │   ├── load.sql         # example
│   │       │   └── load_files.py    # example: Python only if the dataset needs it
│   │       └── silver/          # cleaned data in silver.publisher_dataset_clean
│   │           ├── clean.sql        # example
│   │           └── clean_rules.py   # example: Python only if the dataset needs it
│   ├── gold/                    # shared: combines datasets into dimensions and facts
│   ├── platinum/                # shared: analytics tables built from gold
│   ├── monitoring/              # shared: data quality rules, run log
│   └── common/                  # shared reusable Python
├── resources/                   # job and pipeline definitions (YAML), included by databricks.yml
├── notebooks/                   # experiments only; jobs never run these
│   └── exploration/
├── tests/                       # mirrors src/; data quality checks sit next to the code they check
│   ├── datasets/
│   ├── gold/
│   ├── platinum/
│   └── common/
└── docs/
    ├── architecture/            # data model, source-to-target mapping, data dictionary
    ├── standards/               # naming standard
    ├── decisions/               # decision records, one per decision
    ├── governance/              # ownership
    ├── operations/              # monitoring, runbook, CI documentation check
    ├── getting-started/         # local setup for new team members
    └── stakeholder/             # guide for LGU users
```

### Where things go

- **Code:** under `src/`. Each dataset has its own folder under `src/datasets/` with a `bronze/` and a `silver/` folder for its SQL, and Python where the dataset needs it. Code that is shared or combines datasets goes in the shared folders.
- **Schemas:** folder names match the schema names in the naming standard, with no number prefixes. The `source` schema holds raw files in a Unity Catalog Volume, so it has no folder here.
- **Setup scripts:** files in `src/setup/` have number prefixes (`01_`, `02_`) because they must run in order.
- **Reusable Python:** in `src/common/`, imported by the layer code.
- **Notebooks:** experiments only. Jobs run `.sql` and `.py` files from `src/`.
- **Tests:** in `tests/`, in the same layout as `src/`.
- **Docs:** in `docs/`. Decisions that shape the design are recorded in `docs/decisions/`.

The naming standard is in [docs/standards/naming.md](docs/standards/naming.md), and the team's way of working is in [CONTRIBUTING.md](CONTRIBUTING.md).


