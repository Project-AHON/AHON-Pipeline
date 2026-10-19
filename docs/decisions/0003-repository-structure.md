# 0003: Repository structure

- **Status:** Accepted
- **Date:** 2026-10-04

## Context

The existing draft structure used numbered code folders (`01_bronze_ingest`), mart-style names that no longer match the `platinum` schema, and many mostly empty docs. We need one structure that matches the naming standard (0001) before more code is added, and the project will have several datasets, with SQL, reusable Python and notebooks.

## Decision

- Code folders are named after the schemas: `setup`, `datasets`, `gold`, `platinum`, `monitoring`, `common`. There are no number prefixes, except on files in `setup/`, where run order matters.
- Bronze and silver code is grouped by dataset under `src/datasets/DATASET_NAME/`. Gold, platinum, monitoring, setup and common code is shared.
- SQL and Python files sit together in the same folder, with no language subfolders.
- Reusable Python lives in `src/common/`. Notebooks live in `notebooks/` and are never run by jobs.
- Tests mirror `src/`, and data quality checks sit next to the code they check.
- `databricks.yml` is at the root, and job definitions are in `resources/`.
- `docs/` keeps only the agreed set of files for now (see `README.md`).

## Why

- Folder names that match schema names make code easy to find and match the naming standard.
- Grouping by dataset means adding a dataset adds one folder, and nothing else moves.
- Keeping notebooks out of production paths keeps reviews and tests simple.

## Alternatives considered

- **Layer-first grouping (`src/bronze/`, `src/silver/`):** simpler with few datasets, but each new dataset touches every layer folder.
- **Language folders (`sql/`, `py/`):** rejected for now. Few files per folder, and the extension already shows the language.
- **`sources/`, `pipelines/` instead of `datasets/`:** rejected. `datasets/` matches the words the team already uses.
- **Numbered code folders:** rejected, as in 0001.

## Consequences

- Existing numbered folders and `src/images/` are removed or renamed, and the setup SQL needs updating to the new schema names.
- `README.md` describes the structure.
- Docs removed for now (`overview`, `workflow`, `job-setup`, `deployment`, `source-profiles`, `ingestion`, `validation`) return when their tasks need them.

## Open

- Whether to add a `LICENSE` (the repo is public).
- Whether to reorganize `docs/` subfolders as more docs are written.