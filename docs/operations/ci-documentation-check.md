# CI Documentation Check

This document describes the CI documentation validation check that runs on every pull request.

## Overview

Every pull request to this repository must add or update at least one Markdown (`.md`) documentation file before it can be merged.

## Purpose

This check ensures that all changes—especially code changes—are accompanied by documentation updates. This keeps the documentation in sync with the codebase and makes the knowledge discoverable.

## What counts as documentation

The check looks for any file path ending in `.md` that is:
- **Added** (new file)
- **Modified** (updated file)

Files that are deleted or only renamed do not satisfy the check. At least one file must be genuinely added or updated with new content.

## Where to add or update docs

Documentation can live in:

- **Dataset data dictionary:** `docs/architecture/data-dictionary/` — describes source data and bronze/silver tables
- **Decision records:** `docs/decisions/` — records design decisions
- **Standards:** `docs/standards/` — naming, schema, and process standards
- **Getting started:** `docs/getting-started/` — guides for new team members
- **Operations:** `docs/operations/` — runbooks, troubleshooting, CI checks like this one
- **README files:** `README.md` at the root or in any folder

## Example scenarios

| Change | Required docs | Example |
| --- | --- | --- |
| Add a new dataset | Data dictionary page | `docs/architecture/data-dictionary/my_dataset.md` |
| Change a data model | Update data dictionary or add decision | Update `docs/architecture/data-dictionary/existing_dataset.md` |
| Refactor code | Update relevant docs | Update `docs/operations/runbook.md` or `docs/standards/naming.md` |
| Fix a bug | Optional but encouraged | Add a note to an operations doc or update README |

## How the check works

The CI workflow checks pull requests and compares the base branch (main or dev) with the PR branch. It:

1. Gets the base commit SHA and PR head commit SHA
2. Runs `git diff --name-only --diff-filter=AM` to list added or modified files
3. Filters for files ending in `.md`
4. Exits with success (0) if at least one is found, or failure (1) if none are found

## Bypassing (not recommended)

There is no bypass for this check. Every change should have accompanying documentation. If your change genuinely needs no documentation, that is a signal to reconsider the scope of the change or to document it anyway.

## See also

- [README.md](../../README.md) — repository structure and what goes where
- [docs/decisions/](../decisions/) — decision records, one per design decision
- [docs/architecture/](../architecture/) — data model and schema docs
