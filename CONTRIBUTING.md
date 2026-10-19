# Contributing

How the team works on this repository. Questions or changes to this page go through a PR like any other change.

## Workflow

1. Pick up an issue, or create one. Every change starts from an issue.
2. Create a branch from `main`.
3. Make the change, run the [local checks](#local-checks), commit, and push the branch.
4. Open a pull request into `main`.
5. Get one approval and green CI, then merge.

To set up your machine and Databricks, see [setup](docs/getting-started/setup.md).

## Branches

All work goes through a branch and a PR into `main`. Do not commit to `main` directly.

Name branches `type/ISSUE_NUMBER-short-description`, in lowercase with hyphens, for example `docs/27-agree-on-naming-standards`.

| Prefix | Use for |
| --- | --- |
| `feature/` | New pipeline work or functionality |
| `bugfix/` | Fixing something that is broken |
| `hotfix/` | Urgent fix |
| `docs/` | Documentation only |
| `chore/` | Maintenance, such as config or cleanup |

The prefix is required and CI rejects other names. The issue number is recommended, not required.

## Commit messages

Use a type and a short summary, for example `docs: add naming standard`. The types match the branch prefixes. This is recommended, not required. Keep the summary short and say what changed.

## Pull requests

- Title: type and summary, in the same style as commit messages.
- Description: fill in the PR template and link the issue with `Closes #NUMBER`. Use `Part of #NUMBER` if the issue needs more than one PR. CI fails without an issue link.
- Keep each PR to one issue.
- Every PR adds or changes at least one `.md` file (CI checks this), so update the docs that your change affects. See [which docs to update](#which-docs-to-update).
- Do not include secrets, credentials, personal data, workspace IDs or your private `variable-overrides.json`.

## CI checks

Two workflows run on every pull request. All checks must pass before merging.

| Check | Workflow | What it does | Fails when |
| --- | --- | --- | --- |
| PR links an issue | `pr-governance.yml` | Looks for `Closes`, `Fixes`, `Resolves`, `Part of` or `Related to` followed by `#NUMBER` in the PR description | No issue reference |
| Branch naming convention | `pr-governance.yml` | Checks the branch prefix | The branch does not start with one of the prefixes above |
| Validate documentation update | `ci.yml` | Lists `.md` files added or modified by the PR | None. Deleted or renamed-only files do not count |
| Validate YAML files | `ci.yml` | `yamllint -f parsable .` | A YAML file breaks the rules in `.yamllint` |
| Lint Python files | `ci.yml` | `ruff check src tests --config pyproject.toml` | Any Ruff error in `src` or `tests` |
| Run tests | `ci.yml` | `pytest tests -q` | A test fails |

`ci.yml` also runs on pushes to `main`, without the documentation check.

Common failures:

- **Documentation check:** "This PR must add or update at least one Markdown documentation file." Update the doc your change affects and push to the same branch. Avoid creating a document only to satisfy CI.
- **Issue link:** edit the PR description to add `Closes #NUMBER`; the check reruns on edit.
- **Ruff reports `spark` or `dbutils` as undefined:** these Databricks globals are declared in `pyproject.toml`. Do not add fake assignments such as `spark = None` to silence it.

## Local checks

Run these before pushing. They are the same checks CI runs and need no cloud compute.

```text
python -m ruff check src tests --config pyproject.toml
python -m pytest tests -q
python -m yamllint -f parsable .
git diff --check
```

`ruff check --fix` can fix some errors automatically; review the changes before committing.

To see which Markdown files your branch adds or changes:

```text
git fetch origin main
git diff --name-only --diff-filter=AM origin/main...HEAD -- "*.md"
```

## Which docs to update

| Change | Update |
| --- | --- |
| New dataset | A profile in `docs/data/data-profile/`, a page per table in `docs/data/data-dictionary/`, a row in `docs/data/sources.md`, the dataset section in `docs/operations/runbook.md` |
| New or changed table or column | The table's page in `docs/data/data-dictionary/`; `docs/architecture/source-to-target.md` if the script or task changed |
| New or changed check | The table's dictionary page; `docs/data-quality/dq-gates.md` for a new gate |
| New or changed job task or bundle variable | `docs/operations/runbook.md`, `docs/getting-started/setup.md`, `docs/architecture/source-to-target.md` |
| A choice someone might question later | A decision record in `docs/decisions/` |
| Bug fix | Optional; update the page that described the wrong behaviour |

## Review and merging

- A reviewer is assigned automatically when the PR is opened, by the rotation in `.github/workflows/auto-reviewer.yml`. You can add more reviewers by hand as a backup.
- One approval is enough to merge.
- The author merges by default. If the author cannot get to a computer soon, they can ask the reviewer to merge.
- Merge commit and squash merge are both fine.

## Decisions

If your change involves a choice that someone might question later, record it in `docs/decisions/`. See the README there for when to write one and how.

## Ownership

Who owns which dataset and area is listed in [docs/project/ownership.md](docs/project/ownership.md).