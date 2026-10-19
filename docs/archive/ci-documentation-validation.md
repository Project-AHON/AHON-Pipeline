# CI Pull Request Validation

## Purpose

The pull request CI workflow validates only the files introduced or modified by the pull request.

The workflow currently enforces two repository checks:

1. At least one relevant Markdown documentation file must be added or updated.
2. Python linting must pass for Python files changed by the pull request.

Limiting Python linting to the pull request diff prevents unrelated pre-existing issues elsewhere in the repository from blocking a focused change. Contributors remain responsible for ensuring that every changed Python file meets the current linting standard.

## Documentation Validation

### Rule

Every pull request must add or update at least one Markdown documentation file.

The documentation check passes when at least one file with the `.md` extension is added or modified in the pull request.

The documentation file:

- may be new or existing;
- may be located anywhere in the repository;
- does not need to match the pull request title; and
- must be included in the same pull request as the related code, workflow, or configuration changes.

Deleted or renamed-only Markdown files do not count as documentation updates. At least one Markdown file must contain a genuine addition or modification.

### Contributor Instructions

Before requesting review:

1. Identify the documentation affected by the change.
2. Update the most relevant existing Markdown file when possible.
3. Add a new document only when the change introduces a distinct dataset, design decision, workflow, or operational procedure.
4. Commit the documentation with the related implementation changes.
5. Confirm that the documentation validation check passes.

Acceptable documentation updates include:

- updating setup or usage instructions;
- documenting a new pipeline, dataset, table, or workflow;
- updating architecture or operational guidance;
- recording a design decision;
- documenting configuration or dependency changes; and
- clarifying repository standards or maintenance procedures.

Avoid creating an unnecessary document solely to satisfy CI.

### CI Behavior

For pull request events, the workflow compares the pull request base commit with the pull request head commit.

The check:

1. identifies files added or modified by the pull request;
2. filters the changed paths for files ending in `.md`;
3. prints the matching documentation paths; and
4. fails when no added or modified Markdown file is found.

The documentation validation step does not run on regular push events.

### Common Failure

The check fails when the pull request contains no added or modified Markdown file.

Expected failure message:

```text
ERROR: This PR must add or update at least one Markdown documentation file.
Update an existing .md file or add a new one before merging.
```

To resolve the failure:

1. Identify the documentation affected by the implementation change.
2. Update an existing Markdown file or add a relevant new document.
3. Commit and push the documentation change to the same pull request branch.
4. Rerun CI or allow the new push to trigger the workflow.

### Local Documentation Check

To view files changed between the current branch and the remote `main` branch:

```bash
git fetch origin main
git diff --name-only --diff-filter=AM origin/main...HEAD
```

To show only added or modified Markdown files:

```bash
git diff --name-only --diff-filter=AM origin/main...HEAD   | grep -E '\.md$'
```

If the pull request targets a branch other than `main`, replace `origin/main` with the correct base branch.

## Python Lint Validation

### Rule

Python linting runs only against Python files added or modified by the pull request.

The lint check does not scan every Python file in the repository. This keeps the CI result focused on the current change and prevents unrelated legacy lint issues from blocking the pull request.

A changed Python file must pass the repository's configured Ruff rules before the pull request can be merged.

Ruff configuration is defined in:

```text
pyproject.toml
```

Project-specific runtime globals such as `spark` and `dbutils` are declared there so Databricks source files can be linted without false undefined-name failures.

### Files Included

The lint check includes Python files that are:

- added by the pull request; or
- modified by the pull request.

The lint check excludes Python files that are:

- unchanged outside the pull request diff;
- deleted by the pull request; or
- present only in the base branch without modification.

Renamed Python files are checked when Git reports the renamed file as added or modified content in the pull request diff.

### CI Behavior

For pull request events, the workflow:

1. compares the pull request base and head commits;
2. identifies added or modified `.py` files;
3. prints the Python files included in the lint scope;
4. skips Ruff when the pull request contains no added or modified Python files; and
5. runs Ruff against only the changed Python files when at least one is present.

A Ruff violation in a changed Python file fails the lint job.

### Contributor Instructions

Before requesting review:

1. Identify the Python files changed by the branch.
2. Run Ruff locally against those files.
3. Fix all reported errors in the changed files.
4. Review the diff to ensure lint fixes did not introduce unrelated changes.
5. Commit and push the fixes to the pull request branch.

Do not modify unrelated Python files solely to make the pull request lint check pass.

### Local Python File Check

To list added or modified Python files against `origin/main`:

```bash
git fetch origin main
git diff --name-only --diff-filter=AM origin/main...HEAD   | grep -E '\.py$'
```

If the pull request targets another base branch, replace `origin/main` accordingly.

### Local Ruff Check

Run Ruff against all Python files changed by the pull request:

```bash
git diff --name-only --diff-filter=AM origin/main...HEAD   | grep -E '\.py$'   | xargs -r ruff check
```

If Ruff is installed through the project environment, use the repository's normal environment or package command instead of installing an unpinned version.

### Automatic Fixes

Ruff can automatically fix some violations:

```bash
git diff --name-only --diff-filter=AM origin/main...HEAD   | grep -E '\.py$'   | xargs -r ruff check --fix
```

Review all automatic changes before committing. Some lint findings require a manual correction and cannot be fixed safely by Ruff.

### Common Failures

#### Undefined Databricks globals

If Ruff reports `spark` or `dbutils` as undefined, confirm that `pyproject.toml` contains:

```toml
[tool.ruff]
builtins = ["spark", "dbutils"]
```

Do not add fake assignments such as `spark = None` or `dbutils = None` to production code solely to silence linting.

#### Import or formatting violations

Open the reported file and line, apply the indicated correction, and rerun Ruff against the changed files.

#### No Python files changed

The lint step should exit successfully without running Ruff when the pull request contains no added or modified `.py` files.

#### Pull request diff uses the wrong base

If local results differ from CI, confirm that the local comparison uses the actual pull request base branch. A branch targeting `dev` must be checked against `origin/dev`, not `origin/main`.

## Pull Request Checklist

Before requesting review:

1. Confirm that all intended code and configuration files are included.
2. Add or update relevant Markdown documentation.
3. Run Ruff against the Python files changed by the pull request.
4. Review the staged diff for unrelated files, secrets, and personal configuration.
5. Push the branch and confirm both CI checks pass.

## See Also

- [Repository README](../../README.md)
- [Decision Records](../decisions/)
- [Data Documentation](../data/)
- [Operations Documentation](../operations/)
- [`pyproject.toml`](../../pyproject.toml)
