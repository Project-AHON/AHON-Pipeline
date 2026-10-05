# CI Documentation Validation

## Purpose

Every pull request must add or update at least one Markdown documentation
file. This keeps repository documentation aligned with code, workflow, and
configuration changes.

## Validation Rule

The CI workflow checks the files added or modified in the pull request.

The check passes when at least one file with the `.md` extension is added or
updated. The documentation file:

- May be new or existing.
- May be located anywhere in the repository.
- Does not need to match the pull request title.
- Must be included in the same pull request as the related changes.

The check does not count deleted Markdown files as documentation updates.

## Contributor Instructions

Before requesting review:

1. Identify the documentation affected by the change.
2. Add or update at least one relevant Markdown file.
3. Commit the documentation with the related code changes.
4. Confirm that the `Validate documentation update` check passes.

Examples of acceptable documentation updates include:

- Updating setup or usage instructions.
- Documenting a new pipeline, dataset, or workflow.
- Updating architecture or operational guidance.
- Recording changes to repository governance or standards.

Avoid creating an unnecessary document solely to pass CI. Update the most
relevant existing documentation whenever possible.

## CI Behavior

For pull request events, the workflow compares the pull request base and head
commits. It identifies added or modified Markdown files and prints their paths.

If no Markdown file was added or modified, the workflow fails and asks the
contributor to update the repository documentation.

The documentation validation step does not run on regular push events.

### Common failure and remediation

A pull request fails this check when no Markdown files are added or modified in the branch compared with the base branch. The validation is intentionally strict: it looks for files with the `.md` extension in the PR diff and exits with an error when none are found.

The failure message is:

> ERROR: This PR must add or update at least one Markdown documentation file.
> Update an existing .md file or add a new one before merging.

This check does not count deleted Markdown files as valid documentation updates. It only accepts added or modified files.

To resolve the failure:

1. Identify the documentation affected by the code or workflow change.
2. Update an existing Markdown file or add a new one in the same PR.
3. Commit the documentation change together with the related code changes.
4. Re-run CI or push the updated branch.

Examples of acceptable documentation updates include:
- Updating setup or usage instructions
- Documenting new pipeline steps or configuration
- Explaining operational or maintenance procedures
- Clarifying repository conventions, expectations, or workflows

Example of a relevant check locally:

```bash
git diff --name-only origin/main...HEAD
