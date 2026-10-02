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
