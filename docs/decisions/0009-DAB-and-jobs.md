# 0009: Configure Databricks Asset Bundle deployment

- **Status:** Partly superseded by [0015](0015-reconciled-source-contracts.md)
- **Date:** 2026-10-11

## Context

AHON requires a consistent way to define and deploy Databricks jobs from the GitHub repository to the shared Databricks workspace.

Development and initial testing are performed in personal Databricks workspaces. Code is then pushed to GitHub for review and quality checks before deployment to the shared Databricks workspace.

## Decision

Configure AHON as a Databricks Asset Bundle with:

- `databricks.yml` as the main bundle configuration.
- Job definitions stored under `resources/jobs/`.
- `notebooks/`, `src/`, and `tests/` synchronized with the deployed bundle.
- A single `prod` target representing the shared Databricks workspace.
- Production deployment mode enabled for the `prod` target.
- No default production target, so production deployment must be explicitly selected.
- Cluster and SQL Warehouse variables added only when required by exported job definitions.

Template files are provided for:

- Main AHON job configuration.
- Catalog setup job.
- Data quality job.

## Why

- Personal Databricks workspaces already provide the development environment, so a separate bundle `dev` target would duplicate the existing workflow.
- GitHub provides version control, pull request review, and CI quality checks before deployment.
- The shared Databricks workspace serves as the production environment.
- Synchronizing `tests/` allows quality-check code to be available when Databricks jobs require it.
- Keeping compute variables out of `databricks.yml` until they are required avoids unnecessary or unused configuration.
- Separating job definitions from the main bundle configuration keeps the deployment structure easier to maintain.

The resulting workflow is:

**Personal Databricks -> GitHub review and quality checks -> Shared Databricks (`prod`)**

## Alternatives considered

- **Dev and prod bundle targets:** Not selected because development already occurs in personal Databricks workspaces.
- **Hardcoded cluster or warehouse IDs:** Not selected because these should only be configured when required by the deployed job definitions.
- **Syncing documentation files:** Not selected because documentation is stored in GitHub and is not required for Databricks job execution.

## Consequences

- Production deployments must explicitly use the `prod` target.
- Job YAML files can be maintained separately from the main `databricks.yml`.
- `notebooks/`, `src/`, and `tests/` are available to the deployed bundle.
- If an exported Databricks job references `${var.cluster_id}` or `${var.warehouse_id}`, the corresponding variable must be added to `databricks.yml`.
- Future environments such as staging can be added as additional bundle targets if required.

## Open

- Replace the placeholder job YAML files with the exported Databricks job configurations.
- Confirm whether any exported jobs require a cluster ID or SQL Warehouse ID before adding bundle variables.
