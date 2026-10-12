# Deployment, scheduling, backfill and recovery

## Prerequisites

Authorized development profile: 7474651621867565, workspace dbc-74becaaa-f34b.cloud.databricks.com. Setup creates an absent isolated catalog with serverless default storage when the identity has CREATE CATALOG; otherwise an administrator must provision it first. Setup also needs USE CATALOG, CREATE SCHEMA/CREATE VOLUME. Dataset jobs need USE SCHEMA, READ/WRITE VOLUME, CREATE TABLE and SELECT/MODIFY on their own outputs. Analysts receive SELECT only on approved outputs. Do not grant broad ownership merely to run a job.

Provide a PSA API token under bundle variables psgc_secret_scope/key (defaults psa/psgc_token); never put secrets in Git/logs. Hugging Face source files use the pinned revision in databricks.yml. CMCI requires /Volumes/<catalog>/reference/source/cmci_lgu_map.csv with columns psgc_code,cmci_name,source_period,reviewed,reviewer,evidence. Codes exactly ten digits, Q2_2024 snapshot, reviewed=true, unique code/name, nonempty reviewer/evidence. Human review cannot be replaced with automatic fuzzy matching. BLGF contextual reviewed overrides are packaged beside its mapper.

## Development commands

From the repository root:

```powershell
databricks bundle validate --strict --target dev --profile 7474651621867565
databricks bundle deploy --target dev --profile 7474651621867565
databricks bundle run setup --target dev --profile 7474651621867565
databricks bundle run psgc --target dev --profile 7474651621867565
databricks bundle run ldrrmf --target dev --profile 7474651621867565
databricks bundle run population --target dev --profile 7474651621867565
databricks bundle run earthquake --target dev --profile 7474651621867565
databricks bundle run cmci --target dev --profile 7474651621867565
databricks bundle run evidence --target dev --profile 7474651621867565
```

Alternatively run pipeline after source prerequisites are satisfied. Independent dataset jobs remain runnable when another source is unavailable. Parent uses child defaults; backfill individual jobs with their window parameters.

## Isolated synthetic verification

The suite creates only a dedicated integration catalog. Never use a real source catalog. It runs each exercised stage twice inside one execution; correction history is retained, so a new full-suite execution needs a fresh catalog suffix.

```powershell
databricks bundle deploy --target dev --profile 7474651621867565 --var catalog=ahon_integration_20261013
databricks bundle run integration --target dev --profile 7474651621867565 --var catalog=ahon_integration_20261013
```

Return deployment to ahon_dev before running sources: catalog applies to all bundle jobs. Integration fixtures are synthetic, not scientific or nationwide-source validation.

## Schedules and backfills

All schedules PAUSED, Asia/Manila. PSGC quarterly; fiscal monthly availability check; population/CMCI annual; earthquake monthly day 5. Activate only after source access/coverage review. Earthquake auto means the previous completed Manila month. Historical 404s fail instead of falling back to the latest feed.

```powershell
databricks bundle run earthquake --target dev --profile 7474651621867565 --params start_month=2024-01,end_month=2024-03
databricks bundle run cmci --target dev --profile 7474651621867565 --params start_year=2024,end_year=2024
databricks bundle run evidence --target dev --profile 7474651621867565 --params reporting_year=2024
```

PSGC extraction uses the bounded snapshots/counts in its extractor. Fiscal is FY2018–2024; population 2024. New coverage requires reviewed source-contract/count changes and a pinned revision. Changing source_period does not extend extraction or migrate the dimension.

## Recovery and rollback

Inspect task output, monitoring.dq_result, monitoring.run_log and rejects. ERROR stops dependants; WARN permits retained evidence. SQL assertions fail execution. Inspect failed statement output for its actual rule. Reconcile using each dataset's grain; a generic one-to-one Bronze/fact count is incorrect.

Repair the failed task after fixing its prerequisite. Same-run retry reuses verified immutable landing. Changed source content requires a new run/batch. Never edit a completed batch. Incomplete batches cannot reach Bronze. Repeated rejects merge by observation; valid corrections update newer keys; invalid corrections can retain an earlier accepted value and need explicit review.

Schema differences fail rather than automatically migrate. Archive evidence and approve a migration, or use a new catalog. Active earthquake tables are distinct from legacy event-hash tables; legacy Gold diagnostics are not configured for a fresh catalog.

To roll back code: pause affected schedules and redeploy a reviewed prior Git revision. This does not undo committed Delta data. Record each affected table's version and dependencies before separately approving RESTORE or reconstruction. Multiple table writes are not atomic; reconcile and rerun dependants after recovery.

Production validation: bundle validate --strict --target prod. Configure approved run_as_user identity, minimal grants, secret access, ownership and egress. Production deployment and schedule activation were not authorized or performed.

Troubleshooting: missing catalog → administrator/default-storage SQL provisioning; missing secret → configured scope/key; missing CMCI mapping → reviewed CSV; changed headers/counts → investigate source version, do not weaken checks; uncertain geography → contextual reviewed override, never name-only assignment.

Targeted reference verification (fresh integration catalog): deploy dev with --var catalog=ahon_integration_20261013_ref, then run integration with the same variable and --params verification_entry=tests/integration/verify_reference.py. This exercises setup, actual PSGC Silver/Gold and malformed-code quarantine without accessing the PSA API.

The synthetic integration task deliberately has no automatic retry: intermediate correction assertions require fresh fixture state. Dataset tasks retain bounded retries. To rerun a failed full suite use a fresh integration catalog, or run the targeted components entry against its existing synthetic fixture catalog. Final-accounting parameters follow [Databricks dynamic task-state references](https://docs.databricks.com/aws/en/jobs/dynamic-value-references).

Independent ingestion without a geographic reference is available through task selection, e.g. bundle run population --target dev --profile 7474651621867565 --only extract,bronze,silver, or bundle run ldrrmf with --only extract,bronze. Later mapping/Gold tasks still require PSGC. Run-accounting tasks excluded by --only do not mark that partial run as a completed full dataset release.
