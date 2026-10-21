# Runbook

How to run, rerun, and recover the AHON pipeline.

Before the first run, finish [setup](../getting-started/setup.md): Databricks CLI profile, bundle variable overrides, PSGC token secret, and the reviewed CMCI mapping file.

The pipeline is the job `ahon_pipeline` in `resources/jobs/ahon_job.yml`. Which script builds which table is documented in [source to target](../architecture/source-to-target.md).

## What is what

| Thing | What it is |
| --- | --- |
| Profile | A saved login to your Databricks workspace on your computer. You choose its name. |
| Overrides file | A private JSON file with your settings, such as the catalog name, email, and URLs. It stays on your computer and is never committed. |
| PSGC token | A credential for the PSA PSGC API. It is stored in Databricks as a secret, never in a file, Git, or chat. |
| Bundle | The job definitions in the repository. `deploy` copies them to your workspace, while `run` starts a job. |

## First time

Do the [setup](../getting-started/setup.md) steps once before your first run:

1. [Install the tools](../getting-started/setup.md#1-install-the-tools).
2. [Log in to Databricks](../getting-started/setup.md#2-log-in-to-databricks) with a CLI profile.
3. [Fill in the bundle settings](../getting-started/setup.md#3-fill-in-the-bundle-settings) in your private overrides file, and run its required-key check.
4. [Get and store your PSGC token](../getting-started/setup.md#4-get-and-store-your-psgc-token): request your own token from PSA and store it in your own secret scope.
5. [Place the CMCI mapping file](../getting-started/setup.md#5-place-the-cmci-mapping-file). For a new workspace, this is where `catalog_setup` runs first. Do not run a CMCI task until the uploaded file exists at the configured path.
6. [Validate and deploy](../getting-started/setup.md#7-validate-and-deploy).

## Before every run

1. Pull the branch you intend to run.
2. Validate and deploy that same checkout:

   ```powershell
   databricks bundle validate --strict -t dev --profile <your-profile-name>
   databricks bundle deploy -t dev --profile <your-profile-name>
   ```

3. Confirm that the catalog in your overrides file is the catalog you intend to update.
4. Optionally run the local job-contract test:

   ```powershell
   python -m pytest tests/common/test_job_contract.py -q
   ```

This local test does not use Databricks cloud compute. It confirms that the job tasks reference valid files, dependencies, and settings.

A workspace job does not update automatically when local files change. Deploy the current checkout before running the job.

## Run

The `ahon_pipeline` job creates the catalog and tables, then builds each dataset.

Use a new batch ID for a new source delivery or corrected source load.

### Common run commands

#### Whole pipeline

```powershell
databricks bundle run ahon_pipeline -t dev --profile <your-profile-name>
```

#### One dataset with all required upstream tasks

```powershell
databricks bundle run ahon_pipeline -t dev --profile <your-profile-name> --only +<task>
```

#### Several branches

```powershell
databricks bundle run ahon_pipeline -t dev --profile <your-profile-name> --only "+ldrrmf_gold,+population_gold"
```

#### Different catalog or batch

Add:

```powershell
--params "catalog=<catalog>,batch_id=<batch_id>"
```

`+<task>` includes every required upstream task.

The batch ID defaults to the job run ID. There is no schedule; runs are started manually.

Keep one run at a time because `max_concurrent_runs` is set to `1`.

> **Do not run the whole pipeline until the reviewed Q2_2024 CMCI mapping CSV has been uploaded and `cmci_mapping_file` points to its exact volume path. Otherwise, the CMCI tasks will fail.**

### Everything except CMCI

```powershell
$batch = "<yourname>-run-001"

databricks bundle run ahon_pipeline `
  -t dev `
  --profile <your-profile-name> `
  --params "batch_id=$batch" `
  --only "+population_components,+population_total_gold,+funding_components,+earthquake_components"
```

### PSGC through `dim_lgu`

This is the recommended first test:

```powershell
$batch = "<yourname>-run-001"

databricks bundle run ahon_pipeline `
  -t dev `
  --profile <your-profile-name> `
  --params "batch_id=$batch" `
  --only +dim_lgu
```

The command follows the run and ends with a summary.

Tables appear under:

`Databricks > Catalog > your catalog`

## Run order and dependencies

The job begins with `setup`.

The main PSGC sequence is:

```text
psgc_extract
→ psgc_bronze
→ psgc_silver
→ dim_lgu
```

Dependencies vary by dataset:

- Population, funding, and CMCI use their configured PSGC references.
- Earthquake processing uses PSGC Silver.
- Independent extracts depend only on `setup`.
- One failed source does not necessarily block an unrelated source branch.

## By dataset

### Setup

- **Task:** `setup`
- **Separate job:** `catalog_setup`
- **Does:** Creates the catalog if permitted, the schemas, the `reference.source` volume, and `monitoring.dq_result`.
- **Does not:** Rebuild existing tables.

### PSGC

- **Run:** `--only +dim_lgu`
- **Needs:** `psgc_secret_scope`, `psgc_secret_key`, `psgc_api_base_url`, `psgc_expected_rows`, and `source_period`.
- **Historical baseline:** `Q2_2024` was profiled with 43,760 dimension rows, including 1,642 `CITY` or `MUN` rows.

These historical counts are not current runtime evidence. Verify the selected snapshot and approved counts for the current run.

**If it fails:**

- HTTP or empty-result error: inspect the response for transport failures, rate limiting, outages, permissions, or invalid credentials. Rotate a token only when authentication evidence supports it. The job's `run_as` identity needs `READ` access to the scope.
- `expected N, found M`: PSA may have changed the snapshot. Review the new count before updating `psgc_expected_rows`.
- `invalid_psgc_code`: inspect `silver.psa_psgc_code_reject`.

### PSA Population

- **Run:** `--only +population_components`
- **Needs:** PSGC and `census_year`.
- **Check:** `gold.fact_population` has one row per city and municipality for the census year.
- **Known 2024 condition:** The 19 Sulu municipalities are expected to have `MISSING` status.
- **If it fails:** `city_population_schema_requires_migration` means the catalog contains the old barangay-level table. Use a fresh development catalog or an approved migration.

### PHIVOLCS earthquakes

- **Run:** `--only +earthquake_components`
- **Needs:** `start_month`, `end_month` or `auto`, the archive URL template, latest URL, `source_timezone`, and PSGC Silver.
- **Check:** Reconcile the requested months against the completed landing manifest and source accounting.

A month with no observations does not necessarily produce a grouped Silver-derived summary row.

**If it fails:** `expected exactly one six-column event table` means the PHIVOLCS page layout may have changed or the requested month has not been published. Rerun after the page becomes available.

### CMCI

- **Run:** `--only +cmci_platinum`
- **Mapping source:** `src/sql/datasets/cmci/silver/cmci_reviewed_mapping_Q2_2024.csv`
- **Needs:** The reviewed Q2_2024 mapping uploaded to the `<catalog>.reference.<source_volume>` volume, with `cmci_mapping_file` set to its exact volume path.
- **Also needs:** `cmci_start_year`, `cmci_end_year`, `cmci_run_mode`, the required `cmci_*` request settings, and an available `dim_lgu`.

The mapping columns are:

```text
psgc_code
cmci_name
source_period
reviewed
reviewer
evidence
```

Keep the reviewed mapping unchanged.

Use `cmci_run_mode = test` until a full run is intentionally required.

**Check:**

- `platinum.cmci_pillar_coverage` contains rows for the requested years.
- `silver.cmci_reject` is empty or every rejected row is explained.

**If it fails:**

- Mapping errors: confirm that PSGC codes are unique, CMCI names map one-to-one, `reviewed` is true, and `source_period` matches the snapshot used by `dim_lgu`.
- Indicator dimension mismatch: `resources/cmci_indicators.csv` may have changed. Use a fresh development catalog or review the change and `cmci_metric_version` before proceeding.

### BLGF LDRRMF

- **Run:** `--only +funding_components`
- **Needs:** `hf_repo`, a pinned full-commit `hf_revision`, `ldrrmf_folder`, `ldrrmf_file_pattern`, `ldrrmf_expected_rows`, `ldrrmf_start_year`, `ldrrmf_end_year`, and PSGC.
- **Historical baseline:** 11,434 source reports were profiled for FY2018 through FY2024.

Check the current run's source accounting and match summary. Unresolved matches require explanation instead of relying on a fixed expected number.

**If it fails:**

- `reference_map_coverage`: Bronze contains an LGU name not covered by the mapping. Rerun `ldrrmf_mapping`, review new `needs_review` or `ambiguous` rows, and add a documented override when required.
- `component_totals` or `missing_amounts`: inspect `silver.blgf_ldrrmf_annual_lgu_reject`.
- `unique_matched_lgu_year`: two BLGF spellings map to the same PSGC code in the same year. Resolve the conflict with a reviewed override.

### PSA OpenSTAT population

- **Run:** `--only +population_total_gold`
- **Inputs:** `population_api_url`, `population_source_file`, `population_expected_profile`, `census_year`, and `source_period`.
- **Outputs:** `bronze.psa_population`, `silver.psa_population_clean`, `silver.psa_population_reject`, and `gold.fact_population_total`.

This branch does not provide the canonical census population components.

## Rerun and replay

- **Retry a failed run:** Rerun with the same `batch_id`. Completed landing files are reused.
- **Load a corrected source:** Use a new `batch_id`.
- **Rerun one branch:** Use the same `batch_id` with `--only +<task>`.

Lineage and timestamp preservation depend on each writer's documented update conditions. There is no universal timestamp-stability guarantee.

Keep extraction settings unchanged for a completed batch because the manifest does not bind every requested configuration value.

A rerun does not delete rows. If a row disappears from the source, it may remain in the target unless the writer explicitly implements deletion behavior.

## Recover from a blocked check

A blocking data-quality check stops the current task and skips dependent tasks.

Checks evaluated before a write prevent that write. Earlier successful statements and upstream commits may remain.

1. Open the failed task's output and identify the `check_id` and table.
2. Find the result in `monitoring.dq_result` for the related `run_id`.
3. Inspect the dataset's rejection table or rows where `has_dq_issues` is true.
4. Fix the mapping, override, configuration, or source issue.
5. Rerun the affected branch with `--only +<task>`.

Before treating a Gold table as current, inspect the latest `monitoring.dq_result` rows for its task.

See ../data-quality/README.md for the purpose of each rule.

## When something goes wrong

| What you see | Meaning and fix |
| --- | --- |
| `Valid: NO` | Run the login command in [setup step 2](../getting-started/setup.md#2-log-in-to-databricks) again. |
| Secret scope does not exist | Confirm that you created your developer-specific scope and that `psgc_secret_scope` matches it exactly. |
| Missing required input or JSON error | Rerun the [required-key check](../getting-started/setup.md#check-the-private-overrides-file) and correct the key it identifies. |
| Strict validation warning | Review and fix the warning. Do not remove `--strict` only to bypass it. |
| Deployment appears stuck | Wait several minutes. The first bundle upload can take longer, especially in OneDrive. |
| Pipeline task fails | Open the failed Databricks task and inspect its final error. |
| CMCI task fails | Confirm that the reviewed mapping was uploaded and `cmci_mapping_file` points to its exact reference-volume path. |

When requesting help for a failed task, provide:

- Task name
- Batch ID
- Final error line
- Relevant table or `check_id`

Do not include tokens, private configuration values, or workspace identifiers.

## Do not

- Edit or delete files in a completed batch folder. Keep the batch for investigation and land a new batch.
- Drop or alter tables merely to bypass a schema check. Use a fresh development catalog or an approved migration.
- Point live ingestion at an `ahon_integration_*` catalog. Those catalogs contain fixtures for the `data_quality` job.
- Put tokens, private configuration values, or workspace identifiers in the repository, command arguments, screenshots, logs, or chat.

## Test job

The development-only `data_quality` job runs the real transformations against synthetic fixtures in a fresh `ahon_integration_*` catalog:

```powershell
databricks bundle run data_quality `
  -t dev `
  --profile <your-profile-name> `
  --params "catalog=ahon_integration_<name>"
```

The test job has no automatic retries. Use a fresh integration catalog after a failure.

It tests the transformation code and data-quality behavior, not the availability or current contents of the real external sources.
