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

## Step 1: Log in to Databricks once

Run from PowerShell:

```powershell
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile <your-profile-name>
databricks auth profiles
```

Your profile should show `Valid: YES`. If it shows `Valid: NO`, run the login command again.

## Step 2: Create your private overrides file

The authoritative configuration template is:

`config/variable-overrides.example.json`

Do not copy configuration values from archived documentation, previous job runs, another developer's overrides file, or an older version of this runbook.

From the repository root, where `databricks.yml` is located, run:

```powershell
New-Item -ItemType Directory -Path .databricks/bundle/dev -Force | Out-Null

Copy-Item `
  config/variable-overrides.example.json `
  .databricks/bundle/dev/variable-overrides.json
```

Open the copied file in Notepad:

```powershell
notepad .databricks/bundle/dev/variable-overrides.json
```

Complete every required value according to [setup](../getting-started/setup.md).

At minimum, confirm these developer-specific settings:

- `run_as.user_name`: your Databricks login email
- `catalog`: your unique development catalog
- `workspace_root`: your isolated Databricks workspace path
- `psgc_secret_scope`: your developer-specific secret scope
- `cmci_mapping_file`: the exact volume path of the approved reviewed Q2_2024 CMCI mapping CSV

The repository includes the approved reviewed mapping at:

`src/sql/datasets/cmci/silver/cmci_reviewed_mapping_Q2_2024.csv`

Upload this file using the procedure in [setup](../getting-started/setup.md). Then set `cmci_mapping_file` to its exact uploaded volume path.

Do not create a new mapping, modify the reviewed file, or substitute an unreviewed mapping.

Keep `.databricks/bundle/dev/variable-overrides.json` private. Never commit it or paste its contents into an issue, pull request, log, screenshot, or chat.

### Check the required configuration keys

This check prints key names only. It does not print configuration values:

```powershell
$settings = Get-Content `
  .databricks/bundle/dev/variable-overrides.json `
  -Raw | ConvertFrom-Json

$required = (
  Get-Content config/variable-overrides.example.json -Raw |
  ConvertFrom-Json
).PSObject.Properties.Name

$missing = @(
  $required | Where-Object {
    $_ -notin $settings.PSObject.Properties.Name -or
    $null -eq $settings.$_ -or
    (
      $settings.$_ -is [string] -and
      [string]::IsNullOrWhiteSpace($settings.$_)
    )
  }
)

"Keys expected: $($required.Count)"
"Missing, null, or blank: $($missing.Count)"
$missing
```

Expected result:

```text
Missing, null, or blank: 0
```

If keys are missing, update your private overrides file. Do not add undocumented keys only because they appeared in an older runbook or another developer's configuration.

## Step 3: Request and save your PSGC token in Databricks once

### 3a. Request a token from the Philippine Statistics Authority

1. Open the official [PSA Statistical Classification Systems API page](https://psa.gov.ph/classifications-api).
2. Open the [PSA API Access Guide](https://psa.gov.ph/classifications-api/access-guide).
3. Follow the **Request Your API Token** instructions.
4. Check your email for the token issued by PSA. If you already have a valid token, you can use it instead.

PSA tokens must not be shared, sold, lent, committed to Git, or pasted into chat.

Each developer must follow PSA's current token-request and usage requirements. See the [PSA API terms and conditions](https://psa.gov.ph/classifications-api/terms-and-conditions).

### 3b. Store the token in your Databricks secret scope

Use a developer-specific scope so personal PSA tokens do not overwrite one another in a shared workspace.

For example, use `ahon-dev-maria` for a developer whose identifier is `maria`.

Replace `ahon-dev-maria` below with your own unique scope name. Set `psgc_secret_scope` in `variable-overrides.json` to that exact same name.

A different Databricks CLI profile does not create a separate secret namespace within the same workspace.

Run:

```powershell
databricks secrets create-scope ahon-dev-maria --profile <your-profile-name>
databricks secrets put-secret ahon-dev-maria psgc-api-token --profile <your-profile-name>
```

The second command asks you to paste your token at a hidden prompt. You will not see the characters. Paste the token without quotes and press Enter.

Check that the key exists:

```powershell
databricks secrets list-secrets ahon-dev-maria --profile <your-profile-name>
```

You should see `psgc-api-token`.

If `create-scope` says the scope already exists, do not create it again. Continue only if the scope belongs to you and you intend to use it.

**Important:** `put-secret` overwrites an existing value. Run it again only when intentionally rotating or replacing your own token. Never use another developer's scope for your personal token.

The token belongs only at the hidden prompt. Never put it in the overrides file, command arguments, Git, chat, screenshots, or logs.

## Step 4: Check the setup and deploy

Confirm that you have completed:

- Databricks CLI authentication
- Bundle variable overrides
- PSGC token secret
- `cmci_mapping_file` set to the planned volume path

For a new workspace, deploy and run `catalog_setup` first, then upload the reviewed CMCI mapping using the procedure in [setup](../getting-started/setup.md). Do not run a CMCI task until the uploaded file exists at the configured path.

Run strict validation:

```powershell
databricks bundle validate --strict -t dev --profile <your-profile-name>
```

Do not remove `--strict` to bypass an unexpected warning. Review and resolve the warning.

After validation passes, deploy:

```powershell
databricks bundle deploy -t dev --profile <your-profile-name>
```

Deployment uploads the repository files and creates the configured jobs in your workspace. It may take several minutes, especially if the repository is stored in OneDrive.

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
   python -m pytest tests/common/testjobcontract.py -q
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
| `Valid: NO` | Run the login command from Step 1 again. |
| Secret scope does not exist | Confirm that you created your developer-specific scope and that `psgc_secret_scope` matches it exactly. |
| Missing required input or JSON error | Rerun the required-key check from Step 2 and correct the key it identifies. |
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
