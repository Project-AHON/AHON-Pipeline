# Runbook

How to run, rerun and recover the AHON pipeline. Before the first run, finish [setup](../getting-started/setup.md): Databricks CLI profile, bundle variable overrides, PSGC token secret and the reviewed CMCI mapping file.

The pipeline is the job `ahon_pipeline` in `resources/jobs/ahon_job.yml`. Which script builds which table is in [source to target](../architecture/source-to-target.md).

## What is what

| Thing | What it is |
| --- | --- |
| Profile | A saved login to your Databricks workspace on your computer. You pick its name. |
| Overrides file | A private JSON file with your own settings (catalog name, your email, URLs). The job has blanks and this file fills them. It stays on your computer and is never committed. |
| PSGC token | A password for the PSA PSGC API. It is stored inside Databricks as a secret, never in a file, Git or chat. |
| Bundle | The job definitions in the repo. `deploy` copies them to your workspace, `run` starts a job. |

## Step 1: Log in to Databricks (once)

```powershell
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile <your-profile-name>
databricks auth profiles
```

Your profile should show `Valid: YES`. If it says NO, run the login line again.

## Step 2: Create your overrides file (once)

```powershell
New-Item -ItemType Directory -Path .databricks/bundle/dev -Force | Out-Null
notepad .databricks/bundle/dev/variable-overrides.json
```

Notepad asks to create the file. Paste the template below, change the **three marked values**, save.

```json
{
  "run_as": { "user_name": "<your Databricks login email>" },
  "catalog": "ahon_dev_<yourname>",
  "source_volume": "source",
  "source_period": "Q2_2024",
  "census_year": 2024,
  "hf_repo": "Jess-Christine/Project-AHON",
  "hf_revision": "9273d4b4d5af6d2a14e52451c45ac70a6e3012f5",
  "psgc_secret_scope": "ahon",
  "psgc_secret_key": "psgc-api-token",
  "cmci_mapping_file": "/Volumes/ahon_dev_<yourname>/reference/source/<reviewed-Q2_2024-mapping-filename>.csv",
  "cmci_start_year": 2014,
  "cmci_end_year": 2024,
  "start_month": "2024-01",
  "end_month": "2024-01",
  "workspace_root": "/Workspace/Users/<your Databricks login email>/ahon-<yourname>-dev",
  "environment_version": "6",
  "job_timeout_seconds": 14400,
  "task_timeout_seconds": 3600,
  "task_retries": 1,
  "retry_interval_millis": 30000,
  "max_concurrent_runs": 1,
  "psgc_api_base_url": "https://classification.psa.gov.ph/psgc",
  "psgc_expected_rows": "{\"Q2_2024\":43762,\"April_2024\":43761,\"Q4_2023\":43758,\"Q2_2021\":43798}",
  "population_api_url": "https://openstat.psa.gov.ph/PXWeb/api/v1/en/DB/1A/PO_2024/0241A6DPUP1.px",
  "population_source_file": "0241A6DPUP1.csv",
  "population_expected_profile": "[1,18,118,1607]",
  "ldrrmf_expected_rows": "{\"2018\":1513,\"2019\":1566,\"2020\":1612,\"2021\":1637,\"2022\":1683,\"2023\":1707,\"2024\":1716}",
  "ldrrmf_file_pattern": "FY{year}-LDRRMF-by-LGU.xlsx",
  "ldrrmf_folder": "data/ldrrmf",
  "ldrrmf_start_year": 2018,
  "ldrrmf_end_year": 2024,
  "cmci_portal_url": "https://cmci.dti.gov.ph/data-portal.php",
  "cmci_process_url": "https://cmci.dti.gov.ph/data-portal-process.php",
  "cmci_test_limit": 5,
  "cmci_request_batch_size": 10,
  "cmci_request_delay": 1,
  "cmci_http_retries": 3,
  "cmci_http_backoff": 2,
  "cmci_metric_version": "selected_35_v1",
  "osm_folder": "data/critical-infrastructures",
  "osm_shapefile_stems": "[\"schools\",\"hospitals\",\"police_station\",\"fire_station\"]",
  "request_timeout": 60,
  "earthquake_archive_url_template": "https://earthquake.phivolcs.dost.gov.ph/EQLatest-Monthly/{year}/{year}_{month_name}.html",
  "earthquake_latest_url": "https://earthquake.phivolcs.dost.gov.ph/",
  "cmci_run_mode": "test",
  "source_timezone": "Asia/Manila",
  "cmci_user_agent": "AHON-Pipeline"
}
```

The three values to change:
1. `run_as`: your Databricks login email.
2. `catalog`: your own catalog name, for example `ahon_dev_maria`. Use a name nobody else uses.
3. `workspace_root`: your email and name again, so your files stay separate from everyone else's.

Notes:
- Keep the commas and quotes exactly as shown. A stray comma gives a JSON error.
- Use the reviewed Q2_2024 CMCI mapping CSV shipped in the repository; do not leave `cmci_mapping_file` empty. Upload that file using the repository's existing source-file upload procedure, then replace the example volume path in `cmci_mapping_file` with the exact path returned by that procedure under your `<catalog>.reference.<source_volume>` volume. Keep the reviewed file unchanged; do not create a new mapping or substitute an unreviewed one.
- The values come from the team's earlier dev job. If something fails on a URL or `hf_revision`, ask the dataset owner whether it is still current.
- Check the file has every key (prints only names, never values):

```powershell
$settings = Get-Content .databricks/bundle/dev/variable-overrides.json -Raw | ConvertFrom-Json
$required = (Get-Content config/variable-overrides.example.json -Raw | ConvertFrom-Json).PSObject.Properties.Name
$missing = @($required | Where-Object { $_ -notin $settings.PSObject.Properties.Name -or $null -eq $settings.$_ })
"Keys expected: $($required.Count); missing or null: $($missing.Count)"
$missing
```

## Step 3: Request and save your PSGC token in Databricks (once)

### 3a. Request a token from the Philippine Statistics Authority

1. Open the official [PSA Statistical Classification Systems API page](https://psa.gov.ph/classifications-api).
2. Open the [PSA API Access Guide](https://psa.gov.ph/classifications-api/access-guide) and follow its **Request Your API Token** instructions to submit the request form.
3. Check your email for the token issued by PSA. If you already have a valid token, you can use it instead.

PSA tokens must be kept secure and must not be shared, sold, lent, committed to Git, or pasted into chat. Each developer should follow PSA's current token-request and usage requirements. See the [PSA API terms and conditions](https://psa.gov.ph/classifications-api/terms-and-conditions).

### 3b. Store the token as a Databricks secret

Run these commands. **The second command asks you to paste your token at a hidden prompt** (you will not see the characters). Paste it without quotes and press Enter.

```powershell
databricks secrets create-scope ahon --profile <your-profile-name>
databricks secrets put-secret ahon psgc-api-token --profile <your-profile-name>
```

Check it saved (this shows the key name, never the value):

```powershell
databricks secrets list-secrets ahon --profile <your-profile-name>
```

You should see `psgc-api-token`. If `create-scope` says the scope already exists, skip it and run only `put-secret`. You only do this once, and again only if the token changes.

The token goes only at that prompt. Never put it in the overrides file, a command line, Git or chat. Do not include the token in screenshots or logs.

## Step 4: Check the setup and deploy

Before the first run, finish setup: Databricks CLI profile, bundle variable overrides, PSGC token secret and the reviewed CMCI mapping file. The pipeline is the job `ahon_pipeline` in `resources/jobs/ahon_job.yml`. Which script builds which table is in `../architecture/source-to-target.md`.

Run:

```powershell
databricks bundle validate -t dev --profile <your-profile-name>
databricks bundle deploy -t dev --profile <your-profile-name>
```

Validate may print one warning about `.local/**`. That is harmless. Deploy uploads the repo files and creates three jobs in your workspace. It can take a few minutes, longer if the repo is in OneDrive.

## Before every run

1. Pull the branch you are running and deploy that same checkout. A workspace job does not update when local files change.

   ```powershell
   databricks bundle validate --strict -t dev --profile <your-profile-name>
   databricks bundle deploy -t dev --profile <your-profile-name>
   ```

2. Check that the catalog in your overrides file is the one you mean to write to.
3. Optional local check, without cloud compute:

   ```powershell
   python -m pytest tests/common/test_job_contract.py -q
   ```

   This confirms every task's files, dependencies and settings.

## Run

One job, `ahon_pipeline`, does everything: it creates the catalog and tables, then builds each dataset. Use any new batch name.

### Common run commands

| What | Command |
| --- | --- |
| Whole pipeline | `databricks bundle run ahon_pipeline -t dev --profile <your-profile-name>` |
| One dataset, with everything it needs | `databricks bundle run ahon_pipeline -t dev --profile <your-profile-name> --only +<task>` |
| Several branches | `--only +ldrrmf_gold,+population_gold` |
| A different catalog or batch | add `--params "catalog=<catalog>,batch_id=<batch_id>"` |

`+<task>` includes every upstream task. The batch ID defaults to the job run ID. There is no schedule; runs are started by hand. Keep one run at a time (`max_concurrent_runs` = 1).

**Do not run the whole job without `--only` until the reviewed Q2_2024 CMCI mapping CSV has been uploaded using the existing procedure and `cmci_mapping_file` points to it**, because the CMCI tasks will fail.

### Everything except CMCI

```powershell
$batch = "<yourname>-run-001"
databricks bundle run ahon_pipeline -t dev --profile <your-profile-name> --params "batch_id=$batch" --only "+population_components,+population_total_gold,+funding_components,+earthquake_components"
```

### PSGC only (a good first test, shorter)

```powershell
$batch = "<yourname>-run-001"
databricks bundle run ahon_pipeline -t dev --profile <your-profile-name> --params "batch_id=$batch" --only +psgc_silver
```

The command follows the run and ends with a summary. Tables appear in your catalog (Databricks > Catalog > your catalog).

## Run order and dependencies

Run order inside the job: `setup` first, then PSGC (`psgc_extract` → `psgc_bronze` → `psgc_silver` → `dim_lgu`). Dependencies vary: population, funding and CMCI use their configured PSGC references; earthquakes use PSGC Silver. Independent extracts depend only on `setup`, so one failed source does not block another.

## By dataset

### Setup

- **Task:** `setup` (also the separate job `catalog_setup`)
- **Does:** creates the catalog if permitted, the schemas, the `reference.source` volume and `monitoring.dq_result`. It never rebuilds existing tables.

### PSGC

- **Run:** `--only +dim_lgu`
- **Needs:** the PSA token secret (`psgc_secret_scope`, `psgc_secret_key`), `psgc_api_base_url`, `psgc_expected_rows`, `source_period`.
- **Historical baseline:** `Q2_2024` was profiled with 43,760 dimension rows, including 1,642 `CITY`/`MUN` rows. Verify the selected snapshot and approved counts for the current run; these numbers are not current runtime evidence.
- **If it fails:**
  - HTTP or empty-result error: inspect the actual response for transport failures, rate limiting, outages, permissions or invalid credentials. Rotate a token only when authentication evidence supports it; the job's `run_as` identity needs READ on the scope.
  - "expected N, found M": PSA changed the snapshot. Review the new count before updating `psgc_expected_rows`.
  - `invalid_psgc_code`: see `silver.psa_psgc_code_reject`.

### PSA Population

- **Run:** `--only +population_components`
- **Needs:** PSGC, `census_year`.
- **Check:** `gold.fact_population` has one row per city and municipality for the census year. For 2024, 19 Sulu municipalities are `MISSING`; that is expected.
- **If it fails:** `city_population_schema_requires_migration` means the catalog still has the old barangay-level table. Use a fresh catalog.

### PHIVOLCS earthquakes

- **Run:** `--only +earthquake_components`
- **Needs:** `start_month`, `end_month` (or `auto` for last month), the archive URL template and latest URL, `source_timezone`, and PSGC silver.
- **Check:** reconcile requested months against the completed landing manifest and source accounting. A month with no observations need not produce a grouped Silver-derived summary row.
- **If it fails:** "expected exactly one six-column event table" means the PHIVOLCS page layout changed or the month is not published yet. No file is published, so rerun once the page is available.

### CMCI

- **Run:** `--only +cmci_platinum`
- **Needs:** the reviewed Q2_2024 mapping CSV shipped in the repository, uploaded with the existing source-file upload procedure to the `<catalog>.reference.<source_volume>` volume, and `cmci_mapping_file` set to that uploaded file's path. The CSV columns are `psgc_code,cmci_name,source_period,reviewed,reviewer,evidence`; retain the reviewed file unchanged. Also configure `cmci_start_year`, `cmci_end_year`, `cmci_run_mode` and the `cmci_*` request settings, and ensure `dim_lgu` is available. Use `cmci_run_mode = test` until a full run is intended.
- **Check:** `platinum.cmci_pillar_coverage` has rows for the requested years; `silver.cmci_reject` is empty or explained.
- **If it fails:**
  - Mapping errors: codes must be unique, names one-to-one, `reviewed` true, and `source_period` equal to `dim_lgu`.
  - Indicator dimension mismatch: `resources/cmci_indicators.csv` changed. Use a fresh catalog, or review the change and its `cmci_metric_version` first.

### BLGF LDRRMF

- **Run:** `--only +funding_components`
- **Needs:** `hf_repo` and a pinned full-commit `hf_revision`, `ldrrmf_folder`, `ldrrmf_file_pattern`, `ldrrmf_expected_rows`, `ldrrmf_start_year`, `ldrrmf_end_year`, and PSGC.
- **Historical baseline:** 11,434 source reports were profiled for FY2018–FY2024. Check the selected run's actual source accounting and match summary; unresolved matches require explanation, not a fixed assumption of five unmatched rows.
- **If it fails:**
  - `reference_map_coverage`: bronze has an LGU name the map has not seen. Rerun `ldrrmf_mapping`, review new `needs_review` or `ambiguous` rows, and add an override with a reason if needed.
  - `component_totals` or `missing_amounts`: see `silver.blgf_ldrrmf_annual_lgu_reject`.
  - `unique_matched_lgu_year`: two BLGF spellings map to one PSGC code in the same year. Fix with an override.

### PSA OpenSTAT population

- **Run:** `--only +population_total_gold`
- **Inputs:** `population_api_url`, `population_source_file`, `population_expected_profile`, `census_year` and `source_period`.
- **Outputs:** `bronze.psa_population`, `silver.psa_population_clean`, `silver.psa_population_reject` and `gold.fact_population_total`.
- This branch does not supply canonical census population components.

## Rerun and replay

- **Retry a failed run:** rerun with the same `batch_id`. Completed files are reused. Lineage and timestamp preservation follow each writer's documented update conditions; there is no universal timestamp-stability guarantee. Keep extraction settings unchanged for a completed batch: the manifest does not bind every requested configuration value.
- **Load a correction from the source:** run with a new `batch_id`. Updates follow each writer's business/observation keys and change conditions. Review replay evidence for the affected branch before claiming older-delivery protection.
- **Rerun one branch only:** use `--only +<task>` with the same `batch_id`.

Nothing is deleted by a rerun. A row that disappears from the source stays in the tables.

## Recover from a blocked check

A blocking check stops the task and dependent tasks are skipped. Checks evaluated before a write prevent that write; earlier successful statements and upstream commits may remain. Inspect actual task state and target history before recovery.

1. Open the failed task's output; it names the check (`check_id`) and the table.
2. Look up the result in `monitoring.dq_result` for that `run_id`.
3. Look at the rows behind it: the dataset's `_reject` table, or rows with `has_dq_issues`.
4. Fix the cause: a reviewed mapping or override, a configuration value, or a corrected source in a new batch.
5. Rerun the affected branch with `--only +<task>`.

Before treating a gold table as current, check the latest `monitoring.dq_result` rows for its task. See `../data-quality/README.md` for what each rule does.

## When something goes wrong

| What you see | Meaning and fix |
| --- | --- |
| `Valid: NO` on your profile | Run the login line from Step 1 again. |
| `Error: Scope ahon does not exist!` | Do Step 3. |
| `Fill required inputs` or a JSON error | Re-run the check in Step 2 and look at the key it names. |
| `Warnings are not allowed in strict mode` | Run validate without `--strict`. |
| Deploy seems stuck | Wait a few minutes. The first upload is large. |
| A task fails | Open the run in Databricks (Jobs > your job > the run) and read the failed task's error. Send the task name and the last error line to the team. |
| A CMCI task fails | Confirm the reviewed Q2_2024 mapping CSV was uploaded using the existing procedure and `cmci_mapping_file` points to its path in the reference volume; then inspect the task error. Use `--only` as above. |

## Do not

- Edit or delete files in a completed batch folder. A checksum mismatch blocks reads; keep the batch for investigation and land a new one.
- Drop or alter tables to get past a schema check. Use a fresh development catalog or a reviewed migration.
- Point live ingestion at an `ahon_integration_*` catalog; those hold test fixtures for the `data_quality` job.
- Put tokens or workspace IDs in the repository, command arguments or chat.

## Test job

`data_quality` (development only) runs the real transformations on synthetic fixtures in a fresh `ahon_integration_*` catalog:

```powershell
databricks bundle run data_quality -t dev --profile <your-profile-name> --params "catalog=ahon_integration_<name>"
```

It has no automatic retries; use a fresh catalog after a failure. It tests the code, not the real sources.



It has no automatic retries; use a fresh catalog after a failure. It tests the code, not the real sources.
