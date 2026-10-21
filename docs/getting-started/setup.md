# Setup

Everything you need before the first pipeline run: local tools, a Databricks login, bundle settings, the PSGC token, and the CMCI mapping. Once this is done, run the pipeline using the [runbook](../operations/runbook.md).

Commands are shown for PowerShell on Windows. Use equivalent syntax when working in another shell.

## 1. Install the tools

| Tool | Version | Used for |
| --- | --- | --- |
| Git | Any recent version | Cloning the repository |
| Python | 3.12; 3.10 or newer works | Local linting and tests |
| Databricks CLI | 1.0 or newer | Validating, deploying, and running the bundle |

Install the Databricks CLI, then open a new terminal:

```powershell
winget install Databricks.DatabricksCLI
```

If an old Python `databricks-cli` package is installed, it may come first in `PATH`. Check with:

```powershell
databricks --version
```

Clone the repository and install the Python packages in a virtual environment:

```powershell
git clone https://github.com/Project-AHON/AHON-Pipeline.git
Set-Location AHON-Pipeline

py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1

python -m pip install -r requirements.txt -r requirements-dev.txt
```

Spark and Delta come from Databricks. They are not needed for local tests.

## 2. Log in to Databricks

The bundle has no workspace written into it. The CLI profile you choose decides which workspace you deploy to.

```powershell
databricks auth profiles
databricks auth login --host <your-workspace-URL> --profile <your-profile>
databricks auth describe --profile <your-profile>
```

The Databricks login and PSGC API token are separate credentials. Do not put either one in Git, command arguments, screenshots, logs, or chat.

## 3. Fill in the bundle settings

`databricks.yml` declares 45 required variables with no defaults. Supply them in a private file that Git ignores.

From the repository root, where `databricks.yml` is located, run:

```powershell
$target = "dev"

New-Item -ItemType Directory `
  -Path ".databricks/bundle/$target" `
  -Force |
  Out-Null

Copy-Item `
  config/variable-overrides.example.json `
  ".databricks/bundle/$target/variable-overrides.json"

notepad ".databricks/bundle/$target/variable-overrides.json"
```

Replace every `null` with an approved value.

If you already have a private overrides file, copy it to this path and review every value. Do not copy `.databricks` state files from another checkout.

| Settings | What to supply |
| --- | --- |
| `catalog`, `workspace_root`, `source_volume` | Your development catalog, such as `ahon_dev_<name>`; a workspace folder for the bundle files and state; and the raw-file volume name, normally `source` |
| `run_as` | One identity: `{"user_name":"<user>"}` or `{"service_principal_name":"<application-id>"}` |
| `environment_version`, `job_timeout_seconds`, `task_timeout_seconds`, `task_retries`, `retry_interval_millis`, `max_concurrent_runs` | Settings supported by your workspace. Use `max_concurrent_runs` = 1 and a small number of retries |
| `source_period`, `census_year` | The PSGC snapshot, such as `Q2_2024`, and the population census year, such as 2024 |
| `psgc_api_base_url`, `psgc_expected_rows`, `psgc_secret_scope`, `psgc_secret_key` | The PSA API address, expected rows by snapshot, and the secret scope and key names used in Step 4 |
| `population_api_url`, `population_source_file`, `population_expected_profile` | OpenSTAT population inputs. These remain required even though their tables are not used downstream |
| `hf_repo`, `hf_revision`, `ldrrmf_folder`, `ldrrmf_file_pattern`, `ldrrmf_expected_rows`, `ldrrmf_start_year`, `ldrrmf_end_year` | The Hugging Face repository, pinned full commit SHA, LDRRMF folder and filename pattern, expected rows, and fiscal-year range |
| `cmci_mapping_file`, `cmci_metric_version`, `cmci_run_mode`, `cmci_start_year`, `cmci_end_year`, and other `cmci_*` settings | The reviewed mapping from Step 5, indicator version, request settings, and year range. Use `cmci_run_mode` = `test` until a full run is intended |
| `start_month`, `end_month`, `earthquake_archive_url_template`, `earthquake_latest_url`, `source_timezone` | PHIVOLCS months in `YYYY-MM` format, page addresses, and the publisher time zone |

Some values are JSON stored as strings, with inner quotation marks escaped:

| Setting | Shape |
| --- | --- |
| `psgc_expected_rows` | `"{\"<snapshot>\":<count>}"` |
| `ldrrmf_expected_rows` | `"{\"<fiscal-year>\":<count>}"` |
| `population_expected_profile` | A JSON list of four level counts stored as a string |

These are shapes, not approved values.

All required settings must be supplied even when using `--only` or running the fixture job. Task selection does not remove bundle variable declarations.

Obtain approved endpoints, revisions, and expected counts from the dataset owners. Do not infer them from examples.

### Check the private overrides file

Check that no required variable is missing, null, or blank without printing values:

```powershell
$settings = Get-Content `
  .databricks/bundle/dev/variable-overrides.json `
  -Raw |
  ConvertFrom-Json

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

This check does not validate types, source contracts, credentials, or runtime permissions.

Confirm that Git ignores the file:

```powershell
git check-ignore .databricks/bundle/dev/variable-overrides.json
```

The command must print:

```text
.databricks/bundle/dev/variable-overrides.json
```

Use separate catalogs for different purposes:

- Use your development catalog for real sources.
- Use a fresh `ahon_integration_<name>` catalog for the `data_quality` test job.
- Never point real ingestion at an `ahon_integration_*` catalog.

## 4. Store the PSGC token

The PSGC extract needs a token issued by PSA. Store it in a Databricks secret, never in the overrides file. Only the scope and key names belong in the overrides file.

Use a developer-specific scope, such as:

```text
ahon-dev-maria
```

Set `psgc_secret_scope` in your overrides file to the same scope name.

If the token is already stored, check that the key exists without showing its value:

```powershell
databricks secrets list-secrets <scope> --profile <your-profile>
```

Create the scope only if it does not already exist:

```powershell
databricks secrets create-scope <scope> --profile <your-profile>
```

Store the token:

```powershell
databricks secrets put-secret <scope> <key> --profile <your-profile>
```

Paste the token at the masked prompt without quotation marks. Running `put-secret` again for the same key replaces the existing token.

The job's `run_as` identity needs `READ` access to the scope. If it is not the scope owner, a scope manager must grant access:

```powershell
databricks secrets put-acl `
  <scope> `
  <user-or-application-id> `
  READ `
  --profile <your-profile>
```

See the [Databricks secrets command reference](https://docs.databricks.com/aws/en/dev-tools/cli/reference/secrets-commands) for permission details.

## 5. Place the CMCI mapping file

CMCI requires the approved reviewed Q2_2024 mapping CSV linking CMCI LGU names to PSGC codes.

The repository includes the approved reviewed mapping at:
`src/sql/datasets/cmci/silver/cmci_reviewed_mapping_Q2_2024.csv`

Use it only with the `Q2_2024` PSGC snapshot. A different snapshot requires a separately reviewed mapping.

Do not create a replacement mapping, modify the approved file, or substitute an unreviewed mapping.

Set `cmci_mapping_file` in your overrides file to the planned destination:

```text
/Volumes/<catalog>/reference/<source_volume>/cmci_reviewed_mapping_Q2_2024.csv
```

Replace `<catalog>` and `<source_volume>` with the values from the same overrides file.

Validate and deploy:

```powershell
databricks bundle validate --strict -t dev --profile <your-profile>
databricks bundle deploy -t dev --profile <your-profile>
```

Create the catalog, schemas, and source volume:

```powershell
databricks bundle run catalog_setup -t dev --profile <your-profile>
```

Upload the approved mapping:

```powershell
databricks fs cp `
  "src/sql/datasets/cmci/silver/cmci_reviewed_mapping_Q2_2024.csv" `
  "dbfs:/Volumes/<catalog>/reference/<source_volume>/cmci_reviewed_mapping_Q2_2024.csv" `
  --profile <your-profile>
```

Confirm that the uploaded destination matches `cmci_mapping_file` exactly.

Do not overwrite an existing reviewed mapping with different contents. Use a versioned filename if a different reviewed mapping is required.

The CSV must contain these columns:

```text
psgc_code
cmci_name
source_period
reviewed
reviewer
evidence
```

The mapping must meet these conditions:

- Every row has `reviewed` set to true.
- PSGC codes are unique.
- CMCI names map one-to-one.
- `source_period` matches the selected PSGC snapshot.

Without the approved mapping, CMCI tasks cannot run. Other branches can still run using `--only`; see the ../operations/runbook.md#run.

## 6. Check locally

These checks run on your computer and do not start Databricks cloud compute:

```powershell
python -m ruff check src tests --config pyproject.toml
python -m pytest tests -q
python -m yamllint -f parsable .
git diff --check
```

`tests/common/testjobcontract.py` checks every task's files, dependencies, and settings.

The full local test suite may use additional local CPU and memory. It does not use Databricks serverless or cluster compute.

## 7. Validate and deploy

Run strict validation:

```powershell
databricks bundle validate --strict -t dev --profile <your-profile>
```

After validation passes, deploy:

```powershell
databricks bundle deploy -t dev --profile <your-profile>
```

Deploying creates or updates the jobs in your workspace. It does not start pipeline compute.

In development mode, jobs are named:

```text
[dev <your user name>] ...
```

If `catalog_setup` was not already run during Step 5, the pipeline's `setup` task creates the catalog if permitted, the schemas, the `reference.<source_volume>` volume, and `monitoring.dq_result`.

Then follow the ../operations/runbook.md.

A useful first source run is PSGC:

```text
--only +dim_lgu
```

Dependencies vary by dataset. Earthquake processing uses PSGC Silver.

## Production

Production uses its own private file:

```text
.databricks/bundle/prod/variable-overrides.json
```

Create it from:

```text
config/variable-overrides.example.json
```

Production requires the production catalog, a separate workspace root, the production `run_as` identity, PSGC secret names, and the reviewed CMCI mapping.

Validate production with:

```powershell
databricks bundle validate --strict -t prod --profile <production-profile>
```

Only `ahon_pipeline` and `catalog_setup` are deployed to production. The `data_quality` job is development-only.

Validation does not prove runtime permissions or source credentials. Production deployment and execution require team approval.

## Hugging Face source files

The BLGF LDRRMF workbooks are stored in the public Hugging Face dataset repository [Jess-Christine/Project-AHON](https://huggingface.co/datasets/Jess-Christine/Project-AHON), not in this Git repository.

See ../decisions/0004-source-file-landing-hugging-face.md.

The extract downloads files from the commit pinned in `hf_revision` into:

```text
/Volumes/<catalog>/reference/<source_volume>/<dataset>/<batch_id>/
```

Pinning the full commit SHA ensures that a rerun reads the same source files.

To add or replace a source file:

1. Add the file to the folder configured by `ldrrmf_folder`.
2. Name it according to `ldrrmf_file_pattern`.
3. Push the file and copy the new full commit SHA into `hf_revision`.
4. Update expected row counts in the same pull request if needed.
5. Run the pipeline with a new `batch_id`.

The extract does not pass a Hugging Face token, so the dataset repository must remain public.

Pushing changes requires write access and a Hugging Face login:

```powershell
hf auth login
```
