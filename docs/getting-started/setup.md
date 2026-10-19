# Setup

Everything you need before the first pipeline run: local tools, a Databricks login, the bundle settings, the PSGC token and the CMCI mapping. Once this is done, run the pipeline with the [runbook](../operations/runbook.md).

Commands are shown for PowerShell on Windows; the same steps work in any shell.

## 1. Install the tools

| Tool | Version | Used for |
| --- | --- | --- |
| Git | any recent | Cloning the repository |
| Python | 3.12 (3.10 or newer works) | Local lint and tests |
| Databricks CLI | 1.0 or newer | Validating, deploying and running the bundle |

Install the Databricks CLI with `winget install Databricks.DatabricksCLI`, then open a new terminal. If an old Python `databricks-cli` package is installed, it may come first in PATH; check with `databricks --version`.

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

The bundle has no workspace written into it; the CLI profile you choose decides which workspace you deploy to.

```powershell
databricks auth profiles
databricks auth login --host <your-workspace-URL> --profile <your-profile>
databricks auth describe --profile <your-profile>
```

The Databricks login and the PSGC API token are separate credentials. Do not put either one in Git, command arguments or chat.

## 3. Fill in the bundle settings

`databricks.yml` declares 47 required variables with no defaults. You supply them in a private file that Git ignores:

```powershell
$target = 'dev'
New-Item -ItemType Directory -Path ".databricks/bundle/$target" -Force | Out-Null
Copy-Item config/variable-overrides.example.json ".databricks/bundle/$target/variable-overrides.json"
notepad ".databricks/bundle/$target/variable-overrides.json"
```

Replace every `null` with an approved value. If you already have a private overrides file, copy it to this path and review every value. Do not copy `.databricks` state files from another checkout.

| Settings | What to supply |
| --- | --- |
| `catalog`, `workspace_root`, `source_volume` | Your development catalog (for example `ahon_dev_<name>`), a workspace folder for the bundle's files and state, and the raw-file volume name (`source`) |
| `run_as` | One identity: `{"user_name":"<user>"}` or `{"service_principal_name":"<application-id>"}`. Must be set in this file; the CLI does not accept it any other way |
| `environment_version`, `job_timeout_seconds`, `task_timeout_seconds`, `task_retries`, `retry_interval_millis`, `max_concurrent_runs` | Settings your workspace supports. Use `max_concurrent_runs` = 1 and a small number of retries |
| `source_period`, `census_year` | PSGC snapshot (`Q2_2024`) and population census year (2024). They are different things: the census year is not the PSGC snapshot date |
| `psgc_api_base_url`, `psgc_expected_rows`, `psgc_secret_scope`, `psgc_secret_key` | PSA API address, expected rows per snapshot, and the names of the secret that holds the token (step 4) |
| `population_api_url`, `population_source_file`, `population_expected_profile` | OpenSTAT population inputs. Still required by the job, though their tables are not used downstream |
| `hf_repo`, `hf_revision`, `ldrrmf_folder`, `ldrrmf_file_pattern`, `ldrrmf_expected_rows`, `ldrrmf_start_year`, `ldrrmf_end_year` | The Hugging Face dataset repository (`Jess-Christine/Project-AHON`) and a pinned full commit SHA, the LDRRMF folder and file name pattern (with the year in it), expected rows per fiscal year, and the fiscal year range (FY2018 to FY2024) |
| `cmci_mapping_file`, `cmci_metric_version`, `cmci_run_mode`, `cmci_start_year`, `cmci_end_year`, `cmci_portal_url`, `cmci_process_url`, `cmci_test_limit`, `cmci_request_batch_size`, `cmci_request_delay`, `cmci_http_retries`, `cmci_http_backoff`, `cmci_user_agent`, `request_timeout` | The reviewed mapping CSV (step 5), the indicator set version, request settings and year range. Use `cmci_run_mode` = `test` until a full run is intended |
| `start_month`, `end_month`, `earthquake_archive_url_template`, `earthquake_latest_url`, `source_timezone` | PHIVOLCS months to load (`YYYY-MM`, inclusive), the page addresses, and the publisher's time zone |
| `osm_folder`, `osm_shapefile_stems` | OpenStreetMap inputs. Still required by the job, though their tables are not used downstream |

Some values are JSON written as a string, with the inner quotes escaped:

| Setting | Shape |
| --- | --- |
| `psgc_expected_rows` | `"{\"<snapshot>\":<count>}"` |
| `ldrrmf_expected_rows` | `"{\"<fiscal year>\":<count>}"` |
| `population_expected_profile` | a JSON list of four level counts, as a string |
| `osm_shapefile_stems` | `"[\"<stem>\"]"` |

These are shapes, not values. Expected counts are checks on what the source delivers; a run stops if the source does not match them.

Check that nothing is left empty, without printing any values:

```powershell
python -c "import json,sys; t=json.load(open('config/variable-overrides.example.json')); s=json.load(open(sys.argv[1])); m=[k for k in t if s.get(k) is None]; print('Missing: '+', '.join(m) if m else 'All settings filled')" .databricks/bundle/dev/variable-overrides.json
git check-ignore .databricks/bundle/dev/variable-overrides.json
```

The second command must print the path, which confirms Git ignores the file.

Use a separate catalog for each purpose: your development catalog for real sources, and a fresh `ahon_integration_<name>` catalog for the `data_quality` test job. Never point real ingestion at an `ahon_integration_*` catalog.

## 4. Store the PSGC token

The PSGC extract needs a token issued by PSA. It is kept in a Databricks secret, never in the overrides file. Only the scope and key names go in the overrides file.

If the token is already stored in your workspace, check that the key exists without showing its value:

```powershell
databricks secrets list-secrets <scope> --profile <your-profile>
```

To store or replace it:

```powershell
# Only when the scope does not exist yet:
databricks secrets create-scope <scope> --profile <your-profile>
databricks secrets put-secret <scope> <key> --profile <your-profile>
```

Paste the token at the masked prompt, without quotes. Running `put-secret` again on the same key replaces the token.

The job's `run_as` identity needs READ on the scope. If it is not the scope owner, a scope manager grants it:

```powershell
databricks secrets put-acl <scope> <user-or-application-id> READ --profile <your-profile>
```

See the [Databricks secrets commands](https://docs.databricks.com/aws/en/dev-tools/cli/reference/secrets-commands) for permissions.

## 5. Place the CMCI mapping file

CMCI needs a reviewed CSV that links each CMCI LGU name to a PSGC code. Upload it to a volume the job can read and set `cmci_mapping_file` to its path.

- Columns: `psgc_code,cmci_name,source_period,reviewed,reviewer,evidence`
- Every row reviewed (`reviewed` true), codes unique, names one-to-one, and `source_period` equal to the PSGC snapshot

Without it, the CMCI tasks cannot run. The other datasets still can, using `--only` (see the [runbook](../operations/runbook.md#run)). Do not use a made-up mapping.

## 6. Check locally

These checks run on your machine and start no cloud compute. CI runs the same ones on every pull request.

```powershell
python -m ruff check src tests --config pyproject.toml
python -m pytest tests -q
python -m yamllint -f parsable .
git diff --check
```

`tests/common/test_job_contract.py` checks every task's files, dependencies and settings.

## 7. Validate and deploy

```powershell
databricks bundle validate --strict -t dev --profile <your-profile>
databricks bundle deploy -t dev --profile <your-profile>
```

Deploying creates or updates the jobs in your workspace and costs nothing until a job runs. In development mode, jobs are named `[dev <your user name>] ...`. The first run's `setup` task creates the catalog (if you are allowed to), the schemas, the `reference.<source_volume>` volume and `monitoring.dq_result`.

Then follow the [runbook](../operations/runbook.md). A good first run is the PSGC branch alone (`--only +dim_lgu`), since every other dataset needs it.

## Production

Production uses its own private file, `.databricks/bundle/prod/variable-overrides.json`, made from the same template. It needs the production catalog (`ahon`), a workspace root separate from development, the production `run_as` identity, the token secret names and the reviewed CMCI mapping. Give the identity only the catalog, schema, volume and secret permissions it needs.

```powershell
databricks bundle validate --strict -t prod --profile <production-profile>
```

Only `ahon_pipeline` and `catalog_setup` are deployed to production; `data_quality` is development only. Validating does not prove runtime permissions or source credentials. Deploying and running production needs the team's approval.

## Hugging Face source files

The BLGF LDRRMF workbooks (and the OpenStreetMap files) are stored in the public Hugging Face dataset repository [`Jess-Christine/Project-AHON`](https://huggingface.co/datasets/Jess-Christine/Project-AHON), not in this Git repository ([decision 0004](../decisions/0004-source-file-landing-hugging-face.md)). The extract downloads them from the commit pinned in `hf_revision` into `/Volumes/<catalog>/reference/<source_volume>/<dataset>/<batch_id>/`, so a rerun always reads the same files.

To add or replace a source file:

1. Add it to the dataset repository, in the folder set by `ldrrmf_folder` and named to match `ldrrmf_file_pattern`, and push it (use Git LFS for large files).
2. Copy the new full commit SHA into `hf_revision`.
3. Update the expected row counts if the file changes them, in the same pull request.
4. Run with a new `batch_id`.

The extract passes no Hugging Face token, so the dataset repository must stay public. Pushing to it needs write access and a Hugging Face login (`huggingface-cli login`).
