# Configure, verify and review the pipeline

Use this checkout as the complete repository root. It contains the executable bundle, pipeline code, tests and documentation, including the city/municipality population grain and GX checks. The reviewed `main` baseline is `4d82f6b5010c715ed14eca490c5415692e8fc34e`; this preparation starts from pipeline commit `45b2caa832a5bb1ccf783a550455887ccdcdc22d`. No deployment or source execution is required to prepare the PR.

## 1. Prepare local tools

Open PowerShell in this repository folder. Use Python 3.12 and the modern Databricks CLI (1.0 or newer). If needed, install the CLI with `winget install Databricks.DatabricksCLI`, then open a new terminal. A legacy Python `databricks-cli` installation may otherwise take precedence in PATH.

```powershell
Set-Location (git rev-parse --show-toplevel)
py -3.12 -m venv .venv
$python = Join-Path (Get-Location) '.venv/Scripts/python.exe'
& $python -m pip install -r requirements.txt -r requirements-dev.txt

$cli = (Get-Command databricks -All |
    Where-Object Source -Like '*WinGet*databricks.exe' |
    Select-Object -First 1).Source
if (-not $cli) { $cli = (Get-Command databricks -ErrorAction Stop).Source }
& $cli --version
& $cli auth profiles
$profile = Read-Host 'Selected Databricks profile'
& $cli auth describe --profile $profile
```

If the selected profile needs login, run `databricks auth login --host <your-workspace-URL> --profile <your-profile>` with your chosen values. The Databricks login and the PSA PSGC API token are separate credentials. Do not put either token in Git, shell arguments or chat.

## 2. Fill the target configuration

```powershell
$target = 'dev'
$configDirectory = Join-Path (Get-Location) ".databricks/bundle/$target"
$configPath = Join-Path $configDirectory 'variable-overrides.json'
New-Item -ItemType Directory -Path $configDirectory -Force | Out-Null
if (Test-Path -LiteralPath $configPath) { throw 'Existing configuration preserved; edit it instead.' }
Copy-Item -LiteralPath 'config/variable-overrides.example.json' -Destination $configPath
notepad $configPath
```

Replace every `null` in the private copy with an approved value. The tracked example intentionally supplies no operational defaults. Alternatively, copy your existing private override file to this path, then review every value. For this separate checkout, select a fresh development catalog and a different `workspace_root` before deployment so it cannot change the other checkout's bundle state or jobs. Keep synthetic fixture catalogs separate from live-source catalogs. Do not copy `.databricks` state files from another checkout.

| Configuration shape | Fields and meaning |
| --- | --- |
| Identity object | `run_as`: either `{"user_name":"<approved-user>"}` or `{"service_principal_name":"<approved-application-id>"}`; use one identity |
| Strings | Catalog, volume, isolated workspace root, environment version, URLs, folders, source filenames, source snapshot, secret scope/key, source timezone and CMCI definition/mapping settings |
| Numeric inputs | Census and fiscal/CMCI years, timeouts, retries/delays, request limits and maximum concurrent runs; use the reviewed workspace/source settings |
| JSON encoded as a string | `psgc_expected_rows` maps snapshot names to approved counts; `ldrrmf_expected_rows` maps fiscal years to workbook counts; `population_expected_profile` is the four-level source-count list; `osm_shapefile_stems` is the approved unique list of stems |

For JSON encoded fields, enter a JSON string containing escaped inner quotes, for example the shape `"{\"<snapshot>\":<approved-count>}"`; replace the markers with your actual snapshot/count. An OSM list has the shape `"[\"<approved-stem>\"]"`. These are shapes, not approved source values. The complete input groups and source contracts are in the [runbook](runbook.md#configure-explicit-inputs) and [inventory](../architecture/source-to-target.md).

Use one concurrent writer, bounded retries and the existing `STANDARD` batch mode. No schedules are configured. `census_year` is a population reference year, not the PSGC API snapshot date. `hf_revision` must be a full approved commit SHA. A reviewed CMCI mapping is required for that source: an empty `cmci_mapping_file` can leave independent branches selectable but does not make CMCI runnable. Do not substitute synthetic mappings.

Check completeness locally without printing configuration values:

```powershell
$settings = Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json
$required = (Get-Content 'config/variable-overrides.example.json' -Raw | ConvertFrom-Json).PSObject.Properties.Name
$missing = @($required | Where-Object {
    $_ -notin $settings.PSObject.Properties.Name -or $null -eq $settings.$_
})
if ($missing.Count) { throw "Fill required inputs: $($missing -join ', ')" }
git check-ignore .databricks/bundle/dev/variable-overrides.json
```

This checks template completeness only. Bundle validation checks the resource configuration; task execution validates source semantics.

## 3. Configure PSGC secret access

If the token is already saved in the selected workspace, keep it there and enter only its scope/key names in the private configuration. The configured job identity needs READ on that scope. Verify key metadata without retrieving the value:

```powershell
$scope = Read-Host 'Existing PSGC secret scope'
$key = Read-Host 'Existing PSGC secret key'
& $cli secrets list-secrets $scope --profile $profile
```

Only if you need to create or rotate the token, run `& $cli secrets put-secret $scope $key --profile $profile` and paste the token into its masked prompt **without surrounding quotes**. Create a new scope only when necessary. The [secret setup instructions](runbook.md#enter-the-psgc-token-locally) cover scope creation and identity permissions. The token does not belong in `variable-overrides.json`.

## 4. Verify locally and validate the bundle

```powershell
& $python -m ruff check src tests --config pyproject.toml
if ($LASTEXITCODE -ne 0) { throw 'Python lint failed' }
& $python -m pytest tests -q
if ($LASTEXITCODE -ne 0) { throw 'Tests failed' }
& $python -m compileall -q src tests
if ($LASTEXITCODE -ne 0) { throw 'Compilation failed' }
& $python -m yamllint -f parsable databricks.yml resources .github
if ($LASTEXITCODE -ne 0) { throw 'YAML lint failed' }
git diff --check
if ($LASTEXITCODE -ne 0) { throw 'Whitespace check failed' }
& $cli bundle validate --strict -t dev --profile $profile
if ($LASTEXITCODE -ne 0) { throw 'Bundle validation failed' }
```

These steps do not start cloud compute. Local tests cover task paths, dependencies, duplicate execution, separate population writers, city/year grain and GX behavior. The [validation record](validation.md) describes earlier development runtime evidence and its limits. The new template check ensures configuration fields cannot silently fall behind the bundle.

## 5. Development deployment and focused verification

These commands create/update development resources and start billable compute; run them only when development verification is needed. Deploy from this same configured checkout. Existing barangay population targets require a reviewed rebuild/cutover or a fresh catalog; no automatic migration occurs.

```powershell
& $cli bundle deploy -t dev --profile $profile
if ($LASTEXITCODE -ne 0) { throw 'Development deployment failed' }
$batch = Read-Host 'New delivery batch ID using letters, digits, hyphens or underscores'

# First validate only the live reference branch with the existing secret:
& $cli bundle run ahon_pipeline -t dev --profile $profile --params "batch_id=$batch" --only +psgc_silver
if ($LASTEXITCODE -ne 0) { throw 'PSGC verification failed' }

# Then validate the canonical city/municipality population branch:
& $cli bundle run ahon_pipeline -t dev --profile $profile --params "batch_id=$batch" --only +population_components
if ($LASTEXITCODE -ne 0) { throw 'City population verification failed' }
```

The `+task` syntax includes dependencies. Select only the branch you need; you can run `+population_components` directly when a separate reference-only check is unnecessary. The two commands above reuse one delivery batch so the population check reads the already completed download. For replay of a completed delivery, provide that delivery's stable `batch_id` through `--params "batch_id=$batch"`; reusing it avoids repeated downloads and permits checking for zero changed rows. Use a new batch ID for a source correction. Inspect the selected run's `monitoring.dq_result` GX evidence and dispositions; blocking checks must pass and missing population stays NULL. Resident counts and funding utilization are not validated risk/preparedness scores.

When you need the complete synthetic integration suite, pass a **fresh** catalog beginning `ahon_integration_` explicitly:

```powershell
$fixtureCatalog = Read-Host 'Fresh isolated ahon_integration_* catalog'
& $cli bundle run data_quality -t dev --profile $profile --params "catalog=$fixtureCatalog"
```

## 6. Review the changes and prepare the PR

This worktree has branch `feature/configuration-pr-ready`. The previous checkout is preserved. Git records the entire pipeline diff against `main` and the smaller configuration-only preparation diff against `45b2caa`:

```powershell
git status --short --branch
git diff --stat origin/main HEAD
git diff origin/main HEAD -- databricks.yml resources src tests
git diff 45b2caa HEAD -- config docs/operations/configuration.md README.md tests/common/test_job_contract.py
```

The ignored `.local/` folder contains `PR_TITLE.txt`, `PR_DESCRIPTION.md`, `changes-from-main.patch`, `configuration-preparation.patch` and `review-summary.json` for this handoff. Review them locally; do not add runtime files, private overrides or credentials. The prepared PR body uses the user-selected `Part of #90`, which satisfies the repository's issue-link requirement without asserting that every acceptance criterion of that issue is complete.

After reviewing the branch, publish it with `git push -u origin feature/configuration-pr-ready` and open a PR targeting `main` using the prepared title and description. This preparation does not push, create a PR, merge, or deploy anything automatically. The branch includes the earlier reconciliation and city-grain changes, not only the configuration template.

## 7. Production handoff

After PR approval and merge, create a separate ignored `.databricks/bundle/prod/variable-overrides.json` using the same template with approved production values. Supply the production identity, catalog, isolated bundle state root, readable secret reference, source windows and reviewed CMCI mapping. Validate with `& $cli bundle validate --strict -t prod --profile $productionProfile` using the explicitly selected production profile.

Before the first production run, review population schema cutover and dependent consumers together. Runtime proof in development does not certify production permissions or source methodology. Production deployment/execution requires its own authorization; no production deployment or run is part of this folder preparation.

Once the production rollout is explicitly approved and every source prerequisite is satisfied, the operator deploys and runs from the merged checkout:

```powershell
& $cli bundle deploy -t prod --profile $productionProfile
if ($LASTEXITCODE -ne 0) { throw 'Production deployment failed' }
& $cli bundle run ahon_pipeline -t prod --profile $productionProfile
if ($LASTEXITCODE -ne 0) { throw 'Production run failed; inspect the task and GX results' }
```

Review the resulting task states, selected-period row counts, missing/mapping dispositions and GX results before releasing outputs to consumers. For recovery, preserve raw completed deliveries and use the [replay/correction procedure](runbook.md#replay-and-corrections); do not delete tables or evolve schemas to bypass a failure.
