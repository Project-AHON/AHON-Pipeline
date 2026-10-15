# Development execution and recovery

The supported executable graph is `resources/jobs/ahon_job.yml`. See the [inventory](../architecture/source-to-target.md) for dataset grains, dependencies, PR reuse and external blockers. No production deployment or execution is authorized by this task.

## Configure explicit inputs

`databricks.yml` declares required bundle variables with no committed defaults. Provide them through an ignored `.databricks/bundle/<target>/variable-overrides.json`. Scalar values also support `BUNDLE_VAR_<name>` environment variables or CLI `--var` arguments. The complex `run_as` identity must be supplied in the override JSON; the CLI does not accept complex values through environment variables. Never commit workspace identifiers, credentials or a developer's verification settings. The prepared development input file is available in the isolated checkout, excluded by `.gitignore`.

| Inputs | What to supply |
| --- | --- |
| `catalog`, `workspace_root`, `source_volume` | Fresh development catalog, isolated workspace state/files root, raw volume name |
| `run_as` | Explicit identity object: `{"user_name":"<approved-user>"}` or `{"service_principal_name":"<approved-application-id>"}`. Use the actual approved production identity for production |
| `environment_version`, job/task timeouts, task retries/retry interval, `max_concurrent_runs` | Settings supported in your selected workspace. Use one concurrent writer and bounded retries; the staged fixture job deliberately has zero automatic retries |
| `source_period`, `census_year` | Approved PSGC snapshot and actual population reference year; do not equate the two periods |
| `psgc_api_base_url`, `psgc_expected_rows`, PSA secret scope/key | Approved API base, JSON snapshot→expected-count profile, existing token secret names |
| `population_api_url`, `population_source_file`, `population_expected_profile` | Approved census endpoint/file and JSON counts for national, regional, source province-group and city/municipality levels |
| `hf_repo`, `hf_revision`, `ldrrmf_folder`, `ldrrmf_file_pattern`, `ldrrmf_expected_rows` | Approved dataset and pinned full commit, source folder, filename pattern containing the year, JSON fiscal-year→workbook-count profile |
| `ldrrmf_start_year`, `ldrrmf_end_year` | Reviewed financial reference window; Silver rejects observations outside it |
| CMCI URLs, year window, run mode, test limit, batch size, delay, retry/backoff, User-Agent, `request_timeout` | Explicit source request settings. Use `test` until reviewed full activation. Response coverage must match requested years/indicators |
| `cmci_mapping_file`, `cmci_metric_version` | Reviewed mapping CSV in a readable volume and a reviewed source-definition version |
| `osm_folder`, `osm_shapefile_stems` | Approved vector folder and JSON list of shapefile stems; all required sidecars and known CRSs must be present |
| PHIVOLCS archive URL template/latest URL, `start_month`, `end_month`, `source_timezone` | Approved source locations, inclusive YYYY-MM window and publisher clock used for request selection. Event UTC interpretation remains withheld |

Expected counts are caller-supplied source validation inputs, not invented risk cutoffs. Source file schemas, field typing, units, geographic levels and parsing rules remain documented contracts. CMCI indicator names/codes/pillars are source data in `resources/cmci_indicators.csv`, synchronized as a real job asset rather than embedded in Python. Changes to that asset require source review and a matching metric version; they do not authorize a composite score.

CMCI Gold blocks when the configured code/name/pillar/version set differs from the existing indicator dimension. Use a fresh catalog for a new reviewed definition, or complete a reviewed migration before resuming; no silent reassignment of old measurements occurs.

Profiles are not embedded in repository code. List profiles with `databricks auth profiles`, select the intended personal workspace, then set `$profile` in your shell. Set `$catalog` to a fresh development catalog. Required inputs must be loaded before validation:

```powershell
$profile = Read-Host 'Selected development profile'
$catalog = Read-Host 'Fresh development catalog'
$env:BUNDLE_VAR_catalog = $catalog
databricks bundle validate --strict -t dev --profile $profile
databricks bundle deploy -t dev --profile $profile
$fixtureCatalog = Read-Host 'Fresh isolated fixture catalog beginning ahon_integration_'
databricks bundle run data_quality -t dev --profile $profile --params "catalog=$fixtureCatalog"
```

To validate `prod` without deploying it, supply that target's required variables and run `databricks bundle validate --strict -t prod --profile $profile`. No target is default; schedules are absent. The development target implements reproducible bundle testing in a personal workspace, not a shared development environment. The setup task creates the selected catalog if permitted, schemas, monitoring DQ results and raw volume. It never rebuilds existing incompatible schemas.

Keep the configured development `catalog` for live-source processing. Pass a separate fresh `ahon_integration_*` catalog explicitly for fixture runs; never point live ingestion at completed synthetic fixtures. The fixture guard refuses a source catalog. The prepared private development configuration uses the verified source catalog; fixture commands need the explicit catalog override shown above.

## Enter the PSGC token locally

PSGC requires a PSA-issued API token. The Databricks profile authenticates the secret-management command. Run these commands in your own terminal; the token is entered only at the CLI's interactive prompt:

```powershell
$profile = Read-Host 'Databricks profile'
$scope = Read-Host 'PSGC secret scope name'
$key = Read-Host 'PSGC token key name'
# Run create-scope only when creating a new scope:
databricks secrets create-scope $scope --profile $profile
databricks secrets put-secret $scope $key --profile $profile
# Confirm key metadata without printing the value:
databricks secrets list-secrets $scope --profile $profile
$env:BUNDLE_VAR_psgc_secret_scope = $scope
$env:BUNDLE_VAR_psgc_secret_key = $key
```

For reuse across terminal sessions, place only the scope and key names in the appropriate ignored override JSON as `psgc_secret_scope` and `psgc_secret_key`. Do not place the token in that file, command arguments, repository files or chat. Re-running `put-secret` for an existing key rotates its value. The extractor reads the secret at execution time; an empty secret or failed API request blocks publication. Redirects are rejected, and token-bearing request exceptions are sanitized.

The job's configured `run_as` identity needs READ on the scope. If it differs from the scope owner, an authorized scope manager can grant that specific approved identity access:

```powershell
$principal = Read-Host 'Approved job user name or service principal application ID'
databricks secrets put-acl $scope $principal READ --profile $profile
```

After loading the other explicit development inputs, deploy and test the PSGC branch in a fresh delivery:

```powershell
databricks bundle validate --strict -t dev --profile $profile
databricks bundle deploy -t dev --profile $profile
databricks bundle run ahon_pipeline -t dev --profile $profile --only +psgc_silver
```

The default new run ID creates a new delivery and exercises the token. Replaying an already completed delivery validates its landed files and does not re-fetch the source. See [Databricks secret commands](https://docs.databricks.com/aws/en/dev-tools/cli/reference/secrets-commands) for scope and ACL requirements.

## Production activation

Supply `.databricks/bundle/prod/variable-overrides.json` with the approved production catalog, a workspace root distinct from development, volume, run identity, source windows/profiles, token secret names and reviewed CMCI mapping. Use one concurrent writer (`max_concurrent_runs` set to 1), bounded retries/timeouts, and the intended CMCI source scope. Give the run identity only its required catalog/schema/volume and secret permissions. Do not use synthetic mappings as production references.

Only `ahon_pipeline` and `catalog_setup` are included in `prod`; `data_quality` is scoped to `dev`. Production validation does not certify runtime permissions or source credentials. This task validates production configuration and exercises development; production deployment/execution still requires separate authorization. Schedules remain absent.

## GX quality gates

All supported datasets share `src/sql/common/quality.py` with pinned GX Core. Spark computes rule metrics over the full candidate dataset, including key uniqueness, invalid values, reference coverage, period alignment and cardinality. GX evaluates those bounded metrics with `ExpectColumnValuesToBeBetween(failed_count, 0, 0)`. This is aggregate validation of the complete input, without collecting a growing source table into pandas or duplicating transformations in GX. Financial SQL returns the same named rule metrics instead of using SQL assertions as its DQ engine.

ERROR failures persist as BLOCKED and raise before dependent writes; WARN failures persist and preserve flagged rows. GX import/runtime failures stop execution, with no non-GX fallback. File integrity, request authentication, parsing structure and incompatible-schema guards remain fail-fast ingestion/write contracts.

`monitoring.dq_result` keeps its existing keys/counts/severity/status and adds nullable `validation_result STRING` containing GX expectation configuration, success and version as JSON. The only automatic migration adds that audit column when absent; existing monitoring rows remain and have NULL evidence until their checks rerun. Dataset schemas are not evolved. No additional DQ job, result store, Data Docs site or GX Cloud service is needed.

## Local checks

Use Python 3.10+ from the repository root:

```powershell
python -m pip install -r requirements.txt -r requirements-dev.txt
python -m ruff check src tests --config pyproject.toml
python -m pytest tests -q
python -m compileall -q src tests
python -m yamllint -f parsable databricks.yml resources .github
```

Spark/Delta come from Databricks and are unnecessary for local pure-function tests. Runtime libraries are pinned in the job environments and the public-PyPI lock is regenerated. Tests verify the DAG, source integrity, parsing, source definition assets and conservative geographic matching. Bundle schema validation alone does not prove runtime asset/import correctness.

## Scope and dependencies

The CLI's `+task` form includes upstream tasks. Independent extracts depend only on setup. References and validated Silver gate their dependent facts. Keep otherwise valid source processing selectable when another source lacks inputs:

```powershell
$sourceCatalog = Read-Host 'Separate live-source development catalog'
$batch = Read-Host 'Stable delivery batch ID'
databricks bundle run ahon_pipeline -t dev --profile $profile --params "catalog=$sourceCatalog,batch_id=$batch" --only +osm_silver,+ldrrmf_bronze,+population_silver,+earthquake_bronze

# After supplying the approved PSA token and source profiles:
databricks bundle run ahon_pipeline -t dev --profile $profile --only +barangay_population_gold

# After every source prerequisite is available:
databricks bundle run ahon_pipeline -t dev --profile $profile
```

PSGC Bronze→Silver precedes the existing `dim_lgu`, embedded barangay population and contextual reference lookups. Gold waits for its validated Silver and dimension. OSM runs its own extract→Bronze→Silver chain. Earthquake observation Silver requires the selected PSGC reference for candidate reporting; event Gold is not scheduled. Independent component tasks depend only on their own outputs. All edges use `ALL_SUCCESS`.

CMCI reviewed mapping columns are `psgc_code,cmci_name,source_period,reviewed,reviewer,evidence`. Codes are unique ten-digit strings, source names one-to-one, reviewed must be true, and period must match the selected dimension. The development PSGC secret reference is configured and live authentication has passed. Real approved CMCI mappings remain unavailable; synthetic mappings are test data and must not be used as production evidence.

## Replay and corrections

The default batch ID is the job run ID. Task retries reuse it. To replay a completed delivery, use the same batch ID and task selection. Identical same-batch observations retain first lineage and unchanged Silver/Gold processing timestamps. Source corrections use a new batch ID.

PSGC and LDRRMF Silver only update an existing business key when the candidate's first-ingested timestamp is at least as recent as the accepted row. Replaying an older completed delivery cannot roll back a newer correction. This is delivery ordering, not proof of a publisher's scientific reference date; source snapshot/year fields remain separate. New keys are retained without absence-based deletion. Initial PSGC Silver and dimension creation fail if a competing writer has already created the table.

Standard PSGC, PXWeb, BLGF and CMCI observation IDs include delivery batch ID, source file digest and raw row hash. A new delivery can legitimately repeat previously seen bytes, permitting a source-value reversion after a later correction. Earlier file-only identity tables are guarded: use a fresh catalog or reviewed migration. Earthquake and OSM retain their explicit file/ordinal observation contracts separately.

`_SUCCESS.json` exists to prevent reading interrupted downloads: it records filenames, hashes and source-row counts. A checksum mismatch in a completed batch blocks reads; retain it for investigation. An incomplete batch has no valid marker and a retry repairs it. Do not edit completed files to force recovery. Current PSGC and financial validation is scoped to the requested completed delivery, so prior rejected observations remain auditable without permanently poisoning later corrected releases.

Blocking checks persist to `monitoring.dq_result` before dependent tasks fail or skip. Inspect the failed task, source lineage and row disposition/rejects; correct source or reviewed mapping in a new batch and rerun the affected branch. Facts stay at the prior accepted state on a block; inspect latest DQ status before treating them as current. No absence-based deletion or automatic schema evolution occurs.

`data_quality` only accepts an isolated `ahon_integration_*` catalog. It runs the actual transformations on synthetic data, including same-batch replay, invalid values, geographic cardinality, NULL populations, partial aggregation, financial ratios and valid→invalid→reverted CMCI values. Use a fresh catalog for a new complete suite or after failure: fixtures intentionally retain correction history. The suite has zero automatic retries and replays individual transformations explicitly. It does not establish scientific validity or prove access to every real source.
