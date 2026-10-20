# Runbook

How to run, rerun and recover the AHON pipeline. Before the first run, finish [setup](../getting-started/setup.md): Databricks CLI profile, bundle variable overrides, PSGC token secret and the reviewed CMCI mapping file.

The pipeline is the job `ahon_pipeline` in `resources/jobs/ahon_job.yml`. Which script builds which table is in [source to target](../architecture/source-to-target.md).

## Before every run

1. Pull the branch you are running and deploy that same checkout. A workspace job does not update when local files change.

   ```text
   databricks bundle validate --strict -t dev --profile <profile>
   databricks bundle deploy -t dev --profile <profile>
   ```

2. Check that the catalog in your overrides file is the one you mean to write to.
3. Optional local check, without cloud compute: `python -m pytest tests/common/test_job_contract.py -q` confirms every task's files, dependencies and settings.

## Run

| What | Command |
| --- | --- |
| Whole pipeline | `databricks bundle run ahon_pipeline -t dev --profile <profile>` |
| One dataset, with everything it needs | `databricks bundle run ahon_pipeline -t dev --profile <profile> --only +<task>` |
| Several branches | `--only +ldrrmf_gold,+population_gold` |
| A different catalog or batch | add `--params "catalog=<catalog>,batch_id=<batch_id>"` |

`+<task>` includes every upstream task. The batch ID defaults to the job run ID. There is no schedule; runs are started by hand. Keep one run at a time (`max_concurrent_runs` = 1).

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
- **Needs:** the reviewed mapping CSV in a volume (`cmci_mapping_file`, columns `psgc_code,cmci_name,source_period,reviewed,reviewer,evidence`), `cmci_start_year`, `cmci_end_year`, `cmci_run_mode` and the `cmci_*` request settings, and `dim_lgu`. Use `cmci_run_mode = test` until a full run is intended.
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

Before treating a gold table as current, check the latest `monitoring.dq_result` rows for its task. See [data quality](../data-quality/README.md) for what each rule does.

## Do not

- Edit or delete files in a completed batch folder. A checksum mismatch blocks reads; keep the batch for investigation and land a new one.
- Drop or alter tables to get past a schema check. Use a fresh development catalog or a reviewed migration.
- Point live ingestion at an `ahon_integration_*` catalog; those hold test fixtures for the `data_quality` job.
- Put tokens or workspace IDs in the repository, command arguments or chat.

## Test job

`data_quality` (development only) runs the real transformations on synthetic fixtures in a fresh `ahon_integration_*` catalog:

```text
databricks bundle run data_quality -t dev --profile <profile> --params "catalog=ahon_integration_<name>"
```

It has no automatic retries; use a fresh catalog after a failure. It tests the code, not the real sources.

## Layer gates in this integration candidate

Twelve gates follow their builds. Census and OpenSTAT population have separate Silver/Gold gate entry points; selecting census components does not select OpenSTAT. All population gates receive the configured census year. A gate checks the build already written, so a failure skips dependents but does not roll back that build. See [gate contracts](../data-quality/dq-gates.md).
