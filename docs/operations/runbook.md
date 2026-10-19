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

Run order inside the job: `setup` first, then PSGC (`psgc_extract` → `psgc_bronze` → `psgc_silver` → `dim_lgu`). The other datasets wait for the PSGC tables they match against. Independent extracts depend only on `setup`, so one failed source does not block another.

## By dataset

### Setup

- **Task:** `setup` (also the separate job `catalog_setup`)
- **Does:** creates the catalog if permitted, the schemas, the `reference.source` volume and `monitoring.dq_result`. It never rebuilds existing tables.

### PSGC

- **Run:** `--only +dim_lgu`
- **Needs:** the PSA token secret (`psgc_secret_scope`, `psgc_secret_key`), `psgc_api_base_url`, `psgc_expected_rows`, `source_period`.
- **Check:** `gold.dim_lgu` has 43,760 rows for `Q2_2024`, 1,642 of them `CITY` or `MUN`.
- **If it fails:**
  - HTTP or empty-result error: the token is missing, empty or expired. Rotate it with `databricks secrets put-secret <scope> <key>`; the job's `run_as` identity needs READ on the scope.
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
- **Check:** `platinum.earthquake_observation_summary` has a row for every requested month.
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
- **Check:** `gold.fact_ldrrmf` has 11,434 rows; the match summary at the end of the gold task shows only the known unmatched rows.
- **If it fails:**
  - `reference_map_coverage`: bronze has an LGU name the map has not seen. Rerun `ldrrmf_mapping`, review new `needs_review` or `ambiguous` rows, and add an override with a reason if needed.
  - `component_totals` or `missing_amounts`: see `silver.blgf_ldrrmf_annual_lgu_reject`.
  - `unique_matched_lgu_year`: two BLGF spellings map to one PSGC code in the same year. Fix with an override.

## Rerun and replay

- **Retry a failed run:** rerun with the same `batch_id`. Completed files are not fetched again, and rows already written keep their first lineage and timestamps.
- **Load a correction from the source:** run with a new `batch_id`. Changed rows update by business key; older deliveries never overwrite newer ones.
- **Rerun one branch only:** use `--only +<task>` with the same `batch_id`.

Nothing is deleted by a rerun. A row that disappears from the source stays in the tables.

## Recover from a blocked check

A stop rule fails the task before its table is written, and dependent tasks are skipped. The tables keep their last accepted state.

1. Open the failed task's output; it names the check (`check_id`) and the table.
2. Look up the result in `monitoring.dq_result` for that `run_id`.
3. Look at the rows behind it: the dataset's `_reject` table, or rows with `has_dq_issues`.
4. Fix the cause: a reviewed mapping or override, a configuration value, or a corrected source in a new batch.
5. Rerun the affected branch with `--only +<task>`.

Before treating a gold table as current, check the latest `monitoring.dq_result` rows for its task. See [data quality](monitoring/data-quality.md) for what each rule does.

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