# Pipeline runs and evidence

Databricks Jobs is the source of task execution state. The `run_log_views` task makes it queryable in the catalog through two views, `monitoring.run_log` and `monitoring.task_run_log`, and the AHON Pipeline Runs dashboard shows them (see [Run log views](#run-log-views)). `monitoring.dq_result` stores individual check results.

## Evidence to retain

For each verified run, record:

- Repository commit SHA and deployed bundle target.
- Databricks job run URL and task outcomes.
- Catalog, `batch_id` and selected source periods.
- Pinned source revision and completed landing manifests.
- Check results for the selected `run_id`.
- Unresolved warnings, rejected records and skipped tasks.
- Replay comparison when claiming idempotency.

Do not record tokens or secret values. No new cloud run is claimed by this documentation update.

## Current verification record

| Field | Evidence |
| --- | --- |
| Verification date | 2026-10-21 |
| Commit SHA | `62fa65b1f1edbb42bfbd4d097dd0ce0b5adfcfc9` (main after #120), clean working tree |
| Bundle target | `prod` (`cmci_run_mode = full`) |
| Databricks workspace | The team workspace |
| Job name | `ahon_pipeline` (job 532457890464535) |
| Job run | 109329846879869, started 2026-10-21 10:19:37 UTC, ended 10:51:59 UTC (32 minutes), `SUCCEEDED` |
| Catalog | `ahon` |
| Batch ID | `prod-verify-003` |
| Command executed | `databricks bundle run ahon_pipeline -t prod --profile <profile> --params "batch_id=prod-verify-003" --no-wait` |
| Tasks | 41 of 41 succeeded; 0 failed, 0 skipped, 0 blocked (`monitoring.run_log`) |
| Source periods | PSGC: snapshots Q2_2024 (selected), April_2024, Q4_2023, Q2_2021. Census population: 2024. BLGF LDRRMF: FY2018 to FY2024 (7 fiscal years). PHIVOLCS: 2024-01 to 2026-09 (33 months). CMCI: 2014 to 2024, full mode (1,634 LGUs) |
| Pinned source revision | Hugging Face `Jess-Christine/Project-AHON` at `9273d4b4d5af6d2a14e52451c45ac70a6e3012f5` |
| DQ outcomes | 324 PASS, 28 WARN, 0 BLOCKED, 0 ERROR (`run_id = 'prod-verify-003'`) |
| Same-batch replay | Run 319466650296701, same command and batch, no redeploy: started 11:14:50 UTC, ended 11:45:39 UTC (31 minutes), `SUCCEEDED`, 41 of 41 tasks. All 28 tables in bronze, silver, reference, gold and platinum have the same row count and fingerprint as after the first run |
| Evidence type | Live sources, production catalog |
| Verified by | Crystal |

### Row counts

Counts read after the run with the evidence notebook (row count per table, plus a fingerprint for the replay comparison). Bronze keeps every delivery, so a source loaded in two batches shows twice its rows in bronze; silver keeps one current row per business key. PSGC, OpenSTAT and LDRRMF were also loaded into bronze by the failed attempt `prod-verify-002` (see below), so bronze holds two deliveries of each.

| Dataset | Bronze | Silver | Rejected | Gold / reference | Platinum | Explained by |
| --- | --- | --- | --- | --- | --- | --- |
| PSGC | 350,158 | 175,067 (`psa_psgc_clean`, 4 snapshots) | 12 (`psa_psgc_reject`) | `dim_lgu` 43,760 (Q2_2024 only) | | Bronze holds two deliveries of 175,079 rows each (175,067 + 12) |
| PSA Population (census) | | 4,781 (`psgc_city_population_clean`), 16 not accepted | | `fact_population` 1,642 (one per city and municipality) | `population_components` 1,642 | 1,642 = the CITY and MUN rows in `dim_lgu`; 19 Sulu towns have no 2024 count (`MISSING`) |
| PSA OpenSTAT | 3,488 | 1,744 (`psa_population_clean`) | 0 | `fact_population_total` 1,744 | | Two deliveries of 1,744 in bronze |
| BLGF LDRRMF | 22,868 | 11,434 | 0 | `fact_ldrrmf` 11,434; `blgf_ldrrmf_lgu_map` 2,055 | `funding_components` 10,880 | Two deliveries of 11,434 in bronze. Platinum keeps city and municipality reports only (554 fewer) |
| PHIVOLCS earthquakes | 59,861 | 59,861 | 54 (`philvolcs_earthquake_reject`) | | `earthquake_observation_summary` 33 (one per month) | Silver keeps every bronze row and flags invalid ones; the 54 flagged invalid rows are also copied to the reject table (`reject_matches_flagged` passed) |
| CMCI | 328 (HTML batches) | 629,090 | 0 | `fact_cmci_indicator` 629,090; `dim_cmci_indicator` 35; `cmci_lgu_map` 1,634 | `cmci_pillar_coverage` 89,870 | 1,634 LGUs x 11 years = 17,974 LGU-years; x 35 indicators = 629,090; x 5 pillars = 89,870 |

The volume and reconciliation rules in the gates (33 rules, for example `row_count_matches_silver`, `total_appropriation_is_sum_of_parts`) all passed, except `psa_psgc_silver_rows_per_period_match_bronze` (below).

### Warnings

| Table | Check | Failed | Checked | Explanation |
| --- | --- | --- | --- | --- |
| `silver.psa_psgc_clean` | `area_name_no_broken_enye` | 1,824 | 175,067 | `ñ` stored as `Ã±` in the PSA source. Gold: 456 of 43,760 in `dim_lgu` |
| `silver.psa_psgc_clean` | `region_found` | 30,648 | 175,067 | Q2_2021 codes use PSA's older format; padding to 10 digits gives wrong regions. Q2_2021 is not used downstream |
| `silver.psa_psgc_clean` | `rows_per_period_match_bronze` | 4 | 4 periods | Gate counts every bronze delivery, so a second batch doubles bronze. Follow-up: compare only the current batch |
| `silver.psa_psgc_reject` | `invalid_geographic_fields` | 12 | | Source rows with invalid geographic fields, kept in the reject table |
| `silver.psgc_city_population_clean` | `records_accepted` | 16 | 4,781 | Special Geographic Area codes (`19999…`) not accepted |
| `gold.fact_population` | `population_available`, `missing_city_population` | 19 | 1,642 | Sulu municipalities with no 2024 census count |
| `gold.fact_population_total` | `city_rows_mapped_to_psgc` | 44 | 1,744 | OpenSTAT names with `ñ` and other spellings not matched; this chain is not used downstream |
| `silver.blgf_ldrrmf_annual_lgu_clean` | `psgc_matched`, `unmatched_funding_geography` | 235 | 11,434 | 5 old Maguindanao province rows, and 230 LGU-years the LDRRMF map marks `needs_review` (NCR and highly urbanized cities, cityhood changes). Fix pending |
| `silver.blgf_ldrrmf_annual_lgu_clean` | `overspent`, `zero_appropriation`, `negative_amount`, `funding_flagged_rows` | 388, 187, 3; 407 rows flagged | 11,434 | As reported by BLGF; kept and flagged with `has_dq_issues` |
| `gold.fact_ldrrmf` | `psgc_matched` | 235 | 11,434 | Same as silver |
| `gold.fact_ldrrmf` | `dim_lgu_has_ldrrmf_latest_year` | 43 | 1,724 | Cities and municipalities without FY2024 LDRRMF: the unmatched cities above and the 8 new BARMM towns |
| `gold.fact_ldrrmf` | `lgu_type_matches_psgc_level` | 4 | 11,199 | Baliuag, a town that became a city |
| `gold.fact_ldrrmf` | `utilization_rate_not_negative` | 2 | 11,434 | Negative expenditure in the source |
| `silver.philvolcs_earthquake_observation_clean` | `candidate_coverage` | 59,861 | 59,861 | The locality parser does not handle the source's spacing and degree sign; fix pending |
| `silver.philvolcs_earthquake_observation_clean` | `in_ph_bounds` | 698 | 59,861 | Coordinates outside the Philippine bounding box (offshore events); kept |
| `silver.philvolcs_earthquake_observation_clean` | `event_signature_unique` | 87 | 59,861 | The same event signature appears more than once in the source (for example rows 1267 and 1268 of 2024-08). Kept; cause not yet checked |
| `silver.philvolcs_earthquake_observation_clean` | `event_time_parsed` | 33 | 59,861 | Date-time text that could not be parsed (for example rows 1604 to 1608 of 2024-03). Flagged; cause not yet checked |
| `silver.philvolcs_earthquake_observation_clean` | `event_values_finite` | 24 | 59,861 | Coordinates, depth or magnitude not numeric. Flagged; cause not yet checked |
| `silver.philvolcs_earthquake_reject` | `invalid_observations` | 54 | | Rows flagged invalid by the checks above, copied to the reject table |
| `silver.cmci_indicator_clean`, `gold.fact_cmci_indicator` | `raw_score_present` | 132,797 | 629,090 | Source shows `-` or blank |
| `platinum.cmci_pillar_coverage` | `coverage_complete` | 30,503 | 89,870 | Pillars with too few indicators (`INSUFFICIENT_DATA`) |

### Limitations

- PHIVOLCS covers 2024-01 to 2026-09 only. The planned range from 2019-01 failed at extract (see below); the fix is tracked separately.
- The earthquake locality parser and the LDRRMF map regression are known and not fixed in this commit (see the warnings).
- The DQ outcomes above are the rows for `prod-verify-003`; a same-batch replay rewrites the same rows.

### Earlier runs not used as evidence

- Run 448657024603341 (2026-10-21, target `prod`, batch `prod-verify-002`, PHIVOLCS from 2019-01) failed at `earthquake_extract`: the 2019-06 PHIVOLCS page has six extra empty columns, and the extract expects exactly six. Tasks that did not depend on the earthquake extract ran and loaded PSGC, OpenSTAT and LDRRMF into bronze. The range was narrowed to start at 2024-01 for the run above.
- Run 512035323666586 (2026-10-21, batch `prod-verify-001`) succeeded on the same commit, but wrote to the development catalog with CMCI in test mode because the development and production override files had been swapped. It was the previous record of this page.
- Run 526096692147624 (2026-10-20, batch `crystal-run-001`) succeeded, and its DQ results were recovered from Delta history (version 927 of `monitoring.dq_result`: 328 PASS, 23 WARN). It is not used because it was deployed from a working tree with uncommitted changes, so the code that ran cannot be tied to a commit.

## Verification record template

Copy this section for each verified run.

- **Verification date:**
- **Commit SHA:**
- **Bundle target:**
- **Databricks workspace:**
- **Job name:**
- **Job run URL or ID:**
- **Catalog:**
- **Batch ID:**
- **Source periods:**
- **Pinned source revision:**
- **Command executed:**
- **Tasks selected:**
- **Successful tasks:**
- **Failed or skipped tasks:**
- **Expected versus actual row counts:**
- **DQ PASS results:**
- **DQ WARN results and explanations:**
- **DQ BLOCKED results:**
- **Rejected or unresolved records:**
- **Replay command and result:**
- **Evidence type:** live sources or synthetic fixtures
- **Verified by:**

## Interpreting outcomes

A successful task is not evidence that every branch ran. Check skipped, failed and excluded tasks separately. A partial run verifies only its selected tasks and prerequisites. A same-batch replay verifies reuse of a completed delivery; it does not verify a new source delivery.

## Run log views

`src/sql/monitoring/run_log_views.py` (task `run_log_views`, after `setup`) creates two read-only views on the Databricks system tables `system.lakeflow.jobs`, `job_run_timeline` and `job_task_run_timeline`. They keep only runs of the AHON bundle jobs (`ahon_pipeline`, `ahon_catalog_setup`, `ahon_data_quality`) whose `catalog` parameter is the catalog the views live in. Nothing is copied: each query reads the system tables.

The timeline tables split a run or task into time slices. Only the last slice of a finished run or task has a result; earlier slices have a NULL `result_state`. So `ended_at` and `duration_seconds` stay NULL until the final result arrives, and `last_seen_at` and `elapsed_seconds` show progress while it is still running.

**`monitoring.run_log`**: one row per job run. Key: `job_run_id`.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `job_name` | string | Deployed job name | Dev targets add a prefix, for example `[dev crystal_manas] ahon_pipeline` |
| `batch_id` | string | The `batch_id` job parameter; the `run_id` in `dq_result` | The job run id when left at its default `{{job.run_id}}` |
| `job_run_id` | string | Databricks job run id | Kept as the string the system tables use |
| `trigger_type` | string | How the run started | For example `ONETIME` |
| `run_type` | string | Run type | For example `JOB_RUN` |
| `started_at` | timestamp (UTC) | Run start | |
| `ended_at` | timestamp (UTC) | Run end | NULL until the run has a final result |
| `duration_seconds` | bigint | `ended_at` minus `started_at` | Includes time in queue. NULL until the run finishes |
| `last_seen_at` | timestamp (UTC) | End of the run's latest time slice | Equals `ended_at` once the run finishes |
| `elapsed_seconds` | bigint | `last_seen_at` minus `started_at` | Time so far for a run still in progress |
| `result_state` | string | Final state of the run | `SUCCEEDED`, `ERROR`, `CANCELLED`; NULL while running |
| `termination_code` | string | Why the run ended | For example `SUCCESS`, `RUN_EXECUTION_ERROR`, `USER_CANCELLED` |
| `tasks_total` | bigint | Tasks in the run | Equals the sum of the seven task counters below |
| `tasks_succeeded` | bigint | Tasks with state `SUCCEEDED` | |
| `tasks_failed` | bigint | Tasks with state `FAILED`, `ERROR` or `TIMED_OUT` | |
| `tasks_skipped` | bigint | Tasks with state `SKIPPED` | Not selected with `--only` |
| `tasks_blocked` | bigint | Tasks with state `BLOCKED` | An upstream task failed, for example a DQ gate, so the task did not run |
| `tasks_cancelled` | bigint | Tasks with state `CANCELLED` | |
| `tasks_running` | bigint | Tasks with no final result yet | |
| `tasks_other` | bigint | Tasks with any other final state | Expected to be 0 |
| `dq_pass`, `dq_warn`, `dq_blocked`, `dq_error` | bigint | `dq_result` rows for this `batch_id`, by disposition | Includes execution markers |

**`monitoring.task_run_log`**: one row per task per job run. Key: `job_run_id`, `task_key`.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `job_name`, `batch_id`, `job_run_id` | | As in `run_log` | |
| `task_key` | string | Task name in the job | |
| `started_at` | timestamp (UTC) | Start of the task's first time slice | |
| `ended_at` | timestamp (UTC) | Task end | NULL until the task has a final result |
| `duration_seconds` | bigint | `ended_at` minus `started_at` | NULL until the task finishes |
| `last_seen_at`, `elapsed_seconds` | | As in `run_log` | |
| `result_state` | string | Final state of the task | `SUCCEEDED`, `FAILED`, `ERROR`, `TIMED_OUT`, `SKIPPED`, `BLOCKED`, `CANCELLED`; NULL while running |
| `termination_code` | string | Why the task ended | |
| `attempts` | bigint | Task runs for this task key | More than 1 after a retry or repair |

Things to know:

- The job's `run_as` identity needs `SELECT` on `system.lakeflow`.
- System table data can take up to about an hour to appear, so a new run can be missing or still show as running for a while.
- Runs that reuse a `batch_id` share one set of DQ counts, because `dq_result` keeps one row per `run_id`, check and table. Leave `batch_id` at its default to get counts per run.
- Rows written per table per run are not recorded.

## Pipeline runs dashboard

The AHON Pipeline Runs dashboard (`resources/dashboards/ahon_pipeline_runs.lvdash.json`) reads the two views. The **Runs** page shows the number of runs, average duration, when the last run ended, runs by status, duration over time and the run history. The **Tasks** page shows, for the run picked in its Run filter, each task's duration and final state; with no run picked it shows the tasks of every run. Average duration and the last run use finished runs only. Times are in UTC. Like the data quality dashboard, it is defined in `databricks.yml`, and `bundle deploy` creates it with the bundle's `catalog` and `warehouse_id`. Edit the JSON, not the dashboard in the workspace: a deploy overwrites changes made there.

## DQ evidence

Use the selected `run_id` when reviewing `monitoring.dq_result`. WARN findings require an explanation. BLOCKED findings require investigation and rerunning the affected path after the cause is corrected. Earlier successful statements and tasks are not rolled back.

See [DQ results](../data-quality/dq-result.md) and the [runbook](runbook.md).
