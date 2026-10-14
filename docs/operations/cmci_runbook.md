## CMCI Data Quality Job

The standalone CMCI data-quality job validates persisted data across the Bronze, Silver, Gold, and Platinum layers.

### Task Order

The tasks run sequentially:

1. `cmci_bronze_gx`
2. `cmci_silver_gx`
3. `cmci_gold_gx`
4. `cmci_platinum_gx`

A downstream task runs only after the preceding quality task succeeds.

### Expected Baseline

| Layer | Expected checks | Expected result | Blocking failures |
| --- | ---: | --- | ---: |
| Bronze | 14 | PASS | 0 |
| Silver | 70 | PASS | 0 |
| Gold | 21 | WARN | 0 |
| Platinum | 17 | PASS | 0 |

The expected Gold warning is:

```text
WARN | fact_cmci_indicator.raw_score_not_null | 132797 / 629090
```

This warning does not block the workflow. It represents unavailable CMCI source scores that are intentionally preserved as `NULL` rather than converted to zero or imputed.

Any additional Gold warning or any blocking failure requires investigation.

### Job Environment

The quality tasks use Databricks serverless environment version `6`.

Required dependency:

```text
great-expectations==1.23.1
```

The dependency is declared in:

- `pyproject.toml`
- `resources/jobs/data_quality.yml`

Running `cmci_quality.py` interactively requires the same dependency in the attached Databricks environment. A successful job run does not guarantee that the package is installed in a separate interactive workspace environment.

### Quality Result Tables

Detailed check results are appended to:

```text
ahon.quality.dq_results
```

Run-level summaries are appended to:

```text
ahon.quality.dq_run_log
```

Results are saved before the release gate is enforced, so failed runs remain auditable.

### Running a Single Layer

The quality runner accepts one of these layer arguments:

```text
--layer bronze
--layer silver
--layer gold
--layer platinum
```

Enforcement is enabled with:

```text
--enforce true
```

When enforcement is enabled, blocking failures cause the task to fail after the quality results are saved.

### Troubleshooting

#### `ModuleNotFoundError: No module named 'great_expectations'`

The attached interactive environment does not contain the required package.

Add the following dependency to the Databricks environment and apply the environment:

```text
great-expectations==1.23.1
```

Do not add runtime installation commands inside `cmci_quality.py`.

#### Gold Finishes With `WARN`

Confirm that:

- `Blocking failures` is `0`.
- The only warning is `fact_cmci_indicator.raw_score_not_null`.

This is the accepted baseline.

#### A Blocking Check Fails

1. Open the failed task output.
2. Record the table name, check name, failed rows, and total rows.
3. Query `ahon.quality.dq_results` using the latest `run_id`.
4. Fix or rebuild the affected data layer.
5. Rerun that layer and its downstream quality tasks.

Do not suppress or reclassify a blocking rule solely to make the job pass.
