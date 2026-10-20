# Data quality gates

This integration candidate activates 12 gates in `resources/jobs/ahon_job.yml`. Each gate checks its just-built layer, writes results, and blocks dependent tasks on BLOCKED or ERROR. WARNs do not stop a run. A post-build gate cannot roll back its build or earlier committed statements.

| Path | Silver gate | Gold gate | Platinum gate |
| --- | --- | --- | --- |
| PSGC | dq_psgc_silver | dq_psgc_gold | None |
| Census population | dq_population_silver | dq_population_gold | None |
| OpenSTAT population | dq_population_openstat_silver | dq_population_openstat_gold | None |
| Funding | dq_ldrrmf_silver | dq_ldrrmf_gold | None |
| CMCI | dq_cmci_silver | dq_cmci_gold | dq_cmci_platinum |
| Earthquake observations | dq_earthquake_silver | None | None |

Population entry points are separate files under `src/sql/monitoring/psa_population/`. Each population gate receives the configured census year via the runner's widgets. Census components have no dependency on OpenSTAT extraction, builds or gates. Placeholder gates for unbuilt tables remain inactive.

The shared helper uses a MERGE keyed by run_id, check_id and table_name. Row-count checks persist the distance outside their bounds; other failed aggregate contracts persist at least one failure. PASS metrics are zero. Tests exercise the actual GX engine with a recording writer; full Spark/Delta execution and source-run evidence remain separate validation steps.

See [results](dq-result.md), [finding recovery](handling-rejects.md), and [decision 0020](../decisions/0020-one-dq-results-table.md).
