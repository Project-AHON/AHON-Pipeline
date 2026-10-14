# Reconciliation verification

Development work started from `main` commit `4d82f6b5010c715ed14eca490c5415692e8fc34e` on 2026-10-14 and was verified through 2026-10-15 (Asia/Manila). The isolated branch is `feature/pipeline-reconciliation`. Existing checkouts were preserved. No GitHub publication, review action, merge, or production deployment/execution occurred.

## Local and bundle checks

- 60 pytest tests passed, covering immutable landing and repair, source parsing, conservative matching, schema checks, task assets, DAG edges and parameter contracts.
- Ruff, Python compilation, repository YAML lint and `git diff --check` passed.
- The public-PyPI dependency lock resolved 40 packages and passed offline `uv lock --check`.
- Both `dev` and `prod` passed strict bundle validation with explicit local inputs. Production validation checked the bundle schema using the selected development workspace; it does not certify production permissions or deploy resources there.
- All 32 configured tasks across the three existing job files reference existing assets. The main graph has 30 tasks, with no schedule.

Operational values have no committed defaults. The ignored local override file records the selected development settings. Source definitions, expected profiles and windows are inputs; fixed schema/parsing rules and pinned dependencies are reproducibility contracts.

## Actual source execution and replay

The user selected workspace `7474651621867565`. Actual source runs used an isolated development catalog, `ahon_source_validation_20261014_r2`, with the same completed delivery ID, `source_20261014`.

- [Initial source run 49333559656243](https://dbc-74becaaa-f34b.cloud.databricks.com/jobs/783455110112861/runs/49333559656243?o=7474651621867565): SUCCESS.
- [Replay 728254180109582](https://dbc-74becaaa-f34b.cloud.databricks.com/jobs/783455110112861/runs/728254180109582?o=7474651621867565): SUCCESS with explicit source and execution settings.

| Table | Rows | Distinct observation IDs | Latest replay inserted / updated / deleted |
| --- | ---: | ---: | --- |
| `bronze.psa_population` | 1,744 | 1,744 | 0 / 0 / 0 |
| `silver.psa_population_clean` | 1,744 | 1,744 | 0 / 0 / 0 |
| `bronze.blgf_ldrrmf_annual_lgu` | 11,434 | 11,434 | 0 / 0 / 0 |
| `bronze.philvolcs_earthquake_observation` | 1,896 | 1,896 | 0 / 0 / 0 |
| `bronze.osm_critical_infrastructures` | 55,482 | 55,482 | 0 / 0 / 0 |
| `silver.osm_critical_infrastructures_clean` | 55,482 | 55,482 | 0 / 0 / 0 |

Counts and distinct IDs were read through the existing SQL warehouse. Delta history independently showed zero changed rows in all six latest MERGEs, preserving lineage and processing values. These counts describe the selected source deliveries, not event deduplication, LGU exposure or a scientific score. BLGF and OSM assets were pinned to HF commit `9273d4b4d5af6d2a14e52451c45ac70a6e3012f5`.

## Databricks transformations on synthetic fixtures

[Run 195096659742537](https://dbc-74becaaa-f34b.cloud.databricks.com/jobs/988399241764698/runs/195096659742537?o=7474651621867565) succeeded in `ahon_integration_20261014_r3`. It exercised the actual Silver, dimension, fact and component entry points with deliberately synthetic source records:

- PSGC leading zeros, selected snapshot, unknown-level disposition and reference cardinality.
- Barangay/census uniqueness, NULL population retention, incomplete aggregation and a correction from 1,200 to 1,300 without changing the fact grain.
- Financial zero-denominator NULLs, overspending ratio 2, warning retention and SQL replay.
- CMCI valid 4 → invalid text → reverted 4 in distinct deliveries, individual stage replay, invalid-current-value masking and coverage of the selected indicator set.
- Separate identical earthquake observations, invalid coordinate/time retention, textual candidates and withheld LGU assignment.
- Persisted blocking checks and preservation of accepted facts after a deliberate duplicate-reference failure.

[Run 1061032185124964](https://dbc-74becaaa-f34b.cloud.databricks.com/jobs/988399241764698/runs/1061032185124964?o=7474651621867565) also succeeded in a fresh `ahon_integration_20261014_r4` catalog after the explicit-input changes. This establishes runtime verification of the parameterized entry points and packaged metadata asset, in addition to the earlier correction/replay checks.

[Focused run 702438750132271](https://dbc-74becaaa-f34b.cloud.databricks.com/?o=7474651621867565#job/150338044882251/run/702438750132271) succeeded against completed synthetic fixtures after the final CMCI definition guard was added. Unchanged definitions replayed; a deliberate version change raised a persisted blocking check and left facts unchanged. The first focused attempt had correctly blocked a mismatch between the check's newer fixture version and the earlier catalog. The corrected check reads the existing fixture version before introducing the deliberate mismatch. This was a one-time submitted run, not another saved job.

The initial verification attempt failed on a missing sibling-module import after earlier transforms had passed. The launcher/module bootstrap was corrected; subsequent runs used fresh fixture catalogs. Fixture runs have zero automatic retries, and replay is tested explicitly inside the suite.

## Limits and execution

Actual PSA extraction is blocked by unavailable approved token secrets. Actual CMCI ingestion is blocked by the absence of a reviewed mapping for the selected snapshot. Real-source reference-dependent facts were therefore not claimed as verified; synthetic mappings remain test data. Event Gold, spatial exposure, vulnerability, preparedness scores and priority rankings require approved source identities and scientific methodology.

Use the [development runbook](runbook.md) for explicit configuration, exact validation/deployment commands, independent task selection, replay and correction recovery. The [inventory](../architecture/source-to-target.md) records PR reuse/exclusions and every supported source contract. The [analytical contract](../architecture/metrics.md) maps the four business questions to available evidence and missing inputs.
