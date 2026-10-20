# Reconciliation verification

Development work started from `main` commit `4d82f6b5010c715ed14eca490c5415692e8fc34e` on 2026-10-14 and was verified through 2026-10-15 (Asia/Manila). The isolated branch is `feature/pipeline-reconciliation`. Existing checkouts were preserved. No GitHub publication, review action, merge, or production deployment/execution occurred.

## Local and bundle checks

- 84 pytest tests passed, covering immutable landing and repair, source parsing, conservative matching, schema checks, task assets, DAG edges, real GX expectations, severity/failure behavior, token-safe transport guards, preservation/recovery of runtime errors, city/year grain, missing source observations and exclusion of provincial funding from city-level analysis.
- Ruff, Python compilation, repository YAML lint and `git diff --check` passed.
- The public-PyPI dependency lock resolved 68 packages including pinned GX Core and passed offline `uv lock --check`.
- Both `dev` and `prod` passed strict bundle validation with explicit local inputs. Production validation checked the bundle schema using the selected development workspace; it does not certify production permissions or deploy resources there.
- The recorded historical checks used the then-current task graph. They do not establish current task counts or deployment validation. The fixture job is development-only; no schedule is configured.

Operational values have no committed defaults. The ignored local override file records the selected development settings. Source definitions, expected profiles and windows are inputs; fixed schema/parsing rules and pinned dependencies are reproducibility contracts.

## Actual source execution and replay

The user selected workspace `7474651621867565`. Actual source runs used an isolated development catalog, `ahon_source_validation_20261014_r2`, with the same completed delivery ID, `source_20261014`.

- [Initial source run 49333559656243](https://dbc-74becaaa-f34b.cloud.databricks.com/jobs/783455110112861/runs/49333559656243?o=7474651621867565): SUCCESS.
- [Replay 728254180109582](https://dbc-74becaaa-f34b.cloud.databricks.com/jobs/783455110112861/runs/728254180109582?o=7474651621867565): SUCCESS with explicit source and execution settings.
- [GX replay 544688810964582](https://dbc-74becaaa-f34b.cloud.databricks.com/jobs/783455110112861/runs/544688810964582?o=7474651621867565): SUCCESS. All 16 current source-branch check results contain GX evidence, with no blocking failures. Existing monitoring rows were preserved while the audit column was added; earlier failures in unselected branches remain historical records. Filter by the current attempt's `checked_at` range and selected branches when reviewing a partial replay.

| Table | Rows | Distinct observation IDs | Latest replay inserted / updated / deleted |
| --- | ---: | ---: | --- |
| `bronze.psa_population` | 1,744 | 1,744 | 0 / 0 / 0 |
| `silver.psa_population_clean` | 1,744 | 1,744 | 0 / 0 / 0 |
| `bronze.blgf_ldrrmf_annual_lgu` | 11,434 | 11,434 | 0 / 0 / 0 |
| `bronze.philvolcs_earthquake_observation` | 1,896 | 1,896 | 0 / 0 / 0 |

Counts and distinct IDs were read through the existing SQL warehouse. Delta history independently showed zero changed rows in the replayed tables, preserving lineage and processing values. These counts describe the selected source deliveries, not event deduplication, LGU exposure or a scientific score. BLGF assets were pinned to HF commit `9273d4b4d5af6d2a14e52451c45ac70a6e3012f5`.

### Live PSGC authentication and reference processing

[PSGC run 512402624830874](https://dbc-74becaaa-f34b.cloud.databricks.com/jobs/783455110112861/runs/512402624830874?o=7474651621867565) succeeded after the user configured the token in Databricks Secrets. Setup, extraction, Bronze and Silver all passed. Four complete configured snapshots produced 175,079 Bronze observations and 175,067 Silver reference rows. At initial PSGC branch completion, the delivery recorded 15 PASS rules, one WARN rule and no BLOCKED rule. Later reference-dependent runs reuse its delivery ID and record their own checks. The selected snapshot contains explicit population records for 2015, 2020 and 2024; census year remains a separate configuration input.

The secret value was never retrieved by the local coding agent or written into repository configuration. Only scope/key metadata is configured. This verifies live PSA access; production still requires its approved identity, permissions and secret reference.

### Current city/municipality population grain

Following the user's correction in [decision 0017](../decisions/0017-city-municipality-analysis-grain.md), the canonical population key is `(psgc_code, population_year)` for CITY/MUN entities. Native reference levels and older barangay facts remain preserved. Verification used fresh catalog `ahon_city_validation_20261015042736`, with the already authenticated Q2 2024 PSGC Silver snapshot and dimension copied from the earlier source catalog, retaining their source lineage. It did not re-extract PSA data or migrate the old fact in place.

[City population run 642804757599731](https://dbc-74becaaa-f34b.cloud.databricks.com/jobs/783455110112861/runs/642804757599731?o=7474651621867565) and [replay 883474893848182](https://dbc-74becaaa-f34b.cloud.databricks.com/jobs/783455110112861/runs/883474893848182?o=7474651621867565) succeeded. The existing development SQL warehouse independently verified 1,642 rows and distinct city/year keys: 1,623 direct reported measurements, 19 NULLs and zero non-city/municipality rows. The new catalog contained no blocking GX results or results missing GX evidence. These are direct city totals, not sums of barangay measurements.

[Focused fixture run 804282745817815](https://dbc-74becaaa-f34b.cloud.databricks.com/?o=7474651621867565#job/259429303310152/run/804282745817815) succeeded in `ahon_integration_city_20261015042736`. It verified city/year grain, exclusion of barangays from Gold, NULL reported values, entirely absent city census observations, identical full-row replay including processing timestamps, and correction from 1,200 to 1,300 without adding another city/year fact. This focused mode did not rerun the other dataset transformations; their earlier evidence remains below.

The canonical candidate also requires the typed census row's source hash to match the current PSGC record. A local SQL regression verifies that removing a year from a corrected source array masks its retained older typed observation as NULL. The private development default now points to the city-grain catalog. Production requires a reviewed rebuild/cutover; the old barangay schema is deliberately blocked rather than changed automatically.

[Final replay 239409651199651](https://dbc-74becaaa-f34b.cloud.databricks.com/jobs/783455110112861/runs/239409651199651?o=7474651621867565) succeeded after deploying that source-hash guard and explicit UTC hash serialization to development. Delta history version 2 independently confirmed 1,642 source rows and zero inserted, updated or deleted rows. Counts remained 1,642 / 1,623 / 19, with zero non-city/municipality rows, zero blocking GX checks and no missing GX evidence. Strict dev and prod bundle schema validations also passed; production was not deployed or run.

### Historical population checks before the grain correction

The following barangay-fact and aggregated-coverage runs predate the user's city/municipality requirement and decision 0012. They remain historical engineering evidence, not verification of the current Gold population grain.

[Reference/population run 714585178062145](https://dbc-74becaaa-f34b.cloud.databricks.com/jobs/783455110112861/runs/714585178062145?o=7474651621867565) succeeded using that completed delivery, after native population Silver had succeeded in run 28732734704355. The selected dimension contains 43,760 entities. The earlier 2024 barangay fact contained 41,593 rows and distinct surrogate keys; 12 missing population measurements remained NULL. The initial dimension attempt in run 28732734704355 exposed an unsupported Spark Connect write-mode alias introduced during this review. It was corrected to `error`; the successful run also verified that the unexpected-execution failure marker recovered to PASS. The delivery then had no BLOCKED checks and no missing GX evidence. Initial coverage output exposed LGUs without observations being omitted; the component query was corrected to include the full same-snapshot LGU/year grid.

[Coverage run 431074094031300](https://dbc-74becaaa-f34b.cloud.databricks.com/jobs/783455110112861/runs/431074094031300?o=7474651621867565) succeeded with the earlier component SQL. The existing development SQL warehouse independently verified all 1,642 same-snapshot cities/municipalities: 1,611 COMPLETE and 31 PARTIAL_OR_MISSING. This included 20 LGUs with no barangay observations, retaining NULL population totals. These aggregated coverage states and counts are superseded by the direct city-measurement contract above. Fixture executions still require a separate explicit `ahon_integration_*` catalog.

## Databricks transformations on synthetic fixtures

[Older-delivery replay run 653710300874370](https://dbc-74becaaa-f34b.cloud.databricks.com/?o=7474651621867565#job/1024028664834828/run/653710300874370) succeeded against completed synthetic fixtures. It processed two distinct corrected deliveries for PSGC census records and financial reports, then replayed the older delivery. Silver and Gold values, processing timestamps and lineage all remained identical to the newer accepted state. This verifies the added ingestion-time update guards; the check is also part of the regular development fixture suite.

[GX run 657452296951740](https://dbc-74becaaa-f34b.cloud.databricks.com/jobs/988399241764698/runs/657452296951740?o=7474651621867565) succeeded in fresh catalog `ahon_integration_gx_20261015013052`. It repeated the source correction, replay, financial edge, CMCI definition and blocking-failure scenarios below using the shared GX engine. The suite also asserted that every recorded check contained GX validation evidence and that a deliberate blocking failure contained `success: false` while preserving the accepted target. Together with the actual-source GX replay above, this verifies the shared DQ path across PSGC, population, LDRRMF, CMCI and earthquake observations. Live PSGC authentication was subsequently verified above; the reviewed production CMCI mapping remains an external prerequisite.

The following runs established the transformation behavior before the GX change:

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

Actual CMCI ingestion is blocked by the absence of a reviewed mapping for the selected snapshot. Live PSA reference processing has passed; runtime claims for downstream facts are limited to the specific runs recorded here. Synthetic mappings remain test data. Event Gold, spatial exposure, vulnerability, preparedness scores and priority rankings require approved source identities and scientific methodology.

Use the [development runbook](runbook.md) for explicit configuration, exact validation/deployment commands, independent task selection, replay and correction recovery. The [inventory](../architecture/source-to-target.md) records PR reuse/exclusions and every supported source contract. The [analytical contract](metrics.md) maps the four business questions to available evidence and missing inputs.
