# Validation evidence

Baseline main e1e8b883a1780b3150e6c7cd880530f4d7e42e66 was reconfirmed against the remote after implementation. All 50 PRs were paginated and assessed. Work is isolated in feature/coherent-medallion-pipeline; prior checkouts remain untouched.

## Offline checks

69 pytest tests pass (source failure/retry, immutable manifests, drift, observation and legacy-event diagnostics, conservative matching, reviewed-alias conflicts and actual financial MERGE-source parsing). Ruff check passes without CI mutation. Python compilation, YAML lint, diff whitespace and repository entry-point/dependency/documentation-link checks pass. Dev and prod bundle configurations validate strictly in the selected development workspace. Local tools are pinned to the versions exercised: pytest9.1.1, Ruff0.16.10, PyYAML6.0.3, SQLGlot30.21.0, yamllint1.37.1. Serverless environment6 uses pinned job libraries. Local checks do not establish Databricks execution. Windows sandbox temporary-directory rename permissions required a workspace-local pytest --basetemp; the complete suite then passed.

## Live development checks

Authorized profile 7474651621867565 at dbc-74becaaa-f34b.cloud.databricks.com. Nine paused jobs were deployed. Synthetic fixtures use separate ahon_integration_* catalogs.

| Run | Outcome / scope |
| --- | --- |
| 855558121182629 | Initial launcher failure: direct Databricks script lacked __file__; fixed with path-validating launcher. Pending retry canceled |
| 277907114947490 | Serverless rejected errorifexists write mode; fixed to error |
| [619663474246608](https://dbc-74becaaa-f34b.cloud.databricks.com/jobs/452750146274809/runs/619663474246608?o=7474651621867565) | PASS: actual population Bronze/Silver/Gold replay; 1744 raw,1743 accepted,1 fractional reject; corrected file yields3488 raw,1744 accepted,1 retained reject. Actual earthquake replay retains3 observations including2 identical rows and1 reject, no LGU assignment. CMCI replay yields34 fact records,1 invalid quarantine,missing value null and incomplete pillar. Blocking failure persists before raising |
| [161258103251102](https://dbc-74becaaa-f34b.cloud.databricks.com/jobs/452750146274809/runs/161258103251102?o=7474651621867565) | PASS: actual setup, PSGC Silver and dimension replay;9-digit code normalized with zero,footnote removed,unknown level quarantined,malformed code retained once before repeated blocking failure;4 accepted/dimension rows |
| 267990613538590 | Expanded suite reached funding and found equivalent shipped override aliases rejected by the guard. Fixed to permit identical reviewed targets while blocking conflicting targets. Full-suite automatic retry canceled because intermediate correction fixtures require fresh state |
| [144074574547018](https://dbc-74becaaa-f34b.cloud.databricks.com/jobs/452750146274809/runs/144074574547018?o=7474651621867565) | PASS: corrected population produces1744 Gold rows; actual financial mapping/Silver/Gold twice yields2 facts,zero-denominator NULL and overspent ratio2.0 with flag; later invalid CMCI correction masks Gold,valid correction recovers; evidence withholds risk/gap |
| [644608557954738](https://dbc-74becaaa-f34b.cloud.databricks.com/jobs/83978018181201/runs/644608557954738?o=7474651621867565) | PASS: actual default ahon_dev catalog/schema/volume/monitoring setup |
| 752096235156542 | Financial schema guard rejected newline after parenthesized SELECT in MERGE source. Fixed whitespace parsing and added regression tests against all three actual financial MERGEs |
| [712716407891759](https://dbc-74becaaa-f34b.cloud.databricks.com/jobs/452750146274809/runs/712716407891759?o=7474651621867565) | PASS: invalid financial total saved before blocking twice; quarantine remains1 and Gold remains2; new valid correction recovers3 facts and replays without duplication. Actual earthquake ingestion/Silver accepts finite magnitude -1 |

Per-stage second executions test actual replay rather than relying on local mocks. Quarantine is keyed to observation; original first-ingestion/processing lineage survives identical replay. Valid new content versions update business keys. Later rejected CMCI corrections retain Silver history but mask Gold values and coverage until valid recovery.

The final deployed development configuration points to ahon_dev; scheduled source jobs remain paused. Run IDs, task IDs, outcomes and scope are also recorded in [validation-evidence.json](validation-evidence.json). No validation result is pending.

## Limits and acceptance criteria

Runnable entry points, versioned geography, audited landing, separate observation/entity identities, nondeleting correction merges, explicit quality gates and transparent analytical outputs are implemented. No destructive existing-table migration, source-secret provisioning, schedule activation, production deployment, GitHub merge/review/comment or scientific approval occurred.

Full nationwide source extraction and complete parent orchestration are unverified: the selected workspace lacks the PSA API secret and a reviewed CMCI mapping CSV. Public mirrors are pinned but their complete live retrieval/profile counts were not rerun. Real historical geography and reviewed override accuracy need data-owner review; the tests exercise synthetic mapping contexts, not external scientific truth. Critical infrastructure lacks stable asset/CRS/coverage contracts. Event identity, hazard, exposure, vulnerability and comparable preparedness methodology remain blocked; risk/gap/rankings are withheld. Actual production identity, grants, networking and data migration remain unverified; production validation used the chosen dev profile only.

These results establish development behavior for the stated fixtures and configuration, not production readiness or scientific validity. Synthetic catalogs/volumes are retained as test evidence and should be cleaned up only after evidence retention approval.
