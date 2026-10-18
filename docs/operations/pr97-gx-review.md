# PR #97: review and end-to-end GX integration

Reviewed source PR: https://github.com/Project-AHON/AHON-Pipeline/pull/97

- PR head: `d0c1095e177c1ff9220bd45744537e1360b9b853`.
- Integration base: `90ba73a` on main, including PR #96.
- Branch: `feature/98-gx-end-to-end`.

## Findings addressed

1. **Exported job runs an old release.** Every task in #97 uses one user's absolute `/Workspace/Users/.../releases/20261015-repair` path. A checkout/deployment of the branch would not execute that branch's changes. Keep main's bundle-relative launcher and `${workspace.file_path}` root, preserving all source-to-component branches.
2. **Current pipeline contracts conflict with the export.** #97 already includes GX 1.24.0 in its seven environments, but diverged before the latest main configuration changes. Resolve the job conflict by retaining main's portable DAG, the same GX pins and inline pre-write quality checks. Keep explicit variables, run identity, safe mapping and dataset grain contracts. The CMCI files differ only in formatting relative to the merge base; retain main's executable versions.
3. **No single end-to-end quality completion condition.** Add one `quality_checks` task depending on all six leaves. Require same-run GX evidence, preserve warnings, reject blocked/malformed evidence, and validate actual output availability/key uniqueness. DAG tests require every pipeline task to be an ancestor of this barrier.
4. **Dependency conflict already introduced on main.** Remove the extra `great-expectations==1.23.1` from `pyproject.toml`; keep 1.24.0 matching requirements and job environments. This prevents pip's normalized package-name/version conflict.
5. **Limit deployment to runtime assets.** Explicit `sync.paths` restricts upload to `src`, `tests` and `resources`; local environments, notebooks and private override files are outside the sync roots.
6. **Do not schedule incomplete gate notebooks.** Several new gate files reference `ahon_dev`, old CMCI wide/pillar-score tables and unbuilt risk outputs. The existing shared GX metric engine operates on the actual pipeline schemas and preserves the established monitoring contract. Document the draft status instead of creating nonexistent outputs or weakening checks.

## Scope and cost

The pipeline remains source extraction -> verified Volume delivery -> Bronze -> Silver -> supported Gold -> Platinum component views, with 31 tasks including the final audit. Earthquakes remain observation evidence, not fabricated deduplicated Gold events or exposure/risk classes. The extra task reuses core serverless dependencies in STANDARD mode, adds no schedule, and has zero retries. Production is configuration-validated only; development runs use a separate bundle root/catalog.

## Validation

- Local Python suite: **105 passed**, using installed GX 1.24.0, including missing/invalid evidence, advisory versus blocking results, all-branch DAG coverage and repair of a prior audit failure.
- Ruff, YAML lint and `pip check`: passed.
- Strict bundle validation: passed for `dev` and `prod`; production was not deployed or run.
- [Real-source development run 335667093267745](https://dbc-74becaaa-f34b.cloud.databricks.com/jobs/754394724958313/runs/335667093267745?o=7474651621867565): **overall SUCCESS; all 31 tasks SUCCESS**, with no repair or automatic retries. Duration: 22.24 minutes, including serverless startup; this is evidence from one bounded development run, not a latency SLA.
- Final audit: **PASS**, validating 108 upstream GX records and all six terminal outputs. Including its own checks, the run recorded **122 GX 1.24.0 results: 106 blocking checks PASS, 12 advisory checks PASS, four advisory checks WARN. Zero blocking failed rows.**
- Isolated development catalog: `ahon_dev_pr97_gx_20261019`. CMCI uses the existing reviewed five-LGU mapping, 2014-2024; PHIVOLCS is January 2024. The pinned file-source revision and original source row profiles were preserved. This proves the complete process for that scope, not nationwide CMCI coverage or future publisher availability.

### Observed warnings retained

| Rule | Recorded rows | Interpretation |
| --- | ---: | --- |
| Missing city census population | 19 | Keep population NULL with its recorded disposition |
| Flagged funding observations | 407 | Keep existing source-quality flags |
| Unmatched funding geography | 235 | Keep unresolved PSGC values NULL |
| Invalid PSGC geographic fields | 12 | Retain rejected observations and provenance |

These are counts per rule and can overlap. They are not 673 distinct LGUs or source observations. No threshold was relaxed and no source data was fabricated to obtain success.

### Independently verified outputs

| Table/view | Rows |
| --- | ---: |
| `gold.dim_lgu` | 43,760 |
| `gold.fact_population` | 1,642 |
| `gold.fact_population_total` | 1,744 |
| `gold.fact_ldrrmf` | 11,434 |
| `gold.fact_cmci_indicator` | 1,925 |
| `platinum.cmci_pillar_coverage` | 275 |
| `platinum.population_components` | 1,642 |
| `platinum.funding_components` | 10,880 |
| `platinum.earthquake_observation_summary` | 1 |
| `silver.osm_critical_infrastructures_clean` | 55,482 |

The component views retain their established interpretation limits; these counts do not establish LGU risk or preparedness.

### GitHub delivery

The branch is pushed. Opening a draft PR was attempted, but the GitHub connector returned **403: Resource not accessible by integration**; GitHub CLI has no authenticated session. Review or open a PR from `feature/98-gx-end-to-end` into main using an account with PR write permission. PR #97 and the existing production/deployed pipeline were not merged, closed or overwritten.
