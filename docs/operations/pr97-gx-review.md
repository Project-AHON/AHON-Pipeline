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

- Local Python suite: **105 passed**, using the installed GX 1.24.0 engine, including missing/invalid evidence, advisory vs blocking results, all-branch DAG coverage and repair of a prior audit failure.
- Ruff: passed. YAML lint: passed. `pip check`: no broken requirements.
- Strict bundle validation: passed for `dev` and `prod`; production was not deployed or run.
- Isolated live-source development job: `754394724958313`; run `335667093267745` (still running when this record was prepared). This is not yet a live-source pass.
- Development catalog: `ahon_dev_pr97_gx_20261019`; CMCI verification is scoped to the existing reviewed five-LGU mapping, 2014-2024. PHIVOLCS is bounded to January 2024; the original pinned source revision and row profiles are retained.

A live-source pass will only be claimed after all 31 task results, the overall run and the GX audit are successful.
