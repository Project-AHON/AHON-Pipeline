# 0010: Executable medallion contracts and conservative evidence outputs

- Status: Engineering implementation; stakeholder and scientific approval not claimed
- Date: 2026-10-13

Start from current main `e1e8b883a1780b3150e6c7cd880530f4d7e42e66`. Preserve the existing checkouts; integrate selected files and rules after reviewing all 50 PRs. The working branch uses `feature/` because the repository's governance rejects other prefixes.

Use one shared setup job, a reference job, independent dataset jobs, one optional evidence refresh and a parent orchestrator. Use serverless STANDARD, pinned dependencies, one concurrent run per dataset, bounded retries and paused source schedules. A common launcher establishes repository paths on serverless, where direct script execution may omit `__file__`.

Keep the inherited Q2_2024 reference as an explicit version. Retain Bronze observations and first-ingestion lineage. File-version identity is independent of business keys. Earthquake observations use file digest and source ordinal; timestamps are source wall-clock TIMESTAMP_NTZ. This is a fresh-catalog schema contract, not an automatic migration of existing inferred or event-deduplicated tables. The conservative #85/#88 legacy checks remain available for reviewing existing tables.

Reuse CMCI's 35-indicator source request and validation rules, and #84's indicator dimension and selected-scope pillar coverage purpose. Replace five wide Silver tables with one long indicator table so typing, quarantine and corrections have one implementation. Preserve the `cmci_pillar_score` output name but never call its raw sum official CMCI or preparedness scoring. The old schemas are not automatically migrated.

Reuse #81's contextual BLGF matcher, reviewed override asset and checked financial formulas. Withhold name-only matches and mixed geographic-level matches unless reviewed. Use idempotent upserts instead of deleting absent data. Do not attach a fabricated 1.0 probability to a deterministic textual match.

Risk classification, risk–preparedness subtraction, highest-risk ranking, event identity and geographic exposure bridges remain blocked until the missing hazard, exposure, vulnerability and scoring evidence is reviewed. Runnable source and Silver jobs remain independent of those decisions. Component evidence and explicit insufficient-data statuses are materialized now.

Default job ownership and run identity are restricted to the configured user; no blanket `users` write grant is added. Production operators must select an approved stable identity and grant only the catalog/schema/table/volume privileges documented in the runbook. Production validation is not deployment authorization.

Alternatives rejected: blind branch merges; raw earthquake counts as risk; automatically reviewed fuzzy mappings; deleting Silver records after failed typing; multi-table atomicity claims; bulk untyped HF ingestion; GPU training for ordinary batch data processing.
