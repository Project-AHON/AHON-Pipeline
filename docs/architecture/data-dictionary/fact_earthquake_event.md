# Earthquake event fact: contract and preparation status

**Target:** `<catalog>.gold.fact_earthquake_event`

**Source:** `<catalog>.silver.philvolcs_earthquake_data_clean`

**Status:** BLOCKED for materialization; the current code creates temporary review views only.

The intended grain supplied by the task is one earthquake event. A Silver row is
an observation, and multiple observations may describe a duplicate or correction.
No approved baseline earthquake Schema v1 was recovered: main's
`docs/architecture/data-model.md` is empty and issue #82 supplies no field/key or
correction contract. Proposed ADRs and another dataset's fact do not supply approval.

## Current executable preparation

`run_earthquake.py --stage gold` reaches the Gold SQL only after the existing
Bronze/Silver stop checks and verification succeed in the same session. It reads
the configured PSGC snapshot and existing `gold.dim_lgu`; it never rebuilds them.
The seven dimension attributes are compared exactly with the selected reference
projection, using null-safe equality. Missing rows, additional target-only codes,
changed names/levels/hierarchy and duplicate codes are explicit failures. Missing
tables, incompatible string schemas or an empty selected snapshot are BLOCKED.
The check establishes content equivalence, not formal snapshot approval.

The runner reads historical Silver conflict context using a semi join on matching
signature **or** time/coordinates. This detects cross-batch duplicates, possible
depth/magnitude corrections and signature collisions outside the same coordinates.
Distinct is applied only to lookup keys, never to select an observation winner.
Historical context is excluded from scoped observation totals. Spark may scan
Silver to locate those matches; the routine path does not rebuild all history or
collect growing event datasets on the driver. Only bounded rule summaries are collected.

`src/sql/gold/build_fact_earthquake_event.sql` produces these temporary views:

| View | Purpose |
| --- | --- |
| `earthquake_gold_signature_review` | Count observations and distinct event-field tuples per signature. |
| `earthquake_gold_correction_review` | Count different depth/magnitude tuples at a valid time/coordinate pair; these are review candidates, not confirmed corrections. |
| `earthquake_gold_accounting` | One disposition for every scoped Silver observation, preserving its fields and lineage. |
| `earthquake_gold_candidate` | Provisional, unconflicted signatures; not an approved fact schema or immutable event identity. |
| `earthquake_gold_dimension_comparison` | Compare the selected reference and current dimension, including extra dimension codes. |

## Observation accounting

| Disposition | Meaning |
| --- | --- |
| `invalid_observation_review` | DQ flag, missing/invalid event values or inconsistent derived month/year. Retained in Silver; no approved Gold exclusion is applied. |
| `signature_collision_review` | One signature has different event-field tuples. Requires investigation; no hash winner is selected. |
| `duplicate_signature_review` | More than one observation shares the signature, including historical context. No arbitrary deduplication is applied. |
| `possible_correction_review` | Different depth/magnitude values share time/coordinates. Requires a correction policy, not automatic replacement. |
| `eligible_contract_blocked` | Otherwise usable observation retained in the provisional candidate, including unmatched geography; not yet an included fact event. |

Disposition precedence follows the table above. It gives each scoped observation
exactly one primary reason; independent duplicate/collision/correction counts may
overlap. Scope count equals the sum of disposition counts. Provisional candidate
count equals `eligible_contract_blocked` count. No disposition is silently lost,
and no count is advertised as an actual Gold event count.

## Preserved source fields, not a finalized fact schema

| Column | Current Silver type | Preparation meaning |
| --- | --- | --- |
| `id` | BIGINT | Existing `xxhash64(event_time, latitude, longitude, depth, magnitude)` signature, not an approved event key. |
| `event_time` | TIMESTAMP | Legacy wall-clock convention; not relabeled UTC. |
| `latitude`, `longitude`, `depth`, `magnitude` | DOUBLE | Copied without corrections; invalid values remain reviewable. |
| `month`, `year` | INT | Derived Silver month/year checked against event time. |
| `psgc_code` | STRING, nullable | Remains null while analytical meaning is unapproved; candidate arrays are never promoted. |
| `psgc_match_status`, `psgc_match_method` | STRING | Current `blocked_meaning` and null method are checked, not replaced by invented final statuses. |
| `_psgc_snapshot` | STRING | One configured reference vintage; not historical administrative truth. |
| `_source_name`, `_source_ref`, `_batch_id`, `_row_hash` | STRING | Original source identity, immutable landing reference, first batch and observation hash. |
| `_ingested_at`, `_processed_at` | TIMESTAMP | First ingestion and Silver processing lineage, unchanged by review. |
| `has_dq_issues` | BOOLEAN | Source/Silver DQ flag, separate from mapping gaps. |

No confidence probability, impact measure, activity definition, risk category or
Platinum score is added. Preserve the broader baseline formula: normalized
population exposure multiplied by normalized earthquake activity.

## Checks and write boundary

Aggregate results use the existing stable name/attribute/action/count/denominator/
expected/status/as-of format. Checks cover accounting, identity/fan-out, lineage,
unchanged signatures, mapping/status agreement, snapshot compatibility, candidate
uniqueness and unmatched retention. Duplicate/correction/collision reviews report
WARN when present. Critical failures raise through the existing Python stop gate.
The contract rule always reports BLOCKED and raises before any Gold write. The
SQL also ends with a blocking assertion for standalone execution.

There is no Gold DDL, MERGE, target schema migration, full rewrite or postwrite
verification in this version. Existing Gold contents remain untouched, including
on retries. Running the diagnostic twice does not test Delta MERGE idempotency.

## Exact decisions needed for materialization

The Gold model owner and PHIVOLCS source steward must provide an approved record of:

1. Immutable event identity, key name/type and complete fact field names/types/nullability;
   whether the existing signature is retained as a separate attribute.
2. Duplicate handling, hash-collision handling, correction/version policy, tie resolution
   and how every contributing observation's lineage relates to its included event.
3. Event eligibility and invalid/non-event exclusions, including period-inconsistent
   observations; a DQ flag alone is not an approved event-exclusion policy.
4. Source time semantics, final wall-clock/UTC/NTZ representation, integer year
   derivation and any reviewed timestamp/key migration.
5. Geographic meaning, nullable dim_lgu relationship and allowed level, snapshot
   approval, method/status vocabulary and confidence semantics only if required.

Then implement candidate validation, atomic incremental MERGE, unchanged-row
preservation and postwrite reconciliation against a fresh dev target or reviewed
migration. Validate a second unchanged run and target preservation on critical
failure before production consideration. Do not enable writes by simply deleting
the blocking assertion. Ownership roles remain unassigned in the governance page;
Jessy coordinates issue #82 but is not assumed to own every contract decision.
