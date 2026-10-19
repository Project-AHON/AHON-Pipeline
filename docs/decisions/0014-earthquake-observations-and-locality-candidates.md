# 0014: Preserve earthquake observations and report locality candidates

- **Status:** Proposed
- **Date:** 2026-10-12
- **Tracking:** Part of [#82](https://github.com/Project-AHON/AHON-Pipeline/issues/82)

The earthquake task requires retained invalid observations and PSGC enrichment.
Main's Silver event signature is not an observation identity: corrections to depth
or magnitude change it. No approved earthquake Schema v1, geographic meaning or
correction policy was recovered. ADR 0006 remains Proposed. Numbers 0007 and 0008
are used by open PRs #69 and #81, so this record uses 0009.

The proposed narrow contract keeps the existing typed Bronze schema and legacy
SHA-256 of inferred source fields joined by `||`, skipping nulls. Inference types
must be pinned to reviewed evidence; ambiguity and drift stop writes. Immutable
CSV bytes, their digest, header, row counts and inference types are retained beside
the source. Typed Bronze and trimmed strings cannot prove original text fidelity.
A future raw-text/hash migration needs separate review and source reconciliation.

Silver retains one row per distinct Bronze `_row_hash`, carries all five lineage
fields unchanged, adds `_processed_at` and `has_dq_issues`, and permits nullable
typed event fields. `id = xxhash64(event_time, latitude, longitude, depth, magnitude)`
remains a signature, without a unique/primary-key assertion. New nullable columns
report parsed locality/province and exact PSGC **candidates**, snapshot and method.
The candidate array/status distinguishes one candidate, ambiguity, an unmatched
name, an unparsed description, and a blocked reference. No probability is assigned.

Proposed mapping meaning: the town referenced by the source distance/bearing
description. It is neither epicenter containment nor earthquake impact. Parse only
the demonstrated single-parenthesis form, compare names with case/whitespace
normalization, and use one configured `silver.psa_psgc_clean` snapshot at CITY/MUN
level. Keep significant tokens and accents; no aliases, overrides, nearest point
or fuzzy matching. Independent cities missing a conventional province need
reviewed evidence before any exception. Until the meaning is approved,
`psgc_code` and `psgc_match_method` remain null and `psgc_match_status` is
`blocked_meaning`. Missing reference data blocks candidates, not independent
cleaning. A small temporary distinct-locality map suffices; no persisted map or
growing driver collection is needed.

Use the configured Q2_2024 dimension reference as candidate baseline; never mix
it with the Q2 2026 workbook/master or unverified boundary outputs. This is mapping
to a reference vintage, not historical administrative truth. No validated boundary
loader/crosswalk was found in main, and facility centroids are not boundaries.

The publisher labels listing times Philippine Time (UTC+8). Preserve the legacy
key-bearing TIMESTAMP/wall-clock expression for now; explicitly report a converted
UTC value for lag. Converting existing `event_time`/IDs requires a reviewed
migration against the original runtime/session convention. Do not label the
legacy event_time UTC merely because the job session uses UTC.

Gold currently produces only scoped observation accounting and then raises a
BLOCKED error. A fact will have event grain and retain valid unmatched events,
but final keys, types, correction/dedup policy and relationship semantics must be
approved before materialization. No existing target is replaced. This implements
the requested audit behavior without approving this record or ADR 0006.
