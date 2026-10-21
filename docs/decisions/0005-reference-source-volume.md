# 0005: Raw files land in a volume in the reference schema

- **Status:** Accepted
- **Date:** 2026-10-05
- **Updates:** [0001](0001-naming-standard.md) (the pending `source` schema), [0004](0004-source-file-landing-hugging-face.md) (where files land)

## Context

Decision 0001 listed a `source` schema for raw files as "pending", and 0004 sends files from Hugging Face into a volume. Raw files are not tables, so a whole schema for one volume adds a layer that holds nothing else. The team needed one place for every landed file before the first extract scripts were written.

## Decision

- Raw files land in one volume named `source` in the `reference` schema: `<catalog>.reference.source`. There is no `source` schema.
- Inside the volume: one folder per dataset and one subfolder per batch, `/Volumes/<catalog>/reference/source/<dataset>/<batch_id>/`.
- This applies to files from Hugging Face and to files an extract script downloads from a publisher (PSGC API, PHIVOLCS, CMCI).
- The volume name is a bundle variable (`source_volume`), so code never writes it in.

## Why

- `reference` already holds inputs the pipeline reads but does not build from other tables (lookup and mapping tables), so raw files fit there.
- One volume for all sources means one place to look for any landed file, and one set of permissions.
- Fewer schemas keep the catalog simple for a project this size.

## Alternatives considered

- **A `source` schema with one volume:** rejected. A schema holding a single volume adds nothing.
- **A volume per dataset:** rejected. More objects to create and grant, with no benefit over one folder per dataset.
- **Volumes inside `bronze`:** rejected. Bronze holds tables loaded from the files; keeping the files apart makes it clear that they are never edited.

## Consequences

- The setup task creates `reference.<source_volume>`.
- The naming standard and the architecture docs show the volume path.
- `_source_ref` in bronze holds the file path inside this volume.
