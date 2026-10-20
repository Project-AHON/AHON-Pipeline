# 0005: Reference source volume

- **Status:** Historical decision text unavailable; implementation summary only.

The empty historical file does not establish its original rationale, date or approval. This summary documents the current implementation, without asserting a new architectural decision.

The pipeline stores raw batches in the Unity Catalog volume `<catalog>.reference.<source_volume>`, under `<dataset>/<batch_id>/`. The setup SQL runner creates the configured catalog and volume where permitted; the setup SQL creates the schemas. Extractors publish completed landing manifests with file digests and row counts. Bronze reads completed batches; corrections use a new batch ID.

See [source-file landing](0004-source-file-landing-hugging-face.md), [reconciled contracts](0015-reconciled-source-contracts.md), [setup](../getting-started/setup.md) and [source to target](../architecture/source-to-target.md). Restore the original record if verified historical text becomes available.
