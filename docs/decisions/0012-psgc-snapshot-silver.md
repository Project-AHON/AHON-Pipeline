# 0012: PSGC snapshot and Silver codes

- **Status:** Historical decision text unavailable; implementation summary only.

The empty historical file does not establish its original rationale, date or approval. This summary documents current code rather than declaring historical acceptance.

PSGC Bronze retains source observations by API snapshot. Silver validates original numeric code strings and currently pads accepted 8-, 9- and 10-digit values to ten digits. `gold.dim_lgu` selects the explicitly configured `source_period`; supported analytical joins must use reviewed mappings for that snapshot.

Padding does not prove semantic equivalence between historical code systems. In particular, nine-digit historical codes require review before cross-snapshot analytical use. Unmatched funding codes remain NULL; earthquake locality candidates do not become assigned LGU identities.

See [PSGC profile](../data/data-profile/psgc-profile.md), [PSGC Silver contract](../data/data-dictionary/psa_psgc_clean.md), [funding mapping](0013-ldrrmf-psgc-matching-in-silver.md) and [source contracts](0015-reconciled-source-contracts.md). Restore the original record if verified historical text becomes available.
