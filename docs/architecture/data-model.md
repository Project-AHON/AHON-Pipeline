# Data model

All tables use the configured catalog: dev ahon_dev, prod ahon, synthetic verification ahon_integration_*. Schemas: reference, bronze, silver, gold, platinum, monitoring.

| Entity | Grain / key | Disposition |
| --- | --- | --- |
| Generic Bronze | source-file SHA-256 + ordered raw-row JSON SHA-256 | Identical raw rows within a file deduplicate with a warning; corrections retain a new source version |
| Earthquake observation | file SHA-256 + source-row ordinal | Identical rows remain distinct; no inferred event identity |
| Silver PSGC | code + geographic level + API period | Missing geographic fields quarantined; malformed code blocks |
| Gold dim_lgu | ten-digit string code | One source_period and hierarchy, leading zeros retained |
| BLGF reference map | region + province + source LGU name + type | Contextual aliases/reviewed overrides; ambiguous and name-only matches withheld |
| Silver/Gold LDRRMF | fiscal year + contextual source LGU key | Decimal amounts/ratios, lineage and mapping disposition; administrative grains remain separate |
| Population | source geographic path + census year | Integral persons, source levels 0–3; unmatched rows retained |
| CMCI reference map | PSGC + source_period | Reviewed one-to-one portal name, reviewer and evidence |
| CMCI indicator dimension | indicator code | 35 selected components, selected_35_v1 |
| CMCI indicator fact | PSGC + year + indicator code + source_period | Raw numeric value; missing null; invalid quarantine |
| CMCI pillar output | PSGC + year + pillar + source_period | Available raw sum plus observed/expected coverage, never official score |
| LGU evidence status | city/municipality PSGC + reporting year | Funding, resiliency coverage, freshness, uncertainty, withheld risk/gap |
| DQ result | run + check + table | Count, severity, disposition, timestamp |
| Run log | run + task | Outcome and timestamp |

Landing: /Volumes/<catalog>/reference/<volume>/<dataset>/<run>/. _SUCCESS.json records files, rows and SHA-256. Original documents/workbooks and raw observations remain there. CMCI JSON includes exact response bytes as base64, their digest and decoded HTML.

Generic lineage: _source_name, _source_ref, _source_digest, _ingested_at, _batch_id, _row_hash, _observation_id. Transformations carry their selected lineage; table-specific columns are documented in [dictionaries](data-dictionary/README.md). Ingestion timestamps are UTC retrieval times. Earthquake source wallclock is TIMESTAMP_NTZ, with timezone provenance unresolved.

Newer valid corrections update the business key. Invalid corrections remain quarantined and can leave an earlier accepted value in Silver: inspect the latest rejection and freshness before using it. No absence-based deletion is enabled. Schema differences require review, not automatic evolution. Separate table writes are not atomic.
