# Shared provenance and monitoring

| Column | Type | Meaning |
| --- | --- | --- |
| _source_name | STRING | Dataset identifier |
| _source_ref | STRING | Immutable landing file path |
| _source_digest | STRING | SHA-256 of file bytes |
| _ingested_at | TIMESTAMP | First observation ingestion, UTC |
| _batch_id | STRING | Landing/job-run identity |
| _row_hash | STRING | SHA-256 of ordered raw JSON values, including explicit nulls |
| _observation_id | STRING | File digest plus raw row hash; earthquake instead uses row ordinal |
| _processed_at | TIMESTAMP | Silver/Gold processing time where materialized |
| has_dq_issues | BOOLEAN | Dataset-defined invalid or warning condition |

Generic Bronze hashes exclude retrieval time and run ID. Repeated identical raw rows within a file collapse with a repeated-observation warning; original file and manifest retain their count. Earthquake retains all ordinals. Source corrections add file versions. Some legacy-compatible fiscal/PSGC projections retain source_ref, row_hash and ingestion lineage rather than every generic identity field; consult their dictionaries.

monitoring.dq_result: run_id STRING, check_id STRING, table_name STRING, failed_count BIGINT, severity STRING, disposition STRING, checked_at TIMESTAMP. Key run/check/table; dispositions PASS/WARN/BLOCKED. failed_count counts rows or duplicate-key groups according to the rule; SQL statement wrappers count failed statements (1), with actual failed rule in task output. ERROR persists before raising, WARN continues.

monitoring.run_log: run_id STRING, task_key STRING, status STRING, message STRING, recorded_at TIMESTAMP. Key run/task; final dataset task records success/failure. Checks and run-log writes are separate Delta transactions. Task failures before setup may lack a run-log row; Jobs state remains the authoritative orchestration outcome.

Sources lacking an authoritative publication version use immutable content identity. A byte-identical reappearance keeps first-ingestion lineage rather than creating a new version; a claimed reversion to an earlier content version requires explicit version/correction evidence and review. Ingestion timestamps do not prove publication recency.
