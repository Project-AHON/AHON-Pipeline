# Dataset quality monitoring

All supported sources use the shared GX Core quality gates in `src/sql/common/quality.py`. Spark computes failure counts over the complete candidate input; GX expects each rule's count to equal zero. No source rows are sampled or collected into pandas for these gates.

| Dataset | Rule families |
| --- | --- |
| PSGC / LGU reference | Valid code strings, snapshot selection, unique geographic keys, lookup cardinality and explicit rejects |
| Population sources | Census/year keys, missing versus invalid values, dimension alignment, source accounting and aggregation coverage |
| CMCI | Reviewed mapping identity, parser accounting, indicator keys, current invalid corrections and reviewed definition/version |
| BLGF LDRRMF | Original entity/year uniqueness, required amounts, component totals, year range, map/dimension alignment and retained financial warnings |
| PHIVOLCS | Observation identity, typed invalid records, source accounting and locality-candidate cardinality; exposure assignment remains withheld |
| OSM | Source-feature identity, file/row accounting, geometry validity and no join fan-out |

Each check is keyed by `(run_id, check_id, table_name)` in `<catalog>.monitoring.dq_result`. Fields are `failed_count BIGINT`, `severity STRING`, `disposition STRING`, `checked_at TIMESTAMP` and `validation_result STRING`. The last field contains GX result JSON with expectation metadata and the GX version. Older monitoring rows retain NULL evidence until their checks rerun.

PASS means the expectation succeeded. WARN means an expectation failed at warning severity and flagged rows remain visible. BLOCKED means an error-level expectation failed and a `QualityError` stops dependent writes. The result is persisted before the exception. A GX dependency/runtime failure stops the task; it never becomes an unvalidated pass.

The Python and SQL launchers also record an execution marker for unexpected runtime failures, then preserve the original exception so the task log identifies its cause. A successful retry resets that marker to PASS. Named GX failures keep their original rule identity. For a partial run or a failed attempt followed by recovery, review the selected task status and current `checked_at` interval rather than treating all historical delivery checks as current failures.

File-integrity, authentication, parsing-structure and incompatible-schema guards remain ingestion/write contracts. A failed delivery is not deleted to conceal it. Inspect the task's GX result, source/reject lineage and selected period, correct the source or reviewed mapping, then run a new delivery. Read prior accepted facts together with the latest DQ state.

See the [runbook](../runbook.md#gx-quality-gates) for the single additive monitoring-column upgrade, token entry, deployment, replay and production prerequisites. The fixture job is development-only; there is no additional GX service, result store or DQ orchestration layer.
