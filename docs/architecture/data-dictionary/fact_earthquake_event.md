# Event fact: blocked

The active observation contract does not establish authoritative event identity, correction/supersession, timezone or exposure relationships. gold.fact_earthquake_event is not materialized by the bundle. The retained SQL/test scripts are guarded read-only validation for legacy compatible tables; they do not authorize migration or event exposure assignment.

See [observation dictionary](philvolcs_earthquake_data.md), [metrics](../metrics.md) and [ADR 0010](../../decisions/0010-coherent-medallion-contract.md).
