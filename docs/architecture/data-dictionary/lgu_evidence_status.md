# Analytical evidence

platinum.lgu_evidence_status key psgc_code STRING + reporting_year INT, CITY/MUN only. Dimension fields/source_period retained. total_appropriation/total_expenditure DECIMAL(18,2), utilization_rate_total DECIMAL(18,4), funding_ingested_at TIMESTAMP, funding_has_dq_issues BOOLEAN nullable. resiliency_indicator_count BIGINT nullable counts nonnull selected ten CMCI resiliency values; cmci_ingested_at TIMESTAMP.

risk_level STRING and risk_preparedness_gap DOUBLE always NULL; risk_status STRING = INSUFFICIENT_HAZARD_EXPOSURE_VULNERABILITY_EVIDENCE. preparedness_status STRING = COMPONENT_EVIDENCE_ONLY when ten components and funding exist, otherwise INSUFFICIENT_DATA. This is coverage status, not a validated capacity score. metric_version STRING evidence_v1, as_of TIMESTAMP, uncertainty STRING states methodology limits.

Same reporting year is required for funding/CMCI, selected reference snapshot is explicit, and parent/child levels do not mix. Missing inputs remain null. No normalization/weights/rank thresholds are invented. See [metric definitions](../metrics.md).
