-- Create the data quality results table
-- Defines ahon_dev.monitoring.dq_results, written by src/sql/common/dq_gate.py (decision 0010).
-- One row per rule per DQ gate run, append only (rows are never edited).
-- Safe to rerun: IF NOT EXISTS leaves an existing schema and table unchanged.
-- Change ahon_dev to ahon for production.

CREATE SCHEMA IF NOT EXISTS ahon_dev.monitoring;

CREATE TABLE IF NOT EXISTS ahon_dev.monitoring.dq_results (
  dq_run_id     STRING        COMMENT 'One id per DQ gate run; all rules checked in that run share it',
  dq_rule_id    STRING        COMMENT 'Rule that was checked: <dataset>_<layer>_<rule>, e.g. blgf_ldrrmf_annual_lgu_silver_overspent',
  rule_type     STRING        COMMENT 'Kind of check: null, unique, range, accepted_values, referential_integrity, volume, reconciliation, spatial',
  _source_name  STRING        COMMENT 'Dataset checked',
  _batch_id     STRING        COMMENT 'Load run(s) that were checked, comma-separated; empty when the table has no _batch_id',
  layer         STRING        COMMENT 'bronze, silver, gold or platinum',
  table_name    STRING        COMMENT 'Table checked',
  checked_at    TIMESTAMP     COMMENT 'When the check ran (UTC)',
  rows_checked  BIGINT        COMMENT 'Rows the rule looked at',
  rows_failed   BIGINT        COMMENT 'Rows that broke the rule',
  failed_pct    DECIMAL(7,4)  COMMENT 'rows_failed as a percent of rows_checked',
  status        STRING        COMMENT 'PASS, WARN, FAIL or ERROR',
  action        STRING        COMMENT 'stop, flag, fix, quarantine or report, copied from the rule at run time',
  sample_keys   STRING        COMMENT 'Up to 5 failing rows as key values, or failing values when no keys are given',
  message       STRING        COMMENT 'Short note, e.g. the error text when status is ERROR'
)
COMMENT 'Data quality results, one row per rule per DQ gate run. Append only.';
