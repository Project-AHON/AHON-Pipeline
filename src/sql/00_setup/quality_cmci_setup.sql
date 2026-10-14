CREATE SCHEMA IF NOT EXISTS ahon.quality;

CREATE TABLE IF NOT EXISTS ahon.quality.dq_results (
    run_id STRING NOT NULL,
    run_ts TIMESTAMP NOT NULL,
    dataset STRING NOT NULL,
    layer STRING NOT NULL,
    table_name STRING NOT NULL,
    check_category STRING NOT NULL,
    check_name STRING NOT NULL,
    failed_rows BIGINT NOT NULL,
    total_rows BIGINT NOT NULL,
    failed_pct DOUBLE,
    threshold_pct DOUBLE NOT NULL,
    blocking BOOLEAN NOT NULL,
    status STRING NOT NULL
)
USING DELTA
COMMENT 'Detailed data-quality results produced by Great Expectations';


CREATE TABLE IF NOT EXISTS ahon.quality.dq_run_log (
    run_id STRING NOT NULL,
    run_ts TIMESTAMP NOT NULL,
    dataset STRING NOT NULL,
    layer STRING NOT NULL,
    checks_run INT NOT NULL,
    checks_passed INT NOT NULL,
    checks_warned INT NOT NULL,
    checks_failed INT NOT NULL,
    blocking_failures INT NOT NULL,
    overall_status STRING NOT NULL,
    finished_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Summary of Great Expectations data-quality runs';