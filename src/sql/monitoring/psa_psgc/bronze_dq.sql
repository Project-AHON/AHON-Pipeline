-- PSA PSGC Bronze Data Quality Checks
-- NOTE:
-- Records with a missing geographic_level are NOT silently treated as duplicates.
-- They are reported as a DQ issue because Silver intentionally excludes them.

WITH metrics AS (
    SELECT
        COUNT(*) AS actual_rows,

        -- Fields required to identify and process a PSGC source record.
        COUNT_IF(
            psgc_code IS NULL
            OR TRIM(psgc_code) = ''
            OR area_name IS NULL
            OR TRIM(area_name) = ''
            OR _api_period IS NULL
            OR TRIM(_api_period) = ''
        ) AS incomplete_rows,

        -- Expected source grain:
        -- one PSGC + geographic level + API period per source record.
        COUNT(*) - COUNT(DISTINCT CONCAT_WS(
            '||',
            TRIM(COALESCE(psgc_code, '')),
            TRIM(COALESCE(geographic_level, '')),
            TRIM(COALESCE(_api_period, ''))
        )) AS duplicate_source_keys,

        -- Lineage is required so every Bronze record can be traced back
        -- to its source and ingestion batch.
        COUNT_IF(
            _source_name IS NULL
            OR TRIM(_source_name) = ''
            OR _source_ref IS NULL
            OR TRIM(_source_ref) = ''
            OR _ingested_at IS NULL
            OR _batch_id IS NULL
            OR TRIM(_batch_id) = ''
            OR _row_hash IS NULL
            OR TRIM(_row_hash) = ''
        ) AS null_lineage,

        -- A PSGC code should contain digits only.
        -- We do not enforce a fixed length here because Bronze should
        -- preserve source values and some PSA records are administrative.
        COUNT_IF(
            psgc_code IS NOT NULL
            AND TRIM(psgc_code) <> ''
            AND NOT TRIM(psgc_code) RLIKE '^[0-9]+$'
        ) AS invalid_psgc_codes,

        -- Blank geographic level is an explicit DQ issue.
        -- These records remain in Bronze but are excluded from Silver.
        COUNT_IF(
            geographic_level IS NULL
            OR TRIM(geographic_level) = ''
        ) AS missing_geographic_level,

        -- Confirm that the expected PSA API snapshots are present.
        COUNT(DISTINCT _api_period) AS period_count,

        COUNT_IF(
            _api_period IS NULL
            OR TRIM(_api_period) = ''
        ) AS missing_api_period

    FROM ahon.bronze.psa_psgc
),

checks AS (

    SELECT
        'Completeness' AS check_name,
        incomplete_rows AS failed_rows,
        CASE WHEN incomplete_rows = 0 THEN 'PASS' ELSE 'FAIL' END AS status
    FROM metrics

    UNION ALL

    SELECT
        'Uniqueness',
        duplicate_source_keys,
        CASE WHEN duplicate_source_keys = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM metrics

    UNION ALL

    SELECT
        'Lineage',
        null_lineage,
        CASE WHEN null_lineage = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM metrics

    UNION ALL

    SELECT
        'PSGC Code Validity',
        invalid_psgc_codes,
        CASE WHEN invalid_psgc_codes = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM metrics

    UNION ALL

    SELECT
        'Geographic Level Completeness',
        missing_geographic_level,
        CASE WHEN missing_geographic_level = 0 THEN 'PASS' ELSE 'WARN' END
    FROM metrics

    UNION ALL

    SELECT
        'API Period Completeness',
        missing_api_period,
        CASE WHEN missing_api_period = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM metrics

    UNION ALL

    SELECT
        'Expected API Period Coverage',
        CASE WHEN period_count = 4 THEN 0 ELSE ABS(period_count - 4) END,
        CASE WHEN period_count = 4 THEN 'PASS' ELSE 'FAIL' END
    FROM metrics
)

SELECT
    check_name,
    failed_rows,
    status
FROM checks
ORDER BY check_name;
