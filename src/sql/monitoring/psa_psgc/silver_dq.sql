-- PSA PSGC Silver Data Quality Checks
-- Purpose: validate the standardized Silver table and reconcile it
-- against the valid Bronze source population.
-- Table: ahon.silver.psa_psgc
--
-- Silver business key:
--   psgc_code + geographic_level + _api_period
--
-- The Silver layer intentionally excludes Bronze records with a missing
-- geographic_level. Those source records remain in Bronze for traceability.

WITH bronze_valid AS (
    SELECT
        TRIM(psgc_code) AS psgc_code,
        TRIM(geographic_level) AS geographic_level,
        TRIM(_api_period) AS _api_period
    FROM ahon.bronze.psa_psgc
    WHERE psgc_code IS NOT NULL
      AND TRIM(psgc_code) <> ''
      AND area_name IS NOT NULL
      AND TRIM(area_name) <> ''
      AND geographic_level IS NOT NULL
      AND TRIM(geographic_level) <> ''
      AND _api_period IS NOT NULL
      AND TRIM(_api_period) <> ''
),

bronze_expected AS (
    SELECT COUNT(*) AS expected_rows
    FROM (
        SELECT DISTINCT
            psgc_code,
            geographic_level,
            _api_period
        FROM bronze_valid
    )
),

silver_metrics AS (
    SELECT
        COUNT(*) AS actual_rows,

        COUNT_IF(
            psgc_code IS NULL
            OR TRIM(psgc_code) = ''
            OR geographic_name IS NULL
            OR TRIM(geographic_name) = ''
            OR geographic_level IS NULL
            OR TRIM(geographic_level) = ''
            OR _api_period IS NULL
            OR TRIM(_api_period) = ''
        ) AS incomplete_rows,

        COUNT(*) - COUNT(DISTINCT CONCAT_WS(
            '||',
            TRIM(COALESCE(psgc_code, '')),
            TRIM(COALESCE(geographic_level, '')),
            TRIM(COALESCE(_api_period, ''))
        )) AS duplicate_business_keys,

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

        COUNT_IF(
            psgc_code IS NOT NULL
            AND TRIM(psgc_code) <> ''
            AND NOT TRIM(psgc_code) RLIKE '^[0-9]+$'
        ) AS invalid_psgc_codes,

        COUNT_IF(
            _api_period IS NULL
            OR TRIM(_api_period) = ''
        ) AS missing_api_period

    FROM ahon.silver.psa_psgc
),

reconciliation AS (
    SELECT
        COUNT(*) AS missing_from_silver
    FROM bronze_valid b
    LEFT ANTI JOIN ahon.silver.psa_psgc s
        ON s.psgc_code = b.psgc_code
       AND s.geographic_level = b.geographic_level
       AND s._api_period = b._api_period
),

unexpected_silver AS (
    SELECT
        COUNT(*) AS unexpected_in_silver
    FROM ahon.silver.psa_psgc s
    LEFT ANTI JOIN bronze_valid b
        ON b.psgc_code = s.psgc_code
       AND b.geographic_level = s.geographic_level
       AND b._api_period = s._api_period
),

checks AS (

    SELECT
        'Completeness' AS check_name,
        incomplete_rows AS failed_rows,
        CASE WHEN incomplete_rows = 0 THEN 'PASS' ELSE 'FAIL' END AS status
    FROM silver_metrics

    UNION ALL

    SELECT
        'Uniqueness',
        duplicate_business_keys,
        CASE WHEN duplicate_business_keys = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM silver_metrics

    UNION ALL

    SELECT
        'Lineage',
        null_lineage,
        CASE WHEN null_lineage = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM silver_metrics

    UNION ALL

    SELECT
        'PSGC Code Validity',
        invalid_psgc_codes,
        CASE WHEN invalid_psgc_codes = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM silver_metrics

    UNION ALL

    SELECT
        'API Period Completeness',
        missing_api_period,
        CASE WHEN missing_api_period = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM silver_metrics

    UNION ALL

    SELECT
        'Bronze-to-Silver Reconciliation',
        missing_from_silver,
        CASE WHEN missing_from_silver = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM reconciliation

    UNION ALL

    SELECT
        'Silver-to-Bronze Reconciliation',
        unexpected_in_silver,
        CASE WHEN unexpected_in_silver = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM unexpected_silver

    UNION ALL

    SELECT
        'Volume',
        ABS(actual_rows - expected_rows),
        CASE WHEN actual_rows = expected_rows THEN 'PASS' ELSE 'FAIL' END
    FROM silver_metrics
    CROSS JOIN bronze_expected
)

SELECT
    check_name,
    failed_rows,
    status
FROM checks
ORDER BY check_name;
