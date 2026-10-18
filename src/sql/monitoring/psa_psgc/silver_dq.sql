
-- ============================================================
-- AHON: SILVER LAYER DATA QUALITY CHECKS
-- Table: silver.psa_psgc_clean
--
-- Output statuses: PASS / WARN / FAIL
-- This script is read-only.
-- ============================================================

WITH base AS (
    SELECT *
    FROM silver.psa_psgc_clean
),

-- Independent cities are valid exceptions to conventional
-- province mapping. Their barangays inherit the exception.
independent_city_codes AS (
    SELECT DISTINCT
        psgc_code,
        _api_period
    FROM base
    WHERE geographic_level = 'CITY'
      AND (
          UPPER(COALESCE(city_class, '')) RLIKE
              '(^|[^A-Z])(HUC|ICC)([^A-Z]|$)'
          OR UPPER(COALESCE(city_class, '')) LIKE '%HIGHLY URBANIZED%'
          OR UPPER(COALESCE(city_class, '')) LIKE '%INDEPENDENT COMPONENT%'
      )
),

-- Distinct city/municipality parent codes, scoped by snapshot.
parent_codes AS (
    SELECT DISTINCT
        psgc_code,
        _api_period
    FROM base
    WHERE geographic_level IN ('CITY', 'MUN')
),

-- Pateros and the Special Geographic Area are known
-- exceptions to conventional province mapping.
pateros_codes AS (
    SELECT DISTINCT
        psgc_code,
        _api_period
    FROM base
    WHERE geographic_level = 'MUN'
      AND LOWER(TRIM(area_name)) = 'pateros'
),

sga_codes AS (
    SELECT DISTINCT
        psgc_code,
        _api_period
    FROM base
    WHERE LOWER(TRIM(area_name)) = 'special geographic area'
),

checks AS (

    -- 01. Required business key fields
    SELECT
        1 AS check_id,
        'Required Key Fields' AS check_name,
        'FAIL' AS severity,
        COUNT(*) AS issue_count,
        'PSGC code, geographic level, and API period must be populated.'
            AS details
    FROM base
    WHERE psgc_code IS NULL OR TRIM(psgc_code) = ''
       OR geographic_level IS NULL OR TRIM(geographic_level) = ''
       OR _api_period IS NULL OR TRIM(_api_period) = ''

    UNION ALL

    -- 02. Geographic name completeness
    SELECT
        2,
        'Geographic Name Completeness',
        'FAIL',
        COUNT(*),
        'Geographic names must not be null or empty.'
    FROM base
    WHERE area_name IS NULL
       OR TRIM(area_name) = ''

    UNION ALL

    -- 03. Duplicate business keys
    SELECT
        3,
        'Duplicate Business Keys',
        'FAIL',
        COUNT(*),
        'Each PSGC code, geographic level, and API period combination must be unique.'
    FROM (
        SELECT
            psgc_code,
            geographic_level,
            _api_period
        FROM base
        GROUP BY
            psgc_code,
            geographic_level,
            _api_period
        HAVING COUNT(*) > 1
    ) duplicates

    UNION ALL

    -- 04. PSGC code format: exactly 10 numeric digits
    SELECT
        4,
        'PSGC Code Format',
        'FAIL',
        COUNT(*),
        'Silver PSGC codes must contain exactly 10 numeric digits.'
    FROM base
    WHERE psgc_code IS NULL
       OR NOT (TRIM(psgc_code) RLIKE '^[0-9]{10}$')

    UNION ALL

    -- 05. Expected geographic levels
    SELECT
        5,
        'Unexpected Geographic Levels',
        'WARN',
        COUNT(*),
        'Expected levels are REG, PROV, CITY, MUN, BGY, and SUBMUN.'
    FROM base
    WHERE geographic_level IS NULL
       OR geographic_level NOT IN (
           'REG', 'PROV', 'CITY', 'MUN', 'BGY', 'SUBMUN'
       )

    UNION ALL

    -- 06. PSA footnote markers remaining in names
    SELECT
        6,
        'Possible Footnote Markers',
        'WARN',
        COUNT(*),
        'Names should not end with PSA footnote markers such as 1/ or 2/.'
    FROM base
    WHERE area_name RLIKE '[[:space:]][0-9]+/[[:space:]]*$'

    UNION ALL

    -- 07. Name whitespace
    SELECT
        7,
        'Name Whitespace',
        'WARN',
        COUNT(*),
        'Names should not have leading or trailing whitespace.'
    FROM base
    WHERE area_name IS NOT NULL
      AND area_name <> TRIM(area_name)

    UNION ALL

    -- 08. Bronze provenance completeness
    SELECT
        8,
        'Provenance Completeness',
        'FAIL',
        COUNT(*),
        'Source name, source reference, ingestion timestamp, and batch ID are required.'
    FROM base
    WHERE _source_name IS NULL OR TRIM(_source_name) = ''
       OR _source_ref IS NULL OR TRIM(_source_ref) = ''
       OR _ingested_at IS NULL
       OR _batch_id IS NULL OR TRIM(_batch_id) = ''

    UNION ALL

    -- 09. Bronze SHA-256 row hash
    SELECT
        9,
        'Bronze SHA-256 Hash Format',
        'FAIL',
        COUNT(*),
        'The Bronze row hash must contain 64 hexadecimal characters.'
    FROM base
    WHERE _row_hash IS NULL
       OR NOT (_row_hash RLIKE '^[0-9a-fA-F]{64}$')

    UNION ALL

    -- 10. Silver audit fields
    SELECT
        10,
        'Silver Audit Fields',
        'FAIL',
        COUNT(*),
        'The Silver processing timestamp must be populated.'
    FROM base
    WHERE _processed_at IS NULL

    UNION ALL

    -- 11. Region hierarchy code
    SELECT
        11,
        'Region Hierarchy Code',
        'FAIL',
        COUNT(*),
        'region_psgc must match the first two PSGC digits followed by eight zeros.'
    FROM base
    WHERE psgc_code RLIKE '^[0-9]{10}$'
      AND (
          region_psgc IS NULL
          OR region_psgc <> CONCAT(SUBSTRING(psgc_code, 1, 2), '00000000')
      )

    UNION ALL

    -- 12. Province hierarchy code
    SELECT
        12,
        'Province Hierarchy Code',
        'FAIL',
        COUNT(*),
        'Non-region records must have the expected five-digit province prefix.'
    FROM base
    WHERE geographic_level <> 'REG'
      AND psgc_code RLIKE '^[0-9]{10}$'
      AND (
          province_psgc IS NULL
          OR province_psgc <> CONCAT(SUBSTRING(psgc_code, 1, 5), '00000')
      )

    UNION ALL

    -- 13. City/municipality hierarchy code
    SELECT
        13,
        'City Municipality Hierarchy Code',
        'FAIL',
        COUNT(*),
        'Records below region and province level must have the expected seven-digit prefix.'
    FROM base
    WHERE geographic_level NOT IN ('REG', 'PROV')
      AND psgc_code RLIKE '^[0-9]{10}$'
      AND (
          citymun_psgc IS NULL
          OR citymun_psgc <> CONCAT(SUBSTRING(psgc_code, 1, 7), '000')
      )

    UNION ALL

    -- 14. Display label completeness
    SELECT
        14,
        'Province City Completeness',
        'WARN',
        COUNT(*),
        'province_city should be populated for valid geographic records.'
    FROM base
    WHERE area_name IS NOT NULL
      AND TRIM(area_name) <> ''
      AND (
          province_city IS NULL
          OR TRIM(province_city) = ''
      )

    UNION ALL

    -- 15. Display label should start with the geographic name
    SELECT
        15,
        'Province City Name Prefix',
        'WARN',
        COUNT(*),
        'province_city should begin with the cleaned geographic name.'
    FROM base
    WHERE area_name IS NOT NULL
      AND province_city IS NOT NULL
      AND NOT (
          province_city LIKE CONCAT(area_name, '%')
      )

    UNION ALL

    -- 16. Missing province mapping, excluding valid exceptions
    SELECT
        16,
        'Missing Province Name',
        'WARN',
        COUNT(*),
        'Check missing province mappings after excluding independent cities, Pateros, and SGA.'
    FROM base b
    LEFT JOIN independent_city_codes ic
      ON b.citymun_psgc = ic.psgc_code
     AND b._api_period = ic._api_period
    LEFT JOIN pateros_codes pc
      ON b.citymun_psgc = pc.psgc_code
     AND b._api_period = pc._api_period
    LEFT JOIN sga_codes sg
      ON b.citymun_psgc = sg.psgc_code
     AND b._api_period = sg._api_period
    WHERE b.geographic_level IN ('CITY', 'MUN', 'BGY', 'SUBMUN')
      AND (
          b.province_name IS NULL
          OR TRIM(b.province_name) = ''
      )
      AND ic.psgc_code IS NULL
      AND pc.psgc_code IS NULL
      AND sg.psgc_code IS NULL
      AND COALESCE(TRIM(b.province_code), '') <> '999'
      AND LOWER(TRIM(COALESCE(b.area_name, '')))
          <> 'special geographic area'

    UNION ALL

    -- 17. Barangay must have a derived city/municipality code
    SELECT
        17,
        'Barangay Missing Parent Code',
        'WARN',
        COUNT(*),
        'Barangays should have citymun_psgc populated.'
    FROM base
    WHERE geographic_level = 'BGY'
      AND (
          citymun_psgc IS NULL
          OR TRIM(citymun_psgc) = ''
      )

    UNION ALL

    -- 18. Parent code must resolve within the same snapshot
    SELECT
        18,
        'Barangay Parent Not Found',
        'WARN',
        COUNT(*),
        'Barangay/sub-municipality parent should resolve to a city or municipality in the same API snapshot.'
    FROM base b
    LEFT JOIN parent_codes p_cm
      ON b.citymun_psgc = p_cm.psgc_code
     AND b._api_period = p_cm._api_period
    LEFT JOIN parent_codes p_pv
      ON b.province_psgc = p_pv.psgc_code
     AND b._api_period = p_pv._api_period
    WHERE b.geographic_level IN ('BGY', 'SUBMUN')
      AND COALESCE(TRIM(b.province_code), '') <> '999'
      AND p_cm.psgc_code IS NULL
      AND p_pv.psgc_code IS NULL
)

-- ============================================================
-- FINAL DQ REPORT
-- ============================================================

SELECT
    check_id,
    check_name,
    severity,
    issue_count,
    CASE
        WHEN issue_count = 0 THEN 'PASS'
        WHEN severity = 'WARN' THEN 'WARN'
        ELSE 'FAIL'
    END AS status,
    details
FROM checks
ORDER BY check_id;
