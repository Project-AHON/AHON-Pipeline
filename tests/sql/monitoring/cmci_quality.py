"""Run CMCI Bronze, Silver, Gold, and Platinum quality checks through Great Expectations.

Flow:
Spark metrics -> Great Expectations -> Delta DQ results -> release gate.

Business data stays in Spark. Only aggregated quality metrics are passed to
pandas / Great Expectations.
"""

import argparse
import math
import uuid
from datetime import datetime, timezone

import great_expectations as gx
import pandas as pd
from pyspark.sql import SparkSession


CATALOG = "ahon"
QUALITY_SCHEMA = "quality"

DQ_RESULTS_TABLE = f"{CATALOG}.{QUALITY_SCHEMA}.dq_results"
DQ_RUN_LOG_TABLE = f"{CATALOG}.{QUALITY_SCHEMA}.dq_run_log"

CMCI_MAP_TABLE = "ahon.reference.cmci_lgu_map"
BRONZE_TABLE = "ahon.bronze.cmci_raw_indicator_batch_html"

SILVER_TABLES = [
    "ahon.silver.cmci_economic_dynamism",
    "ahon.silver.cmci_government_efficiency",
    "ahon.silver.cmci_infrastructure",
    "ahon.silver.cmci_resiliency",
    "ahon.silver.cmci_innovation",
]

EXPECTED_LGU_COUNT = 1634

EXPECTED_YEARS = [
    "2014",
    "2015",
    "2016",
    "2017",
    "2018",
    "2019",
    "2020",
    "2021",
    "2022",
    "2023",
    "2024",
]

EXPECTED_YEAR_COUNT = len(EXPECTED_YEARS)
EXPECTED_INDICATOR_COUNT = 35
EXPECTED_LGU_YEAR_COUNT = EXPECTED_LGU_COUNT * EXPECTED_YEAR_COUNT

GOLD_FACT_TABLE = "ahon.gold.fact_cmci_indicator"
GOLD_INDICATOR_TABLE = "ahon.gold.dim_cmci_indicator"

EXPECTED_GOLD_ROW_COUNT = (
    EXPECTED_LGU_COUNT
    * EXPECTED_YEAR_COUNT
    * EXPECTED_INDICATOR_COUNT
)

PLATINUM_PILLAR_TABLE = "ahon.platinum.cmci_pillar_score"

EXPECTED_PILLAR_COUNT = 5

EXPECTED_PLATINUM_ROW_COUNT = (
    EXPECTED_LGU_COUNT
    * EXPECTED_YEAR_COUNT
    * EXPECTED_PILLAR_COUNT
)

# ------------------------------------------------------------------
# RELEASE POLICY
# ------------------------------------------------------------------

BLOCKING_CHECKS = {
    "bronze": {
        "table_not_empty",
        "batch_id_not_null",
        "response_hash_not_null",
        "response_html_not_null",
        "batch_id_unique",
        "row_hash_unique",
        "lgu_count_matches_array",
        "cmci_name_count_matches_array",
        "year_count_matches_array",
        "indicator_count_matches_array",
        "expected_indicator_count_is_35",
        "returned_value_count_matches_expected",
        "approved_psgc_only",
        "psgc_not_repeated_across_batches",
    },
    "silver": {
        "table_not_empty",
        "psgc_code_not_null",
        "lgu_not_null",
        "cmci_name_not_null",
        "year_not_null",
        "source_response_hash_not_null",
        "source_ingestion_timestamp_not_null",
        "silver_processed_timestamp_not_null",
        "one_row_per_psgc_year",
        "psgc_is_approved",
        "cmci_mapping_matches_reference",
        "expected_years_only",
        "pillar_key_coverage_matches",
        "source_hash_matches_bronze",
    },
    "gold": {
        "table_not_empty",
        "psgc_code_not_null",
        "year_not_null",
        "indicator_code_not_null",
        "raw_score_not_negative",
        "one_row_per_psgc_year_indicator",
        "psgc_is_approved",
        "year_in_expected_domain",
        "indicator_resolves",
        "expected_fact_row_count",
        "indicator_dimension_not_empty",
        "indicator_code_unique",
        "indicator_name_not_null",
        "pillar_name_not_null",
        "expected_indicator_count",
        "recent_score_within_range",
        "bre_recent_value_domain",
        "bpls_recent_value_domain",
        "lra_recent_value_domain",
    },
        "platinum": {
        "table_not_empty",
        "psgc_code_not_null",
        "year_not_null",
        "pillar_name_not_null",
        "score_status_not_null",
        "one_row_per_psgc_year_pillar",
        "psgc_is_approved",
        "year_in_expected_domain",
        "pillar_in_selected_scope",
        "expected_indicator_count_matches_pillar",
        "indicator_counts_are_valid",
        "coverage_pct_is_valid",
        "complete_status_is_consistent",
        "insufficient_status_is_consistent",
        "expected_platinum_row_count",
    },
}

# ------------------------------------------------------------------
# QUALITY METRIC QUERIES
# ------------------------------------------------------------------

BRONZE_QUERY = """
WITH base AS (
    SELECT
        COUNT(*) AS row_count,

        SUM(CASE WHEN batch_id IS NULL OR trim(batch_id) = ''
                 THEN 1 ELSE 0 END) AS missing_batch_id,

        SUM(CASE WHEN response_hash IS NULL OR trim(response_hash) = ''
                 THEN 1 ELSE 0 END) AS missing_response_hash,

        SUM(CASE WHEN response_html IS NULL OR trim(response_html) = ''
                 THEN 1 ELSE 0 END) AS missing_response_html,

        SUM(CASE WHEN _row_hash IS NULL OR trim(_row_hash) = ''
                 THEN 1 ELSE 0 END) AS missing_row_hash,

        SUM(
            CASE
                WHEN expected_lgu_count != size(requested_psgc_codes)
                THEN 1 ELSE 0
            END
        ) AS invalid_lgu_count,

        SUM(
            CASE
                WHEN expected_lgu_count != size(requested_cmci_names)
                THEN 1 ELSE 0
            END
        ) AS invalid_cmci_name_count,

        SUM(
            CASE
                WHEN expected_year_count != size(requested_years)
                THEN 1 ELSE 0
            END
        ) AS invalid_year_count,

        SUM(
            CASE
                WHEN expected_indicator_count != size(requested_indicator_codes)
                THEN 1 ELSE 0
            END
        ) AS invalid_indicator_count,

        SUM(
            CASE
                WHEN expected_indicator_count != 35
                THEN 1 ELSE 0
            END
        ) AS invalid_expected_indicator_count,

        SUM(
            CASE
                WHEN returned_value_count !=
                    expected_lgu_count
                    * expected_year_count
                    * expected_indicator_count
                THEN 1 ELSE 0
            END
        ) AS invalid_returned_count

    FROM ahon.bronze.cmci_raw_indicator_batch_html
),

batch_duplicates AS (
    SELECT COALESCE(SUM(n - 1), 0) AS duplicate_count
    FROM (
        SELECT batch_id, COUNT(*) AS n
        FROM ahon.bronze.cmci_raw_indicator_batch_html
        GROUP BY batch_id
        HAVING COUNT(*) > 1
    )
),

row_hash_duplicates AS (
    SELECT COALESCE(SUM(n - 1), 0) AS duplicate_count
    FROM (
        SELECT _row_hash, COUNT(*) AS n
        FROM ahon.bronze.cmci_raw_indicator_batch_html
        GROUP BY _row_hash
        HAVING COUNT(*) > 1
    )
),

bronze_psgc AS (
    SELECT explode(requested_psgc_codes) AS psgc_code
    FROM ahon.bronze.cmci_raw_indicator_batch_html
),

approved AS (
    SELECT DISTINCT trim(psgc_code) AS psgc_code
    FROM ahon.reference.cmci_lgu_map
    WHERE match_status = 'MATCHED'
      AND reviewed = true
      AND is_active = true
),

unauthorized AS (
    SELECT COUNT(*) AS n
    FROM (
        SELECT DISTINCT trim(b.psgc_code) AS psgc_code
        FROM bronze_psgc b
        LEFT ANTI JOIN approved a
            ON trim(b.psgc_code) = a.psgc_code
    )
),

repeated_psgc AS (
    SELECT COALESCE(SUM(n - 1), 0) AS n
    FROM (
        SELECT trim(psgc_code) AS psgc_code, COUNT(*) AS n
        FROM bronze_psgc
        GROUP BY trim(psgc_code)
        HAVING COUNT(*) > 1
    )
),

checks AS (
    SELECT
        'cmci_raw_indicator_batch_html' AS table_name,
        'completeness' AS check_category,
        'table_not_empty' AS check_name,
        CASE WHEN row_count = 0 THEN 1 ELSE 0 END AS failed_rows,
        1 AS total_rows,
        0.0 AS threshold_pct
    FROM base

    UNION ALL SELECT
        'cmci_raw_indicator_batch_html',
        'completeness',
        'batch_id_not_null',
        missing_batch_id,
        row_count,
        0.0
    FROM base

    UNION ALL SELECT
        'cmci_raw_indicator_batch_html',
        'completeness',
        'response_hash_not_null',
        missing_response_hash,
        row_count,
        0.0
    FROM base

    UNION ALL SELECT
        'cmci_raw_indicator_batch_html',
        'completeness',
        'response_html_not_null',
        missing_response_html,
        row_count,
        0.0
    FROM base

    UNION ALL SELECT
        'cmci_raw_indicator_batch_html',
        'uniqueness',
        'batch_id_unique',
        duplicate_count,
        row_count,
        0.0
    FROM base CROSS JOIN batch_duplicates

    UNION ALL SELECT
        'cmci_raw_indicator_batch_html',
        'uniqueness',
        'row_hash_unique',
        duplicate_count,
        row_count,
        0.0
    FROM base CROSS JOIN row_hash_duplicates

    UNION ALL SELECT
        'cmci_raw_indicator_batch_html',
        'consistency',
        'lgu_count_matches_array',
        invalid_lgu_count,
        row_count,
        0.0
    FROM base

    UNION ALL SELECT
        'cmci_raw_indicator_batch_html',
        'consistency',
        'cmci_name_count_matches_array',
        invalid_cmci_name_count,
        row_count,
        0.0
    FROM base

    UNION ALL SELECT
        'cmci_raw_indicator_batch_html',
        'consistency',
        'year_count_matches_array',
        invalid_year_count,
        row_count,
        0.0
    FROM base

    UNION ALL SELECT
        'cmci_raw_indicator_batch_html',
        'consistency',
        'indicator_count_matches_array',
        invalid_indicator_count,
        row_count,
        0.0
    FROM base

    UNION ALL SELECT
        'cmci_raw_indicator_batch_html',
        'validity',
        'expected_indicator_count_is_35',
        invalid_expected_indicator_count,
        row_count,
        0.0
    FROM base

    UNION ALL SELECT
        'cmci_raw_indicator_batch_html',
        'consistency',
        'returned_value_count_matches_expected',
        invalid_returned_count,
        row_count,
        0.0
    FROM base

    UNION ALL SELECT
        'cmci_raw_indicator_batch_html',
        'mapping',
        'approved_psgc_only',
        n,
        GREATEST((SELECT COUNT(*) FROM bronze_psgc), 1),
        0.0
    FROM unauthorized

    UNION ALL SELECT
        'cmci_raw_indicator_batch_html',
        'uniqueness',
        'psgc_not_repeated_across_batches',
        n,
        GREATEST((SELECT COUNT(*) FROM bronze_psgc), 1),
        0.0
    FROM repeated_psgc
)

SELECT *
FROM checks
"""


def silver_query(table_name):
    table_short_name = table_name.split(".")[-1]

    return f"""
    WITH latest_bronze AS (
        SELECT *
        FROM (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY batch_id
                    ORDER BY ingestion_timestamp DESC
                ) AS rn
            FROM {BRONZE_TABLE}
        )
        WHERE rn = 1
    ),

    bronze_keys AS (
        SELECT
            requested_psgc_codes[position] AS psgc_code,
            requested_cmci_names[position] AS cmci_name,
            year,
            response_hash
        FROM latest_bronze
        LATERAL VIEW posexplode(requested_psgc_codes)
            psgc_codes AS position, psgc
        LATERAL VIEW explode(requested_years)
            years AS year
    ),

    approved AS (
        SELECT
            trim(psgc_code) AS psgc_code,
            trim(cmci_name) AS cmci_name
        FROM {CMCI_MAP_TABLE}
        WHERE match_status = 'MATCHED'
          AND reviewed = true
          AND is_active = true
    ),

    metric AS (
        SELECT
            COUNT(*) AS row_count,

            SUM(CASE WHEN s.psgc_code IS NULL OR trim(s.psgc_code) = ''
                     THEN 1 ELSE 0 END) AS missing_psgc,

            SUM(CASE WHEN s.lgu IS NULL OR trim(s.lgu) = ''
                     THEN 1 ELSE 0 END) AS missing_lgu,

            SUM(CASE WHEN s.cmci_name IS NULL OR trim(s.cmci_name) = ''
                     THEN 1 ELSE 0 END) AS missing_cmci_name,

            SUM(CASE WHEN s.year IS NULL OR trim(s.year) = ''
                     THEN 1 ELSE 0 END) AS missing_year,

            SUM(CASE WHEN s.source_response_hash IS NULL
                     THEN 1 ELSE 0 END) AS missing_hash,

            SUM(CASE WHEN s.source_ingestion_timestamp IS NULL
                     THEN 1 ELSE 0 END) AS missing_source_timestamp,

            SUM(CASE WHEN s.silver_processed_timestamp IS NULL
                     THEN 1 ELSE 0 END) AS missing_silver_timestamp,

            SUM(
                CASE WHEN a.psgc_code IS NULL
                     THEN 1 ELSE 0 END
            ) AS unapproved_psgc,

            SUM(
                CASE
                    WHEN a.psgc_code IS NOT NULL
                     AND s.cmci_name <> a.cmci_name
                    THEN 1 ELSE 0
                END
            ) AS mapping_mismatch,

            SUM(
                CASE
                    WHEN s.year NOT IN (
                        '2014','2015','2016','2017','2018','2019',
                        '2020','2021','2022','2023','2024'
                    )
                    THEN 1 ELSE 0
                END
            ) AS invalid_year,

            SUM(
                CASE
                    WHEN b.psgc_code IS NULL
                    THEN 1 ELSE 0
                END
            ) AS missing_bronze_key,

            SUM(
                CASE
                    WHEN b.psgc_code IS NOT NULL
                     AND s.source_response_hash <> b.response_hash
                    THEN 1 ELSE 0
                END
            ) AS stale_source_hash

        FROM {table_name} s

        LEFT JOIN approved a
            ON trim(s.psgc_code) = a.psgc_code

        LEFT JOIN bronze_keys b
            ON trim(s.psgc_code) = trim(b.psgc_code)
           AND s.year = b.year
    ),

    duplicate_keys AS (
        SELECT COALESCE(SUM(n - 1), 0) AS n
        FROM (
            SELECT psgc_code, year, COUNT(*) AS n
            FROM {table_name}
            GROUP BY psgc_code, year
            HAVING COUNT(*) > 1
        )
    ),

    expected_keys AS (
        SELECT COUNT(DISTINCT concat_ws('|', psgc_code, year)) AS n
        FROM bronze_keys
    ),

    actual_keys AS (
        SELECT COUNT(DISTINCT concat_ws('|', psgc_code, year)) AS n
        FROM {table_name}
    ),

    checks AS (
        SELECT
            '{table_short_name}' AS table_name,
            'completeness' AS check_category,
            'table_not_empty' AS check_name,
            CASE WHEN row_count = 0 THEN 1 ELSE 0 END AS failed_rows,
            1 AS total_rows,
            0.0 AS threshold_pct
        FROM metric

        UNION ALL SELECT
            '{table_short_name}',
            'completeness',
            'psgc_code_not_null',
            missing_psgc,
            row_count,
            0.0
        FROM metric

        UNION ALL SELECT
            '{table_short_name}',
            'completeness',
            'lgu_not_null',
            missing_lgu,
            row_count,
            0.0
        FROM metric

        UNION ALL SELECT
            '{table_short_name}',
            'completeness',
            'cmci_name_not_null',
            missing_cmci_name,
            row_count,
            0.0
        FROM metric

        UNION ALL SELECT
            '{table_short_name}',
            'completeness',
            'year_not_null',
            missing_year,
            row_count,
            0.0
        FROM metric

        UNION ALL SELECT
            '{table_short_name}',
            'completeness',
            'source_response_hash_not_null',
            missing_hash,
            row_count,
            0.0
        FROM metric

        UNION ALL SELECT
            '{table_short_name}',
            'completeness',
            'source_ingestion_timestamp_not_null',
            missing_source_timestamp,
            row_count,
            0.0
        FROM metric

        UNION ALL SELECT
            '{table_short_name}',
            'completeness',
            'silver_processed_timestamp_not_null',
            missing_silver_timestamp,
            row_count,
            0.0
        FROM metric

        UNION ALL SELECT
            '{table_short_name}',
            'uniqueness',
            'one_row_per_psgc_year',
            n,
            row_count,
            0.0
        FROM metric CROSS JOIN duplicate_keys

        UNION ALL SELECT
            '{table_short_name}',
            'mapping',
            'psgc_is_approved',
            unapproved_psgc,
            row_count,
            0.0
        FROM metric

        UNION ALL SELECT
            '{table_short_name}',
            'mapping',
            'cmci_mapping_matches_reference',
            mapping_mismatch,
            row_count,
            0.0
        FROM metric

        UNION ALL SELECT
            '{table_short_name}',
            'validity',
            'expected_years_only',
            invalid_year,
            row_count,
            0.0
        FROM metric

        UNION ALL SELECT
            '{table_short_name}',
            'consistency',
            'pillar_key_coverage_matches',
            ABS(
                (SELECT n FROM expected_keys)
                - (SELECT n FROM actual_keys)
            ),
            GREATEST((SELECT n FROM expected_keys), 1),
            0.0
        FROM metric

        UNION ALL SELECT
            '{table_short_name}',
            'consistency',
            'source_hash_matches_bronze',
            stale_source_hash + missing_bronze_key,
            row_count,
            0.0
        FROM metric
    )

    SELECT *
    FROM checks
    """

GOLD_FACT_QUERY = f"""
WITH fact AS (
    SELECT
        COUNT(*) AS row_count,

        SUM(
            CASE
                WHEN psgc_code IS NULL
                  OR trim(psgc_code) = ''
                THEN 1 ELSE 0
            END
        ) AS missing_psgc,

        SUM(
            CASE
                WHEN year IS NULL
                THEN 1 ELSE 0
            END
        ) AS missing_year,

        SUM(
            CASE
                WHEN indicator_code IS NULL
                  OR trim(indicator_code) = ''
                THEN 1 ELSE 0
            END
        ) AS missing_indicator,

        SUM(
            CASE
                WHEN raw_score IS NULL
                THEN 1 ELSE 0
            END
        ) AS null_score,

        SUM(
            CASE
                WHEN raw_score < 0
                THEN 1 ELSE 0
            END
        ) AS negative_score,

        SUM(
            CASE
                WHEN year BETWEEN 2022 AND 2024
                 AND raw_score IS NOT NULL
                 AND raw_score > 2.0001
                THEN 1 ELSE 0
            END
        ) AS invalid_recent_score,

        SUM(
            CASE
                WHEN year BETWEEN 2022 AND 2024
                 AND indicator_code = 'bre'
                 AND raw_score IS NOT NULL
                 AND ROUND(raw_score, 4)
                     NOT IN (0.0, 1.0, 2.0)
                THEN 1 ELSE 0
            END
        ) AS invalid_bre_score,

        SUM(
            CASE
                WHEN year BETWEEN 2022 AND 2024
                 AND indicator_code = 'bpls'
                 AND raw_score IS NOT NULL
                 AND ROUND(raw_score, 4) NOT IN (
                     0.0,
                     0.5,
                     1.0,
                     1.5,
                     2.0
                 )
                THEN 1 ELSE 0
            END
        ) AS invalid_bpls_score,

        SUM(
            CASE
                WHEN year BETWEEN 2022 AND 2024
                 AND indicator_code = 'lra'
                 AND raw_score IS NOT NULL
                 AND ROUND(raw_score, 4)
                     NOT IN (0.0, 1.0, 2.0)
                THEN 1 ELSE 0
            END
        ) AS invalid_lra_score

    FROM {GOLD_FACT_TABLE}
),

duplicate_keys AS (
    SELECT COALESCE(SUM(n - 1), 0) AS n
    FROM (
        SELECT
            psgc_code,
            year,
            indicator_code,
            COUNT(*) AS n
        FROM {GOLD_FACT_TABLE}
        GROUP BY
            psgc_code,
            year,
            indicator_code
        HAVING COUNT(*) > 1
    )
),

approved AS (
    SELECT DISTINCT
        trim(psgc_code) AS psgc_code
    FROM {CMCI_MAP_TABLE}
    WHERE match_status = 'MATCHED'
      AND reviewed = true
      AND is_active = true
),

unapproved AS (
    SELECT COUNT(*) AS n
    FROM {GOLD_FACT_TABLE} f
    LEFT ANTI JOIN approved a
        ON trim(f.psgc_code) = a.psgc_code
),

invalid_years AS (
    SELECT COUNT(*) AS n
    FROM {GOLD_FACT_TABLE}
    WHERE year IS NULL
       OR year NOT BETWEEN 2014 AND 2024
),

unresolved_indicators AS (
    SELECT COUNT(*) AS n
    FROM {GOLD_FACT_TABLE} f
    LEFT ANTI JOIN {GOLD_INDICATOR_TABLE} d
        ON f.indicator_code = d.indicator_code
),

checks AS (
    SELECT
        'fact_cmci_indicator' AS table_name,
        'completeness' AS check_category,
        'table_not_empty' AS check_name,
        CASE
            WHEN row_count = 0 THEN 1
            ELSE 0
        END AS failed_rows,
        1 AS total_rows,
        0.0 AS threshold_pct
    FROM fact

    UNION ALL

    SELECT
        'fact_cmci_indicator',
        'completeness',
        'psgc_code_not_null',
        missing_psgc,
        row_count,
        0.0
    FROM fact

    UNION ALL

    SELECT
        'fact_cmci_indicator',
        'completeness',
        'year_not_null',
        missing_year,
        row_count,
        0.0
    FROM fact

    UNION ALL

    SELECT
        'fact_cmci_indicator',
        'completeness',
        'indicator_code_not_null',
        missing_indicator,
        row_count,
        0.0
    FROM fact

    UNION ALL

    SELECT
        'fact_cmci_indicator',
        'uniqueness',
        'one_row_per_psgc_year_indicator',
        n,
        row_count,
        0.0
    FROM fact
    CROSS JOIN duplicate_keys

    UNION ALL

    SELECT
        'fact_cmci_indicator',
        'mapping',
        'psgc_is_approved',
        n,
        row_count,
        0.0
    FROM fact
    CROSS JOIN unapproved

    UNION ALL

    SELECT
        'fact_cmci_indicator',
        'validity',
        'year_in_expected_domain',
        n,
        row_count,
        0.0
    FROM fact
    CROSS JOIN invalid_years

    UNION ALL

    SELECT
        'fact_cmci_indicator',
        'consistency',
        'indicator_resolves',
        n,
        row_count,
        0.0
    FROM fact
    CROSS JOIN unresolved_indicators

    UNION ALL

    SELECT
        'fact_cmci_indicator',
        'consistency',
        'expected_fact_row_count',
        CASE
            WHEN row_count = {EXPECTED_GOLD_ROW_COUNT}
            THEN 0
            ELSE 1
        END,
        1,
        0.0
    FROM fact

    UNION ALL

    SELECT
        'fact_cmci_indicator',
        'validity',
        'raw_score_not_negative',
        negative_score,
        row_count,
        0.0
    FROM fact

    UNION ALL

    SELECT
        'fact_cmci_indicator',
        'validity',
        'recent_score_within_range',
        invalid_recent_score,
        row_count,
        0.0
    FROM fact

    UNION ALL

    SELECT
        'fact_cmci_indicator',
        'validity',
        'bre_recent_value_domain',
        invalid_bre_score,
        row_count,
        0.0
    FROM fact

    UNION ALL

    SELECT
        'fact_cmci_indicator',
        'validity',
        'bpls_recent_value_domain',
        invalid_bpls_score,
        row_count,
        0.0
    FROM fact

    UNION ALL

    SELECT
        'fact_cmci_indicator',
        'validity',
        'lra_recent_value_domain',
        invalid_lra_score,
        row_count,
        0.0
    FROM fact

    UNION ALL

    SELECT
        'fact_cmci_indicator',
        'completeness',
        'raw_score_not_null',
        null_score,
        row_count,
        100.0
    FROM fact
)

SELECT *
FROM checks
"""

GOLD_DIMENSION_QUERY = """
WITH dim AS (
    SELECT
        COUNT(*) AS row_count,

        SUM(
            CASE
                WHEN indicator_code IS NULL
                  OR trim(indicator_code) = ''
                THEN 1 ELSE 0
            END
        ) AS missing_code,

        SUM(
            CASE
                WHEN indicator_name IS NULL
                  OR trim(indicator_name) = ''
                THEN 1 ELSE 0
            END
        ) AS missing_name,

        SUM(
            CASE
                WHEN pillar_name IS NULL
                  OR trim(pillar_name) = ''
                THEN 1 ELSE 0
            END
        ) AS missing_pillar

    FROM ahon.gold.dim_cmci_indicator
),

duplicate_codes AS (
    SELECT COALESCE(SUM(n - 1), 0) AS n
    FROM (
        SELECT
            indicator_code,
            COUNT(*) AS n
        FROM ahon.gold.dim_cmci_indicator
        GROUP BY indicator_code
        HAVING COUNT(*) > 1
    )
),

checks AS (
    SELECT
        'dim_cmci_indicator' AS table_name,
        'completeness' AS check_category,
        'indicator_dimension_not_empty' AS check_name,
        CASE
            WHEN row_count = 0 THEN 1
            ELSE 0
        END AS failed_rows,
        1 AS total_rows,
        0.0 AS threshold_pct
    FROM dim

    UNION ALL

    SELECT
        'dim_cmci_indicator',
        'completeness',
        'indicator_code_not_null',
        missing_code,
        row_count,
        0.0
    FROM dim

    UNION ALL

    SELECT
        'dim_cmci_indicator',
        'completeness',
        'indicator_name_not_null',
        missing_name,
        row_count,
        0.0
    FROM dim

    UNION ALL

    SELECT
        'dim_cmci_indicator',
        'completeness',
        'pillar_name_not_null',
        missing_pillar,
        row_count,
        0.0
    FROM dim

    UNION ALL

    SELECT
        'dim_cmci_indicator',
        'uniqueness',
        'indicator_code_unique',
        n,
        row_count,
        0.0
    FROM dim
    CROSS JOIN duplicate_codes

    UNION ALL

    SELECT
        'dim_cmci_indicator',
        'consistency',
        'expected_indicator_count',
        CASE
            WHEN row_count = 35 THEN 0
            ELSE 1
        END,
        1,
        0.0
    FROM dim
)

SELECT *
FROM checks
"""

PLATINUM_QUERY = f"""
WITH metric AS (
    SELECT
        COUNT(*) AS row_count,

        SUM(
            CASE
                WHEN psgc_code IS NULL
                  OR trim(psgc_code) = ''
                THEN 1 ELSE 0
            END
        ) AS missing_psgc,

        SUM(
            CASE
                WHEN year IS NULL
                THEN 1 ELSE 0
            END
        ) AS missing_year,

        SUM(
            CASE
                WHEN pillar_name IS NULL
                  OR trim(pillar_name) = ''
                THEN 1 ELSE 0
            END
        ) AS missing_pillar,

        SUM(
            CASE
                WHEN score_status IS NULL
                  OR trim(score_status) = ''
                THEN 1 ELSE 0
            END
        ) AS missing_status,

        SUM(
            CASE
                WHEN year IS NULL
                  OR year NOT BETWEEN 2014 AND 2024
                THEN 1 ELSE 0
            END
        ) AS invalid_year,

        SUM(
            CASE
                WHEN pillar_name NOT IN (
                    'Economic Dynamism',
                    'Government Efficiency',
                    'Infrastructure',
                    'Resiliency',
                    'Innovation'
                )
                THEN 1 ELSE 0
            END
        ) AS invalid_pillar,

        SUM(
            CASE
                WHEN indicators_expected < 0
                  OR indicators_available < 0
                  OR indicators_missing < 0
                  OR indicators_available > indicators_expected
                  OR indicators_missing
                     <> indicators_expected - indicators_available
                THEN 1 ELSE 0
            END
        ) AS invalid_indicator_counts,

        SUM(
            CASE
                WHEN coverage_pct IS NULL
                  OR coverage_pct < 0
                  OR coverage_pct > 100
                  OR ABS(
                      coverage_pct
                      - ROUND(
                          100.0
                          * indicators_available
                          / indicators_expected,
                          2
                      )
                  ) > 0.01
                THEN 1 ELSE 0
            END
        ) AS invalid_coverage,

        SUM(
            CASE
                WHEN score_status = 'COMPLETE'
                 AND (
                     indicators_available <> indicators_expected
                     OR indicators_missing <> 0
                     OR coverage_pct <> 100.0
                     OR pillar_score_raw IS NULL
                 )
                THEN 1 ELSE 0
            END
        ) AS invalid_complete_status,

        SUM(
            CASE
                WHEN score_status = 'INSUFFICIENT_DATA'
                 AND (
                     indicators_available >= indicators_expected
                     OR indicators_missing <= 0
                     OR coverage_pct >= 100.0
                     OR pillar_score_raw IS NOT NULL
                 )
                THEN 1 ELSE 0
            END
        ) AS invalid_insufficient_status,

        SUM(
            CASE
                WHEN score_status NOT IN (
                    'COMPLETE',
                    'INSUFFICIENT_DATA'
                )
                THEN 1 ELSE 0
            END
        ) AS invalid_status_domain,

        SUM(
            CASE
                WHEN pillar_score_raw < 0
                THEN 1 ELSE 0
            END
        ) AS negative_pillar_score

    FROM {PLATINUM_PILLAR_TABLE}
),

duplicate_keys AS (
    SELECT COALESCE(SUM(n - 1), 0) AS n
    FROM (
        SELECT
            psgc_code,
            year,
            pillar_name,
            COUNT(*) AS n
        FROM {PLATINUM_PILLAR_TABLE}
        GROUP BY
            psgc_code,
            year,
            pillar_name
        HAVING COUNT(*) > 1
    )
),

approved AS (
    SELECT DISTINCT
        trim(psgc_code) AS psgc_code
    FROM {CMCI_MAP_TABLE}
    WHERE match_status = 'MATCHED'
      AND reviewed = true
      AND is_active = true
),

unapproved AS (
    SELECT COUNT(*) AS n
    FROM {PLATINUM_PILLAR_TABLE} p
    LEFT ANTI JOIN approved a
        ON trim(p.psgc_code) = a.psgc_code
),

invalid_expected_counts AS (
    SELECT COUNT(*) AS n
    FROM {PLATINUM_PILLAR_TABLE}
    WHERE indicators_expected <>
        CASE pillar_name
            WHEN 'Economic Dynamism' THEN 4
            WHEN 'Government Efficiency' THEN 10
            WHEN 'Infrastructure' THEN 10
            WHEN 'Resiliency' THEN 10
            WHEN 'Innovation' THEN 1
            ELSE -1
        END
),

checks AS (
    SELECT
        'cmci_pillar_score' AS table_name,
        'completeness' AS check_category,
        'table_not_empty' AS check_name,
        CASE
            WHEN row_count = 0 THEN 1
            ELSE 0
        END AS failed_rows,
        1 AS total_rows,
        0.0 AS threshold_pct
    FROM metric

    UNION ALL

    SELECT
        'cmci_pillar_score',
        'completeness',
        'psgc_code_not_null',
        missing_psgc,
        row_count,
        0.0
    FROM metric

    UNION ALL

    SELECT
        'cmci_pillar_score',
        'completeness',
        'year_not_null',
        missing_year,
        row_count,
        0.0
    FROM metric

    UNION ALL

    SELECT
        'cmci_pillar_score',
        'completeness',
        'pillar_name_not_null',
        missing_pillar,
        row_count,
        0.0
    FROM metric

    UNION ALL

    SELECT
        'cmci_pillar_score',
        'completeness',
        'score_status_not_null',
        missing_status,
        row_count,
        0.0
    FROM metric

    UNION ALL

    SELECT
        'cmci_pillar_score',
        'uniqueness',
        'one_row_per_psgc_year_pillar',
        n,
        row_count,
        0.0
    FROM metric
    CROSS JOIN duplicate_keys

    UNION ALL

    SELECT
        'cmci_pillar_score',
        'mapping',
        'psgc_is_approved',
        n,
        row_count,
        0.0
    FROM metric
    CROSS JOIN unapproved

    UNION ALL

    SELECT
        'cmci_pillar_score',
        'validity',
        'year_in_expected_domain',
        invalid_year,
        row_count,
        0.0
    FROM metric

    UNION ALL

    SELECT
        'cmci_pillar_score',
        'validity',
        'pillar_in_selected_scope',
        invalid_pillar,
        row_count,
        0.0
    FROM metric

    UNION ALL

    SELECT
        'cmci_pillar_score',
        'consistency',
        'expected_indicator_count_matches_pillar',
        n,
        row_count,
        0.0
    FROM metric
    CROSS JOIN invalid_expected_counts

    UNION ALL

    SELECT
        'cmci_pillar_score',
        'consistency',
        'indicator_counts_are_valid',
        invalid_indicator_counts,
        row_count,
        0.0
    FROM metric

    UNION ALL

    SELECT
        'cmci_pillar_score',
        'consistency',
        'coverage_pct_is_valid',
        invalid_coverage,
        row_count,
        0.0
    FROM metric

    UNION ALL

    SELECT
        'cmci_pillar_score',
        'consistency',
        'complete_status_is_consistent',
        invalid_complete_status,
        row_count,
        0.0
    FROM metric

    UNION ALL

    SELECT
        'cmci_pillar_score',
        'consistency',
        'insufficient_status_is_consistent',
        invalid_insufficient_status,
        row_count,
        0.0
    FROM metric

    UNION ALL

    SELECT
        'cmci_pillar_score',
        'validity',
        'score_status_in_domain',
        invalid_status_domain,
        row_count,
        0.0
    FROM metric

    UNION ALL

    SELECT
        'cmci_pillar_score',
        'validity',
        'pillar_score_not_negative',
        negative_pillar_score,
        row_count,
        0.0
    FROM metric

    UNION ALL

    SELECT
        'cmci_pillar_score',
        'consistency',
        'expected_platinum_row_count',
        CASE
            WHEN row_count = {EXPECTED_PLATINUM_ROW_COUNT}
            THEN 0
            ELSE 1
        END,
        1,
        0.0
    FROM metric
)

SELECT *
FROM checks
"""

# ------------------------------------------------------------------
# COLLECT METRICS
# ------------------------------------------------------------------

def collect_metrics(spark, layer):
    if layer == "bronze":
        queries = [
            BRONZE_QUERY,
        ]

    elif layer == "silver":
        queries = [
            silver_query(table_name)
            for table_name in SILVER_TABLES
        ]

    elif layer == "gold":
        queries = [
            GOLD_FACT_QUERY,
            GOLD_DIMENSION_QUERY,
        ]

    elif layer == "platinum":
        queries = [
            PLATINUM_QUERY,
        ]

    else:
        raise ValueError(
            "Unsupported CMCI quality layer: "
            + layer
        )

    metrics = []

    for query in queries:
        rows = spark.sql(
            query
        ).collect()

        for row in rows:
            metrics.append(
                row.asDict()
            )

    if not metrics:
        raise RuntimeError(
            "No CMCI quality metrics returned for "
            + layer
        )

    return metrics

# ------------------------------------------------------------------
# PREPARE GX CHECKS
# ------------------------------------------------------------------

def configure_checks(metrics, layer):
    checks = []

    for metric in metrics:
        check = dict(metric)

        check["blocking"] = (
            check["check_name"]
            in BLOCKING_CHECKS[layer]
        )

        total_rows = check["total_rows"]
        failed_rows = check["failed_rows"]

        if total_rows is None or failed_rows is None:
            raise ValueError(
                "Quality metric returned NULL counts: "
                + check["check_name"]
            )

        total_rows = int(total_rows)
        failed_rows = int(failed_rows)

        if total_rows < 0 or failed_rows < 0:
            raise ValueError(
                "Quality metric returned negative counts: "
                + check["check_name"]
            )

        check["total_rows"] = total_rows
        check["failed_rows"] = failed_rows

        if total_rows == 0:
            failed_pct = (
                0.0
                if failed_rows == 0
                else math.inf
            )
        else:
            failed_pct = (
                100.0
                * failed_rows
                / total_rows
            )

        check["failed_pct"] = failed_pct
        check["gx_value"] = failed_pct

        checks.append(check)

    return checks


# ------------------------------------------------------------------
# GX EVALUATION
# ------------------------------------------------------------------

def evaluate_suite(
    context,
    source,
    name,
    checks,
):
    values = {
        check["check_name"]:
            check["gx_value"]
        for check in checks
    }

    suite = gx.ExpectationSuite(
        name=name
    )

    for check in checks:
        threshold = float(
            check["threshold_pct"]
        )

        max_value = (
            None
            if threshold == 100.0
            else threshold
        )

        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column=check["check_name"],
                min_value=0,
                max_value=max_value,
                strict_max=False,
            )
        )

    suite = context.suites.add(
        suite
    )

    asset = source.add_dataframe_asset(
        name=name
    )

    batch_definition = (
        asset.add_batch_definition_whole_dataframe(
            name="summary"
        )
    )

    validation = (
        context.validation_definitions.add(
            gx.ValidationDefinition(
                name=name,
                data=batch_definition,
                suite=suite,
            )
        )
    )

    result = validation.run(
        batch_parameters={
            "dataframe": pd.DataFrame(
                [values]
            )
        }
    )

    outcomes = {}

    for outcome in result.results:
        column = (
            outcome.expectation_config
            .kwargs["column"]
        )

        error = outcome.exception_info or {}

        if (
            error.get("raised_exception")
            or outcome.success is None
        ):
            raise RuntimeError(
                "GX execution failed for "
                + name
                + "."
                + column
            )

        outcomes[column] = bool(
            outcome.success
        )

    return outcomes


def validate_checks(
    context,
    source,
    layer,
    checks,
):
    table_names = sorted(
        {
            check["table_name"]
            for check in checks
        }
    )

    for table_name in table_names:
        table_checks = [
            check
            for check in checks
            if check["table_name"]
            == table_name
        ]

        outcomes = evaluate_suite(
            context=context,
            source=source,
            name=f"cmci_{layer}_{table_name}",
            checks=table_checks,
        )

        for check in table_checks:
            passed = outcomes[
                check["check_name"]
            ]

            if not passed:
                check["status"] = "FAIL"
            elif check["failed_rows"] > 0:
                check["status"] = "WARN"
            else:
                check["status"] = "PASS"


# ------------------------------------------------------------------
# RELEASE GATE
# ------------------------------------------------------------------

def evaluate_gate(checks):
    failures = [
        (
            check["table_name"]
            + "."
            + check["check_name"]
        )
        for check in checks
        if (
            check["blocking"]
            and check["status"] == "FAIL"
        )
    ]

    return failures


# ------------------------------------------------------------------
# SAVE RESULTS
# ------------------------------------------------------------------

def save_results(
    spark,
    run_id,
    run_ts,
    layer,
    checks,
    gate_failures,
):
    result_schema = """
        run_id STRING,
        run_ts TIMESTAMP,
        dataset STRING,
        layer STRING,
        table_name STRING,
        check_category STRING,
        check_name STRING,
        failed_rows LONG,
        total_rows LONG,
        failed_pct DOUBLE,
        threshold_pct DOUBLE,
        blocking BOOLEAN,
        status STRING
    """

    rows = [
        (
            run_id,
            run_ts,
            "cmci",
            layer,
            check["table_name"],
            check["check_category"],
            check["check_name"],
            check["failed_rows"],
            check["total_rows"],
            check["failed_pct"],
            float(
                check["threshold_pct"]
            ),
            check["blocking"],
            check["status"],
        )
        for check in checks
    ]

    (
        spark.createDataFrame(
            rows,
            result_schema,
        )
        .write
        .mode("append")
        .saveAsTable(
            DQ_RESULTS_TABLE
        )
    )

    if gate_failures:
        overall_status = "FAIL"
    elif any(
        check["status"] != "PASS"
        for check in checks
    ):
        overall_status = "WARN"
    else:
        overall_status = "PASS"

    log_schema = """
        run_id STRING,
        run_ts TIMESTAMP,
        dataset STRING,
        layer STRING,
        checks_run INT,
        checks_passed INT,
        checks_warned INT,
        checks_failed INT,
        blocking_failures INT,
        overall_status STRING,
        finished_at TIMESTAMP
    """

    log_row = (
        run_id,
        run_ts,
        "cmci",
        layer,
        len(checks),
        sum(
            check["status"] == "PASS"
            for check in checks
        ),
        sum(
            check["status"] == "WARN"
            for check in checks
        ),
        sum(
            check["status"] == "FAIL"
            for check in checks
        ),
        len(gate_failures),
        overall_status,
        datetime.now(timezone.utc),
    )

    (
        spark.createDataFrame(
            [log_row],
            log_schema,
        )
        .write
        .mode("append")
        .saveAsTable(
            DQ_RUN_LOG_TABLE
        )
    )

    return overall_status


# ------------------------------------------------------------------
# RUN
# ------------------------------------------------------------------

def run(
    spark,
    layer,
    enforce=True,
):
    run_id = str(
        uuid.uuid4()
    )

    run_ts = datetime.now(
        timezone.utc
    )

    for table_name in (
        DQ_RESULTS_TABLE,
        DQ_RUN_LOG_TABLE,
    ):
        if not spark.catalog.tableExists(
            table_name
        ):
            raise RuntimeError(
                "Quality table does not exist: "
                + table_name
            )

    metrics = collect_metrics(
        spark,
        layer,
    )

    checks = configure_checks(
        metrics,
        layer,
    )

    context = gx.get_context(
        mode="ephemeral"
    )

    source = (
        context.data_sources
        .add_pandas(
            name="cmci_quality_metrics"
        )
    )

    validate_checks(
        context,
        source,
        layer,
        checks,
    )

    gate_failures = evaluate_gate(
        checks
    )

    overall_status = save_results(
        spark=spark,
        run_id=run_id,
        run_ts=run_ts,
        layer=layer,
        checks=checks,
        gate_failures=gate_failures,
    )

    print()
    print(
        "CMCI GX QUALITY COMPLETE"
    )
    print(
        "Layer: "
        + layer
    )
    print(
        "Checks: "
        + str(len(checks))
    )
    print(
        "Overall status: "
        + overall_status
    )
    print(
        "Blocking failures: "
        + str(len(gate_failures))
    )

    for check in checks:
        if check["status"] != "PASS":
            print(
                check["status"]
                + " | "
                + check["table_name"]
                + "."
                + check["check_name"]
                + " | "
                + str(check["failed_rows"])
                + " / "
                + str(check["total_rows"])
            )

    # Results are saved before enforcing the gate.
    if gate_failures and enforce:
        raise RuntimeError(
            "CMCI "
            + layer
            + " GX release gate failed: "
            + ", ".join(
                gate_failures
            )
        )

    return overall_status


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--layer",
        required=True,
        choices=(
            "bronze",
            "silver",
            "gold",
            "platinum",
        ),
    )

    parser.add_argument(
        "--enforce",
        choices=(
            "true",
            "false",
        ),
        default="true",
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .getOrCreate()
    )

    spark.conf.set(
        "spark.sql.session.timeZone",
        "UTC",
    )

    run(
        spark=spark,
        layer=args.layer,
        enforce=(
            args.enforce == "true"
        ),
    )


if __name__ == "__main__":
    main()