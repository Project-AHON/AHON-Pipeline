from datetime import datetime, timezone

from bs4 import BeautifulSoup
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =====================================================
# CONFIG
# =====================================================

SOURCE_TABLE = (
    "ahon.bronze.cmci_raw_indicator_batch_html"
)

EXPECTED_INDICATOR_COUNT = 35

CMCI_MAP_TABLE = (
    "ahon.reference.cmci_lgu_map"
)

# =====================================================
# SILVER TARGET TABLES
# =====================================================

ECONOMIC_DYNAMISM_TABLE = (
    "ahon.silver.cmci_economic_dynamism"
)

GOVERNMENT_EFFICIENCY_TABLE = (
    "ahon.silver.cmci_government_efficiency"
)

INFRASTRUCTURE_TABLE = (
    "ahon.silver.cmci_infrastructure"
)

RESILIENCY_TABLE = (
    "ahon.silver.cmci_resiliency"
)

INNOVATION_TABLE = (
    "ahon.silver.cmci_innovation"
)

TARGET_TABLES = [
    ECONOMIC_DYNAMISM_TABLE,
    GOVERNMENT_EFFICIENCY_TABLE,
    INFRASTRUCTURE_TABLE,
    RESILIENCY_TABLE,
    INNOVATION_TABLE,
]


# =====================================================
# INDICATOR-TO-COLUMN MAPPINGS
# =====================================================

ECONOMIC_DYNAMISM_COLUMNS = {
    "Local Economy Size": (
        "local_economy_size"
    ),
    "Local Economy Growth": (
        "local_economy_growth"
    ),
    "Active Establishments in the Locality": (
        "active_establishments"
    ),
    "Employment Generation": (
        "employment_generation"
    ),
}

GOVERNMENT_EFFICIENCY_COLUMNS = {
    "Compliance to National Directives": (
        "compliance_national_directives"
    ),
    "Presence of Investment Promotion Unit": (
        "investment_promotion_unit"
    ),
    "Compliance to ARTA Citizens Charter": (
        "arta_citizens_charter"
    ),
    "Capacity to Generate Local Resource": (
        "local_resource_generation"
    ),
    "Capacity of Health Services": (
        "health_services_capacity"
    ),
    "Capacity of School Services": (
        "school_services_capacity"
    ),
    "Recognition of Performance": (
        "performance_recognition"
    ),
    "Getting Business Permits": (
        "business_permits"
    ),
    "Peace and Order": (
        "peace_and_order"
    ),
    "Social Protection": (
        "social_protection"
    ),
}

INFRASTRUCTURE_COLUMNS = {
    "Road Network": (
        "road_network"
    ),
    "Distance to Ports": (
        "distance_to_ports"
    ),
    "Availability of Basic Utilities": (
        "basic_utilities"
    ),
    "Transportation Vehicles": (
        "transportation_vehicles"
    ),
    "Education": (
        "education"
    ),
    "Health": (
        "health"
    ),
    "LGU Investment": (
        "lgu_investment"
    ),
    "Accommodation Capacity": (
        "accommodation_capacity"
    ),
    "Information Technology Capacity": (
        "information_technology_capacity"
    ),
    "Financial Technology Capacity": (
        "financial_technology_capacity"
    ),
}

RESILIENCY_COLUMNS = {
    "Land Use Plan": (
        "land_use_plan"
    ),
    "Disaster Risk Reduction Plan": (
        "disaster_risk_reduction_plan"
    ),
    "Annual Disaster Drill": (
        "annual_disaster_drill"
    ),
    "Early Warning System": (
        "early_warning_system"
    ),
    "Budget for DRRMP": (
        "budget_for_drrmp"
    ),
    "Local Risk Assessments": (
        "local_risk_assessments"
    ),
    "Emergency Infrastructure": (
        "emergency_infrastructure"
    ),
    "Utilities": (
        "utilities"
    ),
    "Employed Population": (
        "employed_population"
    ),
    "Sanitary System": (
        "sanitary_system"
    ),
}

INNOVATION_COLUMNS = {
    "Internet Capability": (
        "internet_capability"
    ),
}


# =====================================================
# COMBINED EXPECTED INDICATORS
# =====================================================

PILLAR_CONFIGS = {
    ECONOMIC_DYNAMISM_TABLE: (
        ECONOMIC_DYNAMISM_COLUMNS
    ),
    GOVERNMENT_EFFICIENCY_TABLE: (
        GOVERNMENT_EFFICIENCY_COLUMNS
    ),
    INFRASTRUCTURE_TABLE: (
        INFRASTRUCTURE_COLUMNS
    ),
    RESILIENCY_TABLE: (
        RESILIENCY_COLUMNS
    ),
    INNOVATION_TABLE: (
        INNOVATION_COLUMNS
    ),
}

EXPECTED_INDICATOR_LABELS = set()

for indicator_mapping in PILLAR_CONFIGS.values():
    EXPECTED_INDICATOR_LABELS.update(
        indicator_mapping.keys()
    )

if len(EXPECTED_INDICATOR_LABELS) != 35:
    raise RuntimeError(
        "Expected 35 configured indicators, found "
        + str(len(EXPECTED_INDICATOR_LABELS))
    )


# =====================================================
# SPARK CONFIG
# =====================================================

spark.conf.set(
    "spark.sql.session.timeZone",
    "UTC",
)


# =====================================================
# STARTUP SUMMARY
# =====================================================

print("CMCI indicator Silver parser started")

print(
    "Bronze source: "
    + SOURCE_TABLE
)

print(
    "Expected indicators: "
    + str(EXPECTED_INDICATOR_COUNT)
)

print(
    "Silver pillar tables: "
    + str(len(TARGET_TABLES))
)

# =====================================================
# VALIDATE REQUIRED TABLES
# =====================================================

required_tables = [
    SOURCE_TABLE,
    CMCI_MAP_TABLE,
] + TARGET_TABLES

for table_name in required_tables:
    try:
        column_count = len(
            spark.table(table_name).columns
        )

        print(
            "Required table found: "
            + table_name
            + " | Columns: "
            + str(column_count)
        )

    except Exception as error:  # noqa: BLE001
        raise RuntimeError(
            "Required table cannot be accessed: "
            + table_name
            + " | "
            + str(error)
        ) from error


# =====================================================
# VALIDATE BRONZE COLUMNS
# =====================================================

bronze_df = spark.table(
    SOURCE_TABLE
)

required_bronze_columns = {
    "batch_id",
    "requested_psgc_codes",
    "requested_cmci_names",
    "requested_years",
    "requested_indicator_codes",
    "expected_lgu_count",
    "expected_year_count",
    "expected_indicator_count",
    "returned_value_count",
    "response_html",
    "response_hash",
    "ingestion_timestamp",
}

actual_bronze_columns = set(
    bronze_df.columns
)

missing_bronze_columns = (
    required_bronze_columns
    - actual_bronze_columns
)

if missing_bronze_columns:
    raise RuntimeError(
        "Bronze table is missing required columns: "
        + str(
            sorted(
                missing_bronze_columns
            )
        )
    )


# =====================================================
# VALIDATE SILVER TARGET COLUMNS
# =====================================================

COMMON_SILVER_COLUMNS = {
    "psgc_code",
    "lgu",
    "cmci_name",
    "year",
    "source_response_hash",
    "source_ingestion_timestamp",
    "silver_processed_timestamp",
}

for (
    table_name,
    indicator_mapping,
) in PILLAR_CONFIGS.items():

    expected_columns = (
        COMMON_SILVER_COLUMNS
        | set(
            indicator_mapping.values()
        )
    )

    actual_columns = set(
        spark.table(
            table_name
        ).columns
    )

    missing_columns = (
        expected_columns
        - actual_columns
    )

    if missing_columns:
        raise RuntimeError(
            "Silver table is missing required columns: "
            + table_name
            + " | "
            + str(
                sorted(
                    missing_columns
                )
            )
        )

# =====================================================
# LOAD APPROVED CMCI MAPPING LOOKUPS
# =====================================================

approved_mapping_rows = (
    spark.table(
        CMCI_MAP_TABLE
    )
    .where(
        F.col("match_status") == "MATCHED"
    )
    .where(
        F.col("reviewed") == True
    )
    .where(
        F.col("is_active") == True
    )
    .select(
        F.trim("psgc_code").alias(
            "psgc_code"
        ),
        F.trim("psgc_name").alias(
            "psgc_name"
        ),
        F.trim("cmci_name").alias(
            "cmci_name"
        ),
    )
    .collect()
)

PSGC_NAME_BY_CODE = {}
PSGC_CODE_BY_CMCI_NAME = {}

for mapping_row in approved_mapping_rows:
    psgc_code = mapping_row[
        "psgc_code"
    ]

    psgc_name = mapping_row[
        "psgc_name"
    ]

    cmci_name = mapping_row[
        "cmci_name"
    ]

    if cmci_name in PSGC_CODE_BY_CMCI_NAME:
        raise RuntimeError(
            "Duplicate approved CMCI name found: "
            + cmci_name
        )

    PSGC_NAME_BY_CODE[
        psgc_code
    ] = psgc_name

    PSGC_CODE_BY_CMCI_NAME[
        cmci_name
    ] = psgc_code

if not PSGC_CODE_BY_CMCI_NAME:
    raise RuntimeError(
        "No approved CMCI mappings were loaded"
    )

print(
    "Approved CMCI mappings loaded: "
    + str(len(PSGC_CODE_BY_CMCI_NAME))
)


# =====================================================
# CONVERT INDICATOR VALUE
# =====================================================

def convert_indicator_value(
    indicator_label,
    indicator_value,
):
    """
    Convert a CMCI indicator value to a nullable float.
    """

    cleaned_value = str(
        indicator_value
    ).strip()

    if cleaned_value in (
        "",
        "-",
    ):
        return None

    try:
        return float(
            cleaned_value
        )

    except ValueError as error:
        raise ValueError(
            "Indicator value is not numeric: "
            + indicator_label
            + " | "
            + cleaned_value
        ) from error


# =====================================================
# PARSE ONE BATCH RESPONSE
# =====================================================

def parse_batch_response(
    response_html,
    requested_cmci_names,
    requested_years,
):
    """
    Expand one CMCI batch response into values keyed by
    CMCI name, year, and indicator label.
    """

    soup = BeautifulSoup(
        response_html,
        "html.parser",
    )

    if soup.title is None:
        raise ValueError(
            "CMCI batch response has no page title"
        )

    page_title = soup.title.get_text(
        " ",
        strip=True,
    )

    if "Data Portal Results" not in page_title:
        raise ValueError(
            "Unexpected page title: "
            + page_title
        )

    tables = soup.find_all(
        "table"
    )

    if len(tables) != EXPECTED_INDICATOR_COUNT:
        raise ValueError(
            "Expected "
            + str(EXPECTED_INDICATOR_COUNT)
            + " indicator tables, found "
            + str(len(tables))
        )

    expected_cmci_names = set(
        requested_cmci_names
    )

    expected_years = list(
        requested_years
    )

    parsed_values = {}
    returned_indicators = set()

    for table in tables:
        heading = table.find_previous(
            "h2"
        )

        if heading is None:
            raise ValueError(
                "Indicator table has no heading"
            )

        indicator_label = heading.get_text(
            " ",
            strip=True,
        )

        if indicator_label not in EXPECTED_INDICATOR_LABELS:
            raise ValueError(
                "Unexpected indicator table: "
                + indicator_label
            )

        if indicator_label in returned_indicators:
            raise ValueError(
                "Duplicate indicator table: "
                + indicator_label
            )

        returned_indicators.add(
            indicator_label
        )

        table_rows = []

        for table_row in table.find_all("tr"):
            values = [
                cell.get_text(
                    " ",
                    strip=True,
                )
                for cell in table_row.find_all(
                    (
                        "th",
                        "td",
                    )
                )
            ]

            if values:
                table_rows.append(
                    values
                )

        expected_row_count = (
            len(requested_cmci_names)
            + 1
        )

        if len(table_rows) != expected_row_count:
            raise ValueError(
                "Unexpected row count for "
                + indicator_label
                + ". Expected "
                + str(expected_row_count)
                + ", found "
                + str(len(table_rows))
            )

        header_row = table_rows[0]

        if not header_row:
            raise ValueError(
                "Missing table header for "
                + indicator_label
            )

        if header_row[0] != "Province / LGU":
            raise ValueError(
                "Unexpected first header for "
                + indicator_label
                + ": "
                + str(header_row[0])
            )

        returned_years = header_row[1:]

        if returned_years != expected_years:
            raise ValueError(
                "Year columns do not match for "
                + indicator_label
                + ". Expected "
                + str(expected_years)
                + ", found "
                + str(returned_years)
            )

        returned_cmci_names = set()

        for row in table_rows[1:]:
            expected_column_count = (
                len(expected_years)
                + 1
            )

            if len(row) != expected_column_count:
                raise ValueError(
                    "Unexpected column count for "
                    + indicator_label
                    + ": "
                    + str(row)
                )

            cmci_name = row[0].strip()

            if cmci_name in returned_cmci_names:
                raise ValueError(
                    "Duplicate CMCI locality row for "
                    + indicator_label
                    + ": "
                    + cmci_name
                )

            returned_cmci_names.add(
                cmci_name
            )

            for year_position, year in enumerate(
                expected_years,
                start=1,
            ):
                raw_value = row[
                    year_position
                ].strip()

                parsed_values[
                    (
                        cmci_name,
                        year,
                        indicator_label,
                    )
                ] = convert_indicator_value(
                    indicator_label=indicator_label,
                    indicator_value=raw_value,
                )

        if returned_cmci_names != expected_cmci_names:
            missing_names = sorted(
                expected_cmci_names
                - returned_cmci_names
            )

            unexpected_names = sorted(
                returned_cmci_names
                - expected_cmci_names
            )

            raise ValueError(
                "Returned CMCI names do not match for "
                + indicator_label
                + " | Missing: "
                + str(missing_names)
                + " | Unexpected: "
                + str(unexpected_names)
            )

    if returned_indicators != EXPECTED_INDICATOR_LABELS:
        missing_indicators = sorted(
            EXPECTED_INDICATOR_LABELS
            - returned_indicators
        )

        unexpected_indicators = sorted(
            returned_indicators
            - EXPECTED_INDICATOR_LABELS
        )

        raise ValueError(
            "Returned indicator set does not match. "
            + "Missing: "
            + str(missing_indicators)
            + " | Unexpected: "
            + str(unexpected_indicators)
        )

    expected_value_count = (
        len(requested_cmci_names)
        * len(expected_years)
        * EXPECTED_INDICATOR_COUNT
    )

    if len(parsed_values) != expected_value_count:
        raise ValueError(
            "Parsed value count mismatch. Expected "
            + str(expected_value_count)
            + ", found "
            + str(len(parsed_values))
        )

    return parsed_values


# =====================================================
# SELECT LATEST VERSION OF EACH BATCH
# =====================================================

latest_batch_window = (
    Window
    .partitionBy(
        "batch_id"
    )
    .orderBy(
        F.col(
            "ingestion_timestamp"
        ).desc()
    )
)
latest_batch_df = (
    bronze_df
    .where(
        F.col("response_html").isNotNull()
    )
    .where(
        F.col("response_hash").isNotNull()
    )
    .where(
        F.col("expected_indicator_count")
        == EXPECTED_INDICATOR_COUNT
    )
    .withColumn(
        "record_number",
        F.row_number().over(
            latest_batch_window
        ),
    )
    .where(
        F.col("record_number") == 1
    )
    .drop(
        "record_number"
    )
)

latest_batch_count = (
    latest_batch_df.count()
)

if latest_batch_count == 0:
    raise RuntimeError(
        "No valid Bronze batch responses were found"
    )

print(
    "Latest Bronze batches selected: "
    + str(latest_batch_count)
)


# =====================================================
# EXPAND BATCHES INTO LGU-YEAR RECORDS
# =====================================================

latest_lgu_year_records = {}
batch_parse_failures = []

processed_batch_count = 0

for bronze_row in latest_batch_df.toLocalIterator():
    bronze_record = bronze_row.asDict(
        recursive=True
    )

    batch_id = bronze_record[
        "batch_id"
    ]

    requested_psgc_codes = bronze_record[
        "requested_psgc_codes"
    ]

    requested_cmci_names = bronze_record[
        "requested_cmci_names"
    ]

    requested_years = bronze_record[
        "requested_years"
    ]

    try:
        if (
            len(requested_psgc_codes)
            != len(requested_cmci_names)
        ):
            raise ValueError(
                "PSGC and CMCI name arrays have "
                + "different lengths"
            )

        batch_mapping = dict(
            zip(
                requested_cmci_names,
                requested_psgc_codes,
            )
        )

        if len(batch_mapping) != len(
            requested_cmci_names
        ):
            raise ValueError(
                "Batch contains duplicate CMCI names"
            )

        for cmci_name, psgc_code in (
            batch_mapping.items()
        ):
            approved_psgc_code = (
                PSGC_CODE_BY_CMCI_NAME.get(
                    cmci_name
                )
            )

            if approved_psgc_code != psgc_code:
                raise ValueError(
                    "Batch mapping does not match "
                    + "the approved reference mapping: "
                    + cmci_name
                    + " | "
                    + str(psgc_code)
                )

        parsed_values = parse_batch_response(
            response_html=bronze_record[
                "response_html"
            ],
            requested_cmci_names=(
                requested_cmci_names
            ),
            requested_years=requested_years,
        )

        for cmci_name, psgc_code in (
            batch_mapping.items()
        ):
            psgc_name = PSGC_NAME_BY_CODE.get(
                psgc_code
            )

            if psgc_name is None:
                raise ValueError(
                    "PSGC name not found for code: "
                    + psgc_code
                )

            for year in requested_years:
                record_key = (
                    psgc_code,
                    year,
                )

                indicator_results = {}

                for indicator_label in (
                    EXPECTED_INDICATOR_LABELS
                ):
                    value_key = (
                        cmci_name,
                        year,
                        indicator_label,
                    )

                    if value_key not in parsed_values:
                        raise ValueError(
                            "Expected parsed value was "
                            + "not found: "
                            + str(value_key)
                        )

                    indicator_results[
                        indicator_label
                    ] = parsed_values[
                        value_key
                    ]

                candidate_record = {
                    "psgc_code": psgc_code,
                    "lgu": psgc_name,
                    "cmci_name": cmci_name,
                    "year": year,
                    "indicator_results": (
                        indicator_results
                    ),
                    "response_hash": bronze_record[
                        "response_hash"
                    ],
                    "ingestion_timestamp": (
                        bronze_record[
                            "ingestion_timestamp"
                        ]
                    ),
                    "batch_id": batch_id,
                }

                existing_record = (
                    latest_lgu_year_records.get(
                        record_key
                    )
                )

                if (
                    existing_record is None
                    or candidate_record[
                        "ingestion_timestamp"
                    ]
                    > existing_record[
                        "ingestion_timestamp"
                    ]
                ):
                    latest_lgu_year_records[
                        record_key
                    ] = candidate_record

        processed_batch_count += 1

    except Exception as error:
        batch_parse_failures.append(
            {
                "batch_id": batch_id,
                "error": str(error),
            }
        )


# =====================================================
# STOP IF ANY BATCH FAILED
# =====================================================

if batch_parse_failures:
    print()
    print("FAILED BRONZE BATCHES")

    for failure in batch_parse_failures:
        print(
            failure["batch_id"]
            + " | "
            + failure["error"]
        )

    raise RuntimeError(
        str(len(batch_parse_failures))
        + " Bronze batch or batches failed parsing. "
        + "No Silver records were written."
    )


# =====================================================
# VALIDATE EXPANDED COVERAGE
# =====================================================

if processed_batch_count != latest_batch_count:
    raise RuntimeError(
        "Processed batch count mismatch. Expected "
        + str(latest_batch_count)
        + ", processed "
        + str(processed_batch_count)
    )

expanded_lgu_year_count = len(
    latest_lgu_year_records
)

EXPECTED_LGU_YEAR_COUNT = (
    len(PSGC_CODE_BY_CMCI_NAME)
    * 11
)

if expanded_lgu_year_count != (
    EXPECTED_LGU_YEAR_COUNT
):
    raise RuntimeError(
        "Expanded LGU-year coverage mismatch. Expected "
        + str(EXPECTED_LGU_YEAR_COUNT)
        + ", found "
        + str(expanded_lgu_year_count)
    )

print()
print("BATCH BRONZE EXPANSION COMPLETE")

print(
    "Bronze batches parsed: "
    + str(processed_batch_count)
)

print(
    "Unique LGU-year records: "
    + str(expanded_lgu_year_count)
)

# =====================================================
# BUILD PILLAR RECORDS
# =====================================================

processed_timestamp = datetime.now(
    timezone.utc
)

pillar_records = {}

for table_name in TARGET_TABLES:
    pillar_records[
        table_name
    ] = []

for record_key in sorted(
    latest_lgu_year_records.keys()
):
    lgu_year_record = (
        latest_lgu_year_records[
            record_key
        ]
    )

    indicator_results = lgu_year_record[
        "indicator_results"
    ]

    for (
        table_name,
        indicator_mapping,
    ) in PILLAR_CONFIGS.items():

        pillar_record = {
            "psgc_code": lgu_year_record[
                "psgc_code"
            ],
            "lgu": lgu_year_record[
                "lgu"
            ],
            "cmci_name": lgu_year_record[
                "cmci_name"
            ],
            "year": lgu_year_record[
                "year"
            ],
        }

        for (
            indicator_label,
            silver_column,
        ) in indicator_mapping.items():

            if indicator_label not in indicator_results:
                raise RuntimeError(
                    "Indicator missing from expanded record: "
                    + indicator_label
                    + " | "
                    + lgu_year_record[
                        "psgc_code"
                    ]
                    + " | "
                    + lgu_year_record[
                        "year"
                    ]
                )

            pillar_record[
                silver_column
            ] = indicator_results[
                indicator_label
            ]

        pillar_record[
            "source_response_hash"
        ] = lgu_year_record[
            "response_hash"
        ]

        pillar_record[
            "source_ingestion_timestamp"
        ] = lgu_year_record[
            "ingestion_timestamp"
        ]

        pillar_record[
            "silver_processed_timestamp"
        ] = processed_timestamp

        pillar_records[
            table_name
        ].append(
            pillar_record
        )


# =====================================================
# VALIDATE PILLAR RECORD COUNTS
# =====================================================

for table_name in TARGET_TABLES:
    record_count = len(
        pillar_records[
            table_name
        ]
    )

    if record_count != expanded_lgu_year_count:
        raise RuntimeError(
            "Pillar record count mismatch for "
            + table_name
            + ". Expected "
            + str(expanded_lgu_year_count)
            + ", created "
            + str(record_count)
        )

    print(
        "Pillar records prepared: "
        + table_name
        + " | Rows: "
        + str(record_count)
    )


# =====================================================
# CREATE SILVER DATAFRAMES
# =====================================================

silver_dataframes = {}

for table_name in TARGET_TABLES:
    target_schema = spark.table(
        table_name
    ).schema

    silver_df = spark.createDataFrame(
        pillar_records[
            table_name
        ],
        schema=target_schema,
    )

    silver_dataframes[
        table_name
    ] = silver_df


# =====================================================
# VALIDATE SILVER DATAFRAMES
# =====================================================

for table_name in TARGET_TABLES:
    silver_df = silver_dataframes[
        table_name
    ]

    dataframe_count = silver_df.count()

    if dataframe_count != expanded_lgu_year_count:
        raise RuntimeError(
            "Silver DataFrame count mismatch for "
            + table_name
            + ". Expected "
            + str(expanded_lgu_year_count)
            + ", found "
            + str(dataframe_count)
        )

    duplicate_key_count = (
        silver_df
        .groupBy(
            "psgc_code",
            "year",
        )
        .count()
        .where(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_key_count > 0:
        raise RuntimeError(
            "Silver DataFrame contains duplicate "
            + "PSGC code and year combinations: "
            + table_name
        )

    missing_key_count = (
        silver_df
        .where(
            F.col("psgc_code").isNull()
            | (F.trim("psgc_code") == "")
            | F.col("lgu").isNull()
            | (F.trim("lgu") == "")
            | F.col("cmci_name").isNull()
            | (F.trim("cmci_name") == "")
            | F.col("year").isNull()
            | (F.trim("year") == "")
            | F.col(
                "source_response_hash"
            ).isNull()
            | F.col(
                "source_ingestion_timestamp"
            ).isNull()
            | F.col(
                "silver_processed_timestamp"
            ).isNull()
        )
        .count()
    )

    if missing_key_count > 0:
        raise RuntimeError(
            "Silver DataFrame contains "
            + str(missing_key_count)
            + " rows with missing required values: "
            + table_name
        )

    print(
        "Silver DataFrame validated: "
        + table_name
        + " | Rows: "
        + str(dataframe_count)
    )


# =====================================================
# DATAFRAME PREPARATION SUMMARY
# =====================================================

print()
print("SILVER DATAFRAMES READY")

print(
    "LGU-year rows per pillar: "
    + str(expanded_lgu_year_count)
)

print(
    "Pillar DataFrames prepared: "
    + str(len(silver_dataframes))
)

# =====================================================
# BUILD MERGE ASSIGNMENTS
# =====================================================

def build_merge_assignments(
    column_names,
):
    """
    Build column assignments for the Delta MERGE.
    """

    assignments = []

    for column_name in column_names:
        assignments.append(
            "target.`"
            + column_name
            + "` = source.`"
            + column_name
            + "`"
        )

    return ",\n            ".join(
        assignments
    )


# =====================================================
# MERGE EACH PILLAR TABLE
# =====================================================

merge_results = {}

for table_number, table_name in enumerate(
    TARGET_TABLES,
    start=1,
):
    silver_df = silver_dataframes[
        table_name
    ]

    temp_view_name = (
        "cmci_batch_silver_source_"
        + str(table_number)
    )

    silver_df.createOrReplaceTempView(
        temp_view_name
    )

    column_names = silver_df.columns

    update_assignments = build_merge_assignments(
        column_names
    )

    insert_columns = ",\n            ".join(
        "`"
        + column_name
        + "`"
        for column_name in column_names
    )

    insert_values = ",\n            ".join(
        "source.`"
        + column_name
        + "`"
        for column_name in column_names
    )

    merge_sql = (
        "MERGE INTO "
        + table_name
        + " AS target\n"
        + "USING "
        + temp_view_name
        + " AS source\n"
        + "ON target.psgc_code = source.psgc_code\n"
        + "AND target.year = source.year\n"
        + "WHEN MATCHED AND (\n"
        + "    target.source_response_hash "
        + "<> source.source_response_hash\n"
        + "    OR target.source_response_hash IS NULL\n"
        + ") THEN UPDATE SET\n"
        + "            "
        + update_assignments
        + "\n"
        + "WHEN NOT MATCHED THEN INSERT (\n"
        + "            "
        + insert_columns
        + "\n"
        + ") VALUES (\n"
        + "            "
        + insert_values
        + "\n"
        + ")"
    )

    spark.sql(
        merge_sql
    )

    merge_results[
        table_name
    ] = silver_df.count()

    spark.catalog.dropTempView(
        temp_view_name
    )

    print(
        "Silver merge completed: "
        + table_name
    )


# =====================================================
# VERIFY SAVED SILVER TABLES
# =====================================================

for table_name in TARGET_TABLES:
    source_df = silver_dataframes[
        table_name
    ].alias(
        "source"
    )

    saved_df = (
        spark.table(
            table_name
        )
        .alias(
            "saved"
        )
        .join(
            source_df,
            on=[
                "psgc_code",
                "year",
            ],
            how="inner",
        )
    )

    saved_count = saved_df.count()

    if saved_count != expanded_lgu_year_count:
        raise RuntimeError(
            "Saved Silver coverage mismatch for "
            + table_name
            + ". Expected "
            + str(expanded_lgu_year_count)
            + ", found "
            + str(saved_count)
        )

    stale_hash_count = (
        saved_df
        .where(
            F.col(
                "saved.source_response_hash"
            )
            != F.col(
                "source.source_response_hash"
            )
        )
        .count()
    )

    if stale_hash_count > 0:
        raise RuntimeError(
            "Saved Silver table contains "
            + str(stale_hash_count)
            + " rows with stale source hashes: "
            + table_name
        )

    duplicate_key_count = (
        spark.table(
            table_name
        )
        .groupBy(
            "psgc_code",
            "year",
        )
        .count()
        .where(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_key_count > 0:
        raise RuntimeError(
            "Saved Silver table contains duplicate "
            + "PSGC code and year combinations: "
            + table_name
        )

    print(
        "Silver table verified: "
        + table_name
        + " | Rows: "
        + str(saved_count)
    )


# =====================================================
# VERIFY YEAR COVERAGE
# =====================================================

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

expected_lgu_count = len(
    PSGC_CODE_BY_CMCI_NAME
)

for table_name in TARGET_TABLES:
    year_rows = (
        spark.table(
            table_name
        )
        .where(
            F.col("year").isin(
                EXPECTED_YEARS
            )
        )
        .groupBy(
            "year"
        )
        .count()
        .collect()
    )

    year_counts = {
        row["year"]: row["count"]
        for row in year_rows
    }

    missing_years = []
    invalid_year_counts = []

    for year in EXPECTED_YEARS:
        year_count = year_counts.get(
            year,
            0,
        )

        if year_count == 0:
            missing_years.append(
                year
            )

        elif year_count != expected_lgu_count:
            invalid_year_counts.append(
                year
                + "="
                + str(year_count)
            )

    if missing_years:
        raise RuntimeError(
            "Silver table is missing years: "
            + table_name
            + " | "
            + str(missing_years)
        )

    if invalid_year_counts:
        raise RuntimeError(
            "Silver table has incomplete yearly coverage: "
            + table_name
            + " | Expected "
            + str(expected_lgu_count)
            + " rows per year | Found "
            + str(invalid_year_counts)
        )

    print(
        "Year coverage verified: "
        + table_name
        + " | Years: "
        + str(len(EXPECTED_YEARS))
        + " | Rows per year: "
        + str(expected_lgu_count)
    )


# =====================================================
# FINAL SUMMARY
# ====================================================

print()
print("CMCI BATCH SILVER LOAD COMPLETE")

print(
    "Bronze batches processed: "
    + str(processed_batch_count)
)

print(
    "Approved LGUs represented: "
    + str(expected_lgu_count)
)

print(
    "Years represented: "
    + str(len(EXPECTED_YEARS))
)

print(
    "LGU-year rows per pillar: "
    + str(expanded_lgu_year_count)
)

print(
    "Pillar tables updated: "
    + str(len(TARGET_TABLES))
)

for table_name in TARGET_TABLES:
    print(
        table_name
        + " | Rows processed: "
        + str(
            merge_results[
                table_name
            ]
        )
    )
