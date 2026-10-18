# %pip install great_expectations==1.23.1
"""
DQ gate, bronze to silver: cmci (decision 0012).

Run after src/sql/datasets/cmci/silver/cmci_parse.py. Checks the five CMCI
pillar tables with Great Expectations, appends one row per rule to
monitoring.dq_results, and stops the run if a stop rule FAILs. One
run_dq_gate call per pillar table, all with the same rules; rule names start
with the table name so each dq_rule_id is unique. A FAIL in one table stops
the run before the next table is checked.

Moved from tests/sql/monitoring/cmci_quality.py (silver layer, all 14 checks
per table, all blocking there, so all stop here): table_not_empty, the seven
not-null checks, one_row_per_psgc_year, psgc_is_approved,
cmci_mapping_matches_reference, expected_years_only,
pillar_key_coverage_matches (now a row count against the bronze keys) and
source_hash_matches_bronze. cmci_quality.py computed failure counts in SQL and
gave GX one summary row; here GX checks the Spark table directly, so
dq_results gets the failing psgc_code / year keys.

Expected on ahon_dev: not run yet. On ahon (decision 0009 baseline,
2026-10-12) all 70 silver checks passed, 17,974 rows per pillar table
(1,634 LGUs x 11 years).

Update EXPECTED_YEARS when a new CMCI year is loaded.
"""

import os
import sys

import great_expectations as gx
from pyspark.sql import Window
from pyspark.sql import functions as F

# Let Python find src/sql/common/dq_gate.py
try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:  # notebooks have no __file__; Databricks starts them in their own folder
    _here = os.getcwd()
sys.path.append(os.path.abspath(os.path.join(_here, "..", "..", "common")))

from dq_gate import rule, run_dq_gate  # noqa: E402


# ============================================================
# 1. SETTINGS
# ============================================================

CATALOG = "ahon_dev"
SOURCE_NAME = "cmci"
BRONZE_TABLE = f"{CATALOG}.bronze.cmci_raw_indicator_batch_html"
CMCI_MAP_TABLE = f"{CATALOG}.reference.cmci_lgu_map"
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_results"

# Same order as TARGET_TABLES in cmci_parse.py
SILVER_TABLES = [
    "cmci_economic_dynamism",
    "cmci_government_efficiency",
    "cmci_infrastructure",
    "cmci_resiliency",
    "cmci_innovation",
]

# Silver stores year as a string. Changes with new data: add the year when CMCI publishes it.
EXPECTED_YEARS = [str(y) for y in range(2014, 2025)]

# Columns every pillar table has (silver_cmci_setup.sql, all NOT NULL there)
TEXT_COLUMNS = ["psgc_code", "lgu", "cmci_name", "year", "source_response_hash"]
TIMESTAMP_COLUMNS = ["source_ingestion_timestamp", "silver_processed_timestamp"]
KEY_COLUMNS = ["psgc_code", "year", "cmci_name"]


# ============================================================
# 2. REFERENCE DATA (shared by the five tables)
# ============================================================

# Approved mapping, same filter as cmci_parse.py and cmci_quality.py
approved_map = (
    spark.table(CMCI_MAP_TABLE)
    .where((F.col("match_status") == "MATCHED") & F.col("reviewed") & F.col("is_active"))
    .select(
        F.trim("psgc_code").alias("psgc_code_trim"),
        F.trim("cmci_name").alias("map_cmci_name"),
        F.lit(True).alias("in_cmci_map"),
    )
    .dropDuplicates(["psgc_code_trim"])
)

# Bronze keys: the latest response per batch_id, expanded to one row per psgc_code and year.
# Same selection as cmci_parse.py (valid responses only, latest batch, then latest per LGU-year),
# so one row per key and the join below does not duplicate silver rows.
latest_batch = Window.partitionBy("batch_id").orderBy(F.col("ingestion_timestamp").desc())
latest_key = Window.partitionBy("psgc_code_trim", "year").orderBy(F.col("ingestion_timestamp").desc())
bronze_keys = (
    spark.table(BRONZE_TABLE)
    .where(F.col("response_html").isNotNull() & F.col("response_hash").isNotNull())
    .where(F.col("expected_indicator_count") == 35)
    .withColumn("rn", F.row_number().over(latest_batch))
    .where(F.col("rn") == 1)
    .select(
        F.explode("requested_psgc_codes").alias("psgc_code_raw"),
        F.col("requested_years"),
        F.col("response_hash").alias("bronze_response_hash"),
        F.col("ingestion_timestamp"),
    )
    .select(
        F.trim("psgc_code_raw").alias("psgc_code_trim"),
        F.explode("requested_years").alias("year"),
        "bronze_response_hash",
        "ingestion_timestamp",
    )
    .withColumn("rn", F.row_number().over(latest_key))
    .where(F.col("rn") == 1)
    .select("psgc_code_trim", "year", "bronze_response_hash")
)
bronze_key_count = bronze_keys.count()
print(f"Bronze psgc_code-year keys: {bronze_key_count}")


# ============================================================
# 3. HELPER COLUMNS
# ============================================================

# GX skips NULLs and does not treat "" as missing, so *_value columns turn blank text into NULL
# (cmci_quality.py counted NULL or blank as missing).
# in_cmci_map: psgc_code is in the approved mapping (true/false).
# map_cmci_name: the mapping's cmci_name (NULL when not mapped; that row fails psgc_is_approved instead).
# hash_matches_bronze: false when the key is missing from bronze or the hash differs.
def silver_check_df(table):
    df = spark.table(f"{CATALOG}.silver.{table}").withColumn("psgc_code_trim", F.trim("psgc_code"))
    for c in TEXT_COLUMNS:
        df = df.withColumn(f"{c}_value", F.when(F.trim(F.col(c)) != "", F.col(c)))
    return (
        df
        .join(approved_map, on="psgc_code_trim", how="left")
        .join(bronze_keys, on=["psgc_code_trim", "year"], how="left")
        .withColumn("in_cmci_map", F.coalesce(F.col("in_cmci_map"), F.lit(False)))
        .withColumn("hash_matches_bronze",
                    F.coalesce(F.col("source_response_hash") == F.col("bronze_response_hash"), F.lit(False)))
    )


# ============================================================
# 4. RULES
# ============================================================

E = gx.expectations


def silver_rules(table):
    # All stop: blocking in cmci_quality.py, and cmci_parse.py raises on the same conditions before the merge
    return [
        rule(E.ExpectTableRowCountToBeBetween(min_value=1), f"{table}_table_not_empty", "volume", "stop"),
        # Every bronze psgc_code-year key has one silver row (was pillar_key_coverage_matches;
        # with one_row_per_psgc_year this equals the distinct-key comparison)
        rule(E.ExpectTableRowCountToEqual(value=bronze_key_count),
             f"{table}_row_count_matches_bronze_keys", "volume", "stop"),
    ] + [
        rule(E.ExpectColumnValuesToNotBeNull(column=f"{c}_value"), f"{table}_{c}_not_null", "null", "stop")
        for c in TEXT_COLUMNS
    ] + [
        rule(E.ExpectColumnValuesToNotBeNull(column=c), f"{table}_{c}_not_null", "null", "stop")
        for c in TIMESTAMP_COLUMNS
    ] + [
        rule(E.ExpectCompoundColumnsToBeUnique(column_list=["psgc_code", "year"]),
             f"{table}_one_row_per_psgc_year", "unique", "stop"),
        rule(E.ExpectColumnValuesToBeInSet(column="in_cmci_map", value_set=[True]),
             f"{table}_psgc_is_approved", "referential_integrity", "stop"),
        rule(E.ExpectColumnPairValuesToBeEqual(column_A="cmci_name", column_B="map_cmci_name",
                                               ignore_row_if="either_value_is_missing"),
             f"{table}_cmci_mapping_matches_reference", "referential_integrity", "stop"),
        # Changes with new data: EXPECTED_YEARS
        rule(E.ExpectColumnValuesToBeInSet(column="year", value_set=EXPECTED_YEARS),
             f"{table}_expected_years_only", "accepted_values", "stop"),
        rule(E.ExpectColumnValuesToBeInSet(column="hash_matches_bronze", value_set=[True]),
             f"{table}_source_hash_matches_bronze", "reconciliation", "stop"),
    ]


# ============================================================
# 5. RUN THE GATE (one call per pillar table)
# ============================================================

for table in SILVER_TABLES:
    run_dq_gate(
        spark, silver_check_df(table), silver_rules(table),
        source_name=SOURCE_NAME, layer="silver", table_name=table,
        key_columns=KEY_COLUMNS, results_table=RESULTS_TABLE,
    )
