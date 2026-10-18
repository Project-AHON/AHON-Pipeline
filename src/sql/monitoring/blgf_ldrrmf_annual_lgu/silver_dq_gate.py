# %pip install great_expectations==1.23.1
"""
DQ gate, bronze to silver: blgf_ldrrmf_annual_lgu (decision 0012).

Run after clean_blgf_ldrrmf_annual_lgu.sql. Checks the silver table with
Great Expectations, appends one row per rule to monitoring.dq_results, and
stops the run if a stop rule FAILs. Flag rules give WARN and do not stop it.

Expected on ahon_dev (2026-10-15): 14 PASS, 3 WARN
(negative_amount 3 rows, zero_appropriation 187, overspent 388).

Update fiscal_year_valid's max_value when a new BLGF year is loaded.
"""

import os
import sys

import great_expectations as gx
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
SOURCE_NAME = "blgf_ldrrmf_annual_lgu"
BRONZE_TABLE = f"{CATALOG}.bronze.blgf_ldrrmf_annual_lgu"
SILVER_TABLE = f"{CATALOG}.silver.blgf_ldrrmf_annual_lgu_clean"
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_results"

AMOUNT_COLUMNS = [
    "ldrrmf_70pct_appropriation", "ldrrmf_70pct_expenditure",
    "qrf_30pct_appropriation", "qrf_30pct_expenditure",
    "total_appropriation", "total_expenditure",
]


# ============================================================
# 2. HELPER COLUMNS
# ============================================================

# GX checks one or two columns at a time, so rules that span several columns get a helper column:
# parts_* = 70% part + 30% part (compared with the total), min_amount = smallest amount in the row
# (one failing row per LGU-year, however many of its amounts are negative).
bronze_rows = spark.table(BRONZE_TABLE).count()
silver_check = (
    spark.table(SILVER_TABLE)
    .withColumn("parts_appropriation", F.col("ldrrmf_70pct_appropriation") + F.col("qrf_30pct_appropriation"))
    .withColumn("parts_expenditure", F.col("ldrrmf_70pct_expenditure") + F.col("qrf_30pct_expenditure"))
    .withColumn("min_amount", F.least(*[F.col(c) for c in AMOUNT_COLUMNS]))
)


# ============================================================
# 3. RULES
# ============================================================

E = gx.expectations
silver_rules = [
    # Stop rules (same as the assert_true checks in the build, repeated after the write)
    rule(E.ExpectTableRowCountToEqual(value=bronze_rows), "row_count_matches_bronze", "volume", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="fiscal_year"), "fiscal_year_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToBeBetween(column="fiscal_year", min_value=2018, max_value=2024),
         "fiscal_year_valid", "range", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="lgu_type"), "lgu_type_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="lgu_type", value_set=["Province", "City", "Municipality"]),
         "lgu_type_valid", "accepted_values", "stop"),
    rule(E.ExpectCompoundColumnsToBeUnique(column_list=["fiscal_year", "region", "province", "lgu_name", "lgu_type"]),
         "unique_lgu_year", "unique", "stop"),
    rule(E.ExpectColumnPairValuesToBeEqual(column_A="total_appropriation", column_B="parts_appropriation"),
         "total_appropriation_is_sum_of_parts", "reconciliation", "stop"),
    rule(E.ExpectColumnPairValuesToBeEqual(column_A="total_expenditure", column_B="parts_expenditure"),
         "total_expenditure_is_sum_of_parts", "reconciliation", "stop"),
] + [
    rule(E.ExpectColumnValuesToNotBeNull(column=c), f"{c}_not_null", "null", "stop") for c in AMOUNT_COLUMNS
] + [
    # Flag rules (rows stay in silver and are marked has_dq_issues by the build)
    rule(E.ExpectColumnValuesToBeBetween(column="min_amount", min_value=0), "negative_amount", "range", "flag"),
    rule(E.ExpectColumnValuesToNotBeInSet(column="total_appropriation", value_set=[0]),
         "zero_appropriation", "range", "flag"),
    rule(E.ExpectColumnPairValuesAToBeGreaterThanB(column_A="total_appropriation", column_B="total_expenditure",
                                                   or_equal=True),
         "overspent", "range", "flag"),
]


# ============================================================
# 4. RUN THE GATE
# ============================================================

run_dq_gate(
    spark, silver_check, silver_rules,
    source_name=SOURCE_NAME, layer="silver", table_name="blgf_ldrrmf_annual_lgu_clean",
    key_columns=["lgu_name", "province", "fiscal_year"],
    results_table=RESULTS_TABLE,
)
