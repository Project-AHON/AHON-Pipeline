# %pip install great_expectations==1.23.1
"""
DQ gate, bronze to silver: blgf_ldrrmf_annual_lgu.

Run after clean_blgf_ldrrmf_annual_lgu.sql. Checks the silver table with
Great Expectations, writes one row per rule to monitoring.dq_result, and stops
the run if a stop rule FAILs. Flag and report rules give WARN and do not stop
it. Two runs, one per DataFrame checked:

1. blgf_ldrrmf_annual_lgu_clean itself: the build's stop checks repeated after
   the write, the PSGC columns silver now carries, and the has_dq_issues flags.
2. Rows per fiscal year: every bronze LGU-year is in silver (stop), and the
   reject table has no rows for that year (report).

Rows the build cannot use go to silver.blgf_ldrrmf_annual_lgu_reject, and the
build then stops, so a clean run leaves the reject table empty.

Tested on ahon_dev_crystal (2026-10-20): no FAIL, 19 PASS, 4 WARN.
WARN psgc_matched 235 (5 old Maguindanao province rows plus 230 the LGU
map marks needs_review since #94; 5 expected after that fix), and the
flag rules negative_amount, zero_appropriation and overspent.

Update FISCAL_YEARS when a new BLGF year is loaded (same as the bundle
variables ldrrmf_start_year and ldrrmf_end_year).
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

# Set by src/sql/common/run_python.py in the job; set them yourself in a notebook
CATALOG = globals().get("PIPELINE_CATALOG", "ahon_dev")
RUN_ID = globals().get("PIPELINE_RUN_ID", "interactive")
SOURCE_NAME = "blgf_ldrrmf_annual_lgu"
BRONZE_TABLE = f"{CATALOG}.bronze.blgf_ldrrmf_annual_lgu"
SILVER_TABLE = f"{CATALOG}.silver.blgf_ldrrmf_annual_lgu_clean"
REJECT_TABLE = f"{CATALOG}.silver.blgf_ldrrmf_annual_lgu_reject"
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_result"
KEY_COLUMNS = ["lgu_name", "province", "fiscal_year"]

# Update when a new BLGF year is loaded
FISCAL_YEARS = (2018, 2024)

AMOUNT_COLUMNS = [
    "ldrrmf_70pct_appropriation", "ldrrmf_70pct_expenditure",
    "qrf_30pct_appropriation", "qrf_30pct_expenditure",
    "total_appropriation", "total_expenditure",
]
LGU_KEY = ["fiscal_year", "region", "province", "lgu_name", "lgu_type"]

E = gx.expectations
silver_df = spark.table(SILVER_TABLE)


# ============================================================
# 2. HELPER COLUMNS
# ============================================================

# GX checks one or two columns at a time, so rules that span several columns get a helper column:
# parts_* = 70% part + 30% part (compared with the total), min_amount = smallest amount in the row
# (one failing row per LGU-year, however many of its amounts are negative),
# code_matches_status = psgc_code is set exactly when psgc_match_status is 'exact'.
silver_check = (
    silver_df
    .withColumn("parts_appropriation", F.col("ldrrmf_70pct_appropriation") + F.col("qrf_30pct_appropriation"))
    .withColumn("parts_expenditure", F.col("ldrrmf_70pct_expenditure") + F.col("qrf_30pct_expenditure"))
    .withColumn("min_amount", F.least(*[F.col(c) for c in AMOUNT_COLUMNS]))
    .withColumn("code_matches_status",
                F.col("psgc_code").isNotNull() == (F.col("psgc_match_status") == "exact"))
)


# ============================================================
# 3. blgf_ldrrmf_annual_lgu_clean RULES
# ============================================================

silver_rules = [
    # Stop rules (same as the checks in the build, repeated after the write)
    rule(E.ExpectColumnValuesToNotBeNull(column="fiscal_year"), "fiscal_year_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToBeBetween(column="fiscal_year", min_value=FISCAL_YEARS[0], max_value=FISCAL_YEARS[1]),
         "fiscal_year_valid", "range", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="lgu_type"), "lgu_type_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="lgu_type", value_set=["Province", "City", "Municipality"]),
         "lgu_type_valid", "accepted_values", "stop"),
    rule(E.ExpectCompoundColumnsToBeUnique(column_list=LGU_KEY), "unique_lgu_year", "unique", "stop"),
    rule(E.ExpectColumnPairValuesToBeEqual(column_A="total_appropriation", column_B="parts_appropriation"),
         "total_appropriation_is_sum_of_parts", "reconciliation", "stop"),
    rule(E.ExpectColumnPairValuesToBeEqual(column_A="total_expenditure", column_B="parts_expenditure"),
         "total_expenditure_is_sum_of_parts", "reconciliation", "stop"),
] + [
    rule(E.ExpectColumnValuesToNotBeNull(column=c), f"{c}_not_null", "null", "stop") for c in AMOUNT_COLUMNS
] + [
    # PSGC columns (from reference.blgf_ldrrmf_lgu_map)
    rule(E.ExpectColumnValuesToBeInSet(column="psgc_match_status", value_set=["exact", "unmatched"]),
         "psgc_match_status_valid", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="code_matches_status", value_set=[True]),
         "psgc_code_matches_status", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToMatchRegex(column="psgc_code", regex=r"^[0-9]{10}$"),
         "psgc_code_10_digits", "accepted_values", "stop"),
    rule(E.ExpectCompoundColumnsToBeUnique(column_list=["psgc_code", "fiscal_year"],
                                           ignore_row_if="any_value_is_missing"),
         "one_row_per_psgc_year", "unique", "stop"),
    # Report rule: rows without a PSGC code (expected: the old Maguindanao province rows)
    rule(E.ExpectColumnValuesToNotBeNull(column="psgc_code"), "psgc_matched", "null", "report"),
    # Flag rules (rows stay in silver and are marked has_dq_issues by the build)
    rule(E.ExpectColumnValuesToBeBetween(column="min_amount", min_value=0), "negative_amount", "range", "flag"),
    rule(E.ExpectColumnValuesToNotBeInSet(column="total_appropriation", value_set=[0]),
         "zero_appropriation", "range", "flag"),
    rule(E.ExpectColumnPairValuesAToBeGreaterThanB(column_A="total_appropriation", column_B="total_expenditure",
                                                   or_equal=True),
         "overspent", "range", "flag"),
]

run_dq_gate(
    spark, silver_check, silver_rules,
    source_name=SOURCE_NAME, layer="silver", table_name="blgf_ldrrmf_annual_lgu_clean",
    key_columns=KEY_COLUMNS, results_table=RESULTS_TABLE, run_id=RUN_ID,
)


# ============================================================
# 4. ROWS PER FISCAL YEAR: BRONZE vs SILVER AND REJECTS
# ============================================================

# One row per fiscal year. Bronze can hold the same LGU-year from several batches, so it is
# counted by distinct LGU-year (the silver key), the same way the build counts it.
# rows_unaccounted = bronze LGU-years not in silver (stop). reject_rows = rows in the reject
# table for that year (report: the build stops when it rejects a row, so this should be 0).
fiscal_year_from_ref = F.regexp_extract("_source_ref", r"FY([0-9]{4})", 1).cast("int")
bronze_keys = (
    spark.table(BRONZE_TABLE)
    .select(fiscal_year_from_ref.alias("fiscal_year"),
            *[F.trim(c).alias(c) for c in ["region", "province", "lgu_name", "lgu_type"]])
    .distinct()
    .groupBy("fiscal_year").agg(F.count("*").alias("bronze_rows"))
)
silver_counts = silver_df.groupBy("fiscal_year").agg(F.count("*").alias("silver_rows"))
if spark.catalog.tableExists(REJECT_TABLE):
    reject_counts = spark.table(REJECT_TABLE).groupBy("fiscal_year").agg(F.count("*").alias("reject_rows"))
else:
    reject_counts = spark.createDataFrame([], "fiscal_year int, reject_rows long")

volume_check = (
    bronze_keys
    .join(silver_counts, "fiscal_year", "full")
    .join(reject_counts, "fiscal_year", "full")
    .fillna(0, subset=["bronze_rows", "silver_rows", "reject_rows"])
    .withColumn("rows_unaccounted", F.col("bronze_rows") - F.col("silver_rows"))
)
volume_check.orderBy("fiscal_year").show()

run_dq_gate(
    spark, volume_check,
    [rule(E.ExpectColumnValuesToBeInSet(column="rows_unaccounted", value_set=[0]),
          "rows_per_year_match_bronze", "volume", "stop"),
     rule(E.ExpectColumnValuesToBeInSet(column="reject_rows", value_set=[0]),
          "no_rejected_rows", "volume", "report")],
    source_name=SOURCE_NAME, layer="silver", table_name="blgf_ldrrmf_annual_lgu_clean",
    key_columns=["fiscal_year"], results_table=RESULTS_TABLE, run_id=RUN_ID,
)