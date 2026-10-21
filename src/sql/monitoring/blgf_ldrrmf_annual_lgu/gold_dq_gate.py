# %pip install great_expectations==1.23.1
"""
DQ gate, silver to gold: fact_ldrrmf.

Run after build_fact_ldrrmf.sql. Checks gold.fact_ldrrmf with Great Expectations,
writes one row per rule to monitoring.dq_result, and stops the run if a stop
rule FAILs. Report rules give WARN and do not stop it. Three runs, one per
DataFrame checked:

1. fact_ldrrmf itself: the build's stop checks, repeated after the write, plus
   a report rule for rows without a PSGC code. match_confidence is always NULL
   now (the matcher only accepts exact matches), so its rule passes trivially;
   match_status_valid is the real check.
2. lgu_type against the dim_lgu level of the matched code (report).
3. Coverage: every province, city and municipality in dim_lgu has a row in
   the latest fiscal year (report).

Tested on ahon_dev_crystal (2026-10-20): no FAIL.
1. WARN psgc_matched 235 (5 old Maguindanao province rows plus 230 the LGU
   map marks needs_review since #94), utilization_rate_not_negative 2.
2. WARN, 4 rows: Baliuag (Municipality in BLGF, City in PSGC). Before #94 also
   Calaca, Santo Tomas, Carmona and Pateros (20 rows).
3. WARN, 43 rows: the 8 BARMM Special Geographic Area towns (codes 19999...,
   created in 2024) plus 35 cities without a code because of needs_review.
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
SILVER_TABLE = f"{CATALOG}.silver.blgf_ldrrmf_annual_lgu_clean"
GOLD_TABLE = f"{CATALOG}.gold.fact_ldrrmf"
DIM_LGU_TABLE = f"{CATALOG}.gold.dim_lgu"
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_result"
KEY_COLUMNS = ["lgu_name", "province", "fiscal_year"]

E = gx.expectations
gold_df = spark.table(GOLD_TABLE)
dim_lgu = spark.table(DIM_LGU_TABLE)


# ============================================================
# 2. fact_ldrrmf RULES
# ============================================================

# Referential check: a left join to dim_lgu adds in_dim_lgu (true/false, NULL when psgc_code is NULL).
# GX skips NULLs, so rows without a code are left to the psgc_matched report rule.
# min_utilization_rate = smallest of the three rates (NULL when every budget is 0).
silver_rows = spark.table(SILVER_TABLE).count()
dim_periods = [r[0] for r in dim_lgu.select("source_period").distinct().collect()]
dim_codes = dim_lgu.select("psgc_code").distinct().withColumn("in_dim_lgu", F.lit(True))
gold_check = (
    gold_df
    .join(dim_codes, on="psgc_code", how="left")
    .withColumn("in_dim_lgu",
                F.when(F.col("psgc_code").isNotNull(), F.coalesce(F.col("in_dim_lgu"), F.lit(False))))
    .withColumn("min_utilization_rate",
                F.least("utilization_rate_70_pct", "utilization_rate_30_pct", "utilization_rate_total"))
)

gold_rules = [
    # Stop rules (same as the checks in build_fact_ldrrmf.sql, repeated after the write)
    rule(E.ExpectTableRowCountToEqual(value=silver_rows), "row_count_matches_silver", "volume", "stop"),
    rule(E.ExpectColumnValuesToBeUnique(column="ldrrmf_fact_key"), "fact_key_unique", "unique", "stop"),
    rule(E.ExpectCompoundColumnsToBeUnique(column_list=["psgc_code", "fiscal_year"],
                                           ignore_row_if="any_value_is_missing"),
         "one_row_per_psgc_year", "unique", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="in_dim_lgu", value_set=[True]),
         "psgc_code_in_dim_lgu", "referential_integrity", "stop"),
    rule(E.ExpectColumnValuesToBeBetween(column="match_confidence", min_value=0, max_value=1),
         "match_confidence_valid", "range", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="match_status", value_set=["exact", "unmatched"]),
         "match_status_valid", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="source_period", value_set=dim_periods),
         "source_period_in_dim_lgu", "referential_integrity", "stop"),
    # Flag rule: negative amounts give a negative rate (the silver negative_amount rows)
    rule(E.ExpectColumnValuesToBeBetween(column="min_utilization_rate", min_value=0),
         "utilization_rate_not_negative", "range", "flag"),
    # Report rule: rows without a PSGC code (expected: the old Maguindanao province rows)
    rule(E.ExpectColumnValuesToNotBeNull(column="psgc_code"), "psgc_matched", "null", "report"),
]

run_dq_gate(
    spark, gold_check, gold_rules,
    source_name=SOURCE_NAME, layer="gold", table_name="fact_ldrrmf",
    key_columns=KEY_COLUMNS, results_table=RESULTS_TABLE, run_id=RUN_ID,
)


# ============================================================
# 3. CROSS-CHECK: lgu_type MATCHES THE PSGC LEVEL
# ============================================================

# For matched rows, BLGF's lgu_type should agree with dim_lgu.geographic_level.
# Known differences: towns that became cities after 2018, and Pateros (see the data dictionary).
level_map = F.create_map(
    F.lit("Province"), F.lit("PROV"),
    F.lit("City"), F.lit("CITY"),
    F.lit("Municipality"), F.lit("MUN"),
)
fact_vs_dim = (
    gold_df.filter(F.col("psgc_code").isNotNull())
    .join(dim_lgu.select("psgc_code", F.col("geographic_level").alias("dim_level")), on="psgc_code", how="left")
    .withColumn("expected_level", level_map[F.col("lgu_type")])
)

run_dq_gate(
    spark, fact_vs_dim,
    [rule(E.ExpectColumnPairValuesToBeEqual(column_A="expected_level", column_B="dim_level"),
          "lgu_type_matches_psgc_level", "referential_integrity", "report")],
    source_name=SOURCE_NAME, layer="gold", table_name="fact_ldrrmf",
    key_columns=KEY_COLUMNS, results_table=RESULTS_TABLE, run_id=RUN_ID,
)


# ============================================================
# 4. CROSS-CHECK: dim_lgu COVERAGE IN THE LATEST YEAR
# ============================================================

# One row per PSGC province, city and municipality, with has_ldrrmf for the latest fiscal year.
latest_year = gold_df.agg(F.max("fiscal_year")).first()[0]
reported = (
    gold_df.filter(F.col("fiscal_year") == latest_year)
    .select("psgc_code").distinct()
    .withColumn("has_ldrrmf", F.lit(True))
)
coverage = (
    dim_lgu.filter(F.col("geographic_level").isin("PROV", "CITY", "MUN"))
    .join(reported, on="psgc_code", how="left")
    .withColumn("has_ldrrmf", F.coalesce(F.col("has_ldrrmf"), F.lit(False)))
)

print(f"Coverage check uses fiscal year {latest_year}")
run_dq_gate(
    spark, coverage,
    [rule(E.ExpectColumnValuesToBeInSet(column="has_ldrrmf", value_set=[True]),
          "dim_lgu_has_ldrrmf_latest_year", "referential_integrity", "report")],
    source_name=SOURCE_NAME, layer="gold", table_name="fact_ldrrmf",
    key_columns=["area_name", "geographic_level", "psgc_code"], results_table=RESULTS_TABLE, run_id=RUN_ID,
)