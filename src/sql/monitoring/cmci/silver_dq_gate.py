# %pip install great_expectations==1.23.1
"""
DQ gate, bronze to silver: cmci.

Run after the cmci_silver task (src/sql/datasets/cmci/pipeline.py --stage
silver). Checks silver.cmci_indicator_clean with Great Expectations, writes one
row per rule to monitoring.dq_result, and stops the run if a stop rule FAILs.
Report rules give WARN and do not stop it. Two runs, one per DataFrame checked:

1. cmci_indicator_clean itself: one row per LGU, year and indicator.
2. Indicators per LGU-year: every LGU-year has all 35 indicators (report).

Since #94 silver is one long table (psgc_code, year, indicator_code,
raw_score) instead of five pillar tables, so the earlier per-table rules are
rewritten for it. Values that cannot be read as a non-negative number go to
silver.cmci_reject instead (has_dq_issues = true there); "-" and blank become
raw_score NULL and stay in silver. The mapping is reference.cmci_lgu_map for
the job's PSGC period (reviewed rows only, from the reviewed mapping file).

Kept from the earlier gate: not-null keys and lineage, one row per key,
psgc_code in the approved mapping, cmci_name matches the mapping, expected
years. New: indicator_code_valid, source_period_valid, no_flagged_rows,
raw_score_not_negative, raw_score_present (report), indicators per LGU-year.

Tested on ahon_dev_crystal (2026-10-20, test mode, 5 LGUs): no FAIL.
1. WARN raw_score_present 312 rows. 2. PASS.

Update YEARS when CMCI publishes a new year; INDICATOR_CODES follows
resources/cmci_indicators.csv.
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
PSGC_PERIOD = globals().get("PIPELINE_PERIOD", "Q2_2024")
SOURCE_NAME = "cmci"
SILVER_TABLE = f"{CATALOG}.silver.cmci_indicator_clean"
REJECT_TABLE = f"{CATALOG}.silver.cmci_reject"
CMCI_MAP_TABLE = f"{CATALOG}.reference.cmci_lgu_map"
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_result"
KEY_COLUMNS = ["psgc_code", "year", "indicator_code"]

# Update when CMCI publishes a new year
YEARS = (2014, 2024)
# resources/cmci_indicators.csv (35 indicators)
INDICATOR_CODES = [
    "les", "leg", "sle", "job", "cnd", "pipu", "bre", "rat", "chs", "css",
    "lgua", "bpls", "pao", "sp", "road", "dtp", "abu", "trans", "edu", "hea",
    "inv", "acc", "ict", "atm", "lup", "drrp", "add", "ews", "drrmp", "lra",
    "ei", "util", "ep", "ss", "intc",
]
NOT_NULL_COLUMNS = ["psgc_code", "cmci_name", "year", "indicator_code", "raw_value", "source_period",
                    "_source_ref", "_row_hash", "_observation_id", "_ingested_at", "_batch_id", "_processed_at"]

E = gx.expectations
silver_df = spark.table(SILVER_TABLE)


# ============================================================
# 2. HELPER COLUMNS
# ============================================================

# map_cmci_name = the mapping's cmci_name for this psgc_code (NULL when not mapped; that row fails
# psgc_in_cmci_map instead), in_cmci_map = psgc_code is in the mapping for this PSGC period.
cmci_map = (
    spark.table(CMCI_MAP_TABLE)
    .where(F.col("source_period") == PSGC_PERIOD)
    .select("psgc_code", F.col("cmci_name").alias("map_cmci_name"), F.lit(True).alias("in_cmci_map"))
)
silver_check = (
    silver_df
    .join(cmci_map, on="psgc_code", how="left")
    .withColumn("in_cmci_map", F.coalesce(F.col("in_cmci_map"), F.lit(False)))
)


# ============================================================
# 3. cmci_indicator_clean RULES
# ============================================================

silver_rules = [
    # Stop rules
    rule(E.ExpectTableRowCountToBeBetween(min_value=1), "silver_not_empty", "volume", "stop"),
] + [
    rule(E.ExpectColumnValuesToNotBeNull(column=c), f"{c.lstrip('_')}_not_null", "null", "stop")
    for c in NOT_NULL_COLUMNS
] + [
    rule(E.ExpectCompoundColumnsToBeUnique(column_list=KEY_COLUMNS), "one_row_per_lgu_year_indicator",
         "unique", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="in_cmci_map", value_set=[True]),
         "psgc_in_cmci_map", "referential_integrity", "stop"),
    rule(E.ExpectColumnPairValuesToBeEqual(column_A="cmci_name", column_B="map_cmci_name",
                                           ignore_row_if="either_value_is_missing"),
         "cmci_name_matches_map", "referential_integrity", "stop"),
    rule(E.ExpectColumnValuesToBeBetween(column="year", min_value=YEARS[0], max_value=YEARS[1]),
         "year_valid", "range", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="indicator_code", value_set=INDICATOR_CODES),
         "indicator_code_valid", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="source_period", value_set=[PSGC_PERIOD]),
         "source_period_valid", "accepted_values", "stop"),
    # Flagged values go to silver.cmci_reject, never to the clean table
    rule(E.ExpectColumnValuesToBeInSet(column="has_dq_issues", value_set=[False]),
         "no_flagged_rows", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToBeBetween(column="raw_score", min_value=0),
         "raw_score_not_negative", "range", "stop"),
    # Report rule: "-" or blank in the source, kept as NULL on purpose
    rule(E.ExpectColumnValuesToNotBeNull(column="raw_score"), "raw_score_present", "null", "report"),
]

run_dq_gate(
    spark, silver_check, silver_rules,
    source_name=SOURCE_NAME, layer="silver", table_name="cmci_indicator_clean",
    key_columns=KEY_COLUMNS, results_table=RESULTS_TABLE, run_id=RUN_ID,
)


# ============================================================
# 4. INDICATORS PER LGU-YEAR AND REJECTS
# ============================================================

# One row per LGU-year: indicator_rows (should be 35) and reject_rows (rows of that LGU-year in
# silver.cmci_reject). Both report: a rejected value leaves its LGU-year one indicator short.
if spark.catalog.tableExists(REJECT_TABLE):
    reject_counts = spark.table(REJECT_TABLE).groupBy("psgc_code", "year").agg(F.count("*").alias("reject_rows"))
else:
    reject_counts = spark.createDataFrame([], "psgc_code string, year int, reject_rows long")
per_lgu_year = (
    silver_df.groupBy("psgc_code", "year").agg(F.count("*").alias("indicator_rows"))
    .join(reject_counts, ["psgc_code", "year"], "full")
    .fillna(0, subset=["indicator_rows", "reject_rows"])
)
print(f"LGU-years: {per_lgu_year.count():,}, LGUs: {per_lgu_year.select('psgc_code').distinct().count():,}")

run_dq_gate(
    spark, per_lgu_year,
    [rule(E.ExpectColumnValuesToBeInSet(column="indicator_rows", value_set=[len(INDICATOR_CODES)]),
          "all_indicators_per_lgu_year", "volume", "report"),
     rule(E.ExpectColumnValuesToBeInSet(column="reject_rows", value_set=[0]),
          "no_rejected_rows", "volume", "report")],
    source_name=SOURCE_NAME, layer="silver", table_name="cmci_indicator_clean",
    key_columns=["psgc_code", "year"], results_table=RESULTS_TABLE, run_id=RUN_ID,
)