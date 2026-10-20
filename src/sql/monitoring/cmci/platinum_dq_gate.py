# %pip install great_expectations==1.23.1
"""
DQ gate, gold to platinum: cmci (decision 0012).

Run after the cmci_platinum task (src/sql/datasets/cmci/pipeline.py --stage
platinum). Checks platinum.cmci_pillar_coverage with Great Expectations,
writes one row per rule to monitoring.dq_result, and stops the run if a stop
rule FAILs. Report rules give WARN and do not stop it.

Since #94 platinum is cmci_pillar_coverage: one row per LGU, year and pillar
with how many of the pillar's indicators have a score (available) out of how
many it has (expected), and COMPLETE or INSUFFICIENT_DATA. There is no pillar
score any more, so the earlier cmci_pillar_score rules on scores and coverage
percent are dropped; the count and status rules are kept.

Tested on ahon_dev_crystal (2026-10-20, test mode, 5 LGUs): no FAIL;
WARN cmci_pillar_coverage_complete 84 rows (INSUFFICIENT_DATA pillars).

Later: the CMCI resiliency columns of platinum.lgu_risk_profile get their own
run_dq_gate call here once that table is built.
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
PLATINUM_TABLE = f"{CATALOG}.platinum.cmci_pillar_coverage"
DIM_TABLE = f"{CATALOG}.gold.dim_cmci_indicator"
FACT_TABLE = f"{CATALOG}.gold.fact_cmci_indicator"
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_result"
KEY_COLUMNS = ["psgc_code", "year", "pillar_name"]

PILLARS = ["Economic Dynamism", "Government Efficiency", "Infrastructure", "Resiliency", "Innovation"]
STATUSES = ["COMPLETE", "INSUFFICIENT_DATA"]

E = gx.expectations


# ============================================================
# 2. HELPER COLUMNS
# ============================================================

# dim_expected_count = indicators per pillar in dim_cmci_indicator (compared with expected_indicator_count).
# available_or_zero = available_indicator_count, 0 when the pillar has no fact rows (left join in the build).
# status_consistent = COMPLETE exactly when every indicator of the pillar has a score.
# Row count: every LGU-year in the fact has one row per pillar.
expected_per_pillar = (
    spark.table(DIM_TABLE).groupBy("pillar_name").agg(F.count("*").alias("dim_expected_count"))
)
expected_rows = (
    spark.table(FACT_TABLE).select("psgc_code", "year").distinct().count() * len(PILLARS)
)
available = F.coalesce(F.col("available_indicator_count"), F.lit(0))
platinum_check = (
    spark.table(PLATINUM_TABLE)
    .join(expected_per_pillar, on="pillar_name", how="left")
    .withColumn("available_or_zero", available)
    .withColumn("status_consistent",
                (F.col("coverage_status") == "COMPLETE") == (available == F.col("expected_indicator_count")))
)


# ============================================================
# 3. RULES
# ============================================================

platinum_rules = [
    # Stop rules
    rule(E.ExpectTableRowCountToEqual(value=expected_rows),
         "cmci_pillar_coverage_one_row_per_lgu_year_pillar_in_fact", "volume", "stop"),
] + [
    rule(E.ExpectColumnValuesToNotBeNull(column=c), f"cmci_pillar_coverage_{c}_not_null", "null", "stop")
    for c in ["psgc_code", "year", "pillar_name", "source_period", "expected_indicator_count",
              "coverage_status", "metric_version"]
] + [
    rule(E.ExpectCompoundColumnsToBeUnique(column_list=KEY_COLUMNS),
         "cmci_pillar_coverage_one_row_per_key", "unique", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="pillar_name", value_set=PILLARS),
         "cmci_pillar_coverage_pillar_name_valid", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="source_period", value_set=[PSGC_PERIOD]),
         "cmci_pillar_coverage_source_period_valid", "accepted_values", "stop"),
    rule(E.ExpectColumnPairValuesToBeEqual(column_A="expected_indicator_count", column_B="dim_expected_count"),
         "cmci_pillar_coverage_expected_count_matches_dim", "reconciliation", "stop"),
    rule(E.ExpectColumnPairValuesAToBeGreaterThanB(column_A="expected_indicator_count",
                                                   column_B="available_or_zero", or_equal=True),
         "cmci_pillar_coverage_available_not_above_expected", "range", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="coverage_status", value_set=STATUSES),
         "cmci_pillar_coverage_status_valid", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="status_consistent", value_set=[True]),
         "cmci_pillar_coverage_status_consistent", "reconciliation", "stop"),
    # Report rule: pillars with a missing indicator score (INSUFFICIENT_DATA)
    rule(E.ExpectColumnValuesToBeInSet(column="coverage_status", value_set=["COMPLETE"]),
         "cmci_pillar_coverage_complete", "accepted_values", "report"),
]

run_dq_gate(
    spark, platinum_check, platinum_rules,
    source_name=SOURCE_NAME, layer="platinum", table_name="cmci_pillar_coverage",
    key_columns=KEY_COLUMNS, results_table=RESULTS_TABLE, run_id=RUN_ID,
)