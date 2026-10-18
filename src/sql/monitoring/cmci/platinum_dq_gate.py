# %pip install great_expectations==1.23.1
"""
DQ gate, gold to platinum: cmci (decision 0012).

Run after src/sql/platinum/cmci_pillar_score.py. Checks
platinum.cmci_pillar_score with Great Expectations, appends one row per rule
to monitoring.dq_results, and stops the run if a stop rule FAILs. Report
rules give WARN and do not stop it. Rule names start with the table name, like
the other CMCI gates.

Moved from tests/sql/monitoring/cmci_quality.py (platinum layer, 17 checks):
the four not-null checks, one_row_per_psgc_year_pillar, psgc_is_approved,
year_in_expected_domain, pillar_in_selected_scope,
expected_indicator_count_matches_pillar, indicator_counts_are_valid,
coverage_pct_is_valid, complete_status_is_consistent,
insufficient_status_is_consistent and expected_platinum_row_count (all
blocking there, stop here); score_status_in_domain (not blocking there, but
cmci_pillar_score.py raises on it, so stop here); pillar_score_not_negative
(not blocking there: report). Its table_not_empty is covered by the expected
row count.

INSUFFICIENT_DATA rows are valid when their counts, coverage and NULL
pillar_score_raw agree (decision 0009). No maximum is checked on
pillar_score_raw: it is an unnormalised sum, not the official 0-20 score.

Later: the CMCI resiliency columns of platinum.lgu_risk_profile get their own
run_dq_gate call here once that table is built.

Expected on ahon_dev: not run yet. On ahon (decision 0009 baseline,
2026-10-12) all 17 platinum checks passed, 89,870 rows.

Update EXPECTED_LGU_COUNT and LAST_YEAR when CMCI data or the approved
mapping changes.
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
SOURCE_NAME = "cmci"
PLATINUM_TABLE = f"{CATALOG}.platinum.cmci_pillar_score"
CMCI_MAP_TABLE = f"{CATALOG}.reference.cmci_lgu_map"
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_results"

# Selected indicators per pillar (cmci_pillar_score.py, decision 0009)
INDICATORS_BY_PILLAR = {
    "Economic Dynamism": 4,
    "Government Efficiency": 10,
    "Infrastructure": 10,
    "Resiliency": 10,
    "Innovation": 1,
}
SCORE_STATUSES = ["COMPLETE", "INSUFFICIENT_DATA"]

# Change with new data (same constants as cmci_pillar_score.py and cmci_quality.py)
EXPECTED_LGU_COUNT = 1634
FIRST_YEAR, LAST_YEAR = 2014, 2024
EXPECTED_ROWS = EXPECTED_LGU_COUNT * (LAST_YEAR - FIRST_YEAR + 1) * len(INDICATORS_BY_PILLAR)  # 89,870


# ============================================================
# 2. HELPER COLUMNS
# ============================================================

# *_value: NULL when the text is NULL or blank (cmci_quality.py counted both as missing).
# in_cmci_map: psgc_code is in the approved mapping.
# pillar_expected_count: selected indicators for the pillar (-1 for an unknown pillar, so it fails).
# counts_valid, coverage_valid: one true/false per row for the multi-column checks in cmci_quality.py.
# complete_consistent / insufficient_consistent: true/false for rows with that status, NULL otherwise.
approved_codes = (
    spark.table(CMCI_MAP_TABLE)
    .where((F.col("match_status") == "MATCHED") & F.col("reviewed") & F.col("is_active"))
    .select(F.trim("psgc_code").alias("psgc_code_trim"))
    .distinct()
    .withColumn("in_cmci_map", F.lit(True))
)

pillar_map = F.create_map(*[x for p, n in INDICATORS_BY_PILLAR.items() for x in (F.lit(p), F.lit(n))])
expected = F.col("indicators_expected")
available = F.col("indicators_available")
missing = F.col("indicators_missing")
coverage = F.col("coverage_pct")
score = F.col("pillar_score_raw")
status = F.col("score_status")

counts_valid = (
    (expected >= 0) & (available >= 0) & (missing >= 0)
    & (available <= expected) & (missing == expected - available)
)
# Same tolerance as cmci_quality.py: coverage_pct is rounded to 2 decimals by the build
coverage_valid = (
    coverage.between(0, 100)
    & (F.abs(coverage - F.round(F.lit(100.0) * available / expected, 2)) <= 0.01)
)
complete_ok = (available == expected) & (missing == 0) & (coverage == 100.0) & score.isNotNull()
insufficient_ok = (available < expected) & (missing > 0) & (coverage < 100.0) & score.isNull()

platinum_check = spark.table(PLATINUM_TABLE)
for c in ["psgc_code", "pillar_name", "score_status"]:
    platinum_check = platinum_check.withColumn(f"{c}_value", F.when(F.trim(F.col(c)) != "", F.col(c)))
platinum_check = (
    platinum_check
    .withColumn("psgc_code_trim", F.trim("psgc_code"))
    .join(approved_codes, on="psgc_code_trim", how="left")
    .withColumn("in_cmci_map", F.coalesce(F.col("in_cmci_map"), F.lit(False)))
    .withColumn("pillar_expected_count", F.coalesce(pillar_map[F.col("pillar_name")], F.lit(-1)))
    .withColumn("counts_valid", F.coalesce(counts_valid, F.lit(False)))
    # indicators_expected = 0 would divide by zero; such a row fails here and in the pillar count rule
    .withColumn("coverage_valid", F.when(expected > 0, F.coalesce(coverage_valid, F.lit(False)))
                .otherwise(F.lit(False)))
    .withColumn("complete_consistent",
                F.when(status == "COMPLETE", F.coalesce(complete_ok, F.lit(False))))
    .withColumn("insufficient_consistent",
                F.when(status == "INSUFFICIENT_DATA", F.coalesce(insufficient_ok, F.lit(False))))
)


# ============================================================
# 3. RULES
# ============================================================

E = gx.expectations
T = "cmci_pillar_score"
platinum_rules = [
    # Stop rules (blocking in cmci_quality.py; cmci_pillar_score.py raises on the same conditions)
    # Changes with new data: EXPECTED_ROWS
    rule(E.ExpectTableRowCountToEqual(value=EXPECTED_ROWS), f"{T}_expected_platinum_row_count", "volume", "stop"),
] + [
    rule(E.ExpectColumnValuesToNotBeNull(column=f"{c}_value"), f"{T}_{c}_not_null", "null", "stop")
    for c in ["psgc_code", "pillar_name", "score_status"]
] + [
    rule(E.ExpectColumnValuesToNotBeNull(column="year"), f"{T}_year_not_null", "null", "stop"),
    rule(E.ExpectCompoundColumnsToBeUnique(column_list=["psgc_code", "year", "pillar_name"]),
         f"{T}_one_row_per_psgc_year_pillar", "unique", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="in_cmci_map", value_set=[True]),
         f"{T}_psgc_is_approved", "referential_integrity", "stop"),
    # Changes with new data: LAST_YEAR
    rule(E.ExpectColumnValuesToBeBetween(column="year", min_value=FIRST_YEAR, max_value=LAST_YEAR),
         f"{T}_year_in_expected_domain", "range", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="pillar_name", value_set=list(INDICATORS_BY_PILLAR)),
         f"{T}_pillar_in_selected_scope", "accepted_values", "stop"),
    rule(E.ExpectColumnPairValuesToBeEqual(column_A="indicators_expected", column_B="pillar_expected_count"),
         f"{T}_expected_indicator_count_matches_pillar", "reconciliation", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="counts_valid", value_set=[True]),
         f"{T}_indicator_counts_are_valid", "reconciliation", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="coverage_valid", value_set=[True]),
         f"{T}_coverage_pct_is_valid", "reconciliation", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="complete_consistent", value_set=[True]),
         f"{T}_complete_status_is_consistent", "reconciliation", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="insufficient_consistent", value_set=[True]),
         f"{T}_insufficient_status_is_consistent", "reconciliation", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="score_status", value_set=SCORE_STATUSES),
         f"{T}_score_status_in_domain", "accepted_values", "stop"),
    # Report rule (advisory in cmci_quality.py; gold raw_score_not_negative already stops on the inputs)
    rule(E.ExpectColumnValuesToBeBetween(column="pillar_score_raw", min_value=0),
         f"{T}_pillar_score_not_negative", "range", "report"),
]


# ============================================================
# 4. RUN THE GATE
# ============================================================

run_dq_gate(
    spark, platinum_check, platinum_rules,
    source_name=SOURCE_NAME, layer="platinum", table_name=T,
    key_columns=["psgc_code", "year", "pillar_name"], results_table=RESULTS_TABLE,
)
