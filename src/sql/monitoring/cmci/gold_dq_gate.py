# %pip install great_expectations==1.23.1
"""
DQ gate, silver to gold: cmci (decision 0012).

Run after src/sql/gold/dim_cmci_indicator.py and src/sql/gold/fact_cmci.py.
Checks the two CMCI gold tables with Great Expectations, appends one row per
rule to monitoring.dq_results, and stops the run if a stop rule FAILs. Report
rules give WARN and do not stop it. Rule names start with the table name so
each dq_rule_id is unique. Two runs, in this order:

1. dim_cmci_indicator: the fact's indicator_resolves check depends on it.
2. fact_cmci_indicator.

Moved from tests/sql/monitoring/cmci_quality.py (gold layer, 21 checks):
- dim: indicator_code_not_null, indicator_name_not_null, pillar_name_not_null,
  indicator_code_unique, expected_indicator_count (35 rows). Its
  indicator_dimension_not_empty is covered by the 35-row count.
- fact: the not-null key checks, one_row_per_psgc_year_indicator,
  psgc_is_approved, year_in_expected_domain, indicator_resolves,
  expected_fact_row_count, raw_score_not_negative, recent_score_within_range,
  bre/bpls/lra_recent_value_domain (all blocking there, stop here), and
  raw_score_not_null (100% threshold there, so it never blocked: report here).
  Its table_not_empty is covered by the expected row count.
New here: dim pillar_name_valid (stop, the build takes pillars from
cmci_common.py) and fact psgc_code_in_dim_lgu (report).

Expected on ahon_dev: not run yet. On ahon (decision 0009 baseline,
2026-10-12): all blocking checks PASS, 1 WARN (raw_score_not_null:
132,797 of 629,090 rows have no source score, kept as NULL on purpose).
psgc_code_in_dim_lgu has not been run anywhere yet.

Update EXPECTED_LGU_COUNT, LAST_YEAR and the recent-score years when CMCI
data or the approved mapping changes.
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
DIM_TABLE = f"{CATALOG}.gold.dim_cmci_indicator"
FACT_TABLE = f"{CATALOG}.gold.fact_cmci_indicator"
CMCI_MAP_TABLE = f"{CATALOG}.reference.cmci_lgu_map"
DIM_LGU_TABLE = f"{CATALOG}.gold.dim_lgu"
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_results"

# Change with new data (same constants as fact_cmci.py and cmci_quality.py)
EXPECTED_LGU_COUNT = 1634  # approved CMCI mapping rows
FIRST_YEAR, LAST_YEAR = 2014, 2024
EXPECTED_INDICATOR_COUNT = 35  # cmci_common.py, fixed by decision 0009
EXPECTED_FACT_ROWS = EXPECTED_LGU_COUNT * (LAST_YEAR - FIRST_YEAR + 1) * EXPECTED_INDICATOR_COUNT  # 629,090

# 2022 to 2024 scoring rules (decision 0009): non-null scores at most 2.0001, and three indicators
# with fixed values. Older years use other scales (2.5, 3.3333, 4.1667), so they are not checked.
RECENT_FIRST_YEAR, RECENT_LAST_YEAR = 2022, 2024
RECENT_SCORE_MAX = 2.0001
RECENT_VALUE_DOMAINS = {
    "bre": [0.0, 1.0, 2.0],
    "bpls": [0.0, 0.5, 1.0, 1.5, 2.0],
    "lra": [0.0, 1.0, 2.0],
}

PILLARS = ["Economic Dynamism", "Government Efficiency", "Infrastructure", "Resiliency", "Innovation"]

E = gx.expectations
dim_df = spark.table(DIM_TABLE)
fact_df = spark.table(FACT_TABLE)


def blank_to_null(df, columns):
    # GX does not treat "" as missing; cmci_quality.py counted NULL or blank, so *_value is NULL for both
    for c in columns:
        df = df.withColumn(f"{c}_value", F.when(F.trim(F.col(c)) != "", F.col(c)))
    return df


# ============================================================
# 2. dim_cmci_indicator RULES
# ============================================================

DIM_TEXT_COLUMNS = ["indicator_code", "indicator_name", "pillar_name"]
dim_check = blank_to_null(dim_df, DIM_TEXT_COLUMNS)

dim_rules = [
    # Stop rules (blocking in cmci_quality.py; dim_cmci_indicator.py raises on the same conditions)
    rule(E.ExpectTableRowCountToEqual(value=EXPECTED_INDICATOR_COUNT),
         "dim_cmci_indicator_expected_indicator_count", "volume", "stop"),
] + [
    rule(E.ExpectColumnValuesToNotBeNull(column=f"{c}_value"), f"dim_cmci_indicator_{c}_not_null", "null", "stop")
    for c in DIM_TEXT_COLUMNS
] + [
    rule(E.ExpectColumnValuesToBeUnique(column="indicator_code"),
         "dim_cmci_indicator_indicator_code_unique", "unique", "stop"),
    # New: pillar names come from INDICATORS_BY_PILLAR in cmci_common.py, and cmci_pillar_score.py needs them
    rule(E.ExpectColumnValuesToBeInSet(column="pillar_name", value_set=PILLARS),
         "dim_cmci_indicator_pillar_name_valid", "accepted_values", "stop"),
]

run_dq_gate(
    spark, dim_check, dim_rules,
    source_name=SOURCE_NAME, layer="gold", table_name="dim_cmci_indicator",
    key_columns=["indicator_code", "pillar_name"], results_table=RESULTS_TABLE,
)


# ============================================================
# 3. fact_cmci_indicator HELPER COLUMNS
# ============================================================

# in_cmci_map: psgc_code is in the approved mapping (same filter as cmci_quality.py).
# in_dim_indicator: indicator_code is in dim_cmci_indicator.
# in_dim_lgu: psgc_code is in gold.dim_lgu (NULL when psgc_code is NULL; psgc_code_not_null covers those).
# recent_score: raw_score for 2022 to 2024, NULL otherwise (GX skips NULLs).
# <code>_recent_score: raw_score rounded to 4 decimals, for one indicator in 2022 to 2024.
approved_codes = (
    spark.table(CMCI_MAP_TABLE)
    .where((F.col("match_status") == "MATCHED") & F.col("reviewed") & F.col("is_active"))
    .select(F.trim("psgc_code").alias("psgc_code_trim"))
    .distinct()
    .withColumn("in_cmci_map", F.lit(True))
)
indicator_codes = dim_df.select("indicator_code").distinct().withColumn("in_dim_indicator", F.lit(True))
lgu_codes = (
    spark.table(DIM_LGU_TABLE)
    .select(F.col("psgc_code").alias("psgc_code_trim"))
    .distinct()
    .withColumn("in_dim_lgu", F.lit(True))
)

is_recent = F.col("year").between(RECENT_FIRST_YEAR, RECENT_LAST_YEAR)
fact_check = (
    blank_to_null(fact_df, ["psgc_code", "indicator_code"])
    .withColumn("psgc_code_trim", F.trim("psgc_code"))
    .join(approved_codes, on="psgc_code_trim", how="left")
    .join(indicator_codes, on="indicator_code", how="left")
    .join(lgu_codes, on="psgc_code_trim", how="left")
    .withColumn("in_cmci_map", F.coalesce(F.col("in_cmci_map"), F.lit(False)))
    .withColumn("in_dim_indicator", F.coalesce(F.col("in_dim_indicator"), F.lit(False)))
    .withColumn("in_dim_lgu",
                F.when(F.col("psgc_code").isNotNull(), F.coalesce(F.col("in_dim_lgu"), F.lit(False))))
    .withColumn("recent_score", F.when(is_recent, F.col("raw_score")))
)
for code in RECENT_VALUE_DOMAINS:
    fact_check = fact_check.withColumn(
        f"{code}_recent_score",
        F.when(is_recent & (F.col("indicator_code") == code), F.round("raw_score", 4)),
    )


# ============================================================
# 4. fact_cmci_indicator RULES
# ============================================================

fact_rules = [
    # Stop rules (blocking in cmci_quality.py; fact_cmci.py checks keys, years, grain and row count too)
    # Changes with new data: EXPECTED_FACT_ROWS
    rule(E.ExpectTableRowCountToEqual(value=EXPECTED_FACT_ROWS),
         "fact_cmci_indicator_expected_fact_row_count", "volume", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="psgc_code_value"),
         "fact_cmci_indicator_psgc_code_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="year"), "fact_cmci_indicator_year_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="indicator_code_value"),
         "fact_cmci_indicator_indicator_code_not_null", "null", "stop"),
    rule(E.ExpectCompoundColumnsToBeUnique(column_list=["psgc_code", "year", "indicator_code"]),
         "fact_cmci_indicator_one_row_per_psgc_year_indicator", "unique", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="in_cmci_map", value_set=[True]),
         "fact_cmci_indicator_psgc_is_approved", "referential_integrity", "stop"),
    # Changes with new data: LAST_YEAR
    rule(E.ExpectColumnValuesToBeBetween(column="year", min_value=FIRST_YEAR, max_value=LAST_YEAR),
         "fact_cmci_indicator_year_in_expected_domain", "range", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="in_dim_indicator", value_set=[True]),
         "fact_cmci_indicator_indicator_resolves", "referential_integrity", "stop"),
    rule(E.ExpectColumnValuesToBeBetween(column="raw_score", min_value=0),
         "fact_cmci_indicator_raw_score_not_negative", "range", "stop"),
    rule(E.ExpectColumnValuesToBeBetween(column="recent_score", max_value=RECENT_SCORE_MAX),
         "fact_cmci_indicator_recent_score_within_range", "range", "stop"),
] + [
    rule(E.ExpectColumnValuesToBeInSet(column=f"{code}_recent_score", value_set=values),
         f"fact_cmci_indicator_{code}_recent_value_domain", "accepted_values", "stop")
    for code, values in RECENT_VALUE_DOMAINS.items()
] + [
    # Report rules
    # Missing source scores stay NULL (decision 0009). 132,797 rows on ahon, 2026-10-12; changes with new data.
    rule(E.ExpectColumnValuesToNotBeNull(column="raw_score"),
         "fact_cmci_indicator_raw_score_not_null", "null", "report"),
    # New: CMCI codes come from reference.lgu_master through cmci_lgu_map, not from dim_lgu,
    # so a match is not guaranteed (code format or PSGC edition may differ).
    rule(E.ExpectColumnValuesToBeInSet(column="in_dim_lgu", value_set=[True]),
         "fact_cmci_indicator_psgc_code_in_dim_lgu", "referential_integrity", "report"),
]

run_dq_gate(
    spark, fact_check, fact_rules,
    source_name=SOURCE_NAME, layer="gold", table_name="fact_cmci_indicator",
    key_columns=["psgc_code", "year", "indicator_code"], results_table=RESULTS_TABLE,
)
