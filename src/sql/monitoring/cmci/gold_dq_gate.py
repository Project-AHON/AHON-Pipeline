# %pip install great_expectations==1.23.1
"""
DQ gate, silver to gold: cmci.

Run after the cmci_gold task (src/sql/datasets/cmci/pipeline.py --stage gold).
Checks the two CMCI gold tables with Great Expectations, writes one row per
rule to monitoring.dq_result, and stops the run if a stop rule FAILs. Report
rules give WARN and do not stop it. Rule names start with the table name so
each check_id is unique. Two runs, in this order:

1. dim_cmci_indicator: the fact's indicator_in_dim check depends on it.
2. fact_cmci_indicator.

Since #94 the fact has the silver columns (psgc_code, cmci_name, year,
indicator_code, raw_value, raw_score, source_period, lineage). A key whose
latest value was rejected keeps its row with raw_score NULL.

Kept from the earlier gate: dim row count, not-null, unique, pillar names;
fact not-null keys, one row per key, years, indicator in dim, raw_score not
negative, the 2022 to 2024 score rules, raw_score present
(report), psgc_code in dim_lgu (now stop: the build stops on it too). The
earlier fixed fact row count (1,634 LGUs) is now row_count_matches_silver,
because a test run loads only a few LGUs.

Tested on ahon_dev_crystal (2026-10-20, test mode, 5 LGUs): no FAIL.
1. all PASS. 2. WARN raw_score_present 312 rows.

Update YEARS and the recent-score rules when CMCI data changes.
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
DIM_TABLE = f"{CATALOG}.gold.dim_cmci_indicator"
FACT_TABLE = f"{CATALOG}.gold.fact_cmci_indicator"
DIM_LGU_TABLE = f"{CATALOG}.gold.dim_lgu"
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_result"
KEY_COLUMNS = ["psgc_code", "year", "indicator_code"]

EXPECTED_INDICATOR_COUNT = 35  # resources/cmci_indicators.csv
PILLARS = ["Economic Dynamism", "Government Efficiency", "Infrastructure", "Resiliency", "Innovation"]
# Update when CMCI publishes a new year
YEARS = (2014, 2024)

# 2022 to 2024 scoring rules: non-null scores at most 2.0001, and three indicators
# with fixed values. Older years use other scales (2.5, 3.3333, 4.1667), so they are not checked.
RECENT_FIRST_YEAR, RECENT_LAST_YEAR = 2022, 2024
RECENT_SCORE_MAX = 2.0001
RECENT_VALUE_DOMAINS = {
    "bre": [0.0, 1.0, 2.0],
    "bpls": [0.0, 0.5, 1.0, 1.5, 2.0],
    "lra": [0.0, 1.0, 2.0],
}

E = gx.expectations
dim_df = spark.table(DIM_TABLE)
fact_df = spark.table(FACT_TABLE)


# ============================================================
# 2. dim_cmci_indicator RULES
# ============================================================

dim_rules = [
    rule(E.ExpectTableRowCountToEqual(value=EXPECTED_INDICATOR_COUNT),
         "dim_cmci_indicator_expected_indicator_count", "volume", "stop"),
] + [
    rule(E.ExpectColumnValuesToNotBeNull(column=c), f"dim_cmci_indicator_{c}_not_null", "null", "stop")
    for c in ["indicator_code", "indicator_name", "pillar_name", "metric_version"]
] + [
    rule(E.ExpectColumnValuesToBeUnique(column="indicator_code"),
         "dim_cmci_indicator_indicator_code_unique", "unique", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="pillar_name", value_set=PILLARS),
         "dim_cmci_indicator_pillar_name_valid", "accepted_values", "stop"),
]

run_dq_gate(
    spark, dim_df, dim_rules,
    source_name=SOURCE_NAME, layer="gold", table_name="dim_cmci_indicator",
    key_columns=["indicator_code"], results_table=RESULTS_TABLE, run_id=RUN_ID,
)


# ============================================================
# 3. fact_cmci_indicator RULES
# ============================================================

# in_dim_indicator: indicator_code is in dim_cmci_indicator.
# in_dim_lgu: psgc_code is a city or municipality of dim_lgu for the PSGC period.
# recent_score: raw_score for 2022 to 2024, NULL otherwise (GX skips NULLs).
# <code>_recent_score: raw_score rounded to 4 decimals, for one indicator in 2022 to 2024.
silver_rows = spark.table(SILVER_TABLE).count()
indicator_codes = dim_df.select("indicator_code").distinct().withColumn("in_dim_indicator", F.lit(True))
lgu_codes = (
    spark.table(DIM_LGU_TABLE)
    .where((F.col("source_period") == PSGC_PERIOD) & F.col("geographic_level").isin("CITY", "MUN"))
    .select("psgc_code").distinct()
    .withColumn("in_dim_lgu", F.lit(True))
)

is_recent = F.col("year").between(RECENT_FIRST_YEAR, RECENT_LAST_YEAR)
fact_check = (
    fact_df
    .join(indicator_codes, on="indicator_code", how="left")
    .join(lgu_codes, on="psgc_code", how="left")
    .withColumn("in_dim_indicator", F.coalesce(F.col("in_dim_indicator"), F.lit(False)))
    .withColumn("in_dim_lgu", F.coalesce(F.col("in_dim_lgu"), F.lit(False)))
    .withColumn("recent_score", F.when(is_recent, F.col("raw_score")))
)
for code in RECENT_VALUE_DOMAINS:
    fact_check = fact_check.withColumn(
        f"{code}_recent_score",
        F.when(is_recent & (F.col("indicator_code") == code), F.round("raw_score", 4)),
    )

fact_rules = [
    # Stop rules
    rule(E.ExpectTableRowCountToEqual(value=silver_rows),
         "fact_cmci_indicator_row_count_matches_silver", "volume", "stop"),
] + [
    rule(E.ExpectColumnValuesToNotBeNull(column=c), f"fact_cmci_indicator_{c.lstrip('_')}_not_null", "null", "stop")
    for c in ["psgc_code", "year", "indicator_code", "source_period", "_row_hash", "_observation_id"]
] + [
    rule(E.ExpectCompoundColumnsToBeUnique(column_list=KEY_COLUMNS),
         "fact_cmci_indicator_one_row_per_lgu_year_indicator", "unique", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="in_dim_lgu", value_set=[True]),
         "fact_cmci_indicator_psgc_code_in_dim_lgu", "referential_integrity", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="source_period", value_set=[PSGC_PERIOD]),
         "fact_cmci_indicator_source_period_valid", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToBeBetween(column="year", min_value=YEARS[0], max_value=YEARS[1]),
         "fact_cmci_indicator_year_valid", "range", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="in_dim_indicator", value_set=[True]),
         "fact_cmci_indicator_indicator_in_dim", "referential_integrity", "stop"),
    rule(E.ExpectColumnValuesToBeBetween(column="raw_score", min_value=0),
         "fact_cmci_indicator_raw_score_not_negative", "range", "stop"),
    rule(E.ExpectColumnValuesToBeBetween(column="recent_score", max_value=RECENT_SCORE_MAX),
         "fact_cmci_indicator_recent_score_within_range", "range", "stop"),
] + [
    rule(E.ExpectColumnValuesToBeInSet(column=f"{code}_recent_score", value_set=values),
         f"fact_cmci_indicator_{code}_recent_value_domain", "accepted_values", "stop")
    for code, values in RECENT_VALUE_DOMAINS.items()
] + [
    # Report rule: no source score ("-" or blank, or a rejected latest value)
    rule(E.ExpectColumnValuesToNotBeNull(column="raw_score"),
         "fact_cmci_indicator_raw_score_present", "null", "report"),
]

run_dq_gate(
    spark, fact_check, fact_rules,
    source_name=SOURCE_NAME, layer="gold", table_name="fact_cmci_indicator",
    key_columns=KEY_COLUMNS, results_table=RESULTS_TABLE, run_id=RUN_ID,
)