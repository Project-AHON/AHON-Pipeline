# %pip install great_expectations==1.23.1
"""
DQ gate, silver to gold: fact_population (decision 0012).

Run after src/sql/gold/fact_population.py. Checks gold.fact_population with
Great Expectations, appends one row per rule to monitoring.dq_results, and
stops the run if a stop rule FAILs. Report rules give WARN and do not stop it.
Two runs, one per DataFrame checked:

1. fact_population itself: the build's stop checks (assert_joined and
   assert_valid_fact), repeated after the write, plus a report rule that the
   parent municipality code still matches dim_lgu.
2. Coverage: every barangay in dim_lgu has a row in the latest population
   year (report). The build drops barangays with no population value.

Grain: one barangay per census year, keyed by (barangay_code, population_year).
Note: the build reads barangay population from silver.psa_psgc
(populations_json), not from silver.psa_population, so there is no row count
rule against the psa_population silver table.

Expected on ahon_dev: not run yet.

Rules to confirm on the first run:
- municipality_code_matches_dim_lgu (report): the fact copies parent codes
  from dim_lgu at build time; both tables upsert and keep old rows, so a later
  dim_lgu change shows here.
- dim_lgu_has_population_latest_year (report): expect WARN for barangays whose
  source population is "-" or NULL; the count is not known yet.
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
SOURCE_NAME = "psa_population"
GOLD_TABLE = f"{CATALOG}.gold.fact_population"
DIM_LGU_TABLE = f"{CATALOG}.gold.dim_lgu"
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_results"
KEY_COLUMNS = ["barangay_code", "barangay_name", "population_year"]
# Columns the build declares NOT NULL (every fact column except province_code)
REQUIRED_COLUMNS = [
    "population_key", "barangay_code", "barangay_name", "municipality_code",
    "region_code", "population", "population_year",
]
BARANGAY_LEVEL = "BGY"  # dim_lgu.geographic_level of a barangay (dim_lgu.py at 618475b; the reverted file on main uses "Bgy")

E = gx.expectations
gold_df = spark.table(GOLD_TABLE)
dim_lgu = spark.table(DIM_LGU_TABLE)


# ============================================================
# 2. fact_population RULES
# ============================================================

# Referential check: a left join to the barangay rows of dim_lgu adds in_dim_lgu (true/false, NULL
# when barangay_code is NULL; GX skips NULLs, and barangay_code_not_null covers those rows).
# The build joins only barangay rows, so a code at another level counts as not found.
# dim_municipality_code is the parent code dim_lgu holds now, for the report rule.
dim_barangays = (
    dim_lgu.filter(F.col("geographic_level") == BARANGAY_LEVEL)
    .select(F.col("psgc_code").alias("barangay_code"),
            F.col("municipality_code").alias("dim_municipality_code"))
    .dropDuplicates(["barangay_code"])
    .withColumn("in_dim_lgu", F.lit(True))
)
gold_check = (
    gold_df
    .join(dim_barangays, on="barangay_code", how="left")
    .withColumn("in_dim_lgu",
                F.when(F.col("barangay_code").isNotNull(), F.coalesce(F.col("in_dim_lgu"), F.lit(False))))
)

# Stop rules (same as the checks in fact_population.py, repeated after the write)
gold_rules = [
    rule(E.ExpectColumnValuesToNotBeNull(column=c), f"{c}_not_null", "null", "stop") for c in REQUIRED_COLUMNS
] + [
    rule(E.ExpectCompoundColumnsToBeUnique(column_list=["barangay_code", "population_year"]),
         "one_row_per_barangay_year", "unique", "stop"),
    # population_key is a hash of the two key columns, so it is unique when they are
    rule(E.ExpectColumnValuesToBeUnique(column="population_key"), "population_key_unique", "unique", "stop"),
    rule(E.ExpectColumnValuesToBeBetween(column="population", min_value=0),
         "population_not_negative", "range", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="in_dim_lgu", value_set=[True]),
         "barangay_code_in_dim_lgu", "referential_integrity", "stop"),
    # Report rule: parent code copied at build time still matches dim_lgu
    rule(E.ExpectColumnPairValuesToBeEqual(column_A="municipality_code", column_B="dim_municipality_code"),
         "municipality_code_matches_dim_lgu", "referential_integrity", "report"),
]

run_dq_gate(
    spark, gold_check, gold_rules,
    source_name=SOURCE_NAME, layer="gold", table_name="fact_population",
    key_columns=KEY_COLUMNS, results_table=RESULTS_TABLE,
)


# ============================================================
# 3. CROSS-CHECK: dim_lgu COVERAGE IN THE LATEST YEAR
# ============================================================

# One row per PSGC barangay, with has_population for the latest population year.
latest_year = gold_df.agg(F.max("population_year")).first()[0]
reported = (
    gold_df.filter(F.col("population_year") == latest_year)
    .select(F.col("barangay_code").alias("psgc_code")).distinct()
    .withColumn("has_population", F.lit(True))
)
coverage = (
    dim_lgu.filter(F.col("geographic_level") == BARANGAY_LEVEL)
    .join(reported, on="psgc_code", how="left")
    .withColumn("has_population", F.coalesce(F.col("has_population"), F.lit(False)))
)

print(f"Coverage check uses population year {latest_year}")
run_dq_gate(
    spark, coverage,
    [rule(E.ExpectColumnValuesToBeInSet(column="has_population", value_set=[True]),
          "dim_lgu_has_population_latest_year", "referential_integrity", "report")],
    source_name=SOURCE_NAME, layer="gold", table_name="fact_population",
    key_columns=["area_name", "geographic_level", "psgc_code"], results_table=RESULTS_TABLE,
)
