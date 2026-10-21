"""
DQ gate, silver to gold: psa_population.

Writes one row per rule to monitoring.dq_result and stops the run if a stop
rule FAILs. Report rules give WARN and do not stop it. Census chain only; OpenSTAT has its own entry point.

A. Census chain (used downstream): gold.fact_population, built by
   src/sql/gold/fact_population.py. One row per city/municipality and census
   year, keyed by (psgc_code, population_year), every CITY and MUN of dim_lgu
   included. Cities with no usable census value keep population NULL and
   coverage_status MISSING. Read by platinum.population_components.
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


# ############################################################
# PART A. CENSUS CHAIN: gold.fact_population
# ############################################################

# ============================================================
# A1. SETTINGS
# ============================================================

# Set by src/sql/common/run_python.py in the job; set them yourself in a notebook
CATALOG = globals().get("PIPELINE_CATALOG", "ahon_dev")
RUN_ID = globals().get("PIPELINE_RUN_ID", "interactive")
SOURCE_NAME = "psa_population"
GOLD_TABLE = f"{CATALOG}.gold.fact_population"
DIM_LGU_TABLE = f"{CATALOG}.gold.dim_lgu"
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_result"
KEY_COLUMNS = ["psgc_code", "area_name", "population_year"]

# Same as the source_version and census_year widgets of fact_population.py
SOURCE_PERIOD = globals().get("PIPELINE_PERIOD", "Q2_2024")
CENSUS_YEAR = int(dbutils.widgets.get("census_year"))

E = gx.expectations
gold_df = spark.table(GOLD_TABLE)


# ============================================================
# A2. HELPER COLUMNS
# ============================================================

# in_dim_lgu = psgc_code is a CITY/MUN row of dim_lgu (False when not found),
# region_matches_dim_lgu = region_code still equals dim_lgu's (NULL when not found),
# coverage_consistent = AVAILABLE exactly when population is filled.
dim_cities = (
    spark.table(DIM_LGU_TABLE)
    .filter(F.col("geographic_level").isin("CITY", "MUN"))
    .select("psgc_code", F.col("region_code").alias("dim_region_code"))
    .withColumn("in_dim_lgu", F.lit(True))
)
dim_city_count = dim_cities.count()
census_year_rows = gold_df.filter(F.col("population_year") == CENSUS_YEAR).count()

gold_check = (
    gold_df
    .join(dim_cities, on="psgc_code", how="left")
    .withColumn("in_dim_lgu", F.coalesce(F.col("in_dim_lgu"), F.lit(False)))
    .withColumn("region_matches_dim_lgu",
                F.when(F.col("in_dim_lgu"), F.col("region_code").eqNullSafe(F.col("dim_region_code"))))
    .withColumn("coverage_consistent",
                (F.col("coverage_status") == "AVAILABLE") == F.col("population").isNotNull())
)


# ============================================================
# A3. RULES
# ============================================================

gold_rules = [
    # Stop rules (the build checks these before the write; repeated after it)
    rule(E.ExpectColumnValuesToNotBeNull(column="psgc_code"), "psgc_code_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToMatchRegex(column="psgc_code", regex=r"^[0-9]{10}$"),
         "psgc_code_10_digits", "accepted_values", "stop"),
    rule(E.ExpectCompoundColumnsToBeUnique(column_list=["psgc_code", "population_year"]),
         "one_row_per_city_year", "unique", "stop"),
    rule(E.ExpectColumnValuesToBeUnique(column="population_key"), "population_key_unique", "unique", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="area_name"), "area_name_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="region_code"), "region_code_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="population_year"), "population_year_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="geographic_level", value_set=["CITY", "MUN"]),
         "city_or_municipality_only", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToBeBetween(column="population", min_value=0),
         "population_not_negative", "range", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="source_period", value_set=[SOURCE_PERIOD]),
         "source_period_matches", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="in_dim_lgu", value_set=[True]),
         "psgc_code_in_dim_lgu", "referential_integrity", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="coverage_status", value_set=["AVAILABLE", "MISSING"]),
         "coverage_status_known", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="coverage_consistent", value_set=[True]),
         "coverage_status_matches_population", "reconciliation", "stop"),
    # Report rules
    # Cities whose census value is missing or invalid keep population NULL
    rule(E.ExpectColumnValuesToNotBeNull(column="population"), "population_available", "null", "report"),
    rule(E.ExpectColumnValuesToBeInSet(column="region_matches_dim_lgu", value_set=[True]),
         "region_code_matches_dim_lgu", "referential_integrity", "report"),
]

run_dq_gate(
    spark, gold_check, gold_rules,
    source_name=SOURCE_NAME, layer="gold", table_name="fact_population",
    key_columns=KEY_COLUMNS, results_table=RESULTS_TABLE, run_id=RUN_ID,
)


# ============================================================
# A4. VOLUME: ONE ROW PER DIM_LGU CITY IN THE CENSUS YEAR
# ============================================================

# The build stops on this before the write; repeated after it because the MERGE keeps old rows
print(f"dim_lgu cities and municipalities {dim_city_count:,}; fact rows for {CENSUS_YEAR}: {census_year_rows:,}")
run_dq_gate(
    spark, gold_df.filter(F.col("population_year") == CENSUS_YEAR),
    [rule(E.ExpectTableRowCountToEqual(value=dim_city_count),
          "census_year_rows_match_dim_lgu_cities", "volume", "report")],
    source_name=SOURCE_NAME, layer="gold", table_name="fact_population",
    key_columns=KEY_COLUMNS, results_table=RESULTS_TABLE, run_id=RUN_ID,
)
