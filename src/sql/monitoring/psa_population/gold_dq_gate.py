# %pip install great_expectations==1.23.1
"""
DQ gate, silver to gold: psa_population.

Writes one row per rule to monitoring.dq_result and stops the run if a stop
rule FAILs. Report rules give WARN and do not stop it. Two parts:

A. Census chain (used downstream): gold.fact_population, built by
   src/sql/gold/fact_population.py. One row per city/municipality and census
   year, keyed by (psgc_code, population_year), every CITY and MUN of dim_lgu
   included. Cities with no usable census value keep population NULL and
   coverage_status MISSING. Read by platinum.population_components.
B. OpenSTAT chain (not used downstream): gold.fact_population_total, built by
   src/sql/gold/build_fact_population_total.py. One row per OpenSTAT location
   and census year; psgc_code is filled only when region, province and city
   names match exactly one PSGC city/municipality. To drop OpenSTAT, delete
   part B.
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
CENSUS_YEAR = 2024

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


# ############################################################
# PART B. OPENSTAT CHAIN: gold.fact_population_total
# Not used downstream. To drop OpenSTAT, delete from here to the end of the file.
# ############################################################

# ============================================================
# B1. SETTINGS
# ============================================================

OPENSTAT_SOURCE_NAME = "psa_population_openstat"
OPENSTAT_SILVER_TABLE = f"{CATALOG}.silver.psa_population_clean"
OPENSTAT_GOLD_TABLE = f"{CATALOG}.gold.fact_population_total"
OPENSTAT_MAPPING_STATUSES = ["exact_context", "ambiguous", "unmatched_or_aggregate"]
# geo_level 3 = city/municipality
OPENSTAT_CITY_LEVEL = 3


# ============================================================
# B2. HELPER COLUMNS
# ============================================================

# mapping_consistent = exact_context exactly when psgc_code is filled,
# openstat_in_dim_lgu = a filled psgc_code is a CITY/MUN row of dim_lgu (NULL when psgc_code is NULL),
# city_mapped = a city/municipality row got a psgc_code (NULL for other levels).
openstat_silver_rows = spark.table(OPENSTAT_SILVER_TABLE).count()
openstat_codes = dim_cities.select("psgc_code", F.col("in_dim_lgu").alias("openstat_in_dim_lgu"))

openstat_check = (
    spark.table(OPENSTAT_GOLD_TABLE)
    .join(openstat_codes, on="psgc_code", how="left")
    .withColumn("openstat_in_dim_lgu",
                F.when(F.col("psgc_code").isNotNull(), F.coalesce(F.col("openstat_in_dim_lgu"), F.lit(False))))
    .withColumn("mapping_consistent",
                (F.col("mapping_status") == "exact_context") == F.col("psgc_code").isNotNull())
    .withColumn("city_mapped",
                F.when(F.col("geo_level") == OPENSTAT_CITY_LEVEL, F.col("psgc_code").isNotNull()))
)


# ============================================================
# B3. RULES
# ============================================================

openstat_rules = [
    # Stop rules (the build checks fan-out and key uniqueness before the write)
    rule(E.ExpectColumnValuesToBeUnique(column="population_fact_key"),
         "population_fact_key_unique", "unique", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="mapping_status", value_set=OPENSTAT_MAPPING_STATUSES),
         "mapping_status_known", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="mapping_consistent", value_set=[True]),
         "psgc_code_only_for_exact_context", "reconciliation", "stop"),
    rule(E.ExpectColumnValuesToMatchRegex(column="psgc_code", regex=r"^[0-9]{10}$"),
         "psgc_code_10_digits", "accepted_values", "stop"),
    # Report rules
    rule(E.ExpectTableRowCountToEqual(value=openstat_silver_rows),
         "row_count_matches_silver", "volume", "report"),
    rule(E.ExpectColumnValuesToBeInSet(column="openstat_in_dim_lgu", value_set=[True]),
         "psgc_code_in_dim_lgu", "referential_integrity", "report"),
    # City/municipality names that match no PSGC city, or more than one
    rule(E.ExpectColumnValuesToBeInSet(column="city_mapped", value_set=[True]),
         "city_rows_mapped_to_psgc", "referential_integrity", "report"),
]

print(f"OpenSTAT silver rows {openstat_silver_rows:,}")
run_dq_gate(
    spark, openstat_check, openstat_rules,
    source_name=OPENSTAT_SOURCE_NAME, layer="gold", table_name="fact_population_total",
    key_columns=["geographic_location", "census_year"], results_table=RESULTS_TABLE, run_id=RUN_ID,
)