# %pip install great_expectations==1.23.1
"""
DQ gate, bronze to silver: psa_population (decision 0012).

Run after clean_population.py. Checks silver.psa_population with Great
Expectations, appends one row per rule to monitoring.dq_results, and stops the
run if a stop rule FAILs. Report rules give WARN and do not stop it.

The rules port silver_dq_check.py (every check there counts as FAIL, so they
are stop rules here). Most are also guaranteed by the build: rows with a
missing required value, a negative rural population or a non-whole population
go to silver.psa_population_rejects, and duplicate merge keys stop the build.

Expected on ahon_dev: not run yet. From psa_population_dq_checks.md, expect
1,744 rows, 0 rejects and every rule PASS; confirm after the first run.

Rules to confirm on the first run:
- silver_plus_rejects_match_bronze (report): silver and the rejects table only
  insert and update, never delete. A row rejected once and valid later is in
  both tables, and a bronze path that changes only by footnote marker maps to
  one silver key, so the sum can drift from bronze over time.
- census_year_valid (report): only 2024 is loaded today.
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
BRONZE_TABLE = f"{CATALOG}.bronze.psa_population_raw"
SILVER_TABLE = f"{CATALOG}.silver.psa_population"
REJECT_TABLE = f"{CATALOG}.silver.psa_population_rejects"
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_results"

REQUIRED_COLUMNS = [
    "geographic_location", "total_population", "urban_population",
    "percent_urban", "census_year",
]
# geo_level: 0 national, 1 region, 2 province, 3 city/municipality
REGION_LEVEL = 1
PROVINCE_LEVEL = 2
CITY_LEVEL = 3


# ============================================================
# 2. HELPER COLUMNS
# ============================================================

# GX checks one or two columns at a time, so rules that span several columns get a helper column:
# total_minus_urban (compared with rural_population), missing_geo_part / extra_geo_part (the path
# parts region, province and city_municipality against geo_level, NULL when geo_level is NULL),
# location_trimmed (an empty name after trimming).
bronze_rows = spark.table(BRONZE_TABLE).count()
# The build creates the rejects table only the first time a row is rejected
reject_rows = spark.table(REJECT_TABLE).count() if spark.catalog.tableExists(REJECT_TABLE) else 0

geo_level = F.col("geo_level")
silver_check = (
    spark.table(SILVER_TABLE)
    .withColumn("total_minus_urban", F.col("total_population") - F.col("urban_population"))
    .withColumn("location_trimmed", F.trim(F.col("geographic_location")))
    .withColumn("missing_geo_part",
                ((geo_level >= REGION_LEVEL) & F.col("region").isNull())
                | ((geo_level >= PROVINCE_LEVEL) & F.col("province").isNull())
                | ((geo_level >= CITY_LEVEL) & F.col("city_municipality").isNull()))
    .withColumn("extra_geo_part",
                ((geo_level < REGION_LEVEL) & F.col("region").isNotNull())
                | ((geo_level < PROVINCE_LEVEL) & F.col("province").isNotNull())
                | ((geo_level < CITY_LEVEL) & F.col("city_municipality").isNotNull()))
)


# ============================================================
# 3. RULES
# ============================================================

E = gx.expectations
# Stop rules: the silver_dq_check.py checks, most also guaranteed by the build's reject split
silver_rules = [
    rule(E.ExpectColumnValuesToNotBeNull(column=c), f"{c}_not_null", "null", "stop") for c in REQUIRED_COLUMNS
] + [
    rule(E.ExpectColumnValuesToNotBeInSet(column="location_trimmed", value_set=[""]),
         "geographic_location_not_empty", "null", "stop"),
    rule(E.ExpectCompoundColumnsToBeUnique(column_list=["geographic_location", "census_year"]),
         "unique_location_year", "unique", "stop"),
    rule(E.ExpectColumnValuesToBeBetween(column="total_population", min_value=0),
         "total_population_not_negative", "range", "stop"),
    rule(E.ExpectColumnValuesToBeBetween(column="urban_population", min_value=0),
         "urban_population_not_negative", "range", "stop"),
    rule(E.ExpectColumnValuesToBeBetween(column="percent_urban", min_value=0, max_value=100),
         "percent_urban_valid", "range", "stop"),
    rule(E.ExpectColumnPairValuesAToBeGreaterThanB(column_A="total_population", column_B="urban_population",
                                                   or_equal=True),
         "urban_not_above_total_population", "range", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="rural_population"), "rural_population_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToBeBetween(column="rural_population", min_value=0),
         "rural_population_not_negative", "range", "stop"),
    rule(E.ExpectColumnPairValuesToBeEqual(column_A="rural_population", column_B="total_minus_urban"),
         "rural_population_is_total_minus_urban", "reconciliation", "stop"),
    rule(E.ExpectColumnValuesToNotMatchRegex(column="geographic_location", regex="[0-9]+/"),
         "no_footnote_marker_in_name", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="geo_level"), "geo_level_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToBeBetween(column="geo_level", min_value=0, max_value=CITY_LEVEL),
         "geo_level_valid", "range", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="missing_geo_part", value_set=[False]),
         "geo_parts_present_for_level", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="extra_geo_part", value_set=[False]),
         "no_geo_parts_below_level", "accepted_values", "stop"),
    # Report rules (not guaranteed by the build; see the docstring)
    # Every bronze row is either in silver or in the rejects table
    rule(E.ExpectTableRowCountToEqual(value=bronze_rows - reject_rows),
         "silver_plus_rejects_match_bronze", "volume", "report"),
    # Update the value set when a new census year is loaded
    rule(E.ExpectColumnValuesToBeInSet(column="census_year", value_set=[2024]),
         "census_year_valid", "accepted_values", "report"),
]


# ============================================================
# 4. RUN THE GATE
# ============================================================

print(f"Bronze rows {bronze_rows:,}, rejects table rows {reject_rows:,}")
run_dq_gate(
    spark, silver_check, silver_rules,
    source_name=SOURCE_NAME, layer="silver", table_name="psa_population",
    key_columns=["geographic_location", "census_year"],
    results_table=RESULTS_TABLE,
)
