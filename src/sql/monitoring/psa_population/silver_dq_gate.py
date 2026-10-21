"""
DQ gate, bronze to silver: psa_population.

Writes one row per rule to monitoring.dq_result and stops the run if a stop
rule FAILs. Report rules give WARN and do not stop it. Census chain only; OpenSTAT has its own entry point.

A. Census chain (used downstream): silver.psgc_city_population_clean, built by
   src/sql/datasets/psa_psgc/silver/clean_city_population.py. Feeds
   gold.fact_population and platinum.population_components.
Part A:

psgc_city_population_clean is one census record per city/municipality, census
year and PSGC API period, taken from the populations JSON of psa_psgc_clean.
Bad records are kept with a disposition (invalid_year, invalid_population,
missing_population, different_population_psgc) instead of going to a reject
table, so the disposition rules are report rules. fact_population stops on
invalid values in its selected census year.

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
# PART A. CENSUS CHAIN: silver.psgc_city_population_clean
# ############################################################

# ============================================================
# A1. SETTINGS
# ============================================================

# Set by src/sql/common/run_python.py in the job; set them yourself in a notebook
CATALOG = globals().get("PIPELINE_CATALOG", "ahon_dev")
RUN_ID = globals().get("PIPELINE_RUN_ID", "interactive")
SOURCE_NAME = "psa_population"
PSGC_TABLE = f"{CATALOG}.silver.psa_psgc_clean"
SILVER_TABLE = f"{CATALOG}.silver.psgc_city_population_clean"
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_result"
KEY_COLUMNS = ["psgc_code", "population_year", "_api_period"]

# Same as the source_version widget of fact_population.py
SOURCE_PERIOD = globals().get("PIPELINE_PERIOD", "Q2_2024")
DISPOSITIONS = ["accepted", "missing_population", "invalid_year", "invalid_population",
                "different_population_psgc"]
# Census years PSA has run; widen when a new census is loaded
MIN_YEAR, MAX_YEAR = 1900, 2030

E = gx.expectations
silver_df = spark.table(SILVER_TABLE)


# ============================================================
# A2. HELPER COLUMNS
# ============================================================

# accepted_has_values = an accepted record has both a year and a population (NULL for other dispositions),
# lineage_complete = all provenance columns populated.
def populated(column_name):
    return F.col(column_name).isNotNull() & (F.trim(F.col(column_name).cast("string")) != "")


silver_check = (
    silver_df
    .withColumn("accepted_has_values",
                F.when(F.col("disposition") == "accepted",
                       F.col("population_year").isNotNull() & F.col("population").isNotNull()))
    .withColumn("lineage_complete",
                populated("_source_name") & populated("_source_ref")
                & F.col("_ingested_at").isNotNull() & populated("_batch_id") & populated("_row_hash"))
)

# Coverage: every city/municipality of the period has at least one census record
psgc_cities = (
    spark.table(PSGC_TABLE)
    .filter((F.col("_api_period") == SOURCE_PERIOD) & F.col("geographic_level").isin("CITY", "MUN"))
    .select("psgc_code", "area_name", "geographic_level")
)
with_records = (
    silver_df.filter(F.col("_api_period") == SOURCE_PERIOD)
    .select("psgc_code").distinct().withColumn("has_census_records", F.lit(True))
)
coverage = (
    psgc_cities.join(with_records, on="psgc_code", how="left")
    .withColumn("has_census_records", F.coalesce(F.col("has_census_records"), F.lit(False)))
)


# ============================================================
# A3. RULES
# ============================================================

silver_rules = [
    # Stop rules (the build guarantees these)
    rule(E.ExpectColumnValuesToNotBeNull(column="psgc_code"), "psgc_code_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToMatchRegex(column="psgc_code", regex=r"^[0-9]{10}$"),
         "psgc_code_10_digits", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="geographic_level", value_set=["CITY", "MUN"]),
         "city_or_municipality_only", "accepted_values", "stop"),
    rule(E.ExpectCompoundColumnsToBeUnique(column_list=KEY_COLUMNS),
         "unique_city_year_period", "unique", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="disposition"), "disposition_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="disposition", value_set=DISPOSITIONS),
         "disposition_known", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="accepted_has_values", value_set=[True]),
         "accepted_records_have_year_and_population", "null", "stop"),
    rule(E.ExpectColumnValuesToBeBetween(column="population", min_value=0),
         "population_not_negative", "range", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="lineage_complete", value_set=[True]),
         "lineage_complete", "null", "stop"),
    # Report rules (source problems the build keeps visible)
    rule(E.ExpectColumnValuesToBeInSet(column="disposition", value_set=["accepted"]),
         "records_accepted", "accepted_values", "report"),
    rule(E.ExpectColumnValuesToBeBetween(column="population_year", min_value=MIN_YEAR, max_value=MAX_YEAR),
         "population_year_plausible", "range", "report"),
]

run_dq_gate(
    spark, silver_check, silver_rules,
    source_name=SOURCE_NAME, layer="silver", table_name="psgc_city_population_clean",
    key_columns=KEY_COLUMNS, results_table=RESULTS_TABLE, run_id=RUN_ID,
)


# ============================================================
# A4. COVERAGE: EVERY CITY AND MUNICIPALITY HAS CENSUS RECORDS
# ============================================================

print(f"Coverage check uses PSGC period {SOURCE_PERIOD}")
run_dq_gate(
    spark, coverage,
    [rule(E.ExpectColumnValuesToBeInSet(column="has_census_records", value_set=[True]),
          "every_city_has_census_records", "referential_integrity", "report")],
    source_name=SOURCE_NAME, layer="silver", table_name="psgc_city_population_clean",
    key_columns=["area_name", "geographic_level", "psgc_code"], results_table=RESULTS_TABLE, run_id=RUN_ID,
)
