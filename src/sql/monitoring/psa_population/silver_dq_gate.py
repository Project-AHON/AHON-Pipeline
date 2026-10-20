# %pip install great_expectations==1.23.1
"""
DQ gate, bronze to silver: psa_population.

Writes one row per rule to monitoring.dq_result and stops the run if a stop
rule FAILs. Report rules give WARN and do not stop it. Two parts:

A. Census chain (used downstream): silver.psgc_city_population_clean, built by
   src/sql/datasets/psa_psgc/silver/clean_city_population.py. Feeds
   gold.fact_population and platinum.population_components.
B. OpenSTAT chain (not used downstream): silver.psa_population_clean, built by
   src/sql/datasets/psa_population/silver/clean_psa_population.py. Feeds only
   gold.fact_population_total. To drop OpenSTAT, delete part B.

Part A:

psgc_city_population_clean is one census record per city/municipality, census
year and PSGC API period, taken from the populations JSON of psa_psgc_clean.
Bad records are kept with a disposition (invalid_year, invalid_population,
missing_population, different_population_psgc) instead of going to a reject
table, so the disposition rules are report rules. fact_population stops on
invalid values in its selected census year.

Part B: ports the old silver_dq_check.py rules. Rows with a missing required
value or bad arithmetic go to silver.psa_population_reject. Note that the build
also upserts those rows into psa_population_clean, so any reject makes stop
rules here FAIL and silver_plus_rejects_match_bronze WARN.
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

# ############################################################
# PART B. OPENSTAT CHAIN: silver.psa_population_clean
# Not used downstream. To drop OpenSTAT, delete from here to the end of the file.
# ############################################################

# ============================================================
# B1. SETTINGS
# ============================================================

OPENSTAT_SOURCE_NAME = "psa_population_openstat"
OPENSTAT_BRONZE_TABLE = f"{CATALOG}.bronze.psa_population"
OPENSTAT_SILVER_TABLE = f"{CATALOG}.silver.psa_population_clean"
OPENSTAT_REJECT_TABLE = f"{CATALOG}.silver.psa_population_reject"

OPENSTAT_REQUIRED_COLUMNS = [
    "geographic_location", "total_population", "urban_population",
    "percent_urban", "census_year",
]
# geo_level: 0 national, 1 region, 2 province, 3 city/municipality
REGION_LEVEL = 1
PROVINCE_LEVEL = 2
CITY_LEVEL = 3


# ============================================================
# B2. HELPER COLUMNS
# ============================================================

# GX checks one or two columns at a time, so rules that span several columns get a helper column:
# total_minus_urban (compared with rural_population), missing_geo_part / extra_geo_part (the path
# parts region, province and city_municipality against geo_level, NULL when geo_level is NULL),
# location_trimmed (an empty name after trimming).
openstat_bronze_rows = spark.table(OPENSTAT_BRONZE_TABLE).count()
# The build creates the reject table only the first time a row is rejected
openstat_reject_rows = spark.table(OPENSTAT_REJECT_TABLE).count() if spark.catalog.tableExists(OPENSTAT_REJECT_TABLE) else 0

geo_level = F.col("geo_level")
openstat_check = (
    spark.table(OPENSTAT_SILVER_TABLE)
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
# B3. RULES
# ============================================================

# Stop rules: the silver_dq_check.py checks, most also guaranteed by the build's reject split
openstat_rules = [
    rule(E.ExpectColumnValuesToNotBeNull(column=c), f"{c}_not_null", "null", "stop") for c in OPENSTAT_REQUIRED_COLUMNS
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
    rule(E.ExpectTableRowCountToEqual(value=openstat_bronze_rows - openstat_reject_rows),
         "silver_plus_rejects_match_bronze", "volume", "report"),
    # Update the value set when a new census year is loaded
    rule(E.ExpectColumnValuesToBeInSet(column="census_year", value_set=[2024]),
         "census_year_valid", "accepted_values", "report"),
]


# ============================================================
# B4. RUN THE GATE
# ============================================================

print(f"Bronze rows {openstat_bronze_rows:,}, rejects table rows {openstat_reject_rows:,}")
run_dq_gate(
    spark, openstat_check, openstat_rules,
    source_name=OPENSTAT_SOURCE_NAME, layer="silver", table_name="psa_population_clean",
    key_columns=["geographic_location", "census_year"],
    results_table=RESULTS_TABLE, run_id=RUN_ID,
)