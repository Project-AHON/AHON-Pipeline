"""Independent OpenSTAT silver gate; no census-table dependency."""
import os
import sys

import great_expectations as gx
from pyspark.sql import functions as F

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "common")))
from dq_gate import rule, run_dq_gate

CATALOG = globals()["PIPELINE_CATALOG"]
RUN_ID = globals()["PIPELINE_RUN_ID"]
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_result"
CENSUS_YEAR = int(dbutils.widgets.get("census_year"))
E = gx.expectations

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
    rule(E.ExpectColumnValuesToBeInSet(column="census_year", value_set=[CENSUS_YEAR]),
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
