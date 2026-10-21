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
    # Report rule (not guaranteed by the build)
    # Update the value set when a new census year is loaded
    rule(E.ExpectColumnValuesToBeInSet(column="census_year", value_set=[CENSUS_YEAR]),
         "census_year_valid", "accepted_values", "report"),
]


# ============================================================
# B4. DELIVERY ACCOUNTING
# ============================================================

# Bronze keeps every delivery, while the build reads one delivery (bronze _batch_id = the job's
# batch_id) and merges it into silver by geographic_location and census_year, so a valid second
# delivery adds bronze rows but no silver rows. This compares the same scope and grain: every
# bronze observation in this delivery is in silver or in the reject table, matched on
# _observation_id. The writer can put a rejected row in both tables, so nothing is subtracted.
# In a notebook, where RUN_ID is not a bronze batch, the latest delivery is checked.
def delivery_accounting(bronze_df, silver_df, reject_df, batch_id):
    from pyspark.sql import functions as F

    if not bronze_df.where(F.col("_batch_id") == batch_id).limit(1).count():
        batch_id = bronze_df.orderBy(F.col("_ingested_at").desc()).select("_batch_id").first()[0]
    delivery = bronze_df.where(F.col("_batch_id") == batch_id).select("_observation_id").distinct()
    landed = silver_df.select("_observation_id").union(reject_df.select("_observation_id")).distinct()
    unaccounted = delivery.join(landed, "_observation_id", "left_anti")
    return (
        delivery.agg(F.count("*").alias("delivery_rows"))
        .crossJoin(unaccounted.agg(F.count("*").alias("rows_unaccounted")))
        .withColumn("batch_id", F.lit(batch_id))
    )


# The build creates the reject table only the first time a row is rejected
if spark.catalog.tableExists(OPENSTAT_REJECT_TABLE):
    openstat_reject_df = spark.table(OPENSTAT_REJECT_TABLE)
else:
    openstat_reject_df = spark.createDataFrame([], "_observation_id string")
openstat_accounting = delivery_accounting(
    spark.table(OPENSTAT_BRONZE_TABLE), spark.table(OPENSTAT_SILVER_TABLE), openstat_reject_df, RUN_ID)
openstat_accounting.show()


# ============================================================
# B5. RUN THE GATE
# ============================================================

run_dq_gate(
    spark, openstat_check, openstat_rules,
    source_name=OPENSTAT_SOURCE_NAME, layer="silver", table_name="psa_population_clean",
    key_columns=["geographic_location", "census_year"],
    results_table=RESULTS_TABLE, run_id=RUN_ID,
)
run_dq_gate(
    spark, openstat_accounting,
    [rule(E.ExpectColumnValuesToBeInSet(column="rows_unaccounted", value_set=[0]),
          "delivery_rows_in_silver_or_rejects", "volume", "report")],
    source_name=OPENSTAT_SOURCE_NAME, layer="silver", table_name="psa_population_clean",
    key_columns=["batch_id"], results_table=RESULTS_TABLE, run_id=RUN_ID,
)
