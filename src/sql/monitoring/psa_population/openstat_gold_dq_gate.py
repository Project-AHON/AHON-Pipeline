"""Independent OpenSTAT gold gate; no census-table dependency."""
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
DIM_LGU_TABLE = f"{CATALOG}.gold.dim_lgu"
dim_cities = (spark.table(DIM_LGU_TABLE)
              .filter(F.col("geographic_level").isin("CITY", "MUN"))
              .select("psgc_code").withColumn("in_dim_lgu", F.lit(True)))

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
