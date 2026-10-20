# %pip install great_expectations==1.23.1
"""
DQ gate, bronze to silver: psa_psgc (decision 0012).

Run after src/sql/datasets/psa_psgc/silver/psa_psgc_clean.py. Checks
silver.psa_psgc_clean with Great Expectations, appends one row per rule to
monitoring.dq_result, and stops the run if a stop rule FAILs. Report rules
give WARN and do not stop it. Ports src/sql/monitoring/psa_psgc/silver_dq.sql
(checks 01 to 15, 18). Two runs, one per DataFrame checked:

1. psa_psgc_clean itself: the build's checks (stop) and the silver_dq.sql
   checks (stop where silver_dq.sql says FAIL, report where it says WARN).
2. Rows per API period: bronze = silver + reject tables (report).

Silver keeps every API period (Q2_2024, April_2024, Q4_2023, Q2_2021) and its
key is psgc_code + geographic_level + _api_period.

Expected: likely WARN on area_name_no_broken_enye (names such as
"City of DasmariÃ±as"). Rows the build rejects land in silver.psa_psgc_reject
(invalid geographic fields) and silver.psa_psgc_code_reject (invalid codes), so
rows_per_period_match_bronze expects bronze = silver + both reject tables.

Unsure of:
- geographic_level_valid: value set from the data dictionary (SGU added to the
  silver_dq.sql list); PSA may also use other levels (e.g. DIST).
- psgc_code_unique_per_period: not guaranteed by the silver key, but dim_lgu
  stops if its period has a psgc_code twice.
- barangay_parent_found / region_found: the province_code '999' exception is
  copied from silver_dq.sql; other exceptions may show up.
- Not ported: check 16 (missing province name), too many exceptions for one rule.
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
SOURCE_NAME = "psa_psgc"
BRONZE_TABLE = f"{CATALOG}.bronze.psa_psgc"
SILVER_TABLE = f"{CATALOG}.silver.psa_psgc_clean"
REJECT_TABLES = [f"{CATALOG}.silver.psa_psgc_reject", f"{CATALOG}.silver.psa_psgc_code_reject"]
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_result"
KEY_COLUMNS = ["area_name", "psgc_code", "geographic_level", "_api_period"]

# Update when a new PSGC snapshot is loaded
EXPECTED_PERIODS = ["Q2_2024", "April_2024", "Q4_2023", "Q2_2021"]

GEOGRAPHIC_LEVELS = ["REG", "PROV", "CITY", "MUN", "SUBMUN", "BGY", "SGU"]

E = gx.expectations
silver_df = spark.table(SILVER_TABLE)


# ============================================================
# 2. HELPER COLUMNS
# ============================================================

# GX checks one or two columns at a time, so rules that span several columns get a helper column:
# expected_*_psgc = hierarchy code rebuilt from psgc_code (NULL where the build leaves it NULL),
# lineage_complete = all Bronze provenance columns populated,
# province_city_starts_with_name = display label begins with area_name,
# region_found / barangay_parent_found = parent code exists in the same period (NULL when not checked).
code = F.col("psgc_code")
level = F.col("geographic_level")

region_codes = (
    silver_df.filter(level == "REG")
    .select(F.col("psgc_code").alias("_region_code"), F.col("_api_period").alias("_region_period"))
    .distinct()
)
# Same parent list as silver_dq.sql check 18
parent_codes = (
    silver_df.filter(level.isin("CITY", "MUN"))
    .select("psgc_code", "_api_period")
    .distinct()
)
parent_cm = parent_codes.select(F.col("psgc_code").alias("_cm_code"), F.col("_api_period").alias("_cm_period"))
parent_pv = parent_codes.select(F.col("psgc_code").alias("_pv_code"), F.col("_api_period").alias("_pv_period"))


def populated(column_name):
    return F.col(column_name).isNotNull() & (F.trim(F.col(column_name).cast("string")) != "")


silver_check = (
    silver_df
    .withColumn("expected_region_psgc", F.concat(F.substring(code, 1, 2), F.lit("00000000")))
    .withColumn("expected_province_psgc",
                F.when(level != "REG", F.concat(F.substring(code, 1, 5), F.lit("00000"))))
    .withColumn("expected_citymun_psgc",
                F.when(~level.isin("REG", "PROV"), F.concat(F.substring(code, 1, 7), F.lit("000"))))
    .withColumn("lineage_complete",
                populated("_source_name") & populated("_source_ref")
                & F.col("_ingested_at").isNotNull() & populated("_batch_id"))
    .withColumn("province_city_starts_with_name",
                F.when(F.col("province_city").isNotNull(),
                       F.col("province_city").startswith(F.col("area_name"))))
    .join(region_codes,
          (F.col("region_psgc") == F.col("_region_code")) & (F.col("_api_period") == F.col("_region_period")),
          "left")
    .withColumn("region_found", F.when(level != "REG", F.col("_region_code").isNotNull()))
    .join(parent_cm,
          (F.col("citymun_psgc") == F.col("_cm_code")) & (F.col("_api_period") == F.col("_cm_period")),
          "left")
    .join(parent_pv,
          (F.col("province_psgc") == F.col("_pv_code")) & (F.col("_api_period") == F.col("_pv_period")),
          "left")
    .withColumn("barangay_parent_found",
                F.when(level.isin("BGY", "SUBMUN")
                       & (F.coalesce(F.trim(F.col("province_code")), F.lit("")) != "999"),
                       F.col("_cm_code").isNotNull() | F.col("_pv_code").isNotNull()))
)


# ============================================================
# 3. RULES
# ============================================================

silver_rules = [
    # Stop rules (the build filters, asserts or raises on these; silver_dq.sql checks 01-04 and 08-13 say FAIL)
    rule(E.ExpectColumnValuesToNotBeNull(column="psgc_code"), "psgc_code_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToMatchRegex(column="psgc_code", regex=r"^[0-9]{10}$"),
         "psgc_code_10_digits", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="geographic_level"), "geographic_level_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="_api_period"), "api_period_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="area_name"), "area_name_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToMatchRegex(column="area_name", regex=r"\S"), "area_name_not_blank", "null", "stop"),
    rule(E.ExpectCompoundColumnsToBeUnique(column_list=["psgc_code", "geographic_level", "_api_period"]),
         "unique_psgc_level_period", "unique", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="lineage_complete", value_set=[True]),
         "lineage_complete", "null", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="_row_hash"), "row_hash_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToMatchRegex(column="_row_hash", regex=r"^[0-9a-fA-F]{64}$"),
         "row_hash_is_sha256", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="_processed_at"), "processed_at_not_null", "null", "stop"),
    rule(E.ExpectColumnPairValuesToBeEqual(column_A="region_psgc", column_B="expected_region_psgc"),
         "region_psgc_matches_code", "reconciliation", "stop"),
    rule(E.ExpectColumnPairValuesToBeEqual(column_A="province_psgc", column_B="expected_province_psgc"),
         "province_psgc_matches_code", "reconciliation", "stop"),
    rule(E.ExpectColumnPairValuesToBeEqual(column_A="citymun_psgc", column_B="expected_citymun_psgc"),
         "citymun_psgc_matches_code", "reconciliation", "stop"),
    # Report rules (silver_dq.sql says WARN, or not guaranteed by the build)
    rule(E.ExpectColumnValuesToBeInSet(column="geographic_level", value_set=GEOGRAPHIC_LEVELS),
         "geographic_level_valid", "accepted_values", "report"),
    rule(E.ExpectColumnDistinctValuesToEqualSet(column="_api_period", value_set=EXPECTED_PERIODS),
         "api_periods_expected", "accepted_values", "report"),
    rule(E.ExpectCompoundColumnsToBeUnique(column_list=["psgc_code", "_api_period"]),
         "psgc_code_unique_per_period", "unique", "report"),
    rule(E.ExpectColumnValuesToNotMatchRegex(column="area_name", regex=r"\s[0-9]+/\s*$"),
         "area_name_no_footnote_marker", "accepted_values", "report"),
    rule(E.ExpectColumnValuesToNotMatchRegex(column="area_name", regex=r"^\s|\s$"),
         "area_name_trimmed", "accepted_values", "report"),
    # Broken ñ: UTF-8 read as Latin-1 gives "Ã±" (decision 0008); "Ã" alone never appears in PSGC names
    rule(E.ExpectColumnValuesToNotMatchRegex(column="area_name", regex="Ã"),
         "area_name_no_broken_enye", "accepted_values", "report"),
    rule(E.ExpectColumnValuesToNotBeNull(column="province_city"), "province_city_not_null", "null", "report"),
    rule(E.ExpectColumnValuesToBeInSet(column="province_city_starts_with_name", value_set=[True]),
         "province_city_starts_with_name", "accepted_values", "report"),
    rule(E.ExpectColumnValuesToBeInSet(column="region_found", value_set=[True]),
         "region_found", "referential_integrity", "report"),
    rule(E.ExpectColumnValuesToBeInSet(column="barangay_parent_found", value_set=[True]),
         "barangay_parent_found", "referential_integrity", "report"),
]

run_dq_gate(
    spark, silver_check, silver_rules,
    source_name=SOURCE_NAME, layer="silver", table_name="psa_psgc_clean",
    key_columns=KEY_COLUMNS, results_table=RESULTS_TABLE, run_id=RUN_ID,
)


# ============================================================
# 4. VOLUME: ROWS PER PERIOD, SILVER AGAINST BRONZE
# ============================================================

# Every bronze row ends up in silver or in a reject table, so per period
# rows_unaccounted = bronze - silver - rejects should be 0. Compared per period because
# silver keeps old periods. A missing reject table counts as 0 rejects.
def period_rows(table, name):
    return spark.table(table).groupBy("_api_period").agg(F.count("*").alias(name))


period_counts = (
    period_rows(BRONZE_TABLE, "bronze_rows")
    .join(period_rows(SILVER_TABLE, "silver_rows"), on="_api_period", how="full")
)
reject_columns = []
for i, table in enumerate(REJECT_TABLES):
    if spark.catalog.tableExists(table):
        period_counts = period_counts.join(period_rows(table, f"reject_rows_{i}"), on="_api_period", how="left")
        reject_columns.append(f"reject_rows_{i}")
period_counts = period_counts.fillna(0)
rejected = sum((F.col(c) for c in reject_columns), F.lit(0))
period_counts = period_counts.withColumn(
    "rows_unaccounted", F.col("bronze_rows") - F.col("silver_rows") - rejected
)
period_counts.show()

run_dq_gate(
    spark, period_counts,
    [rule(E.ExpectColumnValuesToBeBetween(column="rows_unaccounted", min_value=0, max_value=0),
          "rows_per_period_match_bronze", "volume", "report")],
    source_name=SOURCE_NAME, layer="silver", table_name="psa_psgc_clean",
    key_columns=["_api_period"], results_table=RESULTS_TABLE, run_id=RUN_ID,
)