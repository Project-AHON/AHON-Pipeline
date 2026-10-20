# %pip install great_expectations==1.23.1
"""
DQ gate, silver to gold: dim_lgu.

Run after src/sql/gold/dim_lgu.py. Checks gold.dim_lgu with Great Expectations,
writes one row per rule to monitoring.dq_result, and stops the run if a stop
rule FAILs. Report rules give WARN and do not stop it.

dim_lgu is one row per psgc_code from one silver period (SOURCE_PERIOD, Q2_2024).
Its MERGE keeps rows that are not in the period, so the volume and "in silver"
rules are report rules. region_code, province_code and municipality_code hold
silver's 10-digit region_psgc, province_psgc and citymun_psgc.

Expected: about 43,760 rows (the Q2_2024 silver rows).
Likely WARN: area_name_no_broken_enye (names such as "City of DasmariÃ±as").

Unsure of:
- geographic_level_valid: value set from the data dictionary; PSA may also use
  other levels (e.g. DIST).
- hierarchy_codes_match_psgc and barangay_code_only_for_bgy: WARN on every row
  if dim_lgu on ahon_dev was last built by the older dim_lgu.py now on main
  (raw source codes, not 10-digit ones).
- barangay_parent_in_dim_lgu: the Special Geographic Area exception (province
  digits 999) is a guess from silver_dq.sql check 18.
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
SILVER_TABLE = f"{CATALOG}.silver.psa_psgc_clean"
GOLD_TABLE = f"{CATALOG}.gold.dim_lgu"
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_result"
KEY_COLUMNS = ["area_name", "geographic_level", "psgc_code"]

# Same as the source_period widget of dim_lgu.py; update when dim_lgu moves to a new snapshot
SOURCE_PERIOD = globals().get("PIPELINE_PERIOD", "Q2_2024")

GEOGRAPHIC_LEVELS = ["REG", "PROV", "CITY", "MUN", "SUBMUN", "BGY", "SGU"]

E = gx.expectations
gold_df = spark.table(GOLD_TABLE)


# ============================================================
# 2. HELPER COLUMNS
# ============================================================

# GX checks one or two columns at a time, so rules that span several columns get a helper column:
# hierarchy_matches = region, province and city/municipality codes rebuilt from psgc_code (as in silver),
# barangay_code_ok = barangay_code is psgc_code for BGY rows and NULL otherwise,
# in_silver_period = psgc_code is in the silver period dim_lgu was built from,
# region_found / barangay_parent_found = parent code is a dim_lgu row (NULL when not checked).
silver_rows = spark.table(SILVER_TABLE).filter(F.col("_api_period") == SOURCE_PERIOD).count()
silver_codes = (
    spark.table(SILVER_TABLE).filter(F.col("_api_period") == SOURCE_PERIOD)
    .select("psgc_code").distinct()
    .withColumn("in_silver_period", F.lit(True))
)
region_codes = (
    gold_df.filter(F.col("geographic_level") == "REG")
    .select(F.col("psgc_code").alias("_region_code")).distinct()
)
# Same parent list as silver_dq.sql check 18: cities and municipalities
parent_codes = gold_df.filter(F.col("geographic_level").isin("CITY", "MUN")).select("psgc_code").distinct()
parent_cm = parent_codes.select(F.col("psgc_code").alias("_cm_code"))
parent_pv = parent_codes.select(F.col("psgc_code").alias("_pv_code"))

code = F.col("psgc_code")
level = F.col("geographic_level")
expected_region = F.concat(F.substring(code, 1, 2), F.lit("00000000"))
expected_province = F.when(level != "REG", F.concat(F.substring(code, 1, 5), F.lit("00000")))
expected_citymun = F.when(~level.isin("REG", "PROV"), F.concat(F.substring(code, 1, 7), F.lit("000")))

gold_check = (
    gold_df
    .withColumn("hierarchy_matches",
                F.col("region_code").eqNullSafe(expected_region)
                & F.col("province_code").eqNullSafe(expected_province)
                & F.col("municipality_code").eqNullSafe(expected_citymun))
    .withColumn("barangay_code_ok",
                F.col("barangay_code").eqNullSafe(F.when(level == "BGY", code)))
    .join(silver_codes, on="psgc_code", how="left")
    .withColumn("in_silver_period", F.coalesce(F.col("in_silver_period"), F.lit(False)))
    .join(region_codes, F.col("region_code") == F.col("_region_code"), "left")
    .withColumn("region_found", F.when(level != "REG", F.col("_region_code").isNotNull()))
    .join(parent_cm, F.col("municipality_code") == F.col("_cm_code"), "left")
    .join(parent_pv, F.col("province_code") == F.col("_pv_code"), "left")
    .withColumn("barangay_parent_found",
                F.when(level.isin("BGY", "SUBMUN") & (F.substring(code, 3, 3) != "999"),
                       F.col("_cm_code").isNotNull() | F.col("_pv_code").isNotNull()))
)


# ============================================================
# 3. RULES
# ============================================================

gold_rules = [
    # Stop rules (same as the checks in dim_lgu.py before the write, repeated after it)
    rule(E.ExpectColumnValuesToNotBeNull(column="psgc_code"), "psgc_code_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToBeUnique(column="psgc_code"), "psgc_code_unique", "unique", "stop"),
    rule(E.ExpectColumnValuesToMatchRegex(column="psgc_code", regex=r"^[0-9]{10}$"),
         "psgc_code_10_digits", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="area_name"), "area_name_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToMatchRegex(column="area_name", regex=r"\S"), "area_name_not_blank", "null", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="geographic_level"), "geographic_level_not_null", "null", "stop"),
    # Report rules (not checked by the build, or not guaranteed because the MERGE keeps old rows)
    rule(E.ExpectTableRowCountToEqual(value=silver_rows), "row_count_matches_silver_period", "volume", "report"),
    rule(E.ExpectColumnValuesToBeInSet(column="in_silver_period", value_set=[True]),
         "psgc_code_in_silver_period", "referential_integrity", "report"),
    rule(E.ExpectColumnValuesToBeInSet(column="geographic_level", value_set=GEOGRAPHIC_LEVELS),
         "geographic_level_valid", "accepted_values", "report"),
    rule(E.ExpectColumnValuesToBeInSet(column="hierarchy_matches", value_set=[True]),
         "hierarchy_codes_match_psgc", "reconciliation", "report"),
    rule(E.ExpectColumnValuesToBeInSet(column="barangay_code_ok", value_set=[True]),
         "barangay_code_only_for_bgy", "reconciliation", "report"),
    rule(E.ExpectColumnValuesToBeInSet(column="region_found", value_set=[True]),
         "region_in_dim_lgu", "referential_integrity", "report"),
    rule(E.ExpectColumnValuesToBeInSet(column="barangay_parent_found", value_set=[True]),
         "barangay_parent_in_dim_lgu", "referential_integrity", "report"),
    # Broken ñ: UTF-8 read as Latin-1 gives "Ã±"; fix belongs in silver
    rule(E.ExpectColumnValuesToNotMatchRegex(column="area_name", regex="Ã"),
         "area_name_no_broken_enye", "accepted_values", "report"),
]


# ============================================================
# 4. RUN THE GATE
# ============================================================

print(f"Volume and in-silver checks use silver period {SOURCE_PERIOD}")
run_dq_gate(
    spark, gold_check, gold_rules,
    source_name=SOURCE_NAME, layer="gold", table_name="dim_lgu",
    key_columns=KEY_COLUMNS, results_table=RESULTS_TABLE, run_id=RUN_ID,
)