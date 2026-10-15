"""
AHON PSA PSGC Bronze-to-Silver transformation.

Source: <catalog>.bronze.psa_psgc
Target: <catalog>.silver.psa_psgc_clean

Key:
    psgc_code + geographic_level + _api_period

Main behavior:
    - Keeps Bronze unchanged.
    - Standardizes 8-, 9-, and 10-digit numeric PSGC codes to 10 digits in Silver.
    - Cleans geographic names and trailing PSA footnote markers.
    - Deduplicates deterministically within each API snapshot.
    - Derives hierarchy codes from standardized PSGC codes.
    - Builds province_name and province_city using code-based joins.
    - Handles City of Isabela and Basilan.
    - Uses an idempotent Delta MERGE.
    - Preserves history unless DELETE_MISSING is enabled.
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from quality import check, upsert

# ============================================================
# 1. CONFIGURATION
# ============================================================

dbutils.widgets.text("catalog", "")
CATALOG = dbutils.widgets.get("catalog")

BRONZE_TABLE = f"{CATALOG}.bronze.psa_psgc"
SILVER_SCHEMA = f"{CATALOG}.silver"
SILVER_TABLE = f"{SILVER_SCHEMA}.psa_psgc_clean"

# Keep False to preserve historical records.
DELETE_MISSING = False

# Standardize codes only in Silver.
PAD_CODES_TO_10 = True

ISABELA_NAME = "city of isabela"
BASILAN_NAME = "basilan"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")

bronze = spark.table(BRONZE_TABLE)
run_id = globals().get("PIPELINE_RUN_ID")
if run_id:
    # Each completed PSA delivery contains the validated four whole API snapshots.
    # Prior invalid observations stay in Bronze/rejects and cannot permanently poison a correction.
    bronze = bronze.where(F.col("_batch_id") == run_id)
    check(spark, CATALOG, run_id, "selected_delivery_nonempty", BRONZE_TABLE,
          int(not bronze.limit(1).count()))


# ============================================================
# 2. VALIDATE BRONZE SCHEMA AND PSGC CODES
# ============================================================

required_columns = [
    "psgc_code",
    "area_name",
    "geographic_level",
    "region_code",
    "province_code",
    "municipality_code",
    "barangay_code",
    "city_class",
    "_api_period",
    "_source_name",
    "_source_ref",
    "_ingested_at",
    "_batch_id",
    "_row_hash",
]

missing_columns = [
    column for column in required_columns
    if column not in bronze.columns
]

if missing_columns:
    raise ValueError(
        f"Missing required Bronze columns: {missing_columns}"
    )

# Validate the original code BEFORE padding.
raw_code = F.trim(F.col("psgc_code").cast("string"))

code_length = F.length(raw_code)

invalid_codes = bronze.filter(
    raw_code.isNull()
    | (raw_code == "")
    | (~raw_code.rlike(r"^[0-9]+$"))
    | (~code_length.isin(8, 9, 10))
)

if invalid_codes.limit(1).count() > 0:
    raw_reject = invalid_codes.withColumn("disposition", F.lit("invalid_psgc_code"))
    reject_keys = ["_observation_id"] if "_observation_id" in raw_reject.columns else ["_api_period", "_row_hash"]
    upsert(spark, raw_reject.dropDuplicates(reject_keys), f"{CATALOG}.silver.psa_psgc_code_reject", reject_keys)
    check(spark, CATALOG, globals().get("PIPELINE_RUN_ID", "interactive"), "invalid_psgc_code", "bronze.psa_psgc", invalid_codes.count())
    print("Invalid PSGC codes found in Bronze:")
    invalid_codes.select(
        "psgc_code",
        "area_name",
        "geographic_level",
        "_api_period",
    ).show(20, truncate=False)

    raise ValueError(
        "Silver transformation stopped. Expected numeric PSGC "
        "codes of length 8, 9, or 10 before padding."
    )

print("Bronze PSGC code validation passed.")
check(spark, CATALOG, run_id or "interactive", "invalid_psgc_code", BRONZE_TABLE, 0)

print("Original PSGC code lengths:")
(
    bronze
    .select(
        F.length(
            F.trim(F.col("psgc_code").cast("string"))
        ).alias("code_length")
    )
    .groupBy("code_length")
    .count()
    .orderBy("code_length")
    .show()
)


# ============================================================
# 3. HELPER FUNCTIONS
# ============================================================

def clean_string(column_name):
    """Trim strings and convert empty strings to NULL."""
    return F.expr(
        f"NULLIF(TRIM(CAST(`{column_name}` AS STRING)), '')"
    )


def optional_string(column_name):
    """Return an optional source column or a typed NULL."""
    if column_name in bronze.columns:
        return clean_string(column_name).alias(column_name)

    return F.lit(None).cast("string").alias(column_name)


def assert_unique(df, keys, name):
    """Fail if a lookup or business key contains duplicates."""
    duplicates = (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
    )

    check(spark, CATALOG, run_id or "interactive", "unique_" + "_".join(keys),
          name, duplicates.count())

    return df


# ============================================================
# 4. CLEAN BRONZE RECORDS IN SILVER
# ============================================================

# IMPORTANT:
# lpad operates on a string and does not modify Bronze.
psgc_code_expr = (
    F.lpad(raw_code, 10, "0")
    if PAD_CODES_TO_10
    else raw_code
)

# Replace non-breaking spaces and remove trailing PSA footnotes
# such as "Name 1/", "Name 2/", or "Name 1/ 2/".
source_name = F.regexp_replace(
    F.col("area_name").cast("string"),
    "\u00a0",
    " ",
)

area_name_expr = F.trim(
    F.regexp_replace(
        F.trim(source_name),
        r"(\s+\d+/)+\s*$",
        "",
    )
)

cleaned = bronze.select(
    psgc_code_expr.alias("psgc_code"),

    area_name_expr.alias("area_name"),

    F.upper(
        clean_string("geographic_level")
    ).alias("geographic_level"),

    clean_string("region_code").alias("region_code"),
    clean_string("province_code").alias("province_code"),
    clean_string("municipality_code").alias("municipality_code"),
    clean_string("barangay_code").alias("barangay_code"),

    optional_string("correspondence_code"),
    optional_string("old_name"),
    optional_string("status"),
    optional_string("income_classification"),
    optional_string("city_class"),
    optional_string("urban_rural"),
    optional_string("island_region"),

    (
        F.col("populations_json").cast("string").alias("populations_json")
        if "populations_json" in bronze.columns
        else F.lit(None).cast("string").alias("populations_json")
    ),

    (
        clean_string("version").alias("source_version")
        if "version" in bronze.columns
        else F.lit(None).cast("string").alias("source_version")
    ),

    clean_string("_api_period").alias("_api_period"),

    F.col("_source_name").cast("string").alias("_source_name"),
    F.col("_source_ref").cast("string").alias("_source_ref"),
    F.col("_ingested_at"),
    F.col("_batch_id").cast("string").alias("_batch_id"),
    F.col("_row_hash").cast("string").alias("_row_hash"),
)

# Validate the standardized codes.
invalid_standardized = cleaned.filter(
    F.col("psgc_code").isNull()
    | (~F.col("psgc_code").rlike(r"^[0-9]{10}$"))
)

check(spark, CATALOG, run_id or "interactive", "standardized_psgc_codes", SILVER_TABLE,
      invalid_standardized.count())

print("PSGC codes standardized to 10 digits in Silver.")


# ============================================================
# 5. FILTER, DEDUPLICATE, AND DERIVE HIERARCHY CODES
# ============================================================

# Keep records that have the fields needed for the Silver key
# and geographic interpretation.
valid_condition = (
    F.col("psgc_code").isNotNull()
    & F.col("area_name").isNotNull()
    & (F.col("area_name") != "")
    & F.col("geographic_level").isin("REG", "PROV", "CITY", "MUN", "BGY", "SGU", "SUBMUN")
    & F.col("_api_period").isNotNull()
)

rejected = cleaned.filter(~F.coalesce(valid_condition, F.lit(False))).withColumn("disposition", F.lit("invalid_geographic_fields"))

check(spark, CATALOG, globals().get('PIPELINE_RUN_ID', 'interactive'), 'invalid_geographic_fields', 'silver.psa_psgc_reject', rejected.count(), 'WARN')
upsert(spark, rejected, f"{CATALOG}.silver.psa_psgc_reject", ["_api_period", "_row_hash"])
valid = cleaned.filter(valid_condition)

# A conflicting duplicate in one ingestion has no justified winner.
conflicts = valid.groupBy("psgc_code", "geographic_level", "_api_period", "_ingested_at", "_batch_id").agg(F.countDistinct("_row_hash").alias("versions")).where("versions > 1")
check(spark, CATALOG, globals().get("PIPELINE_RUN_ID", "interactive"), "conflicting_snapshot_observations", BRONZE_TABLE, conflicts.count())


# Deterministically select one record per code, level, and snapshot.
dedup_window = (
    Window
    .partitionBy(
        "psgc_code",
        "geographic_level",
        "_api_period",
    )
    .orderBy(
        F.col("_ingested_at").desc_nulls_last(),
        F.col("_batch_id").desc_nulls_last(),
        F.col("_row_hash").desc_nulls_last(),
    )
)

deduped = (
    valid
    .withColumn(
        "_row_number",
        F.row_number().over(dedup_window),
    )
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
)
check(spark, CATALOG, globals().get("PIPELINE_RUN_ID", "interactive"), "superseded_snapshot_observations", BRONZE_TABLE, valid.count() - deduped.count(), "WARN")

# Check business-key uniqueness AFTER padding and deduplication.
assert_unique(
    deduped,
    ["psgc_code", "geographic_level", "_api_period"],
    "deduped Silver business keys",
)

# Derive hierarchy codes from the standardized 10-digit PSGC.
code = F.col("psgc_code")
level = F.col("geographic_level")

deduped = (
    deduped

    # RR00000000: region
    .withColumn(
        "region_psgc",
        F.concat(
            F.substring(code, 1, 2),
            F.lit("00000000"),
        ),
    )

    # RRPPP00000: province
    .withColumn(
        "province_psgc",
        F.when(
            level != "REG",
            F.concat(
                F.substring(code, 1, 5),
                F.lit("00000"),
            ),
        ),
    )

    # RRPPPMM000: city/municipality
    .withColumn(
        "citymun_psgc",
        F.when(
            ~level.isin("REG", "PROV"),
            F.concat(
                F.substring(code, 1, 7),
                F.lit("000"),
            ),
        ),
    )
)


# ============================================================
# 6. BUILD LOOKUP TABLES
# ============================================================

# Province lookup, within the same API snapshot.
province_lookup = (
    deduped
    .filter(F.col("geographic_level") == "PROV")
    .select(
        F.col("psgc_code").alias("_province_psgc"),
        F.col("_api_period").alias("_province_period"),
        F.col("area_name").alias("_province_name"),
    )
)

# Parent lookup includes cities and municipalities.
parent_lookup = assert_unique(
    (
        deduped
        .filter(F.col("geographic_level").isin("CITY", "MUN", "SGU", "SUBMUN"))
        .select(
            F.col("psgc_code").alias("_parent_psgc"),
            F.col("_api_period").alias("_parent_period"),
            F.col("area_name").alias("_parent_name"),
        )
    ),
    ["_parent_psgc", "_parent_period"],
    "parent_lookup",
)

# Identify independent cities using city_class.
city_class_upper = F.upper(
    F.coalesce(F.col("city_class"), F.lit(""))
)

independent_city_lookup = (
    deduped
    .filter(F.col("geographic_level") == "CITY")
    .filter(
        city_class_upper.rlike(r"\b(HUC|ICC)\b")
        | city_class_upper.contains("HIGHLY URBANIZED")
        | city_class_upper.contains("INDEPENDENT COMPONENT")
    )
    .select(
        F.col("psgc_code").alias("_city_psgc"),
        F.col("_api_period").alias("_city_period"),
        F.col("area_name").alias("_independent_city_name"),
    )
)

# Normalize names for the City of Isabela and Basilan exception.
normalized_name = F.regexp_replace(
    F.lower(F.trim(F.col("area_name"))),
    r"\s*\(capital\)\s*$",
    "",
)

isabela_lookup = assert_unique(
    (
        deduped
        .filter(F.col("geographic_level") == "CITY")
        .filter(normalized_name == ISABELA_NAME)
        .select(
            F.col("psgc_code").alias("_isabela_psgc"),
            F.col("_api_period").alias("_isabela_period"),
        )
    ),
    ["_isabela_period"],
    "isabela_lookup",
)

basilan_lookup = assert_unique(
    (
        deduped
        .filter(F.col("geographic_level") == "PROV")
        .filter(normalized_name == BASILAN_NAME)
        .select(
            F.col("_api_period").alias("_basilan_period"),
            F.col("area_name").alias("_basilan_name"),
        )
    ),
    ["_basilan_period"],
    "basilan_lookup",
)


# ============================================================
# 7. JOIN LOOKUPS USING STANDARDIZED PSGC CODES
# ============================================================

src = deduped.alias("src")
prov = province_lookup.alias("prov")
parent_cm = parent_lookup.alias("parent_cm")
parent_pv = parent_lookup.alias("parent_pv")
city_cm = independent_city_lookup.alias("city_cm")
city_pv = independent_city_lookup.alias("city_pv")
isabela = isabela_lookup.alias("isabela")
basilan = basilan_lookup.alias("basilan")

joined = (
    src

    # Match the province by the 5-digit province prefix.
    .join(
        prov,
        (
            F.col("src.province_psgc")
            == F.col("prov._province_psgc")
        )
        & (
            F.col("src._api_period")
            == F.col("prov._province_period")
        ),
        "left",
    )

    # First try the 7-digit city/municipality prefix.
    .join(
        parent_cm,
        (
            F.col("src.citymun_psgc")
            == F.col("parent_cm._parent_psgc")
        )
        & (
            F.col("src._api_period")
            == F.col("parent_cm._parent_period")
        ),
        "left",
    )

    # Fallback to the province prefix for sub-municipality cases.
    .join(
        parent_pv,
        (
            F.col("src.province_psgc")
            == F.col("parent_pv._parent_psgc")
        )
        & (
            F.col("src._api_period")
            == F.col("parent_pv._parent_period")
        ),
        "left",
    )

    # Independent city lookup by city/municipality prefix.
    .join(
        city_cm,
        (
            F.col("src.citymun_psgc")
            == F.col("city_cm._city_psgc")
        )
        & (
            F.col("src._api_period")
            == F.col("city_cm._city_period")
        ),
        "left",
    )

    # Independent city fallback by province prefix.
    .join(
        city_pv,
        (
            F.col("src.province_psgc")
            == F.col("city_pv._city_psgc")
        )
        & (
            F.col("src._api_period")
            == F.col("city_pv._city_period")
        ),
        "left",
    )

    # City of Isabela special case.
    .join(
        isabela,
        (
            F.col("src.citymun_psgc")
            == F.col("isabela._isabela_psgc")
        )
        & (
            F.col("src._api_period")
            == F.col("isabela._isabela_period")
        ),
        "left",
    )

    # Basilan name for the City of Isabela exception.
    .join(
        basilan,
        F.col("src._api_period")
        == F.col("basilan._basilan_period"),
        "left",
    )
)


# ============================================================
# 8. DERIVE province_name AND province_city
# ============================================================

src_level = F.col("src.geographic_level")

independent_city_name = F.coalesce(
    F.col("city_cm._independent_city_name"),
    F.col("city_pv._independent_city_name"),
)

parent_name = F.coalesce(
    F.col("parent_cm._parent_name"),
    F.col("parent_pv._parent_name"),
)

# Regions have no province. Province records carry their own name.
# City of Isabela uses Basilan when the snapshot contains Basilan.
# No province is invented for Pateros or the Special Geographic Area.
province_name_expr = (
    F.when(
        src_level == "REG",
        F.lit(None).cast("string"),
    )
    .when(
        src_level == "PROV",
        F.col("src.area_name"),
    )
    .when(
        F.col("isabela._isabela_psgc").isNotNull()
        & F.col("basilan._basilan_name").isNotNull(),
        F.col("basilan._basilan_name"),
    )
    .otherwise(F.col("prov._province_name"))
)


# Display field only. Analytical joins should use PSGC codes.
# Barangays: barangay, parent place, independent city (only when it is a
# different place from the parent), province. Empty parts are skipped.
province_city_expr = (
    F.when(
        src_level.isin("REG", "PROV"),
        F.col("src.area_name"),
    )
    .when(
        src_level == "BGY",
        F.concat_ws(
            ", ",
            F.col("src.area_name"),
            parent_name,
            F.when(
                independent_city_name.isNotNull()
                & ~independent_city_name.eqNullSafe(parent_name),
                independent_city_name,
            ),
            province_name_expr,
        ),
    )
    .when(
        independent_city_name.isNotNull()
        & src_level.isin("BGY", "SGU", "SUBMUN"),
        F.concat_ws(
            ", ",
            F.col("src.area_name"),
            independent_city_name,
        ),
    )
    .when(
        province_name_expr.isNotNull(),
        F.concat_ws(
            ", ",
            F.col("src.area_name"),
            province_name_expr,
        ),
    )
    .when(
        parent_name.isNotNull()
        & src_level.isin("BGY", "SGU", "SUBMUN"),
        F.concat_ws(
            ", ",
            F.col("src.area_name"),
            parent_name,
        ),
    )
    .otherwise(F.col("src.area_name"))
)

silver = joined.select(
    F.col("src.psgc_code").alias("psgc_code"),
    F.col("src.area_name").alias("area_name"),
    F.col("src.geographic_level").alias("geographic_level"),

    F.col("src.region_psgc").alias("region_psgc"),
    F.col("src.province_psgc").alias("province_psgc"),
    F.col("src.citymun_psgc").alias("citymun_psgc"),

    F.col("src.region_code").alias("region_code"),
    F.col("src.province_code").alias("province_code"),
    F.col("src.municipality_code").alias("municipality_code"),
    F.col("src.barangay_code").alias("barangay_code"),

    province_name_expr.alias("province_name"),
    province_city_expr.alias("province_city"),

    F.col("src.correspondence_code").alias("correspondence_code"),
    F.col("src.old_name").alias("old_name"),
    F.col("src.status").alias("status"),
    F.col("src.income_classification").alias("income_classification"),
    F.col("src.city_class").alias("city_class"),
    F.col("src.urban_rural").alias("urban_rural"),
    F.col("src.island_region").alias("island_region"),
    F.col("src.populations_json").alias("populations_json"),
    F.col("src.source_version").alias("source_version"),

    F.col("src._api_period").alias("_api_period"),
    F.col("src._source_name").alias("_source_name"),
    F.col("src._source_ref").alias("_source_ref"),
    F.col("src._ingested_at").alias("_ingested_at"),
    F.col("src._batch_id").alias("_batch_id"),
    F.col("src._row_hash").alias("_row_hash"),
)


# ============================================================
# 9. PROCESSED TIMESTAMP
# ============================================================

# Record when this row was processed into Silver.
# _row_hash is carried forward from Bronze unchanged.
silver = silver.withColumn("_processed_at", F.current_timestamp())

assert_unique(
    silver,
    ["psgc_code", "geographic_level", "_api_period"],
    "final Silver business keys",
)

# Final validation before writing.
bad_silver_codes = silver.filter(
    ~F.col("psgc_code").rlike(r"^[0-9]{10}$")
)

check(spark, CATALOG, run_id or "interactive", "typed_psgc_codes", SILVER_TABLE,
      bad_silver_codes.count())

print(f"Silver records prepared: {silver.count():,}")

print("Silver PSGC code lengths:")
(
    silver
    .select(
        F.length("psgc_code").alias("code_length")
    )
    .groupBy("code_length")
    .count()
    .orderBy("code_length")
    .show()
)


# ============================================================
# 10. CREATE SILVER TABLE OR MERGE
# ============================================================

if not spark.catalog.tableExists(SILVER_TABLE):

    (
        silver.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(SILVER_TABLE)
    )

    print(f"Created Silver table: {SILVER_TABLE}")

else:
    # Add newly introduced columns explicitly, without relying on
    # automatic schema evolution.
    existing_df = spark.table(SILVER_TABLE)
    existing_columns = {
        field.name for field in existing_df.schema.fields
    }

    new_fields = [
        field
        for field in silver.schema.fields
        if field.name not in existing_columns
    ]

    if new_fields or {f.name: f.dataType for f in existing_df.schema} != {f.name: f.dataType for f in silver.schema}:
        raise ValueError("Existing PSGC Silver schema differs; reviewed migration required")

    # Check that old Silver records will not create ambiguous
    # matches after their codes are normalized to 10 digits.
    target_df = spark.table(SILVER_TABLE)

    normalized_target = target_df.select(
        F.lpad(
            F.trim(F.col("psgc_code").cast("string")),
            10,
            "0",
        ).alias("_normalized_psgc_code"),
        "geographic_level",
        "_api_period",
    )

    target_duplicates = (
        normalized_target
        .groupBy(
            "_normalized_psgc_code",
            "geographic_level",
            "_api_period",
        )
        .count()
        .filter(F.col("count") > 1)
    )

    check(spark, CATALOG, run_id or "interactive", "normalized_target_keys", SILVER_TABLE,
          target_duplicates.count())

    target = DeltaTable.forName(spark, SILVER_TABLE)

    # Normalize the existing target code in the merge condition.
    # This allows older 8- or 9-digit Silver keys to match the
    # standardized 10-digit source keys and be updated in place.
    merge_condition = """
        LPAD(TRIM(CAST(t.psgc_code AS STRING)), 10, '0') = s.psgc_code
        AND t.geographic_level = s.geographic_level
        AND t._api_period = s._api_period
    """

    # Update only rows whose Bronze hash changed. The null-safe
    # comparison also handles rows where either hash is NULL.
    change_condition = (
        " OR ".join(f"NOT(t.`{c}` <=> s.`{c}`)" for c in silver.columns if c not in {"_processed_at", "_ingested_at", "_batch_id"})
    )

    merge = (
        target.alias("t")
        .merge(
            silver.alias("s"),
            merge_condition,
        )
        .whenMatchedUpdateAll(
            condition=change_condition,
        )
        .whenNotMatchedInsertAll()
    )

    if DELETE_MISSING:
        raise ValueError("Absence-based deletion is disabled without a verified complete-snapshot contract")
        periods = [
            row["_api_period"]
            for row in silver.select("_api_period").distinct().collect()
        ]

        if periods:
            in_list = ", ".join(
                "'" + period.replace("'", "''") + "'"
                for period in periods
            )

            merge = merge.whenNotMatchedBySourceDelete(
                condition=f"t._api_period IN ({in_list})"
            )

    merge.execute()

    print(f"Merged into existing Silver table: {SILVER_TABLE}")


# ============================================================
# 11. POST-MERGE VALIDATION
# ============================================================

final_silver = spark.table(SILVER_TABLE)

final_count = final_silver.count()

print(f"Final Silver table: {SILVER_TABLE}")
print(f"Total Silver records: {final_count:,}")

print("Final PSGC code lengths:")
(
    final_silver
    .select(
        F.length(
            F.trim(F.col("psgc_code").cast("string"))
        ).alias("code_length")
    )
    .groupBy("code_length")
    .count()
    .orderBy("code_length")
    .show()
)

# Confirm the table has no non-10-digit codes.
remaining_bad_codes = final_silver.filter(
    ~F.col("psgc_code").cast("string").rlike(r"^[0-9]{10}$")
)

check(spark, CATALOG, run_id or "interactive", "written_psgc_codes", SILVER_TABLE,
      remaining_bad_codes.count())

print("PASS: All Silver PSGC codes are exactly 10 digits.")

display(
    final_silver
    .select(
        "psgc_code",
        "area_name",
        "geographic_level",
        "region_psgc",
        "province_psgc",
        "citymun_psgc",
        "province_name",
        "province_city",
        "_api_period",
    )
    .orderBy("_api_period", "psgc_code")
    .limit(20)
)
