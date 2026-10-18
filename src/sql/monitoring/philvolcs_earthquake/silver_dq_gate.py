# %pip install great_expectations==1.23.1
"""
DQ gate, bronze to silver: philvolcs_earthquake (decision 0012, observations per ADR 0009).

Run after run_earthquake.py writes silver.philvolcs_earthquake_data_clean. Checks
the whole silver table with Great Expectations, appends one row per rule to
monitoring.dq_results, and stops the run if a stop rule FAILs. Flag and report
rules give WARN and do not stop it.

Ports the checks in src/sql/monitoring/philvolcs_earthquake/silver_dq.sql. One
silver row is one distinct bronze observation (merge key _row_hash). Invalid
observations stay in silver with has_dq_issues = true, so validity rules are flag.
id is an event signature and is NOT unique (duplicates are a report rule).

Expected on ahon_dev: not run yet.

Not ported (need the runner's temp views or the raw CSV, not stored tables):
_raw_period_invalid (original Year text), requested-period coverage, the
candidate map checks on earthquake_lgu_candidates (map key unique), timeliness.

Unsure:
- in_ph_bounds uses an assumed Philippine box (PH_LAT, PH_LON below), not a
  repo constant. Offshore and foreign events are expected, so it is report only.
- psgc_snapshot_valid is stop on "Q2_2024" (the runner's --source-period default).
  Update PSGC_SNAPSHOT if the job runs with another snapshot.
- row_count_matches_bronze is report: a --stage bronze run fills bronze without silver.
- No magnitude or depth upper limit: the data dictionary says no 800 km / 7.8 cutoff.
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
SOURCE_NAME = "philvolcs_earthquake"
BRONZE_TABLE = f"{CATALOG}.bronze.philvolcs_earthquake_data"
SILVER_TABLE = f"{CATALOG}.silver.philvolcs_earthquake_data_clean"
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_results"

PSGC_SNAPSHOT = "Q2_2024"  # run_earthquake.py --source-period default
PH_LAT = (4.0, 21.5)       # assumed Philippine box, generous for near-shore events
PH_LON = (116.0, 127.5)

LINEAGE_COLUMNS = ["_source_name", "_source_ref", "_ingested_at", "_batch_id", "_row_hash"]
CANDIDATE_STATUSES = ["single_candidate", "ambiguous", "unmatched", "unparsed", "blocked_reference"]


# ============================================================
# 2. HELPER COLUMNS
# ============================================================

# GX checks one or two columns at a time, so rules that span several columns get a helper column.
# Bronze is joined on _row_hash; helpers that compare with bronze are NULL (skipped by GX) when the
# row has no bronze match, which row_in_bronze already reports.
INF = float("inf")


def bad_number(c):
    return c.isNull() | F.isnan(c) | (c == INF)


lat, lon = F.col("latitude"), F.col("longitude")
depth, mag = F.col("depth"), F.col("magnitude")
event_time = F.col("event_time")

# Same expression as `invalid` in silver_dq.sql
event_invalid = F.coalesce(
    event_time.isNull()
    | bad_number(lat) | ~lat.between(-90, 90)
    | bad_number(lon) | ~lon.between(-180, 180)
    | bad_number(depth) | (depth < 0)
    | bad_number(mag) | (mag < 0),
    F.lit(True),
)
# Source Month/Year (bronze) against the parsed wall time, as in silver_dq.sql
period_bad = F.coalesce(
    F.col("b_Year").isNull() | F.col("b_Month").isNull() | event_time.isNull()
    | (F.col("b_Year") != F.year(event_time))
    | (F.lower(F.trim(F.col("b_Month"))) != F.lower(F.date_format(event_time, "MMMM"))),
    F.lit(True),
)
derived_period_ok = (
    F.col("month").eqNullSafe(F.month(event_time).cast("int"))
    & F.col("year").eqNullSafe(F.year(event_time).cast("int"))
)
bronze_location = F.when(F.trim(F.col("b_Location")) == "", None).otherwise(F.trim(F.col("b_Location")))
typed_match = (
    lat.eqNullSafe(F.col("b_Latitude")) & lon.eqNullSafe(F.col("b_Longitude"))
    & depth.eqNullSafe(F.col("b_Depth")) & mag.eqNullSafe(F.col("b_Magnitude"))
    & F.col("location_description").eqNullSafe(bronze_location)
)
lineage_match = F.lit(True)
for c in LINEAGE_COLUMNS:
    lineage_match = lineage_match & F.col(c).eqNullSafe(F.col(f"b{c}"))

codes = F.size(F.col("psgc_candidate_codes"))
status = F.col("psgc_candidate_status")
in_bronze = F.col("row_in_bronze")


def when_in_bronze(expr):
    return F.when(in_bronze, expr)


bronze = spark.table(BRONZE_TABLE)
bronze_rows = bronze.count()
bronze_cols = (
    bronze.select(
        "_row_hash",
        *[F.col(c).alias(f"b_{c}") for c in ["Latitude", "Longitude", "Depth", "Magnitude",
                                             "Location", "Month", "Year"]],
        *[F.col(c).alias(f"b{c}") for c in LINEAGE_COLUMNS if c != "_row_hash"],
    )
    .withColumn("b_row_hash", F.col("_row_hash"))
    .withColumn("row_in_bronze", F.lit(True))
)

silver_check = (
    spark.table(SILVER_TABLE)
    .join(bronze_cols, on="_row_hash", how="left")
    .withColumn("row_in_bronze", F.coalesce(in_bronze, F.lit(False)))
    .withColumn("lineage_matches_bronze", when_in_bronze(lineage_match))
    .withColumn("values_match_bronze", when_in_bronze(typed_match))
    # Every invalid observation must carry has_dq_issues = true (NULL flags: has_dq_issues_not_null)
    .withColumn("invalid_is_flagged",
                F.when(in_bronze & F.col("has_dq_issues").isNotNull(),
                       F.col("has_dq_issues") | ~(event_invalid | period_bad)))
    .withColumn("event_values_finite",
                ~(bad_number(lat) | bad_number(lon) | bad_number(depth) | bad_number(mag)))
    .withColumn("source_period_consistent", when_in_bronze(~period_bad & derived_period_ok))
    # NULL when coordinates are invalid (latitude_valid / longitude_valid cover those)
    .withColumn("in_ph_bounds",
                F.when(~bad_number(lat) & ~bad_number(lon)
                       & lat.between(-90, 90) & lon.between(-180, 180),
                       lat.between(*PH_LAT) & lon.between(*PH_LON)))
    # Status agrees with the number of exact candidates, and the method with having any
    .withColumn("candidate_count_matches_status",
                F.when(status == "single_candidate", codes == 1)
                .when(status == "ambiguous", codes >= 2)
                .when(status.isin("unparsed", "unmatched", "blocked_reference"), codes == 0))
    .withColumn("candidate_method_matches",
                F.when(codes > 0, F.col("psgc_candidate_method").eqNullSafe("exact_locality_province_candidate"))
                .otherwise(F.col("psgc_candidate_method").isNull()))
)


# ============================================================
# 3. RULES
# ============================================================

E = gx.expectations
silver_rules = [
    # Stop rules (the stop checks in silver_dq.sql; the runner raises them before the MERGE)
    rule(E.ExpectTableRowCountToBeBetween(min_value=1), "silver_not_empty", "volume", "stop"),
] + [
    rule(E.ExpectColumnValuesToNotBeNull(column=c), f"{c}_not_null", "null", "stop")
    for c in LINEAGE_COLUMNS + ["_processed_at"]
] + [
    rule(E.ExpectColumnValuesToMatchRegex(column="_row_hash", regex="^[0-9a-f]{64}$"),
         "row_hash_sha256_format", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToBeUnique(column="_row_hash"), "row_hash_unique", "unique", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="row_in_bronze", value_set=[True]),
         "row_in_bronze", "referential_integrity", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="lineage_matches_bronze", value_set=[True]),
         "lineage_matches_bronze", "reconciliation", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="values_match_bronze", value_set=[True]),
         "values_match_bronze", "reconciliation", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="has_dq_issues"), "has_dq_issues_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="invalid_is_flagged", value_set=[True]),
         "invalid_is_flagged", "reconciliation", "stop"),
    # PSGC assignment stays blocked until the geographic meaning is approved (ADR 0009)
    rule(E.ExpectColumnValuesToBeNull(column="psgc_code"), "psgc_code_blocked", "null", "stop"),
    rule(E.ExpectColumnValuesToBeNull(column="psgc_match_method"), "psgc_match_method_blocked", "null", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="psgc_match_status"), "psgc_match_status_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="psgc_match_status", value_set=["blocked_meaning"]),
         "psgc_match_status_blocked", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="_psgc_snapshot"), "psgc_snapshot_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="_psgc_snapshot", value_set=[PSGC_SNAPSHOT]),
         "psgc_snapshot_valid", "accepted_values", "stop"),
    # Locality candidates (silver_candidate_cardinality and silver_candidate_method)
    rule(E.ExpectColumnValuesToNotBeNull(column="psgc_candidate_status"),
         "psgc_candidate_status_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="psgc_candidate_status", value_set=CANDIDATE_STATUSES),
         "psgc_candidate_status_valid", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="candidate_count_matches_status", value_set=[True]),
         "candidate_count_matches_status", "reconciliation", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="candidate_method_matches", value_set=[True]),
         "candidate_method_matches", "accepted_values", "stop"),
] + [
    # Flag rules (silver_event_validity and silver_source_period_consistency;
    # rows stay in silver and are marked has_dq_issues by the build)
    rule(E.ExpectColumnValuesToNotBeNull(column="event_time"), "event_time_parsed", "null", "flag"),
    rule(E.ExpectColumnValuesToBeInSet(column="event_values_finite", value_set=[True]),
         "event_values_finite", "range", "flag"),
    rule(E.ExpectColumnValuesToBeBetween(column="latitude", min_value=-90, max_value=90),
         "latitude_valid", "range", "flag"),
    rule(E.ExpectColumnValuesToBeBetween(column="longitude", min_value=-180, max_value=180),
         "longitude_valid", "range", "flag"),
    rule(E.ExpectColumnValuesToBeBetween(column="depth", min_value=0), "depth_nonnegative", "range", "flag"),
    rule(E.ExpectColumnValuesToBeBetween(column="magnitude", min_value=0),
         "magnitude_nonnegative", "range", "flag"),
    rule(E.ExpectColumnValuesToBeInSet(column="source_period_consistent", value_set=[True]),
         "source_period_consistent", "reconciliation", "flag"),
] + [
    # Report rules (information only, nothing is wrong with the row)
    rule(E.ExpectTableRowCountToEqual(value=bronze_rows), "row_count_matches_bronze", "volume", "report"),
    rule(E.ExpectCompoundColumnsToBeUnique(column_list=["event_time", "latitude", "longitude", "depth", "magnitude"],
                                           ignore_row_if="any_value_is_missing"),
         "event_signature_unique", "unique", "report"),
    rule(E.ExpectColumnValuesToBeInSet(column="in_ph_bounds", value_set=[True]),
         "in_ph_bounds", "spatial", "report"),
    rule(E.ExpectColumnValuesToBeInSet(column="psgc_candidate_status", value_set=["single_candidate"]),
         "psgc_candidate_coverage", "accepted_values", "report"),
    rule(E.ExpectColumnValuesToNotBeInSet(column="psgc_candidate_status", value_set=["blocked_reference"]),
         "psgc_candidate_reference_available", "referential_integrity", "report"),
]


# ============================================================
# 4. RUN THE GATE
# ============================================================

run_dq_gate(
    spark, silver_check, silver_rules,
    source_name=SOURCE_NAME, layer="silver", table_name="philvolcs_earthquake_data_clean",
    key_columns=["_source_ref", "_row_hash"],
    results_table=RESULTS_TABLE,
)
