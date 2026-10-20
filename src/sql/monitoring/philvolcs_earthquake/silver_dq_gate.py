# %pip install great_expectations==1.23.1
"""
DQ gate, bronze to silver: philvolcs_earthquake (decision 0012, observations per ADR 0009).

Run after the earthquake_silver task (observation_pipeline.py --stage silver).
Checks silver.philvolcs_earthquake_observation_clean with Great Expectations,
writes one row per rule to monitoring.dq_result, and stops the run if a stop
rule FAILs. Flag and report rules give WARN and do not stop it.

One silver row is one bronze observation (key _observation_id). Invalid
observations stay in silver with has_dq_issues = true and are also copied to
silver.philvolcs_earthquake_reject, so validity rules are flag rules and the
reject table must hold exactly the flagged rows. psgc_code stays NULL: a
locality named in the PHIVOLCS text is not exposure, so the build only lists
candidate codes (ADR 0009).

Rules kept from the earlier gate (written for philvolcs_earthquake_data_clean):
lineage, uniqueness, bronze reconciliation, has_dq_issues, PSGC blocked,
snapshot, candidate status, event validity, the report rules. New:
reject_matches_flagged.

Tested on ahon_dev_crystal (2026-10-20): no FAIL. WARN in_ph_bounds 22 rows,
candidate_coverage 1,896 (every row unparsed: LOCALITY_PATTERN in
earthquake_contract.py does not allow "048  km" or the mis-encoded "Â°";
fix planned in a separate PR).

Unsure:
- in_ph_bounds uses an assumed Philippine box (PH_LAT, PH_LON below), not a
  repo constant. Offshore and foreign events are expected, so it is report only.
- row_count_matches_bronze is report: the build reads only the current
  batch's files, so silver can differ from all of bronze.
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

# Set by src/sql/common/run_python.py in the job; set them yourself in a notebook
CATALOG = globals().get("PIPELINE_CATALOG", "ahon_dev")
RUN_ID = globals().get("PIPELINE_RUN_ID", "interactive")
PSGC_SNAPSHOT = globals().get("PIPELINE_PERIOD", "Q2_2024")
SOURCE_NAME = "philvolcs_earthquake"
BRONZE_TABLE = f"{CATALOG}.bronze.philvolcs_earthquake_observation"
SILVER_TABLE = f"{CATALOG}.silver.philvolcs_earthquake_observation_clean"
REJECT_TABLE = f"{CATALOG}.silver.philvolcs_earthquake_reject"
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_result"
KEY_COLUMNS = ["_source_ref", "_source_row", "_observation_id"]

PH_LAT = (4.0, 21.5)       # assumed Philippine box, generous for near-shore events
PH_LON = (116.0, 127.5)

LINEAGE_COLUMNS = ["_source_name", "_source_ref", "_source_row", "_source_digest", "_ingested_at",
                   "_batch_id", "_row_hash", "_observation_id", "_processed_at"]
CANDIDATE_STATUSES = ["single_candidate", "ambiguous", "unmatched", "unparsed"]
# Raw text kept in silver, compared with the same bronze column
RAW_COLUMNS = {"_raw_latitude": "Latitude", "_raw_longitude": "Longitude", "_raw_depth_km": "Depth",
               "_raw_magnitude": "Magnitude", "Location": "Location", "Date-Time": "Date-Time",
               "Month": "Month", "Year": "Year", "_row_hash": "_row_hash"}

E = gx.expectations


# ============================================================
# 2. HELPER COLUMNS
# ============================================================

# GX checks one or two columns at a time, so rules that span several columns get a helper column.
# Bronze is joined on _observation_id; helpers that compare with bronze are NULL (skipped by GX)
# when the row has no bronze match, which row_in_bronze already reports.
INF = float("inf")


def bad_number(c):
    return c.isNull() | F.isnan(c) | (F.abs(c) == INF)


lat, lon = F.col("latitude"), F.col("longitude")
depth, mag = F.col("depth_km"), F.col("magnitude")
event_time = F.col("event_time")
in_bronze = F.col("row_in_bronze")

# Same expression as `invalid` in observation_pipeline.py
period_bad = (
    F.col("source_year").isNull() | F.col("source_month").isNull()
    | (F.col("source_year") != F.year(event_time)) | (F.col("source_month") != F.month(event_time))
)
event_invalid = F.coalesce(
    event_time.isNull()
    | bad_number(lat) | ~lat.between(-90, 90)
    | bad_number(lon) | ~lon.between(-180, 180)
    | bad_number(depth) | (depth < 0)
    | bad_number(mag) | (mag < 0)
    | period_bad,
    F.lit(True),
)

raw_match = F.lit(True)
for silver_col, bronze_col in RAW_COLUMNS.items():
    raw_match = raw_match & F.col(f"`{silver_col}`").eqNullSafe(F.col(f"`b_{bronze_col}`"))

codes = F.size("candidate_codes")
status = F.col("candidate_status")

bronze = spark.table(BRONZE_TABLE)
bronze_rows = bronze.count()
bronze_cols = (
    bronze.select("_observation_id", *[F.col(f"`{c}`").alias(f"b_{c}") for c in set(RAW_COLUMNS.values())])
    .withColumn("row_in_bronze", F.lit(True))
)
reject_ids = spark.table(REJECT_TABLE).select("_observation_id").distinct().withColumn("in_reject", F.lit(True))

silver_check = (
    spark.table(SILVER_TABLE)
    .join(bronze_cols, on="_observation_id", how="left")
    .withColumn("row_in_bronze", F.coalesce(in_bronze, F.lit(False)))
    .withColumn("values_match_bronze", F.when(in_bronze, raw_match))
    .join(reject_ids, on="_observation_id", how="left")
    .withColumn("in_reject", F.coalesce(F.col("in_reject"), F.lit(False)))
    # Every invalid observation must carry has_dq_issues = true (NULL flags: has_dq_issues_not_null)
    .withColumn("invalid_is_flagged",
                F.when(F.col("has_dq_issues").isNotNull(), F.col("has_dq_issues") | ~event_invalid))
    .withColumn("event_values_finite",
                ~(bad_number(lat) | bad_number(lon) | bad_number(depth) | bad_number(mag)))
    .withColumn("source_period_consistent", F.when(event_time.isNotNull(), ~period_bad))
    # NULL when coordinates are invalid (latitude_valid / longitude_valid cover those)
    .withColumn("in_ph_bounds",
                F.when(~bad_number(lat) & ~bad_number(lon)
                       & lat.between(-90, 90) & lon.between(-180, 180),
                       lat.between(*PH_LAT) & lon.between(*PH_LON)))
    # Status agrees with the number of candidate codes
    .withColumn("candidate_count_matches_status",
                F.when(status == "single_candidate", codes == 1)
                .when(status == "ambiguous", codes >= 2)
                .when(status.isin("unparsed", "unmatched"), codes == 0))
)


# ============================================================
# 3. RULES
# ============================================================

silver_rules = [
    # Stop rules
    rule(E.ExpectTableRowCountToBeBetween(min_value=1), "silver_not_empty", "volume", "stop"),
] + [
    rule(E.ExpectColumnValuesToNotBeNull(column=c), f"{c.lstrip('_')}_not_null", "null", "stop")
    for c in LINEAGE_COLUMNS
] + [
    rule(E.ExpectColumnValuesToBeUnique(column="_observation_id"), "observation_id_unique", "unique", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="row_in_bronze", value_set=[True]),
         "row_in_bronze", "referential_integrity", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="values_match_bronze", value_set=[True]),
         "values_match_bronze", "reconciliation", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="has_dq_issues"), "has_dq_issues_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="invalid_is_flagged", value_set=[True]),
         "invalid_is_flagged", "reconciliation", "stop"),
    # The reject table holds exactly the flagged rows
    rule(E.ExpectColumnPairValuesToBeEqual(column_A="has_dq_issues", column_B="in_reject"),
         "reject_matches_flagged", "reconciliation", "stop"),
    # PSGC assignment stays withheld until the geographic meaning is approved (ADR 0009)
    rule(E.ExpectColumnValuesToBeNull(column="psgc_code"), "psgc_code_withheld", "null", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="geographic_assignment_status",
                                       value_set=["WITHHELD_LOCALITY_IS_NOT_EXPOSURE"]),
         "geographic_assignment_withheld", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToNotBeNull(column="_psgc_snapshot"), "psgc_snapshot_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="_psgc_snapshot", value_set=[PSGC_SNAPSHOT]),
         "psgc_snapshot_valid", "accepted_values", "stop"),
    # Locality candidates
    rule(E.ExpectColumnValuesToNotBeNull(column="candidate_status"), "candidate_status_not_null", "null", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="candidate_status", value_set=CANDIDATE_STATUSES),
         "candidate_status_valid", "accepted_values", "stop"),
    rule(E.ExpectColumnValuesToBeInSet(column="candidate_count_matches_status", value_set=[True]),
         "candidate_count_matches_status", "reconciliation", "stop"),
] + [
    # Flag rules (rows stay in silver, are marked has_dq_issues and copied to the reject table)
    rule(E.ExpectColumnValuesToNotBeNull(column="event_time"), "event_time_parsed", "null", "flag"),
    rule(E.ExpectColumnValuesToBeInSet(column="event_values_finite", value_set=[True]),
         "event_values_finite", "range", "flag"),
    rule(E.ExpectColumnValuesToBeBetween(column="latitude", min_value=-90, max_value=90),
         "latitude_valid", "range", "flag"),
    rule(E.ExpectColumnValuesToBeBetween(column="longitude", min_value=-180, max_value=180),
         "longitude_valid", "range", "flag"),
    rule(E.ExpectColumnValuesToBeBetween(column="depth_km", min_value=0), "depth_nonnegative", "range", "flag"),
    rule(E.ExpectColumnValuesToBeBetween(column="magnitude", min_value=0),
         "magnitude_nonnegative", "range", "flag"),
    rule(E.ExpectColumnValuesToBeInSet(column="source_period_consistent", value_set=[True]),
         "source_period_consistent", "reconciliation", "flag"),
] + [
    # Report rules (information only, nothing is wrong with the row)
    rule(E.ExpectTableRowCountToEqual(value=bronze_rows), "row_count_matches_bronze", "volume", "report"),
    rule(E.ExpectCompoundColumnsToBeUnique(column_list=["event_time", "latitude", "longitude", "depth_km",
                                                        "magnitude"],
                                           ignore_row_if="any_value_is_missing"),
         "event_signature_unique", "unique", "report"),
    rule(E.ExpectColumnValuesToBeInSet(column="in_ph_bounds", value_set=[True]),
         "in_ph_bounds", "spatial", "report"),
    rule(E.ExpectColumnValuesToBeInSet(column="candidate_status", value_set=["single_candidate"]),
         "candidate_coverage", "accepted_values", "report"),
]


# ============================================================
# 4. RUN THE GATE
# ============================================================

run_dq_gate(
    spark, silver_check, silver_rules,
    source_name=SOURCE_NAME, layer="silver", table_name="philvolcs_earthquake_observation_clean",
    key_columns=KEY_COLUMNS, results_table=RESULTS_TABLE, run_id=RUN_ID,
)