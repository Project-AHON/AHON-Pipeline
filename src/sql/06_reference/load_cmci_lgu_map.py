import re
import unicodedata
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from pyspark.sql import functions as F
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =====================================================
# CONFIG
# =====================================================

PORTAL_URL = "https://cmci.dti.gov.ph/data-portal.php"

LGU_MASTER_TABLE = "ahon.reference.lgu_master"

CMCI_MAP_TABLE = "ahon.reference.cmci_lgu_map"

REQUEST_TIMEOUT_SECONDS = 60


# =====================================================
# SAFE AUTOMATIC-MATCH RULES
# =====================================================

MATCH_STATUS_MATCHED = "MATCHED"
MATCH_STATUS_UNMATCHED = "UNMATCHED"
MATCH_STATUS_AMBIGUOUS = "AMBIGUOUS"

MATCH_METHOD_EXACT = "EXACT"
MATCH_METHOD_NORMALIZED = "NORMALIZED"
MATCH_METHOD_COMPACT = "COMPACT"

# Automatically matched records still require review for now.
DEFAULT_REVIEWED = False
DEFAULT_IS_ACTIVE = False


# =====================================================
# NAME NORMALIZATION
# =====================================================

def normalize_lgu_name(name):
    """
    Normalize an LGU name for candidate matching.

    This function does not determine the final mapping.
    It only produces a comparable version of each name.
    """

    if name is None:
        return None

    normalized_name = unicodedata.normalize(
        "NFKD",
        str(name),
    )

    normalized_name = "".join(
        character
        for character in normalized_name
        if not unicodedata.combining(character)
    )

    normalized_name = normalized_name.lower().strip()

    normalized_name = re.sub(
        r"^city of\s+",
        "",
        normalized_name,
    )

    normalized_name = re.sub(
        r"\s+city$",
        "",
        normalized_name,
    )

    normalized_name = re.sub(
        r"[^a-z0-9]+",
        " ",
        normalized_name,
    )

    normalized_name = re.sub(
        r"\s+",
        " ",
        normalized_name,
    ).strip()

    return normalized_name

def compact_lgu_name(name):
    """
    Remove spaces after normalizing a locality name.

    Used only when the compact value is unique in both
    PSGC and CMCI.
    """

    normalized_name = normalize_lgu_name(
        name
    )

    if normalized_name is None:
        return None

    return normalized_name.replace(
        " ",
        ""
    )

# =====================================================
# HTTP SESSION
# =====================================================

def create_http_session():
    """
    Create a reusable HTTP session with controlled retries.
    """

    retry_strategy = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=2,
        status_forcelist=(
            429,
            500,
            502,
            503,
            504,
        ),
        allowed_methods=(
            "GET",
        ),
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy
    )

    session = requests.Session()

    session.mount(
        "https://",
        adapter,
    )

    session.mount(
        "http://",
        adapter,
    )

    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/131.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,"
                "application/xhtml+xml,"
                "application/xml;q=0.9,"
                "*/*;q=0.8"
            ),
            "Accept-Language": (
                "en-US,en;q=0.9"
            ),
            "Referer": PORTAL_URL,
        }
    )

    return session


# =====================================================
# STARTUP
# =====================================================

spark.conf.set(
    "spark.sql.session.timeZone",
    "UTC",
)

print("CMCI LGU mapping loader started")
print("PSGC source: " + LGU_MASTER_TABLE)
print("CMCI mapping target: " + CMCI_MAP_TABLE)

# =====================================================
# VALIDATE REQUIRED TABLES
# =====================================================

required_tables = [
    LGU_MASTER_TABLE,
    CMCI_MAP_TABLE,
]

for table_name in required_tables:
    try:
        spark.table(
            table_name
        ).limit(1).collect()

        print(
            "Required table found: "
            + table_name
        )

    except Exception as error:
        raise RuntimeError(
            "Required table cannot be accessed: "
            + table_name
            + " | "
            + str(error)
        ) from error


# =====================================================
# DOWNLOAD CMCI PORTAL PAGE
# =====================================================

session = create_http_session()

try:
    portal_response = session.get(
        PORTAL_URL,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    portal_response.raise_for_status()

    portal_html = portal_response.text.strip()

    if not portal_html:
        raise RuntimeError(
            "CMCI portal returned an empty response"
        )

finally:
    session.close()

print(
    "CMCI portal downloaded successfully"
)

# =====================================================
# PARSE CMCI PORTAL PAGE
# =====================================================

portal_soup = BeautifulSoup(
    portal_html,
    "html.parser",
)


# =====================================================
# FIND CMCI LGU SELECTOR
# =====================================================

lgu_select = portal_soup.find(
    "select",
    attrs={
        "id": "lgu",
    },
)

if lgu_select is None:
    raise RuntimeError(
        "CMCI LGU select element was not found"
    )


# =====================================================
# EXTRACT EXACT CMCI LGU NAMES
# =====================================================

cmci_names = []

for option in lgu_select.find_all("option"):
    option_value = option.get(
        "value"
    )

    option_text = option.get_text(
        " ",
        strip=True,
    )

    if option_value is None:
        continue

    option_value = option_value.strip()
    option_text = option_text.strip()

    if not option_value:
        continue

    if not option_text:
        continue

    cmci_names.append(
        option_value
    )


# =====================================================
# REMOVE DUPLICATES
# =====================================================

cmci_names = list(
    dict.fromkeys(
        cmci_names
    )
)


# =====================================================
# VALIDATE EXTRACTED NAMES
# =====================================================

if not cmci_names:
    raise RuntimeError(
        "No CMCI locality names were extracted"
    )

if len(cmci_names) < 400:
    raise RuntimeError(
        "Only "
        + str(len(cmci_names))
        + " CMCI locality names were extracted. "
        + "Expected at least 400."
    )

print(
    "CMCI locality names extracted: "
    + str(len(cmci_names))
)

print()
print("First 20 CMCI locality names:")

for cmci_name in cmci_names[0:20]:
    print(
        cmci_name
    )
    
# =====================================================
# LOAD OFFICIAL PSGC LGUS
# =====================================================

psgc_rows = (
    spark.table(LGU_MASTER_TABLE)
    .where(
        F.col("is_active") == True
    )
    .select(
        "psgc_code",
        F.trim("lgu_name").alias(
            "psgc_name"
        ),
    )
    .where(
        F.col("psgc_code").isNotNull()
    )
    .where(
        F.col("psgc_name").isNotNull()
    )
    .where(
        F.col("psgc_name") != ""
        )
    .orderBy(
        "psgc_code"
    )
    .collect()
)

if not psgc_rows:
    raise RuntimeError(
        "No active LGUs were found in "
        + LGU_MASTER_TABLE
    )

print(
    "Active PSGC LGUs loaded: "
    + str(len(psgc_rows))
)


# =====================================================
# NORMALIZE CMCI NAMES
# =====================================================

cmci_exact_lookup = {}
cmci_normalized_lookup = {}
cmci_compact_lookup = {}

for cmci_name in cmci_names:
    exact_key = (
        cmci_name
        .lower()
        .strip()
    )

    cmci_exact_lookup[
        exact_key
    ] = cmci_name

    normalized_key = normalize_lgu_name(
        cmci_name
    )

    if normalized_key not in cmci_normalized_lookup:
        cmci_normalized_lookup[
            normalized_key
        ] = []

    cmci_normalized_lookup[
        normalized_key
    ].append(
        cmci_name
    )

    compact_key = compact_lgu_name(
        cmci_name
    )

    if compact_key not in cmci_compact_lookup:
        cmci_compact_lookup[
            compact_key
        ] = []

    cmci_compact_lookup[
        compact_key
    ].append(
        cmci_name
    )

# =====================================================
# COUNT PSGC COMPACT NAMES
# =====================================================

psgc_compact_counts = {}

for psgc_row in psgc_rows:
    compact_key = compact_lgu_name(
        psgc_row["psgc_name"]
    )

    psgc_compact_counts[
        compact_key
    ] = (
        psgc_compact_counts.get(
            compact_key,
            0,
        )
        + 1
    )

# =====================================================
# GENERATE MATCH CANDIDATES
# =====================================================

mapping_records = []

exact_count = 0
normalized_count = 0
compact_count = 0
ambiguous_count = 0
unmatched_count = 0

load_timestamp = datetime.now(
    timezone.utc
)

for psgc_row in psgc_rows:
    psgc_code = psgc_row[
        "psgc_code"
    ]

    psgc_name = psgc_row[
        "psgc_name"
    ]

    exact_key = (
        psgc_name
        .lower()
        .strip()
    )

    normalized_key = normalize_lgu_name(
        psgc_name
    )

    exact_match = cmci_exact_lookup.get(
        exact_key
    )

    normalized_matches = (
        cmci_normalized_lookup.get(
            normalized_key,
            [],
        )
    )
    
    compact_key = compact_lgu_name(
        psgc_name
    )

    compact_matches = (
        cmci_compact_lookup.get(
            compact_key,
            [],
        )
    )

    psgc_compact_count = (
        psgc_compact_counts.get(
            compact_key,
            0,
        )
    )

    if exact_match is not None:
        cmci_name = exact_match
        match_status = MATCH_STATUS_MATCHED
        match_method = MATCH_METHOD_EXACT
        exact_count += 1

    elif len(normalized_matches) == 1:
        cmci_name = normalized_matches[0]
        match_status = MATCH_STATUS_MATCHED
        match_method = MATCH_METHOD_NORMALIZED
        normalized_count += 1

    elif (
        len(compact_matches) == 1
        and psgc_compact_count == 1
    ):
        cmci_name = compact_matches[0]
        match_status = MATCH_STATUS_MATCHED
        match_method = MATCH_METHOD_COMPACT
        compact_count += 1

    elif len(normalized_matches) > 1:
        cmci_name = None
        match_status = MATCH_STATUS_AMBIGUOUS
        match_method = MATCH_METHOD_NORMALIZED
        ambiguous_count += 1

    else:
        cmci_name = None
        match_status = MATCH_STATUS_UNMATCHED
        match_method = None
        unmatched_count += 1

    mapping_records.append(
        {
            "psgc_code": psgc_code,
            "psgc_name": psgc_name,
            "cmci_name": cmci_name,
            "match_status": match_status,
            "match_method": match_method,
            "reviewed": DEFAULT_REVIEWED,
            "is_active": DEFAULT_IS_ACTIVE,
            "created_timestamp": load_timestamp,
            "updated_timestamp": load_timestamp,
        }
    )

# =====================================================
# RECLASSIFY DUPLICATE CMCI MATCHES
# =====================================================

cmci_to_records = {}

for record in mapping_records:
    if record["match_status"] != MATCH_STATUS_MATCHED:
        continue

    cmci_name = record["cmci_name"]

    if cmci_name not in cmci_to_records:
        cmci_to_records[cmci_name] = []

    cmci_to_records[cmci_name].append(
        record
    )

duplicate_cmci_names = {
    cmci_name
    for cmci_name, records in cmci_to_records.items()
    if len(records) > 1
}

for record in mapping_records:
    if record["cmci_name"] not in duplicate_cmci_names:
        continue

    record["match_status"] = MATCH_STATUS_AMBIGUOUS
    record["match_method"] = None
    record["reviewed"] = False
    record["is_active"] = False

# =====================================================
# RECALCULATE MATCH COUNTS
# =====================================================

exact_count = 0
normalized_count = 0
compact_count = 0
ambiguous_count = 0
unmatched_count = 0

for record in mapping_records:
    if record["match_status"] == MATCH_STATUS_AMBIGUOUS:
        ambiguous_count += 1

    elif record["match_status"] == MATCH_STATUS_UNMATCHED:
        unmatched_count += 1

    elif record["match_method"] == MATCH_METHOD_EXACT:
        exact_count += 1

    elif record["match_method"] == MATCH_METHOD_NORMALIZED:
        normalized_count += 1

    elif record["match_method"] == MATCH_METHOD_COMPACT:
        compact_count += 1

print()
print(
    "Duplicate CMCI names reclassified: "
    + str(len(duplicate_cmci_names))
)

for cmci_name in sorted(
    duplicate_cmci_names
):
    print(
        "AMBIGUOUS CMCI NAME | "
        + cmci_name
        + " | PSGC matches: "
        + str(
            len(
                cmci_to_records[cmci_name]
            )
        )
    )

# =====================================================
# VALIDATE CANDIDATE COUNTS
# =====================================================

total_mapping_count = len(
    mapping_records
)

classified_count = (
    exact_count
    + normalized_count
    + compact_count
    + ambiguous_count
    + unmatched_count
)

if classified_count != total_mapping_count:
    raise RuntimeError(
        "Mapping count validation failed. Expected "
        + str(total_mapping_count)
        + ", classified "
        + str(classified_count)
    )

if total_mapping_count != len(psgc_rows):
    raise RuntimeError(
        "Not every PSGC LGU received a mapping status"
    )


# =====================================================
# PRINT CANDIDATE SUMMARY
# =====================================================

print()
print("CMCI MAPPING CANDIDATES GENERATED")

print(
    "Exact matches: "
    + str(exact_count)
)

print(
    "Normalized unique matches: "
    + str(normalized_count)
)

print(
    "Ambiguous matches: "
    + str(ambiguous_count)
)

print(
    "Unmatched LGUs: "
    + str(unmatched_count)
)

print(
    "Total PSGC LGUs classified: "
    + str(total_mapping_count)
)

print(
    "Compact unique matches: "
    + str(compact_count)
)


# =====================================================
# PREVIEW MATCHED CANDIDATES
# =====================================================

print()
print("FIRST 20 MATCHED CANDIDATES:")

shown_matches = 0

for record in mapping_records:
    if record["match_status"] != MATCH_STATUS_MATCHED:
        continue

    print(
        record["psgc_code"]
        + " | "
        + record["psgc_name"]
        + " -> "
        + str(record["cmci_name"])
        + " | "
        + str(record["match_method"])
    )

    shown_matches += 1

    if shown_matches >= 20:
        break


# =====================================================
# PREVIEW UNRESOLVED CANDIDATES
# =====================================================

print()
print("FIRST 20 UNRESOLVED PSGC LGUS:")

shown_unresolved = 0

for record in mapping_records:
    if record["match_status"] == MATCH_STATUS_MATCHED:
        continue

    print(
        record["psgc_code"]
        + " | "
        + record["psgc_name"]
        + " | "
        + record["match_status"]
    )

    shown_unresolved += 1

    if shown_unresolved >= 20:
        break

# =====================================================
# CREATE MAPPING DATAFRAME
# =====================================================

if not mapping_records:
    raise RuntimeError(
        "No CMCI mapping records were generated"
    )

mapping_df = spark.createDataFrame(
    mapping_records
)

mapping_df = mapping_df.select(
    "psgc_code",
    "psgc_name",
    "cmci_name",
    "match_status",
    "match_method",
    "reviewed",
    "is_active",
    "created_timestamp",
    "updated_timestamp",
)


# =====================================================
# VALIDATE GENERATED MAPPINGS
# =====================================================

generated_total_count = mapping_df.count()

if generated_total_count != total_mapping_count:
    raise RuntimeError(
        "Generated mapping count mismatch. Expected "
        + str(total_mapping_count)
        + ", found "
        + str(generated_total_count)
    )

duplicate_psgc_count = (
    mapping_df
    .groupBy("psgc_code")
    .count()
    .where(
        F.col("count") > 1
    )
    .count()
)

if duplicate_psgc_count > 0:
    raise RuntimeError(
        "Generated mappings contain duplicate PSGC codes"
    )

invalid_status_count = (
    mapping_df
    .where(
        ~F.col("match_status").isin(
            MATCH_STATUS_MATCHED,
            MATCH_STATUS_UNMATCHED,
            MATCH_STATUS_AMBIGUOUS,
        )
    )
    .count()
)

if invalid_status_count > 0:
    raise RuntimeError(
        "Generated mappings contain invalid match statuses"
    )

invalid_matched_count = (
    mapping_df
    .where(
        (F.col("match_status") == MATCH_STATUS_MATCHED)
        & (
            F.col("cmci_name").isNull()
            | (F.col("cmci_name") == "")
        )
    )
    .count()
)

if invalid_matched_count > 0:
    raise RuntimeError(
        "Matched records contain missing CMCI names"
    )

print()
print("Generated mapping validation passed")

print(
    "Generated mapping records: "
    + str(generated_total_count)
)


# =====================================================
# PREPARE MERGE SOURCE
# =====================================================

mapping_df.createOrReplaceTempView(
    "cmci_lgu_map_source"
)


# =====================================================
# MERGE INTO CMCI MAPPING TABLE
# =====================================================

spark.sql(
    """
    MERGE INTO ahon.reference.cmci_lgu_map AS target
    USING cmci_lgu_map_source AS source
        ON target.psgc_code = source.psgc_code

    WHEN MATCHED
        AND target.reviewed = false
    THEN UPDATE SET
        target.psgc_name = source.psgc_name,
        target.cmci_name = source.cmci_name,
        target.match_status = source.match_status,
        target.match_method = source.match_method,
        target.reviewed = source.reviewed,
        target.is_active = source.is_active,
        target.updated_timestamp = source.updated_timestamp

    WHEN NOT MATCHED THEN INSERT (
        psgc_code,
        psgc_name,
        cmci_name,
        match_status,
        match_method,
        reviewed,
        is_active,
        created_timestamp,
        updated_timestamp
    )
    VALUES (
        source.psgc_code,
        source.psgc_name,
        source.cmci_name,
        source.match_status,
        source.match_method,
        source.reviewed,
        source.is_active,
        source.created_timestamp,
        source.updated_timestamp
    )
    """
)


# =====================================================
# READ SAVED MAPPING TABLE
# =====================================================

saved_mapping_df = spark.table(
    CMCI_MAP_TABLE
)


# =====================================================
# VERIFY PSGC COVERAGE
# =====================================================

saved_psgc_codes_df = (
    saved_mapping_df
    .select("psgc_code")
    .dropDuplicates()
)

master_psgc_codes_df = (
    spark.table(LGU_MASTER_TABLE)
    .where(
        F.col("is_active") == True
    )
    .select("psgc_code")
    .dropDuplicates()
)

missing_mapping_count = (
    master_psgc_codes_df
    .join(
        saved_psgc_codes_df,
        on="psgc_code",
        how="left_anti",
    )
    .count()
)

if missing_mapping_count > 0:
    raise RuntimeError(
        "CMCI mapping table is missing "
        + str(missing_mapping_count)
        + " active PSGC LGUs"
    )


# =====================================================
# VERIFY SAVED UNIQUENESS
# =====================================================

saved_duplicate_count = (
    saved_mapping_df
    .groupBy("psgc_code")
    .count()
    .where(
        F.col("count") > 1
    )
    .count()
)

if saved_duplicate_count > 0:
    raise RuntimeError(
        "Saved mapping table contains duplicate PSGC codes"
    )


# =====================================================
# SUMMARIZE SAVED STATUSES
# =====================================================

saved_status_rows = (
    saved_mapping_df
    .groupBy("match_status")
    .count()
    .collect()
)

saved_status_counts = {}

for status_row in saved_status_rows:
    saved_status_counts[
        status_row["match_status"]
    ] = status_row["count"]

saved_matched_count = saved_status_counts.get(
    MATCH_STATUS_MATCHED,
    0,
)

saved_ambiguous_count = saved_status_counts.get(
    MATCH_STATUS_AMBIGUOUS,
    0,
)

saved_unmatched_count = saved_status_counts.get(
    MATCH_STATUS_UNMATCHED,
    0,
)

saved_reviewed_count = (
    saved_mapping_df
    .where(
        F.col("reviewed") == True
    )
    .count()
)

saved_active_count = (
    saved_mapping_df
    .where(
        F.col("is_active") == True
    )
    .count()
)

saved_total_count = (
    saved_mapping_df.count()
)


# =====================================================
# FINAL SUMMARY
# =====================================================

print()
print("CMCI LGU MAPPING LOAD COMPLETE")

print(
    "CMCI names extracted: "
    + str(len(cmci_names))
)

print(
    "PSGC LGUs processed: "
    + str(total_mapping_count)
)

print(
    "Saved matched records: "
    + str(saved_matched_count)
)

print(
    "Saved ambiguous records: "
    + str(saved_ambiguous_count)
)

print(
    "Saved unmatched records: "
    + str(saved_unmatched_count)
)

print(
    "Reviewed records preserved: "
    + str(saved_reviewed_count)
)

print(
    "Active mappings: "
    + str(saved_active_count)
)

print(
    "Missing PSGC mappings: "
    + str(missing_mapping_count)
)

print(
    "Duplicate PSGC codes: "
    + str(saved_duplicate_count)
)

print(
    "Total mapping records: "
    + str(saved_total_count)
)

print()
print(
    "Automatic candidates remain inactive "
    + "until reviewed and approved"
)