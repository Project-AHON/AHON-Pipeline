import re
import unicodedata
from datetime import datetime, timezone

from bs4 import BeautifulSoup
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from cmci_common import (
    PORTAL_URL,
    REQUEST_TIMEOUT_SECONDS,
    create_http_session,
)

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------

LGU_MASTER_TABLE = (
    "ahon.reference.lgu_master"
)

CMCI_MAP_TABLE = (
    "ahon.reference.cmci_lgu_map"
)

EXPECTED_PSGC_COUNT = 1642
EXPECTED_CMCI_NAME_COUNT = 1634

EXPECTED_EXACT_COUNT = 1113
EXPECTED_NORMALIZED_COUNT = 136
EXPECTED_COMPACT_COUNT = 7
EXPECTED_MANUAL_COUNT = 7
EXPECTED_PROVINCE_SUFFIX_COUNT = 347
EXPECTED_SUFFIX_EXCEPTION_COUNT = 4
EXPECTED_MANUAL_ALIAS_COUNT = 20

EXPECTED_FINAL_MATCHED_COUNT = 1634
EXPECTED_FINAL_UNMATCHED_COUNT = 8

EXPECTED_BASELINE_MATCHED_COUNT = 1263
EXPECTED_BASELINE_UNMATCHED_COUNT = 379
EXPECTED_RECONCILIATION_COUNT = 371

MATCH_STATUS_MATCHED = "MATCHED"
MATCH_STATUS_UNMATCHED = "UNMATCHED"

MATCH_METHOD_EXACT = "EXACT"
MATCH_METHOD_NORMALIZED = "NORMALIZED"
MATCH_METHOD_COMPACT = "COMPACT"
MATCH_METHOD_MANUAL = "MANUAL"
MATCH_METHOD_PROVINCE_SUFFIX = (
    "PROVINCE_SUFFIX"
)
MATCH_METHOD_SUFFIX_EXCEPTION = (
    "PROVINCE_SUFFIX_EXCEPTION"
)
MATCH_METHOD_MANUAL_ALIAS = (
    "MANUAL_ALIAS"
)

APPLY_WRITE = True

# ------------------------------------------------------------------
# Reviewed CMCI qualifier-to-province codebook
# ------------------------------------------------------------------

CMCI_SUFFIX_PROVINCE_LOOKUP = {
    "AA": "Abra",
    "AE": "Antique",
    "AK": "Aklan",
    "AN": "Agusan del Norte",
    "AO": "Apayao",
    "AS": "Agusan del Sur",
    "AU": "Aurora",
    "AY": "Albay",
    "BA": "Basilan",
    "BAS": "Basilan",
    "BK": "Bukidnon",
    "BL": "Bohol",
    "BN": "Bataan",
    "BS": "Batangas",
    "BU": "Bulacan",
    "CE": "Cavite",
    "CG": "Cagayan",
    "CM": "Camiguin",
    "CN": "Camarines Norte",
    "CS": "Camarines Sur",
    "CT": "Catanduanes",
    "CU": "Cebu",
    "CV": "Davao de Oro",
    "CZ": "Capiz",
    "DDO": "Davao de Oro",
    "DI": "Dinagat Islands",
    "DN": "Davao del Norte",
    "DO": "Davao Oriental",
    "ES": "Eastern Samar",
    "GS": "Guimaras",
    "IA": "Isabela",
    "IMELDA": "Romblon",
    "IN": "Ilocos Norte",
    "IO": "Iloilo",
    "IS": "Ilocos Sur",
    "KA": "Kalinga",
    "LA": "Laguna",
    "LE": "Leyte",
    "LN": "Lanao del Norte",
    "LS": "Lanao del Sur",
    "LU": "La Union",
    "MA": "Maguindanao del Norte",
    "MC": "Misamis Occidental",
    "ME": "Marinduque",
    "MM": "Metro Manila",
    "MO": "Misamis Oriental",
    "MP": "Mountain Province",
    "MS": "Masbate",
    "NC": "Cotabato",
    "NE": "Nueva Ecija",
    "NO": "Negros Occidental",
    "NR": "Negros Oriental",
    "NS": "Northern Samar",
    "NV": "Nueva Vizcaya",
    "OM": "Occidental Mindoro",
    "OR": "Oriental Mindoro",
    "PA": "Pampanga",
    "PN": "Palawan",
    "PS": "Pangasinan",
    "QN": "Quezon",
    "RL": "Rizal",
    "RN": "Romblon",
    "SC": "South Cotabato",
    "SK": "Sultan Kudarat",
    "SL": "Southern Leyte",
    "SN": "Surigao del Norte",
    "SO": "Sorsogon",
    "SR": "Siquijor",
    "SS": "Surigao del Sur",
    "SU": "Sulu",
    "TC": "Tarlac",
    "WS": "Samar",
    "ZA": "Zambales",
    "ZN": "Zamboanga del Norte",
    "ZR": "Zamboanga del Sur",
    "ZS": "Zamboanga Sibugay",
}

CMCI_CONFLICTING_SUFFIXES = {
    "DS": [
        "Davao del Sur",
        "Davao Occidental",
    ],
}

CMCI_CONFLICTING_SUFFIX_EXCEPTIONS = {
    (
        "hagonoy",
        "davao del sur",
    ): "Hagonoy (DS)",
    (
        "magsaysay",
        "davao del sur",
    ): "Magsaysay (DS)",
    (
        "santa cruz",
        "davao del sur",
    ): "Santa Cruz (DS)",
    (
        "santa maria",
        "davao occidental",
    ): "Santa Maria (DS)",
}

CMCI_REVIEWED_MANUAL_MAPPINGS = {
    "0306905000": "Concepcion (TC)",
    "0402107000": "Gen. Emilio Aguinaldo",
    "0501607000": "Mercedes",
    "0603015000": "Concepcion (IO)",
    "0802616000": "Mercedes (ES)",
    "1004206000": "Concepcion",
    "1705905000": "Concepcion (RN)",
}

CMCI_REVIEWED_MANUAL_ALIASES = {
    "0301405000": "Bulakan",
    "0504116000": "Pio V Corpuz",
    "0701235000": "President Garcia",
    "0702209000": "Bantayan Island",
    "0907211000": "Roxas (ZN)",
    "0907214000": "Sergio Osmena",
    "0907226000": "Bacungan Leon B. Postigo",
    "0908312000": "R.T. Lim",
    "0990101000": "Isabela (BAS)",
    "1004217000": "Don Victoriano Chiongbian",
    "1102317000": "Igacos",
    "1102324000": "San Isidro (DN)",
    "1204711000": "Pigcawayan",
    "1206512000": "Senator Ninoy Aquino",
    "1381300000": "Quezon (MM)",
    "1381400000": "San Juan (MM)",
    "1705323000": "Rizal (PN)",
    "1804508000": "E. B. Magalona",
    "1830200000": "Bacolod (NO)",
    "1908704000": "Datu Blah Sinsuat",
}

FINAL_MAPPING_SCHEMA = StructType(
    [
        StructField(
            "psgc_code",
            StringType(),
            nullable=False,
        ),
        StructField(
            "psgc_name",
            StringType(),
            nullable=False,
        ),
        StructField(
            "cmci_name",
            StringType(),
            nullable=True,
        ),
        StructField(
            "match_status",
            StringType(),
            nullable=False,
        ),
        StructField(
            "match_method",
            StringType(),
            nullable=True,
        ),
        StructField(
            "reviewed",
            BooleanType(),
            nullable=False,
        ),
        StructField(
            "is_active",
            BooleanType(),
            nullable=False,
        ),
        StructField(
            "created_timestamp",
            TimestampType(),
            nullable=False,
        ),
        StructField(
            "updated_timestamp",
            TimestampType(),
            nullable=False,
        ),
    ]
)

# ------------------------------------------------------------------
# Candidate name normalization
# ------------------------------------------------------------------

ABBREVIATION_PATTERNS = [
    (
        r"\bsta\.?\b",
        "santa",
    ),
    (
        r"\bsto\.?\b",
        "santo",
    ),
    (
        r"\bst\.?\b",
        "saint",
    ),
    (
        r"\bgen\.?\b",
        "general",
    ),
    (
        r"\bgov\.?\b",
        "governor",
    ),
    (
        r"\bpres\.?\b",
        "president",
    ),
    (
        r"\bmt\.?\b",
        "mount",
    ),
]

def normalize_baseline_name(
    name,
):
    """
    Normalize an LGU name for conservative automatic matching.

    This intentionally does not expand abbreviations.
    Reviewed aliases and abbreviations are handled separately.
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
        if not unicodedata.combining(
            character
        )
    )

    normalized_name = (
        normalized_name
        .lower()
        .strip()
    )

    normalized_name = re.sub(
        r"^municipality of\s+",
        "",
        normalized_name,
    )

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


def compact_baseline_name(
    name,
):
    """
    Remove spaces from a conservatively normalized LGU name.
    """

    normalized_name = normalize_baseline_name(
        name
    )

    if normalized_name is None:
        return None

    return normalized_name.replace(
        " ",
        ""
    )

def normalize_candidate_name(
    name,
):
    """
    Normalize an LGU name for controlled mapping comparison.
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
        if not unicodedata.combining(
            character
        )
    )

    normalized_name = (
        normalized_name
        .lower()
        .strip()
    )

    normalized_name = re.sub(
        r"^municipality of\s+",
        "",
        normalized_name,
    )

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

    for pattern, replacement in (
        ABBREVIATION_PATTERNS
    ):
        normalized_name = re.sub(
            pattern,
            replacement,
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

def split_cmci_name_suffix(
    cmci_name,
):
    """
    Separate the CMCI base locality name and trailing qualifier.
    """

    if cmci_name is None:
        return None, None

    cleaned_name = str(
        cmci_name
    ).strip()

    suffix_match = re.search(
        r"\s*\(([^()]+)\)\s*$",
        cleaned_name,
    )

    if suffix_match is None:
        return cleaned_name, None

    cmci_suffix = suffix_match.group(
        1
    ).strip().upper()

    cmci_base_name = cleaned_name[
        :suffix_match.start()
    ].strip()

    return cmci_base_name, cmci_suffix

# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    spark.conf.set(
        "spark.sql.session.timeZone",
        "UTC",
    )

    print("CMCI mapping generation started")
    print("PSGC source: " + LGU_MASTER_TABLE)
    print("CMCI mapping target: " + CMCI_MAP_TABLE)


    # ------------------------------------------------------------------
    # Validate required tables
    # ------------------------------------------------------------------

    required_tables = [
        LGU_MASTER_TABLE,
        CMCI_MAP_TABLE,
    ]

    for table_name in required_tables:
        if not spark.catalog.tableExists(
            table_name
        ):
            raise RuntimeError(
                "Required table does not exist: "
                + table_name
            )

    print("Required tables found")


    # ------------------------------------------------------------------
    # Load active PSGC LGUs
    # ------------------------------------------------------------------

    active_lgu_df = (
        spark.table(
            LGU_MASTER_TABLE
        )
        .where(
            F.col("is_active") == True
        )
        .select(
            F.trim("psgc_code").alias(
                "psgc_code"
            ),
            F.trim("lgu_name").alias(
                "psgc_name"
            ),
            F.trim("province_code").alias(
                "province_code"
            ),
            F.trim("province_name").alias(
                "province_name"
            ),
        )
        .where(
            F.col("psgc_code").isNotNull()
        )
        .where(
            F.col("psgc_code") != ""
        )
        .where(
            F.col("psgc_name").isNotNull()
        )
        .where(
            F.col("psgc_name") != ""
        )
    )

    active_lgu_count = (
        active_lgu_df.count()
    )

    if active_lgu_count != EXPECTED_PSGC_COUNT:
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_PSGC_COUNT)
            + " active PSGC LGUs, found "
            + str(active_lgu_count)
        )

    duplicate_psgc_count = (
        active_lgu_df
        .groupBy(
            "psgc_code"
        )
        .count()
        .where(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_psgc_count > 0:
        raise RuntimeError(
            "Active LGU master contains "
            + str(duplicate_psgc_count)
            + " duplicate PSGC codes"
        )

    active_lgu_rows = (
        active_lgu_df
        .orderBy(
            "psgc_code"
        )
        .collect()
    )

    print(
        "Active PSGC LGUs loaded: "
        + str(active_lgu_count)
    )

    # ------------------------------------------------------------------
    # Read live CMCI locality names
    # ------------------------------------------------------------------

    session = create_http_session()

    try:
        portal_response = session.get(
            PORTAL_URL,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

        portal_response.raise_for_status()

        portal_html = portal_response.text.strip()

    finally:
        session.close()

    if not portal_html:
        raise RuntimeError(
            "CMCI portal returned an empty response"
        )

    portal_soup = BeautifulSoup(
        portal_html,
        "html.parser",
    )

    lgu_select = portal_soup.find(
        "select",
        attrs={
            "id": "lgu",
        },
    )

    if lgu_select is None:
        raise RuntimeError(
            "CMCI LGU selector was not found"
        )

    cmci_names = []

    for option in lgu_select.find_all(
        "option"
    ):
        option_value = option.get(
            "value"
        )

        if option_value is None:
            continue

        option_value = option_value.strip()

        if option_value:
            cmci_names.append(
                option_value
            )

    cmci_names = list(
        dict.fromkeys(
            cmci_names
        )
    )

    cmci_name_set = set(
        cmci_names
    )

    if len(cmci_names) != EXPECTED_CMCI_NAME_COUNT:
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_CMCI_NAME_COUNT)
            + " live CMCI locality names, found "
            + str(len(cmci_names))
    )   

    print(
        "Live CMCI locality names: "
        + str(len(cmci_names))
    )

    # ------------------------------------------------------------------
    # Build CMCI name lookups
    # ------------------------------------------------------------------

    cmci_exact_lookup = {}
    cmci_normalized_lookup = {}
    cmci_compact_lookup = {}

    for cmci_name in cmci_names:
        exact_key = (
            cmci_name
            .lower()
            .strip()
        )

        if exact_key in cmci_exact_lookup:
            raise RuntimeError(
                "Duplicate exact CMCI name: "
                + cmci_name
            )

        cmci_exact_lookup[
            exact_key
        ] = cmci_name

        normalized_key = normalize_baseline_name(
            cmci_name
        )

        cmci_normalized_lookup.setdefault(
            normalized_key,
            [],
        ).append(
            cmci_name
        )

        compact_key = compact_baseline_name(
            cmci_name
        )

        cmci_compact_lookup.setdefault(
            compact_key,
            [],
        ).append(
            cmci_name
        )


    # ------------------------------------------------------------------
    # Count PSGC baseline names
    # ------------------------------------------------------------------

    psgc_normalized_counts = {}
    psgc_compact_counts = {}

    for lgu_row in active_lgu_rows:
        normalized_key = normalize_baseline_name(
            lgu_row["psgc_name"]
        )

        compact_key = compact_baseline_name(
            lgu_row["psgc_name"]
        )

        psgc_normalized_counts[
            normalized_key
        ] = (
            psgc_normalized_counts.get(
                normalized_key,
                0,
            )
            + 1
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


    # ------------------------------------------------------------------
    # Validate reviewed mapping configuration
    # ------------------------------------------------------------------

    reviewed_mapping_codes = (
        set(
            CMCI_REVIEWED_MANUAL_MAPPINGS
        )
        | set(
            CMCI_REVIEWED_MANUAL_ALIASES
        )
    )

    duplicate_reviewed_code_count = (
        len(
            CMCI_REVIEWED_MANUAL_MAPPINGS
        )
        + len(
            CMCI_REVIEWED_MANUAL_ALIASES
        )
        - len(
            reviewed_mapping_codes
        )
    )

    if duplicate_reviewed_code_count > 0:
        raise RuntimeError(
            "Reviewed mapping dictionaries contain "
            + str(duplicate_reviewed_code_count)
            + " duplicate PSGC codes"
        )

    reviewed_cmci_names = (
        list(
            CMCI_REVIEWED_MANUAL_MAPPINGS.values()
        )
        + list(
            CMCI_REVIEWED_MANUAL_ALIASES.values()
        )
        + list(
            CMCI_CONFLICTING_SUFFIX_EXCEPTIONS.values()
        )
    )

    duplicate_reviewed_name_count = (
        len(reviewed_cmci_names)
        - len(set(reviewed_cmci_names))
    )

    if duplicate_reviewed_name_count > 0:
        raise RuntimeError(
            "Reviewed mapping configuration contains "
            + str(duplicate_reviewed_name_count)
            + " duplicate CMCI names"
        )

    missing_reviewed_cmci_names = sorted(
        set(reviewed_cmci_names)
        - cmci_name_set
    )

    if missing_reviewed_cmci_names:
        raise RuntimeError(
            "Reviewed CMCI names are missing from "
            + "the live portal: "
            + str(missing_reviewed_cmci_names)
        )

    print("CMCI lookup validation passed")

    # ------------------------------------------------------------------
    # Generate baseline mapping records
    # ------------------------------------------------------------------

    mapping_records_by_psgc = {}
    assigned_cmci_names = set()

    baseline_counts = {
        MATCH_METHOD_EXACT: 0,
        MATCH_METHOD_NORMALIZED: 0,
        MATCH_METHOD_COMPACT: 0,
        MATCH_METHOD_MANUAL: 0,
    }

    load_timestamp = datetime.now(
        timezone.utc
    )

    for lgu_row in active_lgu_rows:
        psgc_code = lgu_row[
            "psgc_code"
        ]

        psgc_name = lgu_row[
            "psgc_name"
        ]

        exact_key = (
            psgc_name
            .lower()
            .strip()
        )

        normalized_key = normalize_baseline_name(
            psgc_name
        )

        compact_key = compact_baseline_name(
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

        compact_matches = (
            cmci_compact_lookup.get(
                compact_key,
                [],
            )
        )

        reviewed_manual_name = (
            CMCI_REVIEWED_MANUAL_MAPPINGS.get(
                psgc_code
            )
        )

        cmci_name = None
        match_method = None

        if reviewed_manual_name is not None:
            cmci_name = reviewed_manual_name
            match_method = MATCH_METHOD_MANUAL

        elif exact_match is not None:
            cmci_name = exact_match
            match_method = MATCH_METHOD_EXACT

        elif (
            len(normalized_matches) == 1
            and psgc_normalized_counts.get(
                normalized_key,
                0,
            ) == 1
        ):
            cmci_name = normalized_matches[0]
            match_method = MATCH_METHOD_NORMALIZED

        elif (
            len(compact_matches) == 1
            and psgc_compact_counts.get(
                compact_key,
                0,
            ) == 1
        ):
            cmci_name = compact_matches[0]
            match_method = MATCH_METHOD_COMPACT

        if cmci_name is not None:
            if cmci_name in assigned_cmci_names:
                raise RuntimeError(
                    "Baseline mapping reused CMCI name: "
                    + cmci_name
                )

            assigned_cmci_names.add(
                cmci_name
            )

            match_status = MATCH_STATUS_MATCHED
            reviewed = True
            is_active = True

            baseline_counts[
                match_method
            ] += 1

        else:
            match_status = MATCH_STATUS_UNMATCHED
            reviewed = False
            is_active = False

        mapping_records_by_psgc[
            psgc_code
        ] = {
            "psgc_code": psgc_code,
            "psgc_name": psgc_name,
            "cmci_name": cmci_name,
            "match_status": match_status,
            "match_method": match_method,
            "reviewed": reviewed,
            "is_active": is_active,
            "created_timestamp": load_timestamp,
            "updated_timestamp": load_timestamp,
        }


    # ------------------------------------------------------------------
    # Validate baseline mapping counts
    # ------------------------------------------------------------------

    expected_baseline_counts = {
        MATCH_METHOD_EXACT: (
            EXPECTED_EXACT_COUNT
        ),
        MATCH_METHOD_NORMALIZED: (
            EXPECTED_NORMALIZED_COUNT
        ),
        MATCH_METHOD_COMPACT: (
            EXPECTED_COMPACT_COUNT
        ),
        MATCH_METHOD_MANUAL: (
            EXPECTED_MANUAL_COUNT
        ),
    }

    for (
        match_method,
        expected_count,
    ) in expected_baseline_counts.items():
        actual_count = baseline_counts.get(
            match_method,
            0,
        )

        if actual_count != expected_count:
            raise RuntimeError(
                "Expected "
                + str(expected_count)
                + " "
                + match_method
                + " mappings, found "
                + str(actual_count)
            )

    baseline_matched_count = sum(
        baseline_counts.values()
    )

    baseline_unmatched_count = (
        len(mapping_records_by_psgc)
        - baseline_matched_count
    )

    if (
    baseline_matched_count
    != EXPECTED_BASELINE_MATCHED_COUNT
    ):
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_BASELINE_MATCHED_COUNT)
            + " baseline matched mappings, found "
            + str(baseline_matched_count)
        )

    if (
        baseline_unmatched_count
        != EXPECTED_BASELINE_UNMATCHED_COUNT
    ):
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_BASELINE_UNMATCHED_COUNT)
            + " baseline unmatched mappings, found "
            + str(baseline_unmatched_count)
        )

    print()
    print("BASELINE MAPPING COMPLETE")

    for match_method in (
        MATCH_METHOD_EXACT,
        MATCH_METHOD_NORMALIZED,
        MATCH_METHOD_COMPACT,
        MATCH_METHOD_MANUAL,
    ):
        print(
            match_method
            + ": "
            + str(
                baseline_counts[
                    match_method
                ]
            )
        )

    print(
        "Baseline matched: "
        + str(baseline_matched_count)
    )

    print(
        "Baseline unmatched: "
        + str(baseline_unmatched_count)
    )


    # ------------------------------------------------------------------
    # Prepare unused CMCI names and unmatched PSGC rows
    # ------------------------------------------------------------------

    unused_cmci_names = sorted(
        cmci_name_set
        - assigned_cmci_names
    )

    if (
        len(unused_cmci_names)
        != EXPECTED_RECONCILIATION_COUNT
    ):
        raise RuntimeError(
            "Expected 371 unused CMCI names after "
            + "baseline mapping, found "
            + str(len(unused_cmci_names))
        )

    unmatched_rows = []

    for lgu_row in active_lgu_rows:
        psgc_code = lgu_row[
            "psgc_code"
        ]

        mapping_record = (
            mapping_records_by_psgc[
                psgc_code
            ]
        )

        if (
            mapping_record["match_status"]
            != MATCH_STATUS_UNMATCHED
        ):
            continue

        unmatched_rows.append(
            {
                "psgc_code": psgc_code,
                "psgc_name": lgu_row[
                    "psgc_name"
                ],
                "province_code": lgu_row[
                    "province_code"
                ],
                "province_name": lgu_row[
                    "province_name"
                ],
            }
        )

    if (
        len(unmatched_rows)
        != EXPECTED_BASELINE_UNMATCHED_COUNT
    ):
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_BASELINE_UNMATCHED_COUNT)
            + " baseline unmatched mappings, found "
            + str(baseline_unmatched_count)
        )

    print(
        "Unused CMCI locality names: "
        + str(len(unused_cmci_names))
    )

    print(
        "Unmatched PSGC rows: "
        + str(len(unmatched_rows))
    )

    # ------------------------------------------------------------------
    # Generate province-suffix updates
    # ------------------------------------------------------------------

    province_suffix_updates = []

    for cmci_name in unused_cmci_names:
        cmci_base_name, cmci_suffix = (
            split_cmci_name_suffix(
                cmci_name
            )
        )

        if cmci_suffix is None:
            continue

        if cmci_suffix in CMCI_CONFLICTING_SUFFIXES:
            continue

        suffix_province_name = (
            CMCI_SUFFIX_PROVINCE_LOOKUP.get(
                cmci_suffix
            )
        )

        if suffix_province_name is None:
            continue

        normalized_cmci_base_name = (
            normalize_candidate_name(
                cmci_base_name
            )
        )

        matching_psgc_rows = []

        for unmatched_row in unmatched_rows:
            if unmatched_row["province_name"] is None:
                continue

            normalized_psgc_name = (
                normalize_candidate_name(
                    unmatched_row["psgc_name"]
                )
            )

            normalized_psgc_province = (
                normalize_candidate_name(
                    unmatched_row["province_name"]
                )
            )

            normalized_cmci_province = (
                normalize_candidate_name(
                    suffix_province_name
                )
            )

            if (
                normalized_psgc_name
                == normalized_cmci_base_name
                and normalized_psgc_province
                == normalized_cmci_province
            ):
                matching_psgc_rows.append(
                    unmatched_row
                )

        if len(matching_psgc_rows) != 1:
            continue

        matched_row = matching_psgc_rows[0]

        province_suffix_updates.append(
            {
                "psgc_code": matched_row[
                    "psgc_code"
                ],
                "cmci_name": cmci_name,
                "match_method": (
                    "PROVINCE_SUFFIX"
                ),
            }
        )

    if (
        len(province_suffix_updates)
        != EXPECTED_PROVINCE_SUFFIX_COUNT
    ):
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_PROVINCE_SUFFIX_COUNT)
            + " province-suffix updates, found "
            + str(
                len(
                    province_suffix_updates
                )
            )
        )

    print(
        "Province-suffix updates: "
        + str(
            len(
                province_suffix_updates
            )
        )
    )

    for update_record in province_suffix_updates:
        psgc_code = update_record[
            "psgc_code"
        ]

        mapping_record = (
            mapping_records_by_psgc[
                psgc_code
            ]
        )

        if (
            mapping_record["match_status"]
            != MATCH_STATUS_UNMATCHED
        ):
            raise RuntimeError(
                "Province-suffix target is already matched: "
                + psgc_code
            )

        cmci_name = update_record[
            "cmci_name"
        ]

        if cmci_name in assigned_cmci_names:
            raise RuntimeError(
                "Province-suffix mapping reuses CMCI name: "
                + cmci_name
            )

        mapping_record.update(
            {
                "cmci_name": cmci_name,
                "match_status": (
                    MATCH_STATUS_MATCHED
                ),
                "match_method": (
                    MATCH_METHOD_PROVINCE_SUFFIX
                ),
                "reviewed": True,
                "is_active": True,
                "updated_timestamp": load_timestamp,
            }
        )

        assigned_cmci_names.add(
            cmci_name
        )

    # ------------------------------------------------------------------
    # Generate reviewed DS exception updates
    # ------------------------------------------------------------------

    suffix_exception_updates = []

    unused_cmci_name_set = set(
        unused_cmci_names
    )

    for unmatched_row in unmatched_rows:
        if unmatched_row["province_name"] is None:
            continue

        exception_key = (
            normalize_candidate_name(
                unmatched_row["psgc_name"]
            ),
            normalize_candidate_name(
                unmatched_row["province_name"]
            ),
        )

        cmci_name = (
            CMCI_CONFLICTING_SUFFIX_EXCEPTIONS.get(
                exception_key
            )
        )

        if cmci_name is None:
            continue

        if cmci_name not in unused_cmci_name_set:
            raise RuntimeError(
                "Reviewed suffix exception is not an "
                + "unused CMCI name: "
                + cmci_name
            )

        suffix_exception_updates.append(
            {
                "psgc_code": unmatched_row[
                    "psgc_code"
                ],
                "cmci_name": cmci_name,
                "match_method": (
                    "PROVINCE_SUFFIX_EXCEPTION"
                ),
            }
        )

    if (
        len(suffix_exception_updates)
        != EXPECTED_SUFFIX_EXCEPTION_COUNT
    ):
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_SUFFIX_EXCEPTION_COUNT)
            + " suffix-exception updates, found "
            + str(
                len(
                    suffix_exception_updates
                )
            )
        )

    print(
        "Suffix-exception updates: "
        + str(
            len(
                suffix_exception_updates
            )
        )
    )

    for update_record in suffix_exception_updates:
        psgc_code = update_record[
            "psgc_code"
        ]

        mapping_record = (
            mapping_records_by_psgc[
                psgc_code
            ]
        )

        if (
            mapping_record["match_status"]
            != MATCH_STATUS_UNMATCHED
        ):
            raise RuntimeError(
                "Suffix exception target is already matched: "
                + psgc_code
            )

        cmci_name = update_record[
            "cmci_name"
        ]

        if cmci_name in assigned_cmci_names:
            raise RuntimeError(
                "Suffix exception reuses CMCI name: "
                + cmci_name
            )

        mapping_record.update(
            {
                "cmci_name": cmci_name,
                "match_status": (
                    MATCH_STATUS_MATCHED
                ),
                "match_method": (
                    MATCH_METHOD_SUFFIX_EXCEPTION
                ),
                "reviewed": True,
                "is_active": True,
                "updated_timestamp": load_timestamp,
            }
        )

        assigned_cmci_names.add(
            cmci_name
        )

    # ------------------------------------------------------------------
    # Generate reviewed manual-alias updates
    # ------------------------------------------------------------------

    manual_alias_updates = []

    unmatched_psgc_code_set = {
        row["psgc_code"]
        for row in unmatched_rows
    }

    for (
        psgc_code,
        cmci_name,
    ) in CMCI_REVIEWED_MANUAL_ALIASES.items():

        if psgc_code not in unmatched_psgc_code_set:
            raise RuntimeError(
                "Reviewed manual PSGC code is not "
                + "currently unmatched: "
                + psgc_code
            )

        if cmci_name not in cmci_name_set:
            raise RuntimeError(
                "Reviewed manual CMCI name is missing "
                + "from the live portal: "
                + cmci_name
            )

        if cmci_name not in unused_cmci_name_set:
            raise RuntimeError(
                "Reviewed manual CMCI name is already "
                + "assigned: "
                + cmci_name
            )

        manual_alias_updates.append(
            {
                "psgc_code": psgc_code,
                "cmci_name": cmci_name,
                "match_method": "MANUAL_ALIAS",
            }
        )

    if (
        len(manual_alias_updates)
        != EXPECTED_MANUAL_ALIAS_COUNT
    ):
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_MANUAL_ALIAS_COUNT)
            + " manual alias updates, found "
            + str(
                len(
                    manual_alias_updates
                )
            )
        )

    print(
        "Manual-alias updates: "
        + str(
            len(
                manual_alias_updates
            )
        )
    )

    for update_record in manual_alias_updates:
        psgc_code = update_record[
            "psgc_code"
        ]

        mapping_record = (
            mapping_records_by_psgc[
                psgc_code
            ]
        )

        if (
            mapping_record["match_status"]
            != MATCH_STATUS_UNMATCHED
        ):
            raise RuntimeError(
                "Manual alias target is already matched: "
                + psgc_code
            )

        cmci_name = update_record[
            "cmci_name"
        ]

        if cmci_name in assigned_cmci_names:
            raise RuntimeError(
                "Manual alias reuses CMCI name: "
                + cmci_name
            )

        mapping_record.update(
            {
                "cmci_name": cmci_name,
                "match_status": (
                    MATCH_STATUS_MATCHED
                ),
                "match_method": (
                    MATCH_METHOD_MANUAL_ALIAS
                ),
                "reviewed": True,
                "is_active": True,
                "updated_timestamp": load_timestamp,
            }
        )

        assigned_cmci_names.add(
            cmci_name
        )

    # ------------------------------------------------------------------
    # Build final mapping records
    # ------------------------------------------------------------------

    final_mapping_records = [
        mapping_records_by_psgc[psgc_code]
        for psgc_code in sorted(
            mapping_records_by_psgc
        )
    ]

    final_total_count = len(
        final_mapping_records
    )

    final_matched_records = [
        record
        for record in final_mapping_records
        if record["match_status"]
        == MATCH_STATUS_MATCHED
    ]

    final_unmatched_records = [
        record
        for record in final_mapping_records
        if record["match_status"]
        == MATCH_STATUS_UNMATCHED
    ]

    final_matched_count = len(
        final_matched_records
    )

    final_unmatched_count = len(
        final_unmatched_records
    )

    if final_total_count != EXPECTED_PSGC_COUNT:
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_PSGC_COUNT)
            + " final mapping records, found "
            + str(final_total_count)
        )

    if (
        final_matched_count
        != EXPECTED_FINAL_MATCHED_COUNT
    ):
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_FINAL_MATCHED_COUNT)
            + " final matched mappings, found "
            + str(final_matched_count)
        )

    if (
        final_unmatched_count
        != EXPECTED_FINAL_UNMATCHED_COUNT
    ):
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_FINAL_UNMATCHED_COUNT)
            + " final unmatched mappings, found "
            + str(final_unmatched_count)
        )


    # ------------------------------------------------------------------
    # Validate final mapping uniqueness
    # ------------------------------------------------------------------

    final_psgc_codes = [
        record["psgc_code"]
        for record in final_mapping_records
    ]

    final_cmci_names = [
        record["cmci_name"]
        for record in final_matched_records
    ]

    duplicate_psgc_count = (
        len(final_psgc_codes)
        - len(set(final_psgc_codes))
    )

    duplicate_cmci_count = (
        len(final_cmci_names)
        - len(set(final_cmci_names))
    )

    if duplicate_psgc_count > 0:
        raise RuntimeError(
            "Final mapping contains "
            + str(duplicate_psgc_count)
            + " duplicate PSGC codes"
        )

    if duplicate_cmci_count > 0:
        raise RuntimeError(
            "Final mapping contains "
            + str(duplicate_cmci_count)
            + " duplicate CMCI names"
        )

    unused_final_cmci_names = sorted(
        cmci_name_set
        - set(final_cmci_names)
    )

    if unused_final_cmci_names:
        raise RuntimeError(
            "Final mapping leaves unused CMCI names: "
            + str(unused_final_cmci_names)
        )


    # ------------------------------------------------------------------
    # Validate matched and unmatched records
    # ------------------------------------------------------------------

    invalid_matched_records = [
        record
        for record in final_matched_records
        if (
            not record["cmci_name"]
            or not record["match_method"]
            or record["reviewed"] is not True
            or record["is_active"] is not True
        )
    ]

    invalid_unmatched_records = [
        record
        for record in final_unmatched_records
        if (
            record["cmci_name"] is not None
            or record["match_method"] is not None
            or record["reviewed"] is not False
            or record["is_active"] is not False
        )
    ]

    if invalid_matched_records:
        raise RuntimeError(
            "Final mapping contains "
            + str(len(invalid_matched_records))
            + " invalid matched records"
        )

    if invalid_unmatched_records:
        raise RuntimeError(
            "Final mapping contains "
            + str(len(invalid_unmatched_records))
            + " invalid unmatched records"
        )


    # ------------------------------------------------------------------
    # Validate final method counts
    # ------------------------------------------------------------------

    final_method_counts = {}

    for record in final_matched_records:
        match_method = record[
            "match_method"
        ]

        final_method_counts[
            match_method
        ] = (
            final_method_counts.get(
                match_method,
                0,
            )
            + 1
        )

    expected_final_method_counts = {
        MATCH_METHOD_EXACT: (
            EXPECTED_EXACT_COUNT
        ),
        MATCH_METHOD_NORMALIZED: (
            EXPECTED_NORMALIZED_COUNT
        ),
        MATCH_METHOD_COMPACT: (
            EXPECTED_COMPACT_COUNT
        ),
        MATCH_METHOD_MANUAL: (
            EXPECTED_MANUAL_COUNT
        ),
        MATCH_METHOD_PROVINCE_SUFFIX: (
            EXPECTED_PROVINCE_SUFFIX_COUNT
        ),
        MATCH_METHOD_SUFFIX_EXCEPTION: (
            EXPECTED_SUFFIX_EXCEPTION_COUNT
        ),
        MATCH_METHOD_MANUAL_ALIAS: (
            EXPECTED_MANUAL_ALIAS_COUNT
        ),
    }

    if (
        final_method_counts
        != expected_final_method_counts
    ):
        raise RuntimeError(
            "Final mapping method counts do not match. "
            + "Expected "
            + str(expected_final_method_counts)
            + ", found "
            + str(final_method_counts)
        )

    print()
    print("FINAL MAPPING VALIDATION PASSED")

    print(
        "Total mapping records: "
        + str(final_total_count)
    )

    print(
        "Matched mappings: "
        + str(final_matched_count)
    )

    print(
        "Unmatched mappings: "
        + str(final_unmatched_count)
    )

    print(
        "Duplicate PSGC codes: "
        + str(duplicate_psgc_count)
    )

    print(
        "Duplicate CMCI names: "
        + str(duplicate_cmci_count)
    )

    print(
        "Unused CMCI names: "
        + str(len(unused_final_cmci_names))
    )

    print()
    print("FINAL MATCH METHOD COUNTS")

    for match_method in sorted(
        final_method_counts
    ):
        print(
            match_method
            + ": "
            + str(
                final_method_counts[
                    match_method
                ]
            )
        )

    print()
    print("REMAINING UNMATCHED PSGC LGUS")

    for record in final_unmatched_records:
        print(
            record["psgc_code"]
            + " | "
            + record["psgc_name"]
        )

    # ------------------------------------------------------------------
    # Create final mapping DataFrame
    # ------------------------------------------------------------------

    final_mapping_df = spark.createDataFrame(
        final_mapping_records,
        schema=FINAL_MAPPING_SCHEMA,
    )

    final_dataframe_count = (
        final_mapping_df.count()
    )

    if final_dataframe_count != EXPECTED_PSGC_COUNT:
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_PSGC_COUNT)
            + " final DataFrame rows, found "
            + str(final_dataframe_count)
        )

    target_columns = spark.table(
        CMCI_MAP_TABLE
    ).columns

    missing_target_columns = sorted(
        set(target_columns)
        - set(final_mapping_df.columns)
    )

    unexpected_source_columns = sorted(
        set(final_mapping_df.columns)
        - set(target_columns)
    )

    if missing_target_columns:
        raise RuntimeError(
            "Generated mapping is missing target columns: "
            + str(missing_target_columns)
        )

    if unexpected_source_columns:
        raise RuntimeError(
            "Generated mapping contains unexpected columns: "
            + str(unexpected_source_columns)
        )

    final_mapping_df = final_mapping_df.select(
        *target_columns
    )

    print()
    print("FINAL MAPPING DATAFRAME READY")

    print(
        "Rows prepared: "
        + str(final_dataframe_count)
    )

    print(
        "Columns prepared: "
        + str(len(final_mapping_df.columns))
    )

    print(
        "No mapping records were written."
    )

    # ------------------------------------------------------------------
    # Guarded snapshot write
    # ------------------------------------------------------------------

    if not APPLY_WRITE:
        print()
        print("DRY RUN COMPLETE")
        print(
            "Set APPLY_WRITE = True to replace "
            + "the CMCI mapping snapshot."
        )
        return

    print()
    print("WRITE ENABLED")

    (
        final_mapping_df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "false",
        )
        .saveAsTable(
            CMCI_MAP_TABLE
        )
    )

    print(
        "Final CMCI mapping snapshot written"
    )

    # ------------------------------------------------------------------
    # Validate saved mapping table
    # ------------------------------------------------------------------

    saved_mapping_df = spark.table(
        CMCI_MAP_TABLE
    )

    saved_total_count = (
        saved_mapping_df.count()
    )

    saved_matched_count = (
        saved_mapping_df
        .where(
            F.col("match_status")
            == MATCH_STATUS_MATCHED
        )
        .where(
            F.col("reviewed") == True
        )
        .where(
            F.col("is_active") == True
        )
        .count()
    )

    saved_unmatched_count = (
        saved_mapping_df
        .where(
            F.col("match_status")
            == MATCH_STATUS_UNMATCHED
        )
        .count()
    )

    saved_duplicate_psgc_count = (
        saved_mapping_df
        .groupBy(
            "psgc_code"
        )
        .count()
        .where(
            F.col("count") > 1
        )
        .count()
    )

    saved_duplicate_cmci_count = (
        saved_mapping_df
        .where(
            F.col("match_status")
            == MATCH_STATUS_MATCHED
        )
        .groupBy(
            "cmci_name"
        )
        .count()
        .where(
            F.col("count") > 1
        )
        .count()
    )

    if saved_total_count != EXPECTED_PSGC_COUNT:
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_PSGC_COUNT)
            + " saved rows, found "
            + str(saved_total_count)
        )

    if (
        saved_matched_count
        != EXPECTED_FINAL_MATCHED_COUNT
    ):
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_FINAL_MATCHED_COUNT)
            + " saved matched mappings, found "
            + str(saved_matched_count)
        )

    if (
        saved_unmatched_count
        != EXPECTED_FINAL_UNMATCHED_COUNT
    ):
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_FINAL_UNMATCHED_COUNT)
            + " saved unmatched mappings, found "
            + str(saved_unmatched_count)
        )

    if saved_duplicate_psgc_count > 0:
        raise RuntimeError(
            "Saved mapping contains duplicate PSGC codes"
        )

    if saved_duplicate_cmci_count > 0:
        raise RuntimeError(
            "Saved mapping contains duplicate CMCI names"
        )

    print()
    print("CMCI MAPPING LOAD COMPLETE")

    print(
        "Total mappings saved: "
        + str(saved_total_count)
    )

    print(
        "Matched mappings saved: "
        + str(saved_matched_count)
    )

    print(
        "Unmatched mappings saved: "
        + str(saved_unmatched_count)
    )

    print(
        "Duplicate PSGC codes: "
        + str(saved_duplicate_psgc_count)
    )

    print(
        "Duplicate CMCI names: "
        + str(saved_duplicate_cmci_count)
    )

if __name__ == "__main__":
    main()  
