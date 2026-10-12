# Match LDRRMF LGU names to PSGC codes
# Builds the lookup table reference.blgf_ldrrmf_lgu_map: one row per distinct LGU as BLGF wrote it
# (region + province + LGU name + LGU type), with the PSGC code it matches and how it was matched.
# Run this AFTER the PSGC silver table (silver.psa_psgc_clean) and BEFORE clean_blgf_ldrrmf_annual_lgu.sql,
# which joins the map to add psgc_code to silver.
# What it does:
#   1. Reads the distinct LGUs from bronze and trims them the same way silver does (2,055 across the seven years:
#      the same LGU can appear under two regions because BLGF writes some regions differently by year).
#   2. Reads the provinces, cities and municipalities of one PSGC snapshot (PSGC_PERIOD) from silver.psa_psgc_clean.
#      A city or municipality whose province_psgc has no PROV row (43 of them: NCR, HUCs, Isabela City and the like) gets no province.
#   3. Matches each LGU in this order, stopping at the first rule that finds exactly one PSGC entry:
#        override          a row in blgf_ldrrmf_lgu_map_overrides.csv (checked by a person)
#        province_name     same name inside the same province
#        no_province_name  same name among PSGC LGUs with no province (NCR, HUCs, Isabela City)
#        name_only         same name anywhere in the country (needs review)
#        old_name          the PSGC "old name" inside the same province (needs review; psa_psgc_clean has no
#                          old names, so this rule finds nothing for now)
#   4. Writes the map (overwrite) and prints the match summary.
# Only "matched" rows give silver a PSGC code. A "needs_review" row becomes "matched" once someone checks it
# and adds it to blgf_ldrrmf_lgu_map_overrides.csv.

import re
import unicodedata
from pathlib import Path

import pandas as pd

# Settings: change here if the team renames anything
CATALOG = "ahon_dev"
BRONZE_TABLE = f"{CATALOG}.bronze.blgf_ldrrmf_annual_lgu"
PSGC_TABLE = f"{CATALOG}.silver.psa_psgc_clean"  # see read_psgc()
PSGC_PERIOD = "Q2_2024"  # the snapshot gold.dim_lgu uses; keep the two the same
MAP_TABLE = f"{CATALOG}.reference.blgf_ldrrmf_lgu_map"
OVERRIDES_FILE = "blgf_ldrrmf_lgu_map_overrides.csv"  # next to this script, or a full path (for example a workspace file path)

KEY_COLUMNS = ["region", "province", "lgu_name", "lgu_type"]

# BLGF province names that are not the current PSGC name. A list means the province was split.
PROVINCE_ALIASES = {
    "COMPOSTELAVALLEY": ["DAVAODEORO"],  # renamed 2019
    "NORTHCOTABATO": ["COTABATO"],
    "WESTERNSAMAR": ["SAMAR"],
    "MAGUINDANAO": ["MAGUINDANAODELNORTE", "MAGUINDANAODELSUR"],  # split 2022
}

# Broken spellings of ñ / Ñ found in the data (UTF-8 read as Windows-1252, once or twice)
BROKEN_ENYE = ["Ãƒâ€˜", "ãƒâ€˜", "Ã±", "Ã‘", "ã‘"]

# Short forms written out before comparing (BLGF and PSGC abbreviate differently)
WORD_FORMS = [
    (r"\bSTO\b\.?", "SANTO"),
    (r"\bSTA\b\.?", "SANTA"),
    (r"\bGEN\b\.?", "GENERAL"),
    (r"\bPRES\b\.?", "PRESIDENT"),
]


def name_key(value):
    # Compare names on a simplified form: upper case, no accents, no text in brackets,
    # no "CITY OF" / "CITY" / "PROVINCE OF", no spaces or punctuation.
    # "City of Las Piñas", "LAS PINAS CITY" and "Las Piñas" all become "LASPINAS".
    if value is None or pd.isna(value):
        return ""
    text = str(value)
    # Repair broken ñ before comparing: PSGC has "Ã±" and the BLGF FY2019 file has "Ãƒâ€˜" (title-cased "ãƒâ€˜" or "ã‘")
    for broken in BROKEN_ENYE:
        text = text.replace(broken, "n")
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().upper()
    text = re.sub(r"\(.*?\)", " ", text)
    for pattern, full in WORD_FORMS:
        text = re.sub(pattern, full, text)
    text = re.sub(r"^\s*(CITY|PROVINCE|MUNICIPALITY) OF\b", " ", text)
    text = re.sub(r"\bCITY\s*$", " ", text)
    return re.sub(r"[^A-Z0-9]", "", text)


def province_keys(value):
    # The PSGC province keys a BLGF province name stands for (more than one if the province was split)
    key = name_key(value)
    return PROVINCE_ALIASES.get(key, [key])


def blgf_level(lgu_type):
    # BLGF "LGU TYPE" to the PSGC geographic level
    return {"PROVINCE": "Prov", "CITY": "City", "MUNICIPALITY": "Mun"}.get(str(lgu_type).strip().upper())


def pick(candidates, level):
    # Exactly one candidate wins. If several, prefer the one with the same level (City vs Mun),
    # because names like "Quezon" exist as both a city and municipalities.
    if len(candidates) > 1:
        same_level = candidates[candidates["level"] == level]
        if len(same_level) >= 1:
            candidates = same_level
    return candidates


def match_one(row, psgc, overrides):
    # Returns psgc_code, psgc_name, match_method, match_status, candidate_count for one BLGF LGU
    level = blgf_level(row["lgu_type"])
    override_key = (name_key(row["province"]), name_key(row["lgu_name"]), str(row["lgu_type"]).strip().upper())

    # 1. A person already checked this LGU
    if override_key in overrides:
        code = overrides[override_key]
        hit = psgc[psgc["psgc_code"] == code]
        if code and len(hit) == 0:
            raise ValueError(f"Override for {override_key} uses a PSGC code that is not in the PSGC table: {code}")
        name = hit["name"].iloc[0] if code else None
        return code or None, name, "override", "matched" if code else "unmatched", len(hit)

    lgu_key = name_key(row["lgu_name"])

    # Province rows: compare with PSGC provinces only
    if level == "Prov":
        hits = psgc[(psgc["level"] == "Prov") & psgc["key"].isin(province_keys(row["lgu_name"]))]
        if len(hits) == 1:
            return hits["psgc_code"].iloc[0], hits["name"].iloc[0], "province_name", "matched", 1
        return None, None, "province_name", "ambiguous" if len(hits) > 1 else "unmatched", len(hits)

    lgus = psgc[psgc["level"].isin(["City", "Mun"])]
    in_province = lgus["province_key"].isin(province_keys(row["province"]))

    steps = [
        ("province_name", lgus[in_province & (lgus["key"] == lgu_key)], "matched"),
        ("no_province_name", lgus[(lgus["province_key"] == "") & (lgus["key"] == lgu_key)], "matched"),
        ("name_only", lgus[lgus["key"] == lgu_key], "needs_review"),
        ("old_name", lgus[in_province & (lgus["old_key"] == lgu_key) & (lgus["old_key"] != "")], "needs_review"),
    ]
    found_several = 0
    for method, candidates, status in steps:
        candidates = pick(candidates, level)
        if len(candidates) == 1:
            return candidates["psgc_code"].iloc[0], candidates["name"].iloc[0], method, status, 1
        found_several = max(found_several, len(candidates))
    return None, None, None, "ambiguous" if found_several > 1 else "unmatched", found_several


def build_map(blgf, psgc, overrides):
    # blgf: distinct KEY_COLUMNS. psgc: psgc_code, name, level (Prov/City/Mun), province_name, old_name.
    # overrides: {(province key, lgu name key, LGU TYPE): psgc_code or ""}
    psgc = psgc.copy()
    psgc["key"] = psgc["name"].map(name_key)
    psgc["province_key"] = psgc["province_name"].map(name_key)
    psgc["old_key"] = psgc["old_name"].map(name_key)
    result = blgf.copy()
    cols = ["psgc_code", "psgc_name", "match_method", "match_status", "candidate_count"]
    result[cols] = [match_one(row, psgc, overrides) for _, row in blgf.iterrows()]
    result["candidate_count"] = result["candidate_count"].astype(int)
    return result


def read_overrides(path):
    # blgf_ldrrmf_lgu_map_overrides.csv columns: province, lgu_name, lgu_type, psgc_code, reason.
    # An empty psgc_code means "checked, no PSGC code" (for example the pre-2022 Maguindanao province row).
    # Stop if the file is missing: without it, every checked LGU silently falls back to "unmatched"
    if not Path(path).exists():
        raise FileNotFoundError(f"Overrides file not found: {Path(path).resolve()}. Put blgf_ldrrmf_lgu_map_overrides.csv there "
                                "or set OVERRIDES_FILE to its full path.")
    table = pd.read_csv(path, dtype=str).fillna("")
    return {
        (name_key(r.province), name_key(r.lgu_name), r.lgu_type.strip().upper()): r.psgc_code.strip()
        for r in table.itertuples()
    }


def read_psgc(spark):
    # Reads one PSGC snapshot from psa_psgc_clean into the columns build_map() expects:
    # psgc_code, name, level (Prov/City/Mun), province_name, old_name.
    # The province name of a city/municipality is looked up from the PROV row with the same code as
    # province_psgc. HUCs and NCR cities point to a code with no PROV row, so they get no province.
    # psa_psgc_clean has no old names, so old_name is empty.
    # If the PSGC silver columns change, change only this function.
    dim = (
        spark.table(PSGC_TABLE)
        .where(f"_api_period = '{PSGC_PERIOD}'")
        .selectExpr("psgc_code", "area_name", "geographic_level", "province_psgc AS province_code")
        .toPandas()
    )
    if dim.empty:
        raise ValueError(f"No rows in {PSGC_TABLE} for _api_period = {PSGC_PERIOD}")
    dim = dim[dim["geographic_level"].isin(["PROV", "CITY", "MUN"])]
    province_names = dict(zip(dim.loc[dim["geographic_level"] == "PROV", "psgc_code"],
                              dim.loc[dim["geographic_level"] == "PROV", "area_name"]))
    is_province = dim["geographic_level"] == "PROV"
    return pd.DataFrame({
        "psgc_code": dim["psgc_code"],
        "name": dim["area_name"],
        "level": dim["geographic_level"].map({"PROV": "Prov", "CITY": "City", "MUN": "Mun"}),
        # Provinces have no parent. A city whose province_code is not a PROV row (an HUC, an NCR city) gets None
        "province_name": [None if prov else province_names.get(code)
                          for prov, code in zip(is_province, dim["province_code"])],
        "old_name": None,
    }).reset_index(drop=True)


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()

    # Distinct LGUs as BLGF wrote them, trimmed like silver, with the years they appear in (useful when reviewing)
    blgf = (
        spark.table(BRONZE_TABLE)
        .select(
            *[F.trim(c).alias(c) for c in KEY_COLUMNS],
            F.regexp_extract("_source_ref", r"FY(\d{4})", 1).cast("int").alias("fiscal_year"),
        )
        .groupBy(*KEY_COLUMNS)
        .agg(F.min("fiscal_year").alias("first_year"), F.max("fiscal_year").alias("last_year"))
        .toPandas()
    )

    overrides_path = Path(__file__).parent / OVERRIDES_FILE if "__file__" in globals() else Path(OVERRIDES_FILE)
    result = build_map(blgf, read_psgc(spark), read_overrides(overrides_path))

    # Every BLGF LGU must appear exactly once in the map, or the silver join would add or lose rows
    if result.duplicated(subset=KEY_COLUMNS).any():
        raise ValueError("The map has the same BLGF LGU twice")

    schema = (
        "region string, province string, lgu_name string, lgu_type string, first_year int, last_year int, "
        "psgc_code string, psgc_name string, match_method string, match_status string, candidate_count int"
    )
    records = result.astype(object).where(result.notna(), None).values.tolist()
    (
        spark.createDataFrame(records, schema)
        .withColumn("_processed_at", F.current_timestamp())
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(MAP_TABLE)
    )

    # Summary to read after each run: how many LGUs matched, by type and status
    print(f"Wrote {len(result)} LGUs to {MAP_TABLE}")
    print(result.groupby(["lgu_type", "match_status"]).size().unstack(fill_value=0))
    print(result.groupby(["match_method"], dropna=False).size())