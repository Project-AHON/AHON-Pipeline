# Load LDRRMF Excel files into bronze
# Reads the 7 BLGF LDRRMF Excel files from the source volume and writes ONE bronze table with all years stacked.
# Suggested repo path: src/sql/datasets/blgf_ldrrmf_annual_lgu/bronze/load_blgf_ldrrmf_annual_lgu.py
# What it does:
#   1. Finds the data sheet and the header rows in each file (the layout differs between years).
#   2. Maps columns by header name, not by position, and renames them to the bronze column names.
#   3. Checks each file's row count against the profile, and stops before writing if any file differs.
#   4. Adds the provenance columns from the naming standard and merges immutable delivery observations.
# Bronze keeps the values as received. Cleaning (fiscal_year, PSGC, flags) happens in silver.
# Run the file through the configured bundle task.

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
from runtime import batch_path, completed_batch, merge_observations, parser, provenance, row_counts

SOURCE_NAME = "blgf_ldrrmf_annual_lgu"

# LGU rows per year from the bronze profile, used to check each file after reading
def source_parser():
    p = parser(__doc__)
    p.add_argument("--expected-rows", required=True, type=row_counts)
    p.add_argument("--file-pattern", required=True)
    return p


def source_name(args, year):
    name = args.file_pattern.format(year=year)
    if Path(name).name != name:
        raise ValueError("file-pattern must produce a safe single filename")
    return name

# Source header -> bronze column name (the dev table's names, kept for now; column naming rules are still open)
TEXT_COLUMNS = {
    "REGION": "region",
    "PROVINCE": "province",
    "LGU NAME": "lgu_name",
    "LGU TYPE": "lgu_type",
    "LGU CODE": "lgu_type",  # FY2021 labels this column "LGU CODE", but it holds the LGU type (Province, City, ...)
}
# The amount columns have a two-level header: a group name over a sub-header
AMOUNT_GROUPS = {
    "70% LDRRMF": "70pct_ldrrmf",
    "30% QUICK RESPONSE FUND": "30pct_quick_response_fund",
    "TOTAL": "total",
}
AMOUNT_PARTS = {
    "Budget Appropriation": "budget_appropriation",
    "Expenditures": "expenditures",
}

# Final column order (the source order differs between years, so this fixes it)
TEXT_NAMES = ["region", "province", "lgu_name", "lgu_type"]
AMOUNT_NAMES = [
    f"{g}_{p}" for g in AMOUNT_GROUPS.values() for p in AMOUNT_PARTS.values()
]
RAW_COLUMNS = TEXT_NAMES + AMOUNT_NAMES


def clean_header(value):
    # Header cell as text, with empty cells as ""
    return "" if pd.isna(value) else str(value).strip()


def read_lgu_file(file):
    # Returns one pandas table with the bronze column names, one row per LGU line of the file

    # The data sheet is the one that is not called Metadata (its name and position change by year)
    excel = pd.ExcelFile(file)
    data_sheets = [s for s in excel.sheet_names if s.strip().lower() != "metadata"]
    if len(data_sheets) != 1:
        raise ValueError(
            f"{file.name}: expected 1 data sheet, found {excel.sheet_names}"
        )
    raw = excel.parse(
        data_sheets[0], header=None
    )  # header=None: every row stays data, read by position

    # The first header row is the one that holds "REGION"; the sub-header row is right below it
    header_rows = [
        i for i in range(len(raw)) if "REGION" in [clean_header(v) for v in raw.iloc[i]]
    ]
    if len(header_rows) != 1:
        raise ValueError(f"{file.name}: could not find the header row")
    first = header_rows[0]
    group_row = raw.iloc[
        first
    ].ffill()  # group names sit over merged cells, so fill them across
    part_row = raw.iloc[first + 1]

    # Map every source column to its bronze name by header text
    names = {}
    for col in raw.columns:
        group = clean_header(group_row[col])
        part = clean_header(part_row[col])
        if group in TEXT_COLUMNS:
            names[col] = TEXT_COLUMNS[group]
        elif group in AMOUNT_GROUPS and part in AMOUNT_PARTS:
            names[col] = f"{AMOUNT_GROUPS[group]}_{AMOUNT_PARTS[part]}"

    # Stop if any expected column is missing or a column is mapped twice
    if sorted(names.values()) != sorted(RAW_COLUMNS):
        raise ValueError(f"{file.name}: unexpected columns {sorted(names.values())}")

    # Data starts two rows below the first header row
    data = raw.iloc[first + 2 :][list(names.keys())].rename(columns=names)[RAW_COLUMNS]

    # Text stays text; amounts become numbers (an amount that is not a number raises an error, on purpose)
    for name in TEXT_NAMES:
        data[name] = data[name].map(lambda v: None if pd.isna(v) else str(v))
    for name in AMOUNT_NAMES:
        data[name] = pd.to_numeric(data[name], errors="raise").astype(
            "float64"
        )  # always float, as Spark expects

    data["_source_ref"] = str(
        file
    )  # where the row came from; silver reads the fiscal year from this
    return data


def main():
    from pyspark.sql import SparkSession
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    args = source_parser().parse_args()
    directory = batch_path(args, SOURCE_NAME)
    manifest = completed_batch(directory)
    expected_names = {source_name(args, year) for year in args.expected_rows}
    if {item["name"] for item in manifest["files"]} != expected_names:
        raise ValueError("Expected exactly the configured BLGF workbooks")
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    schema = StructType(
        [StructField(c, StringType(), True) for c in TEXT_NAMES]
        + [StructField(c, DoubleType(), True) for c in AMOUNT_NAMES]
        + [StructField("_source_ref", StringType(), True)]
    )
    frames = []
    # The seven annual files are bounded by the documented 11,434-row profile.
    # Validate every file before the single Delta commit.
    for year, expected in args.expected_rows.items():
        file = directory / source_name(args, year)
        table = read_lgu_file(file)
        if len(table) != expected:
            raise ValueError(f"FY{year}: expected {expected} rows, found {len(table)}")
        records = table.astype(object).where(table.notna(), None).values.tolist()
        frames.append(spark.createDataFrame(records, schema))
    frame = frames[0]
    for other in frames[1:]:
        frame = frame.unionByName(other)
    frame = provenance(frame, SOURCE_NAME, args.batch_id, RAW_COLUMNS)
    merge_observations(spark, frame, f"{args.catalog}.bronze.{SOURCE_NAME}")


if __name__ == "__main__":
    main()
