"""Load the explicitly configured source snapshot without altering geographic labels."""

import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
from runtime import batch_path, completed_batch, merge_observations, parser, provenance

RAW_COLUMNS = [
    "geographic_location",
    "total_population",
    "urban_population",
    "percent_urban",
    "census_year",
]


def source_parser():
    p = parser(__doc__)
    p.add_argument("--source-file", required=True)
    p.add_argument("--census-year", required=True, type=int)
    p.add_argument("--expected-profile", required=True, type=json.loads)
    return p


def source_rows(args):
    directory = batch_path(args, "psa_population")
    manifest = completed_batch(directory)
    if Path(args.source_file).name != args.source_file or not isinstance(args.expected_profile, list) or len(args.expected_profile) != 4 or any(type(v) is not int or v < 0 for v in args.expected_profile):
        raise ValueError("Expected a safe CSV filename and four approved geographic-level row counts")
    if [item["name"] for item in manifest["files"]] != [args.source_file]:
        raise ValueError("Expected the configured PSA census file")
    file = directory / args.source_file
    rows, parents = [], []
    depths = [0, 0, 0, 0]
    with file.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != [
            "Geographic Location",
            "Total Population",
            "Urban Population",
            "Percent Urban",
        ]:
            raise ValueError("PSA source CSV header differs")
        for row in reader:
            if len(rows) >= sum(args.expected_profile) or None in row or None in row.values():
                raise ValueError(
                    "PSA source exceeds the bounded profile or has a malformed row"
                )
            raw_name = row["Geographic Location"]
            dots = len(raw_name) - len(raw_name.lstrip("."))
            depth = dots // 2
            if dots % 2 or depth > 3 or depth > len(parents):
                raise ValueError("PSA dot-indented hierarchy changed")
            name = raw_name[dots:].strip()
            # PR review requests removing the documented national footnote only in Silver.
            if depth == 0:
                name = name.removesuffix("1/").rstrip()
            parents = parents[:depth] + [name]
            depths[depth] += 1
            rows.append(
                (
                    raw_name,
                    row["Total Population"],
                    row["Urban Population"],
                    row["Percent Urban"],
                    args.census_year,
                    " > ".join(parents),
                )
            )
    if depths != args.expected_profile:
        raise ValueError(f"PSA hierarchy profile differs: {depths}")
    return file, rows


def source_frame(spark, args):
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    file, rows = source_rows(args)
    schema = StructType(
        [
            StructField(c, IntegerType() if c == "census_year" else StringType(), True)
            for c in RAW_COLUMNS + ["geographic_path"]
        ]
    )
    # Only the fixed 1,744-row census file is read on the driver; no growing table collect.
    return spark.createDataFrame(rows, schema).withColumn(
        "_source_ref", F.lit(str(file))
    )


def main():
    from pyspark.sql import SparkSession

    args = source_parser().parse_args()
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    frame = provenance(
        source_frame(spark, args).drop("geographic_path"),
        "psa_population",
        args.batch_id,
        RAW_COLUMNS,
    )
    if frame.select("_row_hash").distinct().count() != sum(args.expected_profile):
        raise ValueError(
            "Source observations collide; keep raw file, request source identity decision"
        )
    merge_observations(spark, frame, f"{args.catalog}.bronze.psa_population")


if __name__ == "__main__":
    main()
