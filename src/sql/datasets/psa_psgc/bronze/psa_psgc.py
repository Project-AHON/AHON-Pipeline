"""Load completed PSGC files, retaining the existing source-value aliases."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "extract"))
from psa_psgc_ingest import EXPECTED_ROWS
from runtime import batch_path, completed_batch, merge_observations, parser, provenance

ALIASES = {
    "code": "psgc_code",
    "area_name": "area_name",
    "correspondence_code": "correspondence_code",
    "geographic_level": "geographic_level",
    "reg": "region_code",
    "prv": "province_code",
    "mun": "municipality_code",
    "bgy": "barangay_code",
    "old_name": "old_name",
    "city_class": "city_class",
    "income_classification": "income_classification",
    "urban_rural": "urban_rural",
    "island_region": "island_region",
    "status": "status",
    "_api_period": "_api_period",
    "version": "version",
}
RAW_COLUMNS = list(ALIASES.values()) + ["populations_json"]


def main():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    args = parser(__doc__).parse_args()
    directory = batch_path(args, "psa_psgc")
    manifest = completed_batch(directory)
    if {item["name"] for item in manifest["files"]} != {
        f"{p}.jsonl" for p in EXPECTED_ROWS
    }:
        raise ValueError("PSGC batch lacks the documented snapshots")
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    frames = []
    for period, expected in EXPECTED_ROWS.items():
        file = directory / f"{period}.jsonl"
        raw = spark.read.option("mode", "FAILFAST").json(str(file))
        if raw.count() != expected or not (set(ALIASES) | {"populations"}) <= set(
            raw.columns
        ):
            raise ValueError("PSGC source structure/count differs from contract")
        if raw.where(F.col("_api_period") != period).limit(1).count():
            raise ValueError("PSGC file contains a different snapshot")
        frame = raw.select(
            *[F.col(c).cast("string").alias(alias) for c, alias in ALIASES.items()],
            F.to_json("populations", options={"ignoreNullFields": "false"}).alias(
                "populations_json"
            ),
        )
        frames.append(frame.withColumn("_source_ref", F.lit(str(file))))
    frame = frames[0]
    for other in frames[1:]:
        frame = frame.unionByName(other)
    merge_observations(
        spark,
        provenance(frame, "psa_psgc", args.batch_id, RAW_COLUMNS),
        f"{args.catalog}.bronze.psa_psgc",
    )


if __name__ == "__main__":
    main()
