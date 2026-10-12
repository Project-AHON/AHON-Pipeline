"""Exercise actual PSGC Silver, shared dimension, reject persistence and replay."""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/sql/common"))
sys.path.insert(0, str(Path(__file__).parent))
from quality import check, upsert
from runtime import parser
from verify_delta import invoke


def verify(spark, args):
    from pyspark.sql import functions as F

    if "integration" not in args.catalog:
        raise ValueError("Use an isolated synthetic integration catalog")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {args.catalog}")
    spark.sql(f"USE CATALOG {args.catalog}")
    invoke(
        "src/sql/common/run_sql.py",
        args,
        ["--file", "src/sql/00_setup/00_catalog_schema_setup.sql"],
    )
    rows = [
        ("0100000000", "Region fixture", "REG"),
        ("0100100000", "Province fixture", "PROV"),
        ("100101000", "Town fixture 1/", "MUN"),
        ("0100101001", "Barangay fixture", "BGY"),
        ("0100109999", "Unrecognized level", "UNKNOWN"),
    ]

    def frame(values):
        raw = spark.createDataFrame(
            values, "psgc_code string, area_name string, geographic_level string"
        )
        result = raw.withColumn(
            "_row_hash", F.sha2(F.to_json(F.struct(*raw.columns)), 256)
        )
        for name in [
            "region_code",
            "province_code",
            "municipality_code",
            "barangay_code",
            "city_class",
        ]:
            result = result.withColumn(name, F.lit(None).cast("string"))
        return (
            result.withColumn("_observation_id", F.col("_row_hash"))
            .withColumn("_api_period", F.lit("Q2_2024"))
            .withColumn("_source_name", F.lit("synthetic_psgc"))
            .withColumn("_source_ref", F.lit("fixture/Q2_2024.jsonl"))
            .withColumn("_batch_id", F.lit(args.batch_id))
            .withColumn("_ingested_at", F.current_timestamp())
        )

    upsert(spark, frame(rows), f"{args.catalog}.bronze.psa_psgc", ["_observation_id"])
    silver_file = ["--file", "src/sql/datasets/psa_psgc/silver/psa_psgc_clean.py"]
    dim_file = ["--file", "src/sql/gold/dim_lgu.py"]
    invoke("src/sql/common/run_python.py", args, silver_file)
    invoke("src/sql/common/run_python.py", args, dim_file)
    before = set(map(tuple, spark.table("silver.psa_psgc_clean").collect()))
    invoke("src/sql/common/run_python.py", args, silver_file)
    invoke("src/sql/common/run_python.py", args, dim_file)
    assert before == set(map(tuple, spark.table("silver.psa_psgc_clean").collect()))
    assert spark.table("gold.dim_lgu").count() == 4
    assert (
        spark.table("gold.dim_lgu")
        .where("psgc_code = '0100101000' AND area_name = 'Town fixture'")
        .count()
        == 1
    )
    assert spark.table("silver.psa_psgc_reject").count() == 1
    upsert(
        spark,
        frame([("invalid-code", "Invalid fixture", "MUN")]),
        f"{args.catalog}.bronze.psa_psgc",
        ["_observation_id"],
    )
    for _ in range(2):
        failed = False
        try:
            invoke("src/sql/common/run_python.py", args, silver_file)
        except ValueError:
            failed = True
        assert failed
    assert spark.table("silver.psa_psgc_code_reject").count() == 1
    assert spark.table("silver.psa_psgc_clean").count() == 4
    check(spark, args.catalog, args.batch_id, "reference_integration", "fixture", 0)
    print(
        json.dumps(
            {
                "status": "PASS",
                "suite": "reference",
                "checks": [
                    "actual setup",
                    "PSGC leading zeros and footnotes",
                    "Silver/Gold replay",
                    "unknown level quarantine",
                    "malformed code quarantine before block",
                ],
            }
        )
    )


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    verify(SparkSession.builder.getOrCreate(), parser(__doc__).parse_args())
