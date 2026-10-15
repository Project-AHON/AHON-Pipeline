"""Exercise older-after-newer delivery replay on completed synthetic fixtures only."""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/sql/common"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from quality import upsert
from runtime import parser
from verify_pipeline import invoke, module


def verify_delivery_replay(spark, args):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    if not args.catalog.startswith("ahon_integration_"):
        raise ValueError("Delivery replay checks require completed synthetic fixtures")
    spark.sql(f"USE CATALOG {args.catalog}")
    reference = spark.table("bronze.psa_psgc")
    fiscal = spark.table("bronze.blgf_ldrrmf_annual_lgu")
    if reference.count() > 1000 or fiscal.count() > 1000:
        raise ValueError("Expected the small synthetic integration fixtures")
    # Anchor each check to the earliest immutable fixture, even when rerun later.
    reference = (reference.withColumn("_seed", F.row_number().over(
        Window.partitionBy("psgc_code", "geographic_level", "_api_period")
        .orderBy(F.col("_ingested_at").asc(), F.col("_row_hash").asc())
    )).where("_seed = 1").drop("_seed"))
    fiscal = (fiscal.withColumn("_seed", F.row_number().over(
        Window.partitionBy("_source_ref", "region", "province", "lgu_name", "lgu_type")
        .orderBy(F.col("_ingested_at").asc(), F.col("_row_hash").asc())
    )).where("_seed = 1").drop("_seed"))
    reference = spark.createDataFrame(reference.collect(), reference.schema)
    fiscal = spark.createDataFrame(fiscal.collect(), fiscal.schema)
    population = module("delivery_population", "src/sql/datasets/psa_psgc/silver/clean_barangay_population.py")
    timestamp = datetime.now(timezone.utc)
    scopes = []
    for index, value in enumerate((3000, 4000)):
        current = SimpleNamespace(**{**vars(args), "batch_id": f"{args.batch_id}_delivery_{index}"})
        scopes.append(current)
        observed_at = timestamp + timedelta(seconds=index)
        changed = reference.withColumn("populations_json", F.when(
            F.col("psgc_code") == "0100101001",
            F.lit(json.dumps([{"population": str(value), "year": "2024", "psgc": "0100101001"}]))
        ).otherwise(F.col("populations_json")))
        changed = delivery(changed, current.batch_id, observed_at)
        upsert(spark, changed, f"{args.catalog}.bronze.psa_psgc", ["_observation_id"])
        build_reference(spark, current, population)
        assert spark.table("gold.fact_population").where(f"population = {value}").count() == 1

        changed = fiscal
        for column in [c for c in fiscal.columns if c.endswith("appropriation") or c.endswith("expenditures")]:
            changed = changed.withColumn(column, F.col(column) * F.lit(index + 2))
        changed = delivery(changed, current.batch_id, observed_at)
        upsert(spark, changed, f"{args.catalog}.bronze.blgf_ldrrmf_annual_lgu", ["_observation_id"])
        build_funding(current)
        assert spark.table("gold.fact_ldrrmf").where(f"fiscal_year = 2024 AND total_appropriation = {10 * (index + 2)}").count() == 1

    before = {table: set(map(tuple, spark.table(table).collect())) for table in (
        "silver.psa_psgc_clean", "gold.fact_population",
        "silver.blgf_ldrrmf_annual_lgu_clean", "gold.fact_ldrrmf",
    )}
    build_reference(spark, scopes[0], population)
    build_funding(scopes[0])
    for table, rows in before.items():
        assert rows == set(map(tuple, spark.table(table).collect())), f"Older delivery changed {table}"
    print(json.dumps({"status": "PASS", "catalog": args.catalog,
                      "checks": ["PSGC older delivery preserves newer census correction",
                                 "LDRRMF older delivery preserves newer financial correction",
                                 "unchanged Silver/Gold values, processing timestamps and lineage"]}))



def delivery(frame, batch_id, timestamp):
    from pyspark.sql import functions as F

    raw = [column for column in frame.columns if not column.startswith("_")]
    return (frame.withColumn("_row_hash", F.sha2(F.to_json(F.struct(*raw), options={"ignoreNullFields": "false"}), 256))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ingested_at", F.lit(timestamp))
            .withColumn("_observation_id", F.sha2(F.to_json(F.struct("_batch_id", "_row_hash")), 256)))


def build_reference(spark, args, population):
    invoke("src/sql/common/run_python.py", args, ["--file", "src/sql/datasets/psa_psgc/silver/psa_psgc_clean.py"])
    population.transform(spark, SimpleNamespace(**{**vars(args), "source_period": "Q2_2024"}))
    invoke("src/sql/common/run_python.py", args, ["--file", "src/sql/gold/fact_population.py"])


def build_funding(args):
    invoke("src/sql/common/run_sql.py", args, ["--file", "src/sql/datasets/blgf_ldrrmf_annual_lgu/silver/clean_blgf_ldrrmf_annual_lgu.sql"])
    invoke("src/sql/common/run_sql.py", args, ["--file", "src/sql/gold/build_fact_ldrrmf.sql"])


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    verify_delivery_replay(SparkSession.builder.getOrCreate(), parser(__doc__).parse_args())
