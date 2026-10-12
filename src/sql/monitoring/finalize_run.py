"""Write task outcome; keep failed dependencies failed rather than turning runs green."""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from quality import ensure_monitoring, upsert

if __name__ == "__main__":
    from pyspark.sql import SparkSession

    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--task-key", required=True)
    p.add_argument("--status", required=True)
    args = p.parse_args()
    from runtime import identifier

    identifier(args.catalog)
    spark = SparkSession.builder.getOrCreate()
    ensure_monitoring(spark, args.catalog)
    data = [
        (
            args.run_id,
            args.task_key,
            args.status,
            "Dependency state supplied by Lakeflow",
            datetime.now(timezone.utc),
        )
    ]
    frame = spark.createDataFrame(
        data,
        "run_id string, task_key string, status string, message string, recorded_at timestamp",
    )
    upsert(
        spark,
        frame,
        f"{args.catalog}.monitoring.run_log",
        ["run_id", "task_key"],
        "NOT(t.status <=> s.status)",
    )
    if args.status != "success":
        raise RuntimeError(f"Dependent processing did not succeed: {args.status}")
