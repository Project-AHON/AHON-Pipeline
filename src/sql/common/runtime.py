"""Small shared job configuration and completed-batch landing contract."""

import argparse
import hashlib
import json
import re
from pathlib import Path


def identifier(value):
    if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise argparse.ArgumentTypeError("Expected a lowercase SQL identifier")
    return value


def parser(description):
    result = argparse.ArgumentParser(description=description)
    result.add_argument("--catalog", required=True, type=identifier)
    result.add_argument("--source-volume", required=True, type=identifier)
    result.add_argument("--batch-id", required=True)
    return result


def row_counts(value):
    """Explicit approved source profile supplied by the caller, never an inferred cutoff."""
    parsed = json.loads(value)
    if not isinstance(parsed, dict) or not parsed or any(
        not re.fullmatch(r"[A-Za-z0-9_-]+", key) or type(count) is not int or count < 1
        for key, count in parsed.items()
    ):
        raise argparse.ArgumentTypeError("Expected a nonempty JSON map of source periods to positive row counts")
    return parsed


def batch_path(args, dataset):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", args.batch_id):
        raise ValueError("batch-id must be a single safe path component")
    return Path(
        f"/Volumes/{args.catalog}/reference/{args.source_volume}/{dataset}/{args.batch_id}"
    )


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def completed_batch(path):
    """Only a verified completion marker authorizes a Bronze read."""
    path = Path(path)
    marker = path / "_SUCCESS.json"
    manifest = json.loads(marker.read_text(encoding="utf-8"))
    if not manifest["files"]:
        raise ValueError("Empty completed batch")
    for item in manifest["files"]:
        name = item["name"]
        if Path(name).name != name or name.startswith("."):
            raise ValueError("Invalid manifest filename")
        if digest(path / name) != item["sha256"]:
            raise ValueError("Completed batch failed file integrity validation")
    return manifest


def batch_is_complete(path):
    marker = Path(path) / "_SUCCESS.json"
    if not marker.exists():
        return False
    try:
        completed_batch(path)
    except json.JSONDecodeError:
        # Interrupted marker write never constituted a published batch.
        return False
    return True


def publish_batch(path, files):
    """Publish an immutable batch, marker last; retries repair only incomplete batches.

    files is a bounded list of (local_file, validated_row_count). The caller
    validates ALL files before this function touches the volume. Earlier valid
    batches are never replaced. A partial copy cannot be read by Bronze.
    """
    import shutil

    path = Path(path)
    manifest = {
        "files": [
            {"name": Path(file).name, "sha256": digest(file), "rows": count}
            for file, count in files
        ]
    }
    if len({item["name"] for item in manifest["files"]}) != len(files) or not files:
        raise ValueError("Empty batch or duplicate filenames")
    if batch_is_complete(path):
        if completed_batch(path) != manifest:
            raise ValueError("A completed batch cannot be changed; use a new batch-id")
        return manifest
    path.mkdir(parents=True, exist_ok=True)
    for file, _ in files:
        shutil.copyfile(file, path / Path(file).name)
    for item in manifest["files"]:
        if digest(path / item["name"]) != item["sha256"]:
            raise ValueError(
                "Landing integrity check failed; batch remains unpublished"
            )
    (path / "_SUCCESS.json").write_text(
        json.dumps(manifest, sort_keys=True), encoding="utf-8"
    )
    completed_batch(path)
    print(
        json.dumps(
            {
                "batch": path.name,
                "files": len(files),
                "rows": sum(item["rows"] for item in manifest["files"]),
            }
        )
    )
    return manifest


def merge_observations(spark, frame, table):
    """Identity is delivery batch + file digest + raw row hash; identical rows within a delivery are counted."""
    from quality import check, unique, upsert

    candidate = frame.dropDuplicates(["_observation_id"])
    if not candidate.limit(1).count():
        raise ValueError("Empty Bronze candidate")
    catalog = table.split(".")[0]
    if spark.catalog.tableExists(table):
        from pyspark.sql import functions as F

        existing = spark.table(table)
        if not {"_batch_id", "_source_digest", "_row_hash", "_observation_id"} <= set(existing.columns):
            raise ValueError("Observation identity migration requires review or a fresh catalog")
        expected = F.sha2(F.to_json(F.struct("_batch_id", "_source_digest", "_row_hash")), 256)
        if existing.where(~F.col("_observation_id").eqNullSafe(expected)).limit(1).count():
            raise ValueError("Existing observations use a different delivery identity; reviewed migration or fresh catalog required")
    run_id = frame.select("_batch_id").first()[0]
    unique(spark, catalog, run_id, candidate, ["_observation_id"], table)
    upsert(spark, candidate, table, ["_observation_id"])
    check(
        spark,
        catalog,
        run_id,
        "repeated_identical_raw_rows",
        table,
        frame.count() - candidate.count(),
        "WARN",
    )
    check(
        spark,
        catalog,
        run_id,
        "bronze_observation_accounting",
        table,
        candidate.join(spark.table(table), "_observation_id", "left_anti").count(),
    )


def provenance(frame, dataset, batch_id, raw_columns):
    from pyspark.sql import functions as F

    # Source files are bounded manifests, never a growing table collected on driver.
    paths = [r[0] for r in frame.select("_source_ref").distinct().collect()]
    evidence = frame.sparkSession.createDataFrame(
        [(p, digest(p)) for p in paths], "_source_ref string, _source_digest string"
    )
    frame = frame.join(evidence, "_source_ref")
    return (
        frame.withColumn("_source_name", F.lit(dataset))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn(
            "_row_hash",
            F.sha2(
                F.to_json(
                    F.struct(*[F.col(c) for c in raw_columns]),
                    options={"ignoreNullFields": "false"},
                ),
                256,
            ),
        )
        .withColumn(
            "_observation_id",
            F.sha2(F.to_json(F.struct("_batch_id", "_source_digest", "_row_hash")), 256),
        )
    )
