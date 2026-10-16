"""GX validates bounded Spark rule metrics; persist results before blocking writes."""

import json
from datetime import datetime, timezone
from functools import lru_cache
from numbers import Integral

_MONITORING_READY = set()


class QualityError(ValueError):
    """A recorded GX expectation failed at blocking severity."""


@lru_cache(maxsize=1)
def _metric_batch():
    import great_expectations as gx
    from great_expectations.data_context.types.base import ProgressBarsConfig

    context = gx.get_context(mode="ephemeral")
    context.enable_analytics(False)
    context.variables.progress_bars = ProgressBarsConfig(globally=False)
    source = context.data_sources.add_pandas(name="spark_rule_metrics")
    asset = source.add_dataframe_asset(name="quality_checks")
    return asset.add_batch_definition_whole_dataframe("rule")


def validate_metric(check_id, table, failed, severity="ERROR"):
    """Spark scans the complete dataset; GX evaluates its bounded failure count."""
    import great_expectations as gx
    import pandas as pd

    if severity not in {"ERROR", "WARN"}:
        raise ValueError("DQ severity must be ERROR or WARN")
    if isinstance(failed, bool) or not isinstance(failed, Integral) or failed < 0:
        raise ValueError("DQ failure count must be a nonnegative integer")
    batch = _metric_batch().get_batch(
        batch_parameters={"dataframe": pd.DataFrame({"failed_count": [int(failed)]})}
    )
    expectation = gx.expectations.ExpectColumnValuesToBeBetween(
        column="failed_count", min_value=0, max_value=0,
        severity="warning" if severity == "WARN" else "critical",
        meta={"check_id": check_id, "table_name": table},
    )
    result = batch.validate(expectation, result_format="BOOLEAN_ONLY")
    payload = result.to_json_dict()
    payload["meta"].update({"engine": "great_expectations", "gx_version": gx.__version__})
    status = "PASS" if result.success else "BLOCKED" if severity == "ERROR" else "WARN"
    return status, json.dumps(payload)


def ensure_monitoring(spark, catalog):
    key = (id(spark), catalog)
    if key in _MONITORING_READY:
        return
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.monitoring")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {catalog}.monitoring.dq_result (
      run_id STRING, check_id STRING, table_name STRING, failed_count BIGINT,
      severity STRING, disposition STRING, checked_at TIMESTAMP,
      validation_result STRING) USING DELTA""")
    # Add only GX evidence to the pre-existing monitoring contract; retain all rows.
    if "validation_result" not in spark.table(f"{catalog}.monitoring.dq_result").columns:
        spark.sql(f"ALTER TABLE {catalog}.monitoring.dq_result ADD COLUMNS (validation_result STRING)")
    _MONITORING_READY.add(key)


def check(spark, catalog, run_id, check_id, table, failed, severity="ERROR"):
    ensure_monitoring(spark, catalog)
    status, validation_result = validate_metric(check_id, table, failed, severity)
    row = [
        (
            run_id,
            check_id,
            table,
            int(failed),
            severity,
            status,
            datetime.now(timezone.utc),
            validation_result,
        )
    ]
    frame = spark.createDataFrame(
        row,
        "run_id string, check_id string, table_name string, failed_count long, severity string, disposition string, checked_at timestamp, validation_result string",
    )
    frame.createOrReplaceTempView("dq_result_candidate")
    spark.sql(f"""MERGE INTO {catalog}.monitoring.dq_result t
      USING dq_result_candidate s ON t.run_id=s.run_id AND t.check_id=s.check_id AND t.table_name=s.table_name
      WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    if status == "BLOCKED":
        raise QualityError(
            f"Blocking quality check {check_id}: {failed} failures in {table}"
        )


def unique(spark, catalog, run_id, frame, keys, table):
    failed = frame.groupBy(*keys).count().where("count > 1").count()
    check(spark, catalog, run_id, "unique_" + "_".join(keys), table, failed)


def upsert(spark, frame, table, keys, changed=None):
    """Delta table transaction; no deletion, schema evolution, or multi-table atomicity."""
    from delta.tables import DeltaTable

    if not spark.catalog.tableExists(table):
        frame.write.format("delta").mode("error").saveAsTable(table)
        return
    existing = spark.table(table)
    if existing.groupBy(*keys).count().where("count > 1").limit(1).count():
        raise ValueError(f"Existing target has duplicate merge keys: {table}")
    if {f.name: f.dataType for f in existing.schema} != {
        f.name: f.dataType for f in frame.schema
    }:
        raise ValueError(f"Schema migration requires review: {table}")
    condition = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    merge = (
        DeltaTable.forName(spark, table).alias("t").merge(frame.alias("s"), condition)
    )
    if changed:
        merge = merge.whenMatchedUpdateAll(condition=changed)
    merge.whenNotMatchedInsertAll().execute()
