"""Repeated OSM features must reuse CRS setup without changing output rows."""

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call


def test_silver_constructs_one_transformer_per_crs(monkeypatch, tmp_path):
    root = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(root / "src/sql/common"))
    spec = importlib.util.spec_from_file_location(
        "osm_transformer_test", root / "src/sql/datasets/osm_critical_infrastructures/pipeline.py"
    )
    osm = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(osm)

    factory = MagicMock()
    geometry = SimpleNamespace(is_empty=False, is_valid=True, geom_type="Point",
                               wkt="POINT (120 10)", x=120., y=10., bounds=(120., 10., 120., 10.))
    monkeypatch.setitem(sys.modules, "pyproj", SimpleNamespace(Transformer=factory))
    monkeypatch.setitem(sys.modules, "shapely", SimpleNamespace(wkt=SimpleNamespace(loads=lambda _: geometry)))
    monkeypatch.setitem(sys.modules, "shapely.ops", SimpleNamespace(transform=lambda _, shape: shape))
    monkeypatch.setitem(sys.modules, "pyspark.sql", SimpleNamespace(functions=MagicMock()))
    monkeypatch.setattr(osm, "batch_path", lambda *_: tmp_path)
    monkeypatch.setattr(osm, "completed_batch", lambda _: {})
    monkeypatch.setattr(osm, "digest", lambda _: "fixture_digest")
    monkeypatch.setattr(osm, "check", MagicMock())
    monkeypatch.setattr(osm, "upsert", MagicMock())

    spark = MagicMock()
    source = spark.table.return_value.where.return_value
    source.toLocalIterator.return_value = iter([
        SimpleNamespace(_observation_id=str(index), attributes_json="{}",
                        geometry_wkt_raw=geometry.wkt, source_crs=crs)
        for index, crs in enumerate(["EPSG:4326", "EPSG:4326", "EPSG:3857"])
    ])
    source.count.return_value = 3
    source.join.return_value.withColumn.return_value.count.return_value = 3
    osm.silver(spark, SimpleNamespace(catalog="fixture", batch_id="batch", shapefile_stems=["fixture"]))

    assert factory.from_crs.call_args_list == [
        call("EPSG:4326", 4326, always_xy=True), call("EPSG:3857", 4326, always_xy=True)
    ]
    rows = spark.createDataFrame.call_args.args[0]
    assert rows == [(str(index), None, None, geometry.wkt, 10., 120., False) for index in range(3)]
