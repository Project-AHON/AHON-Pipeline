"""OSM feature observations from pinned shapefiles; coordinates do not assign LGUs."""

import hashlib
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from quality import check, unique, upsert
from runtime import batch_is_complete, batch_path, completed_batch, digest, parser, publish_batch

SIDECARS = [".shp", ".shx", ".dbf", ".prj", ".cpg"]


def validate_assets(directory, stems):
    import geopandas as gpd

    frames = []
    for stem in stems:
        if Path(stem).name != stem or not stem:
            raise ValueError("Expected safe source shapefile stems")
        for suffix in SIDECARS:
            if not (directory / (stem + suffix)).is_file():
                raise ValueError(f"Missing OSM sidecar: {stem + suffix}")
        frame = gpd.read_file(directory / (stem + ".shp"))
        if frame.crs is None:
            raise ValueError(f"Unknown OSM CRS: {stem}; no coordinate assumption permitted")
        if frame.empty:
            raise ValueError(f"Empty OSM shapefile: {stem}")
        frames.append(frame)
    return frames


def extract(args):
    from huggingface_hub import snapshot_download

    import re

    if not re.fullmatch(r"[0-9a-f]{40}", args.hf_revision):
        raise ValueError("hf-revision must be a full pinned commit")
    destination = batch_path(args, "osm_critical_infrastructures")
    if batch_is_complete(destination):
        return
    with tempfile.TemporaryDirectory() as temp:
        folder = args.hf_folder
        if Path(folder).is_absolute() or ".." in Path(folder).parts:
            raise ValueError("hf-folder must stay within the dataset repository")
        names = [stem + suffix for stem in args.shapefile_stems for suffix in SIDECARS]
        snapshot_download(repo_id=args.hf_repo, repo_type="dataset", revision=args.hf_revision,
                          allow_patterns=[f"{folder}/{name}" for name in names], local_dir=temp)
        directory = Path(temp) / folder
        frames = validate_assets(directory, args.shapefile_stems)
        counts = dict(zip(args.shapefile_stems, [len(frame) for frame in frames]))
        publish_batch(destination, [(directory / name, counts[Path(name).stem] if name.endswith('.shp') else 0) for name in names])


def bronze(spark, args):
    from pyspark.sql import functions as F

    directory = batch_path(args, "osm_critical_infrastructures")
    manifest = completed_batch(directory)
    frames = validate_assets(directory, args.shapefile_stems)
    records = []
    for stem, frame in zip(args.shapefile_stems, frames):
        file_digest = hashlib.sha256(json.dumps(
            [(suffix, digest(directory / (stem + suffix))) for suffix in SIDECARS]
        ).encode()).hexdigest()
        # Process each file's own CRS; never concatenate mixed CRSs before conversion.
        for ordinal, (_, row) in enumerate(frame.iterrows(), 1):
            attrs = row.drop(labels=frame.geometry.name).to_json(force_ascii=False)
            geometry = row[frame.geometry.name]
            records.append((stem + '.shp', ordinal, attrs, geometry.wkt if geometry is not None else None,
                            frame.crs.to_wkt(), str(directory / (stem + '.shp')), file_digest))
    raw = spark.createDataFrame(records, "source_file string, _source_row long, attributes_json string, geometry_wkt_raw string, source_crs string, _source_ref string, _source_digest string")
    frame = raw.withColumn("_source_name", F.lit("osm_critical_infrastructures"))
    frame = frame.withColumn("_batch_id", F.lit(args.batch_id)).withColumn("_ingested_at", F.current_timestamp())
    frame = frame.withColumn("_row_hash", F.sha2(F.to_json(F.struct("attributes_json", "geometry_wkt_raw", "source_crs"), options={"ignoreNullFields": "false"}), 256))
    frame = frame.withColumn("_observation_id", F.sha2(F.to_json(F.struct("_source_digest", "source_file", "_source_row")), 256))
    unique(spark, args.catalog, args.batch_id, frame, ["_observation_id"], "bronze.osm_critical_infrastructures")
    check(spark, args.catalog, args.batch_id, "osm_landing_accounting", "bronze.osm_critical_infrastructures",
          abs(len(records) - sum(item['rows'] for item in manifest['files'])))
    upsert(spark, frame, f"{args.catalog}.bronze.osm_critical_infrastructures", ["_observation_id"])


def silver(spark, args):
    from pyproj import Transformer
    from shapely import wkt
    from shapely.ops import transform
    from pyspark.sql import functions as F

    directory = batch_path(args, "osm_critical_infrastructures")
    completed_batch(directory)
    file_digests = [hashlib.sha256(json.dumps(
        [(suffix, digest(directory / (stem + suffix))) for suffix in SIDECARS]
    ).encode()).hexdigest() for stem in args.shapefile_stems]
    # Fixed set of four source files; filtering paths bounds driver geometry processing.
    source = spark.table(f"{args.catalog}.bronze.osm_critical_infrastructures").where(
        F.col("_source_digest").isin(file_digests))
    rows = []
    transformers = {}
    for row in source.toLocalIterator():
        attrs = json.loads(row.attributes_json)
        geometry, lat, lon = None, None, None
        invalid = True
        if row.geometry_wkt_raw:
            original = wkt.loads(row.geometry_wkt_raw)
            if not original.is_empty and original.is_valid:
                if row.source_crs not in transformers:
                    transformers[row.source_crs] = Transformer.from_crs(row.source_crs, 4326, always_xy=True)
                converter = transformers[row.source_crs]
                projected = transform(converter.transform, original)
                geometry = projected.wkt
                if projected.geom_type == 'Point':
                    lon, lat = projected.x, projected.y
                invalid = not projected.is_valid or projected.is_empty or any(
                    not (-180 <= x <= 180 and -90 <= y <= 90)
                    for x, y in [(projected.bounds[0], projected.bounds[1]), (projected.bounds[2], projected.bounds[3])])
        rows.append((row._observation_id, str(attrs.get('amenity')) if attrs.get('amenity') is not None else None,
                     str(attrs.get('name')) if attrs.get('name') is not None else None,
                     geometry, lat, lon, invalid))
    typed = spark.createDataFrame(rows, "_observation_id string, amenity string, name string, geometry_wkt string, latitude double, longitude double, has_dq_issues boolean")
    frame = source.join(typed, '_observation_id').withColumn('_processed_at', F.current_timestamp())
    check(spark, args.catalog, args.batch_id, "osm_no_fanout", "silver.osm_critical_infrastructures_clean", abs(frame.count() - source.count()))
    upsert(spark, frame, f"{args.catalog}.silver.osm_critical_infrastructures_clean", ['_observation_id'])
    check(spark, args.catalog, args.batch_id, "osm_invalid_geometry", "silver.osm_critical_infrastructures_clean", frame.where('has_dq_issues').count(), 'WARN')


if __name__ == '__main__':
    p = parser(__doc__)
    p.add_argument('--stage', choices=['extract', 'bronze', 'silver'], required=True)
    p.add_argument('--hf-repo', required=True)
    p.add_argument('--hf-revision', required=True)
    p.add_argument('--hf-folder', required=True)
    p.add_argument('--shapefile-stems', required=True, type=json.loads)
    args = p.parse_args()
    if not isinstance(args.shapefile_stems, list) or not args.shapefile_stems or len(set(args.shapefile_stems)) != len(args.shapefile_stems):
        raise ValueError("shapefile-stems must be an explicit unique JSON list")
    if args.stage == 'extract':
        extract(args)
    else:
        from pyspark.sql import SparkSession
        globals()[args.stage](SparkSession.builder.getOrCreate(), args)
