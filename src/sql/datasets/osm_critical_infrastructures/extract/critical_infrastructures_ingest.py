"""
extract_critical_infrastructure.py
=====================================
Consolidated pipeline that reads OpenStreetMap critical-infrastructure shapefiles
from a Unity Catalog volume, reprojects geometries to WGS84, enriches them with
WKT / GeoJSON / centroid columns, and writes the result to a Delta table.

Source volume : /Volumes/ahon/reference/source/huggingface/critical_infrastructures
Shapefiles    : fire_station.shp, hospitals.shp, police_station.shp, schools.shp
Target table  : ahon.bronze.critical_infrastructures

Expected output columns:
    amenity, name, source_file, geometry_wkt, geometry_geojson,
    centroid_lat, centroid_lng, loaded_at

Usage (inside a Databricks notebook or job task):
    from extract_critical_infrastructure import run_pipeline
    run_pipeline()

Or override defaults:
    run_pipeline(
        volume_path="/Volumes/ahon/reference/source/huggingface/critical_infrastructures",
        table_name="ahon.bronze.critical_infrastructures",
    )
"""

import json
import logging
import os

import geopandas as gpd
import pandas as pd
from pyspark.sql.functions import current_timestamp

# Configuration                                                               
DEFAULT_VOLUME_PATH = "/Volumes/ahon/reference/source/huggingface/critical_infrastructures"
DEFAULT_TABLE_NAME = "ahon.bronze.critical_infrastructures"  # catalog.schema.table
TARGET_EPSG = 4326          # WGS84
# Projected CRS used for accurate centroid computation (EPSG:3857 – Web Mercator).
# For point geometries centroids are identical regardless of CRS, but we use a
# projected CRS to silence the GeoPandas warning and stay correct for polygons.
CENTROID_CRS = 3857

logger = logging.getLogger(__name__)



# Step 1 – Discover shapefiles                                       
def discover_shapefiles(volume_path: str) -> list[str]:
    """Walk *volume_path* recursively and return a list of .shp file paths."""
    shp_files: list[str] = []
    for root, _dirs, files in os.walk(volume_path):
        for fname in files:
            if fname.lower().endswith(".shp"):
                shp_files.append(os.path.join(root, fname))

    logger.info("Found %d shapefile(s) under %s", len(shp_files), volume_path)
    for f in shp_files:
        logger.info("  • %s", f)
    return shp_files



# Step 2 – Read & combine shapefiles                                          
def read_and_combine_shapefiles(shp_files: list[str]) -> gpd.GeoDataFrame:
    """Read every .shp file into a GeoDataFrame, tag it with source_file, and
    concatenate them into a single GeoDataFrame."""
    gdfs: list[gpd.GeoDataFrame] = []

    for shp_path in shp_files:
        logger.info("Reading %s", shp_path)
        gdf = gpd.read_file(shp_path)

        # Tag each row with the source file name so downstream queries know
        # which shapefile a feature originated from.
        gdf["source_file"] = os.path.basename(shp_path)

        logger.info(
            "  shape=%s  crs=%s  columns=%s",
            gdf.shape, gdf.crs, list(gdf.columns),
        )
        gdfs.append(gdf)

    if not gdfs:
        raise ValueError("No shapefiles were read – check the volume path.")

    combined = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), geometry="geometry")
    logger.info(
        "Combined dataset: %d features, geometry types: %s",
        len(combined),
        combined.geometry.type.value_counts().to_dict(),
    )
    return combined



# Step 3 – Reproject to WGS84 and enrich with WKT / GeoJSON / centroids       
def enrich_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Reproject to WGS84 (EPSG:4326) and add geometry_wkt, geometry_geojson,
    centroid_lat, and centroid_lng columns."""

    # -- Reproject if necessary -------------------------------------------------
    if gdf.crs is None:
        logger.info("No CRS defined – assuming WGS84 (EPSG:4326)")
        gdf = gdf.set_crs(epsg=TARGET_EPSG)
    elif gdf.crs.to_epsg() != TARGET_EPSG:
        logger.info("Reprojecting from %s to EPSG:%d", gdf.crs, TARGET_EPSG)
        gdf = gdf.to_crs(epsg=TARGET_EPSG)
    else:
        logger.info("Already in WGS84 (EPSG:%d)", TARGET_EPSG)

    # -- WKT -------------------------------------------------------------------
    gdf["geometry_wkt"] = gdf["geometry"].apply(lambda g: g.wkt if g is not None else None)

    # -- GeoJSON ---------------------------------------------------------------
    gdf["geometry_geojson"] = gdf["geometry"].apply(
        lambda g: json.dumps(g.__geo_interface__) if g is not None else None
    )

    # -- Centroids (computed in a projected CRS, then expressed in WGS84) -------
    centroids = gdf["geometry"].to_crs(epsg=CENTROID_CRS).centroid.to_crs(epsg=TARGET_EPSG)
    gdf["centroid_lat"] = centroids.y
    gdf["centroid_lng"] = centroids.x

    logger.info("Enriched %d features with WKT, GeoJSON, and centroid columns", len(gdf))
    return gdf



# Step 4 – Convert to Spark DataFrame and save to Delta                        
def save_to_delta(gdf: gpd.GeoDataFrame, table_name: str, spark) -> None:
    """Drop the non-serialisable Shapely geometry column, convert to a Spark
    DataFrame, add a *loaded_at* timestamp, and write to a Delta table."""

    # The Shapely geometry objects are not serialisable by Spark; we keep the
    # WKT and GeoJSON string representations instead.
    pdf = gdf.drop(columns=["geometry"])

    logger.info("Converting %d rows to Spark DataFrame", len(pdf))
    spark_df = spark.createDataFrame(pdf)
    spark_df = spark_df.withColumn("loaded_at", current_timestamp())

    spark_df.printSchema()
    row_count = spark_df.count()
    logger.info("Row count: %,d", row_count)

    logger.info("Writing to Delta table: %s", table_name)
    (
        spark_df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    logger.info("✓ Successfully saved %,d rows to %s", row_count, table_name)



# Orchestrator                                                            
def run_pipeline(
    volume_path: str = DEFAULT_VOLUME_PATH,
    table_name: str = DEFAULT_TABLE_NAME,
    spark=None,
) -> None:
    """Run the full shapefile → Delta extraction pipeline.

    Parameters
    ----------
    volume_path
        UC Volume path containing the .shp files.
    table_name
        Fully-qualified Delta table name (catalog.schema.table).
    spark
        An active SparkSession. If *None*, the built-in notebook session is used.
    """
    if spark is None:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    logger.info("=== Critical Infrastructure Shapefile Extraction ===")
    logger.info("Volume : %s", volume_path)
    logger.info("Table  : %s", table_name)

    # Step 1
    shp_files = discover_shapefiles(volume_path)
    if not shp_files:
        raise FileNotFoundError(f"No .shp files found under {volume_path}")

    # Step 2
    combined_gdf = read_and_combine_shapefiles(shp_files)

    # Step 3
    enriched_gdf = enrich_geometries(combined_gdf)

    # Step 4
    save_to_delta(enriched_gdf, table_name, spark)

    logger.info("=== Pipeline complete ===")



# Entry point for direct execution (e.g. as a job task)                      
if __name__ == "__main__":
    run_pipeline()
