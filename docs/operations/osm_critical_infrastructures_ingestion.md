# OSM critical infrastructure ingestion setup

This document explains how to move OpenStreetMap critical-infrastructure files from the raw source location into a structured Delta table that is ready for analytics.

The workflow covers the full path from raw shapefiles and other geospatial files to a cleaned bronze table named:

- `ahon.bronze.critical_infrastructures`

The ingestion logic is implemented in:

- `src/sql/datasets/osm_critical_infrastructures/extract/critical_infrastructures_ingest.py`
- `src/sql/datasets/osm_critical_infrastructures/bronze/critical_infrastructures.sql`

## What this pipeline does

The Python job performs the following steps:

1. discovers all `.shp` files under a configured source volume
2. reads each shapefile into a GeoPandas GeoDataFrame
3. adds the source file name to each feature
4. reprojects geometries to WGS84
5. creates geometry WKT and GeoJSON strings
6. calculates centroid latitude and longitude
7. writes the result to a Delta table in Unity Catalog

This converts unstructured geospatial source files into a structured, queryable table with machine-readable fields.

## Source layout

The expected source structure is:

```text
/Volumes/ahon/reference/source/huggingface/critical_infrastructures/
├── fire_station.shp
├── fire_station.shx
├── fire_station.dbf
├── hospitals.shp
├── hospitals.shx
├── hospitals.dbf
├── police_station.shp
├── police_station.shx
├── police_station.dbf
├── schools.shp
├── schools.shx
├── schools.dbf
└── ...
```

The script looks for files ending in `.shp` anywhere under the base directory and ignores non-shapefile files unless they are part of the shapefile bundle.

## Prerequisites

Before running the pipeline, make sure the following are in place:

- a Databricks workspace with Unity Catalog enabled
- a cluster with Python support and the correct geospatial libraries installed
- access to the volume `/Volumes/ahon/reference/source/huggingface/critical_infrastructures`
- the source `.shp` files already uploaded to the volume
- permission to create or overwrite the target table `ahon.bronze.critical_infrastructures`

### Python libraries

Install the required packages on the cluster:

```python
%pip install geopandas pandas pyarrow shapely fiona
```

If the environment uses a shared cluster, make sure the libraries are available to the notebook or job task that runs the pipeline.

## Step 1: Upload the raw files

Put the original shapefile bundle into the source volume. The expected folder is:

```text
/Volumes/ahon/reference/source/huggingface/critical_infrastructures
```

This is the folder the extraction script scans by default.

If the data is being brought in from Hugging Face, keep the raw files under a dataset folder such as:

```text
data/critical_infrastructures/
```

and then copy or mount them into the Databricks volume before running the pipeline.

## Step 2: Confirm the file path and target table

The extraction script uses these default settings:

```python
DEFAULT_VOLUME_PATH = "/Volumes/ahon/reference/source/huggingface/critical_infrastructures"
DEFAULT_TABLE_NAME = "ahon.bronze.critical_infrastructures"
```

If the files are stored elsewhere, override the parameters when running the pipeline:

```python
from extract_critical_infrastructure import run_pipeline

run_pipeline(
    volume_path="/Volumes/ahon/reference/source/huggingface/critical_infrastructures",
    table_name="ahon.bronze.critical_infrastructures",
)
```

## Step 3: Run the extraction job

Open a Databricks notebook or job task, then run:

```python
from extract_critical_infrastructure import run_pipeline
run_pipeline()
```

This executes the full pipeline:

- `discover_shapefiles()` finds all `.shp` files
- `read_and_combine_shapefiles()` loads them into a GeoDataFrame
- `enrich_geometries()` converts to WGS84 and adds WKT, GeoJSON, and centroids
- `save_to_delta()` converts the frame to Spark and writes it to Delta

## Step 4: Validate the conversion result

After the job completes, inspect the table schema and row count.

```sql
DESCRIBE TABLE ahon.bronze.critical_infrastructures;
SELECT COUNT(*) FROM ahon.bronze.critical_infrastructures;
```

The expected key columns include:

- `amenity`
- `name`
- `source_file`
- `geometry_wkt`
- `geometry_geojson`
- `centroid_lat`
- `centroid_lng`
- `loaded_at`

The bronze load also adds provenance and DQ columns:

- `_source_name`
- `_source_ref`
- `_ingested_at`
- `_batch_id`
- `_row_hash`
- `_dq_valid_lat`
- `_dq_valid_lng`
- `_dq_valid_amenity`
- `_dq_valid_name`

## Step 5: Understand the data model

The bronze table stores one row per feature extracted from the shapefile set. Each row contains:

- the original feature attributes such as `amenity` and `name`
- the source shapefile name
- the WKT and GeoJSON geometry representation
- centroid coordinates in latitude and longitude
- load-time provenance metadata

This gives you a structured record that is easy to query and join in downstream layers.

## Step 6: Use the bronze layer as the source for silver logic

Once the bronze layer is loaded, the silver table can be built using the SQL in:

- `src/sql/datasets/osm_critical_infrastructures/silver/critical_infrastructures_clean.sql`

The silver layer applies logic such as:

- standardizing `amenity` values
- trimming and cleaning text fields
- validating coordinate ranges
- dropping invalid rows
- excluding empty or null required values
- extracting the geometry type from WKT

This is the structured, analytics-ready version built from the bronze data.

## How the pipeline handles unstructured files

The major challenge with shapefiles is that they are not a single data file; they are a file bundle. A shapefile usually consists of multiple related files:

- `.shp` — geometry
- `.shx` — index
- `.dbf` — attribute table
- `.prj` — projection information
- optional `.cpg`, `.qpj`, or other files

GeoPandas reads the `.shp` file and uses the accompanying files automatically when they are all present in the same folder. That means the pipeline can ingest the spatial record even though the source is not a single flat CSV or JSON table.

## Data quality rules used in bronze

The bronze table checks these conditions before accepting a row:

- latitude is present and between `-90` and `90`
- longitude is present and between `-180` and `180`
- amenity is not blank
- name is not blank when present
- the row is not missing all key fields

These DQ flags are stored as columns and used by downstream silver processing.

## Troubleshooting

### No shapefiles found

Check that:

- the folder path is correct
- files actually end with `.shp`
- the files are in the volume and accessible by the cluster

### GeoPandas import errors

Install geospatial dependencies on the cluster:

```python
%pip install geopandas pandas shapely fiona pyarrow
```

### Geometry conversion warning or CRS issues

The script reprojects to WGS84 and uses a projected CRS for centroid calculation to improve correctness.

### Table write fails

Verify:

- the Unity Catalog schema and table path are valid
- the user has write permissions
- the cluster can access the volume
- the table name matches the expected catalog and schema naming pattern

## Recommended operational pattern

Use this order for repeatable ingestion:

1. upload or refresh the shapefile bundle into the source volume
2. validate the files under the folder path
3. run the extraction notebook or job task
4. inspect the bronze Delta table
5. run the silver SQL for cleaning and validation
6. confirm output counts and key columns before downstream analysis

## Summary

The setup is straightforward once the raw shapefiles are in the correct volume location:

- store the source shapefile bundle in `/Volumes/ahon/reference/source/huggingface/critical_infrastructures`
- run the Python extraction script
- let it reproject, enrich, and write the results to `ahon.bronze.critical_infrastructures`
- use the silver SQL to validate and normalize the data for downstream use

This is the standard pattern for turning unstructured geospatial inputs into a structured, queryable Delta table.
