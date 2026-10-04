# AHON NOAH GeoSpatial Pipeline: GeoPackage to Parquet

## 1. Purpose

This guide explains how to run the AHON NOAH Parquet export process and how it fits into the overall data pipeline.

The process converts the **local Bronze GeoPackages** created by `ingest_noah.py` into Parquet files that can be transferred to Databricks.

The pipeline is intentionally separated into stages so that each script has one clear responsibility:

```text
NOAH ZIP Archives
        │
        ▼
extract_noah.py
ZIP → Shapefiles
        │
        ▼
Raw NOAH Shapefiles
        │
        ▼
ingest_noah.py
Shapefiles → GeoPackages
        │
        ▼
Local Bronze GeoPackages
        │
        ▼
extract_noah_parquet.py
GeoPackages → Parquet
        │
        ▼
Parquet Transfer Files
        │
        ▼
Databricks
Parquet → Delta Bronze
        │
        ▼
Silver
        │
        ▼
Gold / Analytics
```

This document focuses on the **GeoPackage → Parquet** stage, while also explaining the preceding and following stages so another team member can reproduce the workflow.

---

# 2. Pipeline Responsibilities

Before running anything, it is important to understand which script does what.

| Script / Stage | Input | Output | Responsibility |
|---|---|---|---|
| `extract_noah.py` | NOAH ZIP archives | Shapefiles | Extract source GIS files |
| `ingest_noah.py` | Shapefiles | GeoPackages | Standardize and combine source files |
| `extract_noah_parquet.py` | GeoPackages | Parquet | Prepare files for Databricks |
| Databricks ingestion | Parquet | Delta tables | Create the actual Databricks Bronze layer |
| Silver pipeline | Bronze Delta | Silver Delta | Clean and standardize analytical data |
| Gold pipeline | Silver Delta | Gold tables | Create analytical datasets |

### Important distinction

The local GeoPackages are **not the final Databricks Bronze tables**.

They are local GIS Bronze/staging artifacts used before the data is transferred to Databricks.

The Databricks Delta tables are the actual cloud Bronze layer.

---

# 3. Prerequisites

Before running the Parquet export, make sure the following are available:

- WSL
- Python
- The `ahon-gis` virtual environment
- GDAL / `ogr2ogr`
- `pyogrio`
- `pyarrow`
- The AHON repository
- The extracted NOAH datasets
- The Bronze GeoPackages created by `ingest_noah.py`

The GIS environment used by the project is:

```text
Python
GDAL
GeoPandas
Shapely
Pyogrio
PyArrow
```

The Parquet exporter specifically relies on:

```text
pyogrio
pyarrow
GDAL / ogr2ogr
```

---

# 4. Activate the GIS Environment

### Why this is needed

The NOAH pipeline uses a separate GIS-oriented Python environment because the GIS dependencies, especially GDAL and Pyogrio, need to be available.

### Command

Run the following from WSL:

```bash
source ~/ahon-gis/bin/activate
```

You should see something similar to:

```text
(ahon-gis) allea@Leanne:~$
```

The `(ahon-gis)` part confirms that the GIS virtual environment is active.

---

# 5. Move to the AHON Repository

### Why this is needed

The scripts use project-relative paths such as:

```text
data/raw/noah/
data/bronze/noah/
data/export/noah/
```

Running the command from the repository keeps those paths consistent.

### Command

```bash
cd "/mnt/c/Users/allea/Downloads/Ahon Pipeline/AHON-Pipeline"
```

Confirm your location:

```bash
pwd
```

Expected result:

```text
/mnt/c/Users/allea/Downloads/Ahon Pipeline/AHON-Pipeline
```

---

# 6. Confirm the Source Archives

### Why this is needed

`extract_noah.py` needs the original NOAH ZIP archives before it can produce the raw Shapefiles.

The archives should be located under:

```text
data/raw/noah/archives/
```

### Command

```bash
ls -lh data/raw/noah/archives/
```

Expected datasets include files similar to:

```text
Flood_25year_01.zip
Flood_25year_02.zip

Landslide_01.zip
Landslide_02.zip
...

Storm-Surge_01.zip
```

The exact archive sizes may differ depending on the downloaded source files.

---

# 7. Step 1 — Extract the NOAH Archives

### What this step does

`extract_noah.py` is responsible only for unpacking the NOAH archives.

It does **not** create the GeoPackages and it does **not** create Parquet files.

Its responsibility is:

```text
NOAH ZIP
   ↓
Shapefiles
```

It preserves the original source archive files and extracts the required ESRI Shapefile components.

### Run

```bash
python src/ingestion/noah/extract_noah.py
```

The extracted files should appear under:

```text
data/raw/noah/datasets/
```

The resulting structure will look approximately like:

```text
data/raw/noah/datasets/
├── NOAH-Flood-Hazard/
│   └── 25-year/
│       ├── Abra_Flood_25year.shp
│       ├── Abra_Flood_25year.shx
│       ├── Abra_Flood_25year.dbf
│       ├── Abra_Flood_25year.prj
│       └── ...
│
├── NOAH-Landslide-Hazard/
│   └── LandslideHazards/
│       ├── ...
│
└── NOAH-Storm-Surge/
    └── ...
```

---

# 8. Step 2 — Profile the NOAH Data

### What this step does

Profiling is performed before ingestion so the team understands the source structure.

The profiling process checks things such as:

- Feature counts
- Geometry types
- CRS
- Hazard fields
- Filename patterns
- Missing values
- Source inconsistencies

The project has separate profiling utilities under:

```text
src/profiling/noah/
```

The profiling outputs are stored under:

```text
data/profiling/noah/
```

### Why profiling matters

NOAH datasets are not perfectly uniform.

For example:

- Flood uses `Var`
- Landslide uses `LH`
- Storm Surge uses `HAZ`
- Storm Surge also contains advisory levels such as `SSA1` through `SSA4`

The profiling stage allows these source characteristics to be documented before standardization.

---

# 9. Step 3 — Create the Local Bronze GeoPackages

### What this step does

`ingest_noah.py` reads the extracted Shapefiles and creates standardized GeoPackages.

Its responsibility is:

```text
Shapefiles
    ↓
ingest_noah.py
    ↓
GeoPackages
```

The GeoPackages are stored under:

```text
data/bronze/noah/
```

Expected structure:

```text
data/bronze/noah/
├── flood_25yr/
│   └── flood_25yr.gpkg
│
├── landslide/
│   └── landslide.gpkg
│
└── storm_surge/
    └── storm_surge.gpkg
```

### Run

```bash
python src/ingestion/noah/ingest_noah.py
```

---

# 10. What `ingest_noah.py` Does

### Why this stage exists

The raw Shapefiles are source-oriented. The GeoPackage stage creates a more consistent local GIS representation before the data is converted to Parquet.

The ingestion process:

- Reads all relevant Shapefiles
- Combines them by hazard type
- Standardizes geometry representation
- Keeps the data in EPSG:4326
- Adds source metadata
- Preserves the original source filename
- Preserves the original source field
- Adds an ingestion timestamp

For example, Flood source field:

```text
Var
```

becomes:

```text
var
```

and metadata is added such as:

```text
source_file
source_dataset
scenario
source_field
source_crs
ingest_timestamp
```

For Landslide, the actual source field `LH` is preserved as `lh`.

For Storm Surge, both:

```text
haz
advisory_level
```

are retained because they represent different concepts.

---

# 11. Validate the GeoPackages Before Parquet Export

### Why this is needed

The Parquet exporter should not be run against an incomplete or missing Bronze GeoPackage.

The expected baseline feature counts are:

| Dataset | Expected Features |
|---|---:|
| Flood 25-year | 227 |
| Landslide | 2,692 |
| Storm Surge | 804 |

You can inspect the files using:

```bash
ls -lh data/bronze/noah/flood_25yr/
ls -lh data/bronze/noah/landslide/
ls -lh data/bronze/noah/storm_surge/
```

The GeoPackages should exist before continuing.

---

# 12. Step 4 — Export GeoPackages to Parquet

## What this step does

This is the purpose of:

```text
src/ingestion/noah/extract_noah_parquet.py
```

It reads the GeoPackages produced by `ingest_noah.py` and creates Parquet files.

The flow is:

```text
GeoPackage
     ↓
2D GeoPackage
     ↓
Arrow streaming
     ↓
Parquet
```

The original GeoPackages are not modified.

---

# 13. Why a Temporary 2D GeoPackage Is Used

### Explanation

Some NOAH source geometries contain Z coordinates.

For downstream Parquet transfer, the pipeline standardizes the exported geometry to 2D.

This is done using:

```text
-dim XY
```

The important principle is:

> The original source and Bronze GeoPackage remain unchanged. The 2D standardization happens only in the export process.

The temporary files are stored under:

```text
data/export/noah/_temp/
```

For example:

```text
data/export/noah/_temp/
├── flood_25yr_2d.gpkg
├── landslide_2d.gpkg
└── storm_surge_2d.gpkg
```

---

# 14. Why Parquet Is Stored in WSL

### Explanation

The NOAH geometries can be large, especially Landslide.

The project previously encountered limited free space on the Windows C: drive.

Therefore the Parquet exporter writes large files to the WSL filesystem:

```text
/home/allea/ahon-parquet/
```

The script determines the user's WSL home directory dynamically using:

```python
WSL_PARQUET_DIR = Path.home() / "ahon-parquet"
```

This makes the script more portable between WSL users.

---

# 15. Run the Parquet Export

### Before running

Make sure you are:

1. In WSL
2. Inside the `ahon-gis` environment
3. Inside the AHON repository
4. Finished running `ingest_noah.py`

### Command

```bash
python src/ingestion/noah/extract_noah_parquet.py
```

The script runs the datasets in this order:

```text
1. Flood
2. Landslide
3. Storm Surge
```

---

# 16. Flood Export

### What happens

The script reads:

```text
data/bronze/noah/flood_25yr/flood_25yr.gpkg
```

Layer:

```text
bronze_noah_flood
```

It creates/reuses:

```text
data/export/noah/_temp/flood_25yr_2d.gpkg
```

Then it writes:

```text
/home/allea/ahon-parquet/flood_25yr.parquet
```

Expected feature count:

```text
227
```

The Parquet file is written using Snappy compression.

---

# 17. Landslide Export

### What happens

Landslide is the most memory-intensive of the three datasets because some geometries are very large and complex.

Instead of creating one large Parquet file, the exporter creates 100-row chunks.

Input:

```text
data/bronze/noah/landslide/landslide.gpkg
```

Temporary 2D GeoPackage:

```text
data/export/noah/_temp/landslide_2d.gpkg
```

Output:

```text
/home/allea/ahon-parquet/landslide_chunks/
```

Expected structure:

```text
landslide_chunks/
├── landslide_part_001.parquet
├── landslide_part_002.parquet
├── ...
└── landslide_part_027.parquet
```

Expected row distribution:

```text
Part 001 → 100
Part 002 → 100
...
Part 026 → 100
Part 027 → 92
```

Total:

```text
2,692 rows
```

Each chunk is independently finalized and validated.

This is important because if one later chunk fails, the previously completed chunks remain valid Parquet files.

---

# 18. Storm Surge Export

### What happens

The script reads:

```text
data/bronze/noah/storm_surge/storm_surge.gpkg
```

Layer:

```text
bronze_noah_storm_surge
```

It creates/reuses:

```text
data/export/noah/_temp/storm_surge_2d.gpkg
```

Then creates:

```text
/home/allea/ahon-parquet/storm_surge.parquet
```

Expected feature count:

```text
804
```

---

# 19. Expected Final Output

After successful execution:

```text
/home/allea/ahon-parquet/
│
├── flood_25yr.parquet
│
├── storm_surge.parquet
│
└── landslide_chunks/
    ├── landslide_part_001.parquet
    ├── landslide_part_002.parquet
    ├── ...
    └── landslide_part_027.parquet
```

Expected totals:

| Dataset | Expected Rows | Output |
|---|---:|---|
| Flood 25-year | 227 | `flood_25yr.parquet` |
| Landslide | 2,692 | 27 Parquet chunks |
| Storm Surge | 804 | `storm_surge.parquet` |

---

# 20. Validate the Parquet Files

### Why this is needed

The export process performs validation automatically, but it is useful to manually inspect the outputs when troubleshooting.

### List the outputs

```bash
ls -lh ~/ahon-parquet/
```

For Landslide:

```bash
ls -lh ~/ahon-parquet/landslide_chunks/
```

---

# 21. Check Parquet Files from WSL

### Why this is useful

You can inspect the Parquet metadata without loading the entire geometry dataset into memory.

Run:

```bash
python -c "import pyarrow.parquet as pq; print(pq.ParquetFile('/home/allea/ahon-parquet/flood_25yr.parquet').metadata)"
```

For Storm Surge:

```bash
python -c "import pyarrow.parquet as pq; print(pq.ParquetFile('/home/allea/ahon-parquet/storm_surge.parquet').metadata)"
```

For Landslide:

```bash
python -c "import pyarrow.parquet as pq; print(pq.ParquetFile('/home/allea/ahon-parquet/landslide_chunks/landslide_part_001.parquet').metadata)"
```

---

# 22. View the Parquet Directory in Windows

### Why this is useful

The files are stored in WSL, but Windows Explorer can access the WSL filesystem.

Run:

```bash
explorer.exe /home/allea/ahon-parquet
```

This opens the Parquet output directory in Windows Explorer.

---

# 23. Important: Do Not Commit the Parquet Files to GitHub

### Why

The NOAH datasets are large binary GIS artifacts.

GitHub should contain:

- Python scripts
- Documentation
- Configuration
- Tests
- Small test fixtures where appropriate

GitHub should not contain the full NOAH archives, GeoPackages, or Parquet outputs.

The `.gitignore` should include:

```gitignore
# NOAH raw archives
data/raw/noah/archives/

# Extracted NOAH datasets
data/raw/noah/datasets/

# Local NOAH Bronze GeoPackages
data/bronze/noah/

# NOAH temporary/export files
data/export/noah/

# NOAH profiling outputs
data/profiling/noah/

# GIS files
*.shp
*.shx
*.dbf
*.prj
*.cpg
*.sbn
*.sbx
*.qix
*.qpj

# GeoPackage
*.gpkg

# Parquet
*.parquet
```

The scripts and documentation remain version-controlled.

---

# 24. Reproducible End-to-End Run

A new team member can reproduce the complete process using the following sequence.

## Step 1 — Open WSL

```bash
wsl
```

## Step 2 — Activate the GIS environment

```bash
source ~/ahon-gis/bin/activate
```

## Step 3 — Go to the repository

```bash
cd "/mnt/c/Users/allea/Downloads/Ahon Pipeline/AHON-Pipeline"
```

## Step 4 — Extract NOAH archives

```bash
python src/ingestion/noah/extract_noah.py
```

## Step 5 — Create local Bronze GeoPackages

```bash
python src/ingestion/noah/ingest_noah.py
```

## Step 6 — Export GeoPackages to Parquet

```bash
python src/ingestion/noah/extract_noah_parquet.py
```

## Step 7 — Verify outputs

```bash
ls -lh ~/ahon-parquet/
```

At this point the local GIS pipeline is complete.

---

# 25. Pipeline Checkpoints

The pipeline should be treated as a series of validation gates.

```text
ZIP Archives
     │
     ▼
[CHECKPOINT 1]
Archives exist
     │
     ▼
Shapefiles
     │
     ▼
[CHECKPOINT 2]
Expected source files / CRS / fields verified
     │
     ▼
GeoPackages
     │
     ▼
[CHECKPOINT 3]
Feature counts and CRS verified
     │
     ▼
Parquet
     │
     ▼
[CHECKPOINT 4]
Parquet files readable + row counts verified
     │
     ▼
Databricks
     │
     ▼
[CHECKPOINT 5]
Delta Bronze row counts/schema verified
```

This makes troubleshooting easier because a failure can be isolated to a specific stage.

---

# 26. Troubleshooting

## Error: Bronze GeoPackage not found

Example:

```text
Bronze GeoPackage not found
```

### Cause

`ingest_noah.py` has not been run successfully, or the file is in a different location.

### Fix

Run:

```bash
python src/ingestion/noah/ingest_noah.py
```

Then verify:

```bash
ls -lh data/bronze/noah/
```

---

## Error: 2D GeoPackage feature count mismatch

Example:

```text
Expected: 2,692
Found:    2,691
```

### Meaning

The temporary 2D GeoPackage does not contain the same number of features as the Bronze source.

Do not continue to Databricks until the mismatch is investigated.

---

## Error: Parquet magic bytes not found

Example:

```text
ArrowInvalid:
Parquet magic bytes not found in footer
```

### Meaning

The Parquet file was not finalized correctly or is incomplete/corrupted.

### Recommended response

Delete the affected Parquet output and rerun the export.

For Landslide:

```bash
rm -f ~/ahon-parquet/landslide_chunks/*.parquet
```

Then rerun:

```bash
python src/ingestion/noah/extract_noah_parquet.py
```

The exporter validates each chunk after writing it.

---

## Error: `ogr2ogr: command not found`

### Meaning

GDAL is not available in the current environment.

### Check

```bash
ogr2ogr --version
```

If the command is unavailable, make sure the WSL GIS environment is correctly installed and activated.

---

## Error: Python module not found

Example:

```text
ModuleNotFoundError: No module named 'pyogrio'
```

### Check the environment

```bash
which python
```

It should point to the `ahon-gis` environment.

Then:

```bash
python -c "import pyogrio; print(pyogrio.__version__)"
```

and:

```bash
python -c "import pyarrow; print(pyarrow.__version__)"
```

---

# 27. Why We Use Arrow Streaming

The NOAH datasets contain complex polygon geometries.

Loading an entire GeoPackage into a GeoPandas DataFrame can require a large amount of memory.

Instead, the exporter uses:

```python
pyogrio.open_arrow(...)
```

and processes the data in batches.

Conceptually:

```text
GeoPackage
    │
    ├── Batch 1
    ├── Batch 2
    ├── Batch 3
    ├── ...
    └── Batch N
          ↓
       Parquet
```

This reduces peak memory usage compared with loading the entire dataset at once.

---

# 28. Why Landslide Uses Smaller Chunks

The Landslide dataset contains very complex geometries.

Therefore:

```text
Arrow batch size = 25
Parquet chunk size = 100 rows
```

The exporter accumulates four 25-row Arrow batches:

```text
25 + 25 + 25 + 25 = 100
```

and then writes one Parquet file.

This keeps memory usage more controlled while still producing manageable transfer files.

---

# 29. Why Geometry Is Preserved

The pipeline does not remove the hazard geometry.

The geometry represents the actual mapped spatial extent of the hazard.

For example:

```text
Flood polygon
    +
Barangay polygon
    ↓
Spatial intersection
    ↓
Area affected
    ↓
Exposure percentage
```

The geometry is therefore necessary for later AHON analysis.

Parquet is being used as a transfer format; it is not intended to replace the spatial information with only latitude/longitude points.

---

# 30. What Happens After Parquet

Once the Parquet files are created, they can be transferred to Databricks.

Conceptually:

```text
WSL
/home/allea/ahon-parquet/
        │
        ▼
Databricks Volume / accessible storage
        │
        ▼
Spark
        │
        ▼
Delta Bronze
```

For example, Flood can be read as:

```python
df = spark.read.parquet(
    "/Volumes/<catalog>/<schema>/<volume>/noah/flood_25yr.parquet"
)
```

Storm Surge:

```python
df = spark.read.parquet(
    "/Volumes/<catalog>/<schema>/<volume>/noah/storm_surge.parquet"
)
```

Landslide:

```python
df = spark.read.parquet(
    "/Volumes/<catalog>/<schema>/<volume>/noah/landslide_chunks/*.parquet"
)
```

The exact catalog, schema, and volume names should be replaced with the project's actual Databricks configuration.

---

# 31. Databricks Bronze

The Parquet files are **not themselves the Databricks Bronze layer**.

The intended flow is:

```text
Parquet
   ↓
Spark
   ↓
Delta
   ↓
Databricks Bronze
```

For example:

```python
df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "bronze.noah_flood_25yr"
    )
```

The same pattern can be used for Landslide and Storm Surge.

The exact table naming convention should follow the project's Databricks governance standards.

---

# 32. Data Lineage

The complete lineage is:

```text
NOAH Source Archive
        │
        ▼
Raw Shapefile
        │
        ▼
Local Bronze GeoPackage
        │
        ▼
Temporary 2D GeoPackage
        │
        ▼
Parquet
        │
        ▼
Databricks Delta Bronze
        │
        ▼
Silver
        │
        ▼
Gold
```

The local Bronze GeoPackage retains source metadata such as:

```text
source_file
source_dataset
scenario
source_field
source_crs
ingest_timestamp
```

This allows the downstream dataset to retain traceability back to the original NOAH source.

---

# 33. Recommended GitHub Repository Structure

The relevant project structure should look approximately like:

```text
AHON-Pipeline/
│
├── data/
│   ├── raw/
│   │   └── noah/
│   │       ├── archives/
│   │       └── datasets/
│   │
│   ├── bronze/
│   │   └── noah/
│   │
│   ├── export/
│   │   └── noah/
│   │
│   └── profiling/
│       └── noah/
│
├── src/
│   ├── ingestion/
│   │   └── noah/
│   │       ├── extract_noah.py
│   │       ├── ingest_noah.py
│   │       └── extract_noah_parquet.py
│   │
│   └── profiling/
│       └── noah/
│
├── docs/
│   └── data-profiling/
│
├── tests/
│
├── .gitignore
│
└── README.md
```

The important principle is that the **scripts are version-controlled**, while the large generated data artifacts are not.

---

# 34. Quick Reference

For experienced team members, the complete workflow is simply:

```bash
# Enter WSL
wsl

# Activate GIS environment
source ~/ahon-gis/bin/activate

# Enter repository
cd "/mnt/c/Users/allea/Downloads/Ahon Pipeline/AHON-Pipeline"

# Extract NOAH source archives
python src/ingestion/noah/extract_noah.py

# Create local Bronze GeoPackages
python src/ingestion/noah/ingest_noah.py

# Export Bronze GeoPackages to Parquet
python src/ingestion/noah/extract_noah_parquet.py

# Check Parquet outputs
ls -lh ~/ahon-parquet/
```

---

# 35. Definition of Done

The local NOAH-to-Parquet process is complete when:

- [ ] NOAH archives are available
- [ ] NOAH Shapefiles have been extracted
- [ ] Source profiling has been completed
- [ ] Flood Bronze GeoPackage exists
- [ ] Landslide Bronze GeoPackage exists
- [ ] Storm Surge Bronze GeoPackage exists
- [ ] Flood contains 227 features
- [ ] Landslide contains 2,692 features
- [ ] Storm Surge contains 804 features
- [ ] Temporary 2D GeoPackages validate successfully
- [ ] Flood Parquet is readable
- [ ] Landslide Parquet chunks are readable
- [ ] Storm Surge Parquet is readable
- [ ] Parquet row counts match the Bronze GeoPackages
- [ ] Parquet files are available under `~/ahon-parquet/`
- [ ] Large data files are not committed to GitHub
- [ ] Parquet files are ready for Databricks ingestion

---

# 36. Summary

The AHON NOAH pipeline separates extraction, local GIS ingestion, transfer preparation, and cloud processing:

```text
extract_noah.py
    ↓
ZIP → Shapefile

ingest_noah.py
    ↓
Shapefile → GeoPackage

extract_noah_parquet.py
    ↓
GeoPackage → Parquet

Databricks
    ↓
Parquet → Delta Bronze

Silver
    ↓
Clean + standardize

Gold
    ↓
Exposure + risk analytics
```

This separation makes the pipeline easier to understand, test, troubleshoot, reproduce, and maintain.

The most important rule is:

> **Do not skip stages. Each stage creates and validates the artifact required by the next stage.**

For the local GIS portion, the normal execution order is therefore:

```bash
python src/ingestion/noah/extract_noah.py
python src/ingestion/noah/ingest_noah.py
python src/ingestion/noah/extract_noah_parquet.py
```

After the final command succeeds, the NOAH datasets are ready to be transferred to Databricks for Delta Bronze ingestion.
