# CHIRPS v3.0 Extraction and Ingestion Process

## 1. Purpose

This document describes the extraction and ingestion process for the **CHIRPS v3.0 precipitation dataset** used by Project AHON.

The goal is to make the process reproducible and understandable to another Data Engineer without relying on undocumented manual steps.

The process is responsible for:

1. Identifying available CHIRPS monthly datasets.
2. Downloading monthly CHIRPS GeoTIFF files from the official source.
3. Supporting historical backfill from 2018 onward.
4. Detecting the latest available CHIRPS month automatically.
5. Avoiding unnecessary re-downloads of files that already exist.
6. Validating downloaded raster files.
7. Preserving the original GeoTIFF files as raw/source assets.
8. Maintaining an ingestion manifest.
9. Providing the raw files for downstream Bronze ingestion.
10. Registering raster assets in the Bronze Delta table.

---

# 2. Dataset Overview

| Attribute | Value |
|---|---|
| Dataset | CHIRPS v3.0 |
| Full Name | Climate Hazards Center InfraRed Precipitation with Station Data |
| Provider | Climate Hazards Center, UCSB |
| Data Type | Gridded precipitation |
| Product | Monthly global GeoTIFF |
| Spatial Resolution | 0.05° |
| CRS | EPSG:4326 |
| Raster Width | 7200 |
| Raster Height | 2400 |
| Bands | 1 |
| Data Type | float32 |
| Historical Product Start | 1981 |
| AHON Ingestion Start | 2018 |
| Current AHON Grain | 1 monthly raster asset |

Official source:

```text
https://data.chc.ucsb.edu/products/CHIRPS/v3.0/
```

Monthly GeoTIFF endpoint:

```text
https://data.chc.ucsb.edu/products/CHIRPS/v3.0/monthly/global/tifs/{filename}
```

---

# 3. Why CHIRPS Is Used in AHON

CHIRPS provides gridded precipitation estimates that can support rainfall and hazard analysis for Philippine local government units (LGUs).

Potential downstream use cases include:

- rainfall exposure analysis
- rainfall trend analysis
- LGU rainfall profiles
- high-rainfall area identification
- rainfall and hazard overlays
- disaster-risk analysis

CHIRPS does not provide Philippine PSGC or LGU identifiers. Administrative attribution is therefore performed later using the curated AHON LGU spatial reference layer.

---

# 4. Architecture

```text
Official CHIRPS Repository
            |
            v
     Extraction Process
     chirps_ingestion.py
            |
            +-- Detect available month
            +-- Download missing TIFFs
            +-- Validate raster
            +-- Create manifest
            |
            v
       Raw GeoTIFF Files
            |
            v
      Bronze Ingestion
      ahon.bronze.chc_chirps
            |
            v
     Silver Transformation
            |
            +-- Handle NoData
            +-- Clip/process Philippines
            +-- Raster-cell processing
            +-- Spatial LGU mapping
            |
            v
       Gold Analytics
            |
            +-- LGU rainfall indicators
            +-- Hazard analysis
            +-- Dashboards
```

## Layer Responsibilities

### Extraction / Raw
Responsible for retrieving and validating source files. No analytical transformations are performed.

### Bronze
Responsible for registering raw raster assets and preserving provenance.

**Bronze grain: 1 row = 1 monthly CHIRPS raster asset.**

### Silver
Responsible for spatial and data transformations such as handling `-9999`, clipping to the Philippines, extracting raster cells, and associating cells with LGUs.

### Gold
Responsible for business-ready analytics such as LGU-level rainfall indicators and hazard analysis.

---

# 5. Repository Structure

```text
src/
└── sql/
    └── datasets/
        └── chc_chirps/
            ├── extract/
            │   └── chirps_ingestion.py
            ├── raw/
            │   ├── chirps-v3.0.2018.01.tif
            │   ├── chirps-v3.0.2018.02.tif
            │   ├── ...
            │   └── chirps-v3.0.2026.08.tif
            ├── bronze/
            │   └── chc_chirps.py
            ├── silver/
            └── metadata/
                └── ingestion_manifest.csv
```

The raw files are ultimately stored in:

```text
/Volumes/ahon_dev/reference/source/chc_chirps
```

Expected Volume structure:

```text
/Volumes/ahon_dev/reference/source/chc_chirps/
├── chirps-v3.0.2018.01.tif
├── chirps-v3.0.2018.02.tif
├── ...
└── chirps-v3.0.2026.08.tif
```

---

# 6. Extraction Process

The extraction process is implemented in:

```text
src/sql/datasets/chc_chirps/extract/chirps_ingestion.py
```

The process performs:

```text
Start
  |
  v
Determine requested date range
  |
  v
Determine latest available CHIRPS month
  |
  v
Loop through each requested month
  |
  v
Does raw TIFF already exist?
  |
  +---- YES ---> Skip download
  |
  +---- NO ----> Download TIFF
                    |
                    v
                 Validate
                    |
              +-----+-----+
              |           |
            PASS         FAIL
              |           |
              v           v
          Keep file     Delete file
              |
              v
       Record metadata
              |
              v
        Create manifest
              |
              v
             End
```

---

# 7. Source File Naming Convention

CHIRPS monthly files follow:

```text
chirps-v3.0.YYYY.MM.tif
```

Examples:

```text
chirps-v3.0.2018.01.tif
chirps-v3.0.2018.02.tif
chirps-v3.0.2026.08.tif
```

The filename provides the temporal partition:

```text
chirps-v3.0.2026.08.tif
               |    |
             year month
```

The ingestion process parses the year and month from this filename.

---

# 8. Source URL Construction

The extraction process uses:

```python
CHIRPS_BASE_URL = (
    "https://data.chc.ucsb.edu/products/CHIRPS/v3.0/"
    "monthly/global/tifs"
)
```

A filename is appended to the base URL:

```python
filename = "chirps-v3.0.2026.08.tif"

source_url = (
    f"{CHIRPS_BASE_URL}/{filename}"
)
```

Result:

```text
https://data.chc.ucsb.edu/products/CHIRPS/v3.0/monthly/global/tifs/chirps-v3.0.2026.08.tif
```

---

# 9. Latest Available Month Detection

The ingestion does not assume that the current calendar month is available.

It starts from the current UTC year/month and checks backward until an available monthly dataset is found.

```text
Current month
     |
     v
Check YYYY.MM
     |
     +-- 200 OK --> Latest available month
     |
     +-- 404 -----> Check previous month
                       |
                       v
                    Repeat
```

The function is:

```python
get_latest_available_chirps_month()
```

A `404` means that month is not available at the source, so the process checks the previous month.

Other HTTP errors are raised.

---

# 10. Historical Backfill

AHON currently starts CHIRPS ingestion at:

```text
2018-01
```

The ingestion can automatically determine the latest available month and process the entire range.

Example:

```python
latest_year, latest_month = (
    get_latest_available_chirps_month()
)

results = ingest_chirps_range(
    start_year=2018,
    start_month=1,
    end_year=latest_year,
    end_month=latest_month,
    output_dir=raw_dir,
)
```

The same process can therefore support both historical backfill and future monthly ingestion.

---

# 11. Idempotent Download Behavior

Before downloading a file, the ingestion checks whether the expected output file already exists.

If it exists, the download is skipped:

```text
SKIP: chirps-v3.0.2026.08.tif already exists.
```

Example:

```text
First run:
2018.01 -> downloaded

Second run:
2018.01 -> skipped
```

This reduces unnecessary network requests and avoids overwriting an existing raw source file.

---

# 12. Download and Error Handling

For a missing file, the ingestion downloads the file from the official CHIRPS endpoint.

Statuses include:

```text
downloaded
skipped
not_available
```

A `404` is treated as `not_available`.

Network errors and unexpected HTTP errors are raised. Incomplete output files are removed so they cannot be mistaken for valid source data.

---

# 13. Raster Validation

Every newly downloaded TIFF is validated using Rasterio.

Expected characteristics:

| Check | Expected |
|---|---|
| File exists | Yes |
| File size | > 0 bytes |
| Width | 7200 |
| Height | 2400 |
| Bands | 1 |
| CRS | EPSG:4326 |
| Data type | float32 |
| Resolution | approximately 0.05° |

Configuration:

```python
EXPECTED_WIDTH = 7200
EXPECTED_HEIGHT = 2400
EXPECTED_BANDS = 1
EXPECTED_DTYPE = "float32"
EXPECTED_CRS = "EPSG:4326"
EXPECTED_RESOLUTION = 0.05
```

---

# 14. NoData Handling

CHIRPS can use:

```text
-9999
```

as the invalid/fill value.

The source GeoTIFF metadata may not always expose this value through the formal `nodata` field.

The raw extraction process therefore preserves the original file unchanged.

Explicit NoData handling belongs in the Silver transformation layer.

---

# 15. Ingestion Manifest

The extraction process creates:

```text
src/sql/datasets/chc_chirps/metadata/ingestion_manifest.csv
```

The manifest provides an operational record of ingestion attempts.

Typical fields:

```text
year
month
filename
status
source_url
path
file_size
ingestion_timestamp
```

The manifest can answer:

- Which months were downloaded?
- Which files already existed?
- Which months were unavailable?
- Where was each file stored?
- When was each file processed?

---

# 16. Current Backfill Result

The completed AHON backfill covered:

```text
2018-01 → 2026-08
```

Expected monthly files:

```text
104
```

Validation:

```text
Expected months: 104
Actual months:   104
Missing months:  0
Extra months:    0
```

Status:

```text
Downloaded: 95
Skipped:     9
Total:       104
```

---

# 17. Bronze Ingestion

After extraction, raw TIFFs are registered in:

```text
ahon.bronze.chc_chirps
```

The Bronze implementation uses Spark's `binaryFile` reader:

```python
chirps_df = (
    spark.read
    .format("binaryFile")
    .load(f"{BASE_PATH}/*.tif")
)
```

This creates one record per source file.

Therefore:

```text
104 TIFF files
      |
      v
104 Bronze asset records
```

The TIFF pixels are not expanded into raster-cell rows in Bronze.

---

# 18. Bronze Provenance

The Bronze table adds:

| Column | Purpose |
|---|---|
| `_source_name` | Dataset identifier |
| `_source_ref` | Source location |
| `_ingested_at` | Bronze ingestion timestamp |
| `_batch_id` | Bronze ingestion run identifier |
| `_row_hash` | SHA-256 hash of source file content |

For CHIRPS:

```text
_source_name = chc_chirps
```

---

# 19. Row Hash

The Bronze process generates `_row_hash` using SHA-256:

```python
.withColumn(
    "_row_hash",
    F.sha2(
        F.col("content"),
        256
    )
)
```

The hash is calculated from the binary TIFF content.

Conceptually:

```text
TIFF binary content
        |
        v
      SHA-256
        |
        v
   _row_hash
```

The hash provides a content-level identity for the source asset.

---

# 20. Data Lineage

```text
Climate Hazards Center / UCSB
            |
            v
     CHIRPS v3.0 source
            |
            v
   Monthly GeoTIFF download
            |
            v
      AHON raw storage
            |
            v
  ahon.bronze.chc_chirps
            |
            v
    Silver transformation
            |
            v
     LGU spatial mapping
            |
            v
       Gold analytics
            |
            v
      AHON dashboards
```

---

# 21. Reproduction Instructions

A Data Engineer reproducing the ingestion should:

## Step 1 — Confirm repository module

```text
src/sql/datasets/chc_chirps/extract/chirps_ingestion.py
```

## Step 2 — Confirm raw Volume

```text
/Volumes/ahon_dev/reference/source/chc_chirps
```

## Step 3 — Confirm official source

```text
https://data.chc.ucsb.edu/products/CHIRPS/v3.0/
```

## Step 4 — Run the historical backfill

Start at:

```text
2018-01
```

and allow the ingestion to detect the latest available month.

## Step 5 — Validate coverage

Verify:

```text
Expected months
Actual months
Missing months
Extra months
```

Expected:

```text
Missing months = 0
Extra months = 0
```

## Step 6 — Verify raw files

Every expected month should have a non-zero TIFF.

## Step 7 — Run Bronze ingestion

Read:

```text
/Volumes/ahon_dev/reference/source/chc_chirps/*.tif
```

and write:

```text
ahon.bronze.chc_chirps
```

## Step 8 — Validate Bronze

Check:

- table exists
- expected asset count
- provenance fields populated
- `_row_hash` populated
- no unexpected duplicate assets

---

# 22. Future Monthly Operation

The intended recurring process is:

```text
Run pipeline
     |
     v
Check latest available month
     |
     v
Compare with existing raw files
     |
     v
Download only missing months
     |
     v
Validate new files
     |
     v
Update manifest
     |
     v
Bronze ingestion
     |
     v
Silver processing
```

Example:

```text
Existing:
2026-08

New source:
2026-09

Next run:
2026-08 -> SKIP
2026-09 -> DOWNLOAD
```

---

# 23. Duplicate Prevention

The extraction process prevents duplicate downloads by checking filenames before downloading.

The current Bronze implementation uses:

```python
.mode("append")
```

Therefore, rerunning Bronze over all existing TIFFs can create duplicate Bronze records.

Before production scheduling, Bronze should use a duplicate-safe Delta strategy based on a stable asset key such as:

```text
filename
```

or:

```text
year + month
```

The `_row_hash` should remain available for source-content comparison.

---

# 24. Layer Responsibilities

| Responsibility | Layer |
|---|---|
| Locate official source | Extraction |
| Detect latest month | Extraction |
| Download TIFF | Extraction |
| Validate raw TIFF | Extraction |
| Preserve raw TIFF | Raw |
| Maintain manifest | Extraction / Metadata |
| Register raster asset | Bronze |
| Add provenance | Bronze |
| Handle `-9999` | Silver |
| Clip/process Philippines | Silver |
| Raster-cell transformation | Silver |
| LGU spatial mapping | Silver |
| LGU-level metrics | Gold |
| Dashboard/business analysis | Gold / Analytics |

This separation keeps source acquisition, raw preservation, transformation, and analytics independent.

---

# 25. Quality Gates

The ingestion is considered successful only when:

### Source

- Official CHIRPS source is reachable.
- Requested files are available or correctly marked unavailable.

### File

- File exists.
- File is non-zero.
- File can be opened as a raster.

### Raster

- Width = 7200.
- Height = 2400.
- Bands = 1.
- CRS = EPSG:4326.
- Dtype = float32.
- Resolution approximately 0.05°.

### Temporal coverage

- Requested months are present.
- No expected month is missing.
- No unexpected month is included.

### Bronze

- Delta table exists.
- Provenance fields are populated.
- `_row_hash` is populated.
- Source assets can be traced to the raw files.

---

# 26. Known Limitations

## Raster versus administrative boundaries

CHIRPS is a raster dataset and does not directly contain PSGC or LGU identifiers. LGU attribution requires spatial processing.

## Spatial resolution

The 0.05° grid should not be interpreted as a direct measurement for an individual barangay or household.

## Large source files

The global monthly raster covers a large geographic area. Downstream processing should avoid unnecessary processing outside the Philippines.

## Publication timing

The latest calendar month may not yet be available. The ingestion therefore detects the latest available month.

## NoData metadata

The source may contain `-9999` as a fill value even when the raster metadata does not expose it as the formal NoData value. This must be handled explicitly downstream.

---

# 27. Maintenance

Update the ingestion code and this documentation when:

- CHIRPS changes its URL structure.
- CHIRPS changes its filename convention.
- CHIRPS changes raster dimensions.
- CHIRPS changes CRS.
- CHIRPS changes resolution.
- A new CHIRPS version is adopted.
- AHON changes its historical start date.
- Raw storage location changes.

When changes occur, review:

1. `chirps_ingestion.py`
2. this documentation
3. validation expectations
4. tests/quality checks
5. downstream transformations

---

# 28. Quick Reference

| Item | Value |
|---|---|
| Official source | `https://data.chc.ucsb.edu/products/CHIRPS/v3.0/` |
| Monthly TIFF endpoint | `https://data.chc.ucsb.edu/products/CHIRPS/v3.0/monthly/global/tifs/` |
| Filename pattern | `chirps-v3.0.YYYY.MM.tif` |
| Extraction module | `src/sql/datasets/chc_chirps/extract/chirps_ingestion.py` |
| Raw Volume | `/Volumes/ahon_dev/reference/source/chc_chirps` |
| Manifest | `src/sql/datasets/chc_chirps/metadata/ingestion_manifest.csv` |
| Bronze table | `ahon.bronze.chc_chirps` |
| Bronze grain | 1 row = 1 monthly raster asset |
| AHON start | 2018-01 |
| Current documented coverage | 2018-01 to 2026-08 |
| Current documented asset count | 104 |

---

# 29. Final Data Flow

```text
                 OFFICIAL CHIRPS
                       |
                       v
          CHIRPS Monthly GeoTIFF
                       |
                       v
          chirps_ingestion.py
                       |
          +------------+------------+
          |            |            |
          v            v            v
       Detect       Download      Validate
       latest       missing       raster
       month         files
          |            |            |
          +------------+------------+
                       |
                       v
                 Raw GeoTIFF
                       |
                       v
             Ingestion Manifest
                       |
                       v
           Spark binaryFile reader
                       |
                       v
             ahon.bronze.chc_chirps
                       |
                       v
            Silver transformation
                       |
                       v
             Spatial LGU mapping
                       |
                       v
               Gold analytics
                       |
                       v
                 AHON outputs
```

## Core Principle

> **Extract and preserve the source faithfully first. Transform only in downstream layers.**
