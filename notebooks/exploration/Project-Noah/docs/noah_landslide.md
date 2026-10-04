# NOAH Landslide — Data Profiling Results

## Overview
The Project NOAH Landslide dataset contains mapped landslide-hazard areas and source hazard classifications.

## Source Profile
- Source format: ESRI Shapefile
- Source files: **82**
- Total features: **2,692**
- CRS: **EPSG:4326 / WGS 84**
- Common hazard field: `LH`

### Hazard classification

| LH | Classification |
|---:|---|
| 1 | Low |
| 2 | Medium |
| 3 | High |

Most files use `LH`. Profiling also found a small number with additional fields such as `GRIDCODE`, `GRID`, or `aud`.

## Geometry
Profiling identified 81 Polygon files and 1 3D Polygon file. Bronze ingestion promoted geometry to **3D Multi Polygon** while preserving the Z dimension.

A large Isabela source file was found to contain only 19 features but very complex geometry, demonstrating that file size does not necessarily indicate feature count.

## Source Methodology Notes
The supplied source documentation describes use of 1m LiDAR / 5m IfSAR-derived DEM data and methods including Matterocking/Conefall, SINMAP, and Flow-R. It also describes Conefall runout and Flow-R debris-flow extents as high-hazard components.

A separate Debris Flow source is profiled independently and is not assumed to be identical to this Landslide dataset.

## Bronze Validation
| Check | Result |
|---|---|
| Source features | 2,692 |
| Bronze features | 2,692 |
| CRS | EPSG:4326 |
| Geometry | 3D Multi Polygon |
| Expected fields | Present |

## Data Quality Findings
- Internal field structures vary slightly across files.
- Source geometries include 2D and 3D polygons.
- Some files are very large because of complex geometries.
- Source metadata should be preserved through ingestion.

## Analytical Use
The dataset can support community landslide exposure, barangay-level hazard-area analysis, infrastructure exposure, and multi-hazard overlays.

## Profiling Output
`data/profiling/noah/landslide_profile.csv`

## Status
**Profiled:** Complete  
**Bronze ingestion:** Complete  
**Bronze structural validation:** Complete  
