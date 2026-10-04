# NOAH Storm Surge — Data Profiling Results

## Overview
The Project NOAH Storm Surge dataset contains modeled coastal storm-surge hazard areas organized by advisory/scenario level.

## Source Profile
- Source format: ESRI Shapefile
- Source files: **268**
- Total features: **804**
- Files per advisory level: **67**
- CRS: **EPSG:4326 / WGS 84**
- Hazard field: `HAZ`
- Advisory levels: `SSA1`, `SSA2`, `SSA3`, `SSA4`

### Hazard classification

| HAZ | Classification |
|---:|---|
| 1 | Low |
| 2 | Medium |
| 3 | High |

### Advisory levels

| Level | Source definition |
|---|---|
| SSA1 | Up to 2 m |
| SSA2 | Up to 3 m |
| SSA3 | Up to 4 m |
| SSA4 | Greater than 4 m |

`HAZ` and `SSA1–SSA4` represent different concepts and should remain separate in Bronze.

## Geometry
All profiled source files contain polygon geometry and use EPSG:4326. Bronze ingestion promoted geometry to **3D Multi Polygon** and preserved the Z dimension.

## Feature Counts

| Advisory | Files | Features |
|---|---:|---:|
| SSA1 | 67 | 201 |
| SSA2 | 67 | 201 |
| SSA3 | 67 | 201 |
| SSA4 | 67 | 201 |
| **Total** | **268** | **804** |

## Bronze Validation
| Check | Result |
|---|---|
| Source features | 804 |
| Bronze features | 804 |
| CRS | EPSG:4326 |
| Geometry | 3D Multi Polygon |
| Expected fields | Present |

## Source Methodology Notes
The supplied source documentation describes JMA storm-surge modeling, WXTide, Flo-2D, and 1m LiDAR or 5m IfSAR-derived DEM data. Hazard definitions include maximum depth and depth × velocity thresholds.

## Analytical Use
The dataset can support coastal community exposure, storm-surge scenario comparison, infrastructure exposure, and multi-hazard analysis.

## Profiling Output
`data/profiling/noah/storm_surge_profile.csv`

## Status
**Profiled:** Complete  
**Bronze ingestion:** Complete  
**Bronze structural validation:** Complete  
