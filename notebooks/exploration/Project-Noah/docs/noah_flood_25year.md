# NOAH Flood 25-Year — Data Profiling Results

## Overview
The Project NOAH 25-year Flood Hazard dataset represents a modeled flood-hazard scenario for a 25-year return period. It is not a 25-year historical event record.

## Source Profile
- Source format: ESRI Shapefile
- Source files: **72**
- Total features: **227**
- CRS: **EPSG:4326 / WGS 84**
- Main hazard field: `Var`

### Hazard classification

| Var | Classification |
|---:|---|
| 1 | Low |
| 2 | Medium |
| 3 | High |

The flood hazard classification considers water depth/height and velocity. `Var` is therefore a hazard classification, not a raw depth measurement.

## Geometry
Profiling identified Polygon and some 3D Polygon source geometries. Bronze ingestion promoted geometry to **3D Multi Polygon** while preserving the Z dimension.

## Bronze Validation
| Check | Result |
|---|---|
| Source features | 227 |
| Bronze features | 227 |
| CRS | EPSG:4326 |
| Geometry | 3D Multi Polygon |
| Expected fields | Present |

## Data Quality Findings
- Source filenames are not completely uniform.
- Feature counts vary between source files.
- Some source geometry is 3D.
- Spatial area calculations should use a suitable projected CRS rather than geographic degrees.

## Analytical Use
The dataset can support modeled flood-exposure analysis at community and infrastructure level, including the area affected by each flood-hazard class.

## Profiling Output
`data/profiling/noah/flood_25year_profile.csv`

## Status
**Profiled:** Complete  
**Bronze ingestion:** Complete  
**Bronze structural validation:** Complete  
