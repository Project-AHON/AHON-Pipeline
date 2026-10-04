from pathlib import Path

import geopandas as gpd

RAW_DIR = Path("data/raw/noah/datasets")
FLOOD_DIR = RAW_DIR / "NOAH-Flood-Hazard"


def explore_shapefile(file_path: Path) -> None:
    """Explore one NOAH Shapefile."""

    print("\n" + "=" * 80)
    print(f"FILE: {file_path.name}")
    print("=" * 80)

    gdf = gpd.read_file(file_path)

    print(f"Rows: {len(gdf)}")
    print(f"Columns: {gdf.columns.tolist()}")
    print(f"CRS: {gdf.crs}")
    print(
        "Geometry types:",
        gdf.geometry.geom_type.value_counts().to_dict(),
    )

    print("\nMissing values:")
    print(gdf.isnull().sum().to_dict())

    print("\nUnique values:")
    for column in gdf.columns:
        if column != "geometry":
            print(
                f"{column}:",
                gdf[column].unique().tolist(),
            )


def explore_noah() -> None:
    """Explore all NOAH flood Shapefiles."""

    shapefiles = list(FLOOD_DIR.rglob("*.shp"))

    if not shapefiles:
        raise FileNotFoundError(
            f"No Shapefiles found in {FLOOD_DIR}"
        )

    print(f"Found {len(shapefiles)} Shapefiles.")

    for shapefile in sorted(shapefiles):
        explore_shapefile(shapefile)


if __name__ == "__main__":
    explore_noah()
