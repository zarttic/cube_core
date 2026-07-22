from __future__ import annotations

import os

import pytest
import rasterio
from rasterio.features import geometry_window
from rasterio.warp import transform_bounds, transform_geom
from shapely.geometry import box, shape

from grid_core.app.models.grid_address import GridAddress
from grid_core.sdk import CubeEncoderSDK

pytestmark = pytest.mark.real_aoi


def _real_aoi_uri() -> str:
    uri = os.environ.get("CUBE_GRID_REAL_AOI_URI", "").strip()
    if not uri:
        pytest.fail("CUBE_GRID_REAL_AOI_URI is required for the production real-AOI gate")
    return uri


def test_three_grids_cover_real_raster_with_positive_windows() -> None:
    uri = _real_aoi_uri()
    sdk = CubeEncoderSDK()
    with rasterio.open(uri) as dataset:
        if dataset.crs is None:
            pytest.fail("real AOI raster must declare a CRS")
        min_x, min_y, max_x, max_y = dataset.bounds
        min_lon, min_lat, max_lon, max_lat = transform_bounds(dataset.crs, "EPSG:4326", min_x, min_y, max_x, max_y)
        width = max_lon - min_lon
        height = max_lat - min_lat
        if width <= 0 or height <= 0:
            pytest.fail("real AOI raster has invalid WGS84 bounds")
        aoi = box(min_lon + width * 0.375, min_lat + height * 0.375, min_lon + width * 0.625, min_lat + height * 0.625)
        specs = (("geohash", 6), ("mgrs", 2), ("isea4h", 4))
        for grid_type, requested_grid_level in specs:
            cells = sdk.cover(
                grid_type=grid_type,
                requested_grid_level=requested_grid_level,
                cover_mode="intersect",
                boundary_type="polygon",
                geometry=aoi.__geo_interface__,
                crs="EPSG:4326",
            )
            assert cells, grid_type
            windows = 0
            for cell in cells:
                assert cell.grid_level <= requested_grid_level
                assert cell.topology_code is None
                geometry = sdk.code_to_geometry(
                    GridAddress(
                        grid_type=cell.grid_type,
                        grid_level=cell.grid_level,
                        space_code=cell.space_code,
                        topology_code=cell.topology_code,
                    )
                )
                overlap = shape(geometry).intersection(aoi)
                if overlap.is_empty or overlap.area == 0:
                    continue
                native_geometry = transform_geom("EPSG:4326", dataset.crs, overlap.__geo_interface__)
                window = geometry_window(dataset, [native_geometry])
                assert window.width > 0 and window.height > 0
                assert window.col_off >= 0 and window.row_off >= 0
                assert window.col_off + window.width <= dataset.width
                assert window.row_off + window.height <= dataset.height
                windows += 1
            assert windows > 0, grid_type
