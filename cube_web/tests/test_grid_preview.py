from __future__ import annotations

from shapely.geometry import box, shape
from shapely.ops import unary_union

from grid_core.sdk import CubeEncoderSDK
from cube_web.services.grid_preview import build_continuous_mgrs_preview


def test_continuous_mgrs_preview_uses_one_utm_lattice_across_a_zone_boundary() -> None:
    bbox = [100.6447907229, 23.2863720005, 104.8299474060, 27.0611663017]
    cells = CubeEncoderSDK().cover(
        "mgrs", 0, "intersect", "polygon", bbox=bbox, crs="EPSG:4326"
    )

    preview = build_continuous_mgrs_preview(
        cells,
        requested_grid_level=0,
        target_geometry=box(*bbox).__geo_interface__,
        preview_crs="EPSG:32648",
    )

    assert len(cells) == 40
    assert len(preview) == 30
    geometries = [shape(cell.geometry) for cell in preview]
    assert all(cell.metadata["preview_only"] is True for cell in preview)
    assert all(geometry.is_valid for geometry in geometries)
    assert abs(sum(geometry.area for geometry in geometries) - unary_union(geometries).area) < 1e-9
