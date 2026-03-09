import pytest

from grid_core.app.core.enums import BoundaryType, GridType
from grid_core.app.core.exceptions import ValidationError
from grid_core.app.services.grid_service import GridService


def test_cover_rejects_non_epsg4326():
    service = GridService()
    with pytest.raises(ValidationError):
        service.cover(
            grid_type=GridType.GEOHASH,
            level=6,
            geometry={"type": "Point", "coordinates": [116.391, 39.907]},
            bbox=None,
            cover_mode="intersect",
            boundary_type=BoundaryType.POLYGON,
            crs="EPSG:3857",
        )


def test_cover_requires_geometry_or_bbox():
    service = GridService()
    with pytest.raises(ValidationError):
        service.cover(
            grid_type=GridType.GEOHASH,
            level=6,
            geometry=None,
            bbox=None,
            cover_mode="intersect",
            boundary_type=BoundaryType.POLYGON,
            crs="EPSG:4326",
        )


def test_cover_bbox_boundary_type_sets_cell_geometry_none():
    service = GridService()
    cells = service.cover(
        grid_type=GridType.GEOHASH,
        level=6,
        geometry=None,
        bbox=[116.385, 39.903, 116.397, 39.911],
        cover_mode="intersect",
        boundary_type=BoundaryType.BBOX,
        crs="EPSG:4326",
    )
    assert len(cells) > 0
    assert all(cell.geometry is None for cell in cells)
