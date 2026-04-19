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


def test_locate_rejects_invalid_point_range():
    service = GridService()
    with pytest.raises(ValidationError):
        service.locate(grid_type=GridType.GEOHASH, level=6, point=[200.0, 39.9])
    with pytest.raises(ValidationError):
        service.locate(grid_type=GridType.GEOHASH, level=6, point=[116.3, 100.0])


def test_cover_compact_returns_bbox_only_cells():
    service = GridService()
    cells = service.cover_compact(
        grid_type=GridType.GEOHASH,
        level=6,
        geometry=None,
        bbox=[116.385, 39.903, 116.397, 39.911],
        cover_mode="intersect",
        crs="EPSG:4326",
    )

    assert len(cells) > 0
    assert all(cell.level == 6 for cell in cells)
    assert all(len(cell.bbox) == 4 for cell in cells)
    assert all(cell.space_code for cell in cells)


def test_cover_bbox_boundary_type_matches_compact_cover_codes_and_bbox():
    service = GridService()
    bbox = [116.385, 39.903, 116.397, 39.911]

    full_bbox_cells = service.cover(
        grid_type=GridType.GEOHASH,
        level=6,
        geometry=None,
        bbox=bbox,
        cover_mode="intersect",
        boundary_type=BoundaryType.BBOX,
        crs="EPSG:4326",
    )
    compact_cells = service.cover_compact(
        grid_type=GridType.GEOHASH,
        level=6,
        geometry=None,
        bbox=bbox,
        cover_mode="intersect",
        crs="EPSG:4326",
    )

    assert all(cell.geometry is None for cell in full_bbox_cells)
    assert {cell.space_code for cell in full_bbox_cells} == {cell.space_code for cell in compact_cells}
    assert {cell.space_code: cell.bbox for cell in full_bbox_cells} == {
        cell.space_code: cell.bbox for cell in compact_cells
    }


def test_cover_bbox_boundary_type_uses_compact_engine_path(monkeypatch):
    service = GridService()
    engine = service._registry.get_engine(GridType.GEOHASH)
    calls = {"compact": 0, "full": 0}

    original_compact = engine.cover_geometry_compact
    original_full = engine.cover_geometry

    def wrapped_compact(*args, **kwargs):
        calls["compact"] += 1
        return original_compact(*args, **kwargs)

    def wrapped_full(*args, **kwargs):
        calls["full"] += 1
        return original_full(*args, **kwargs)

    monkeypatch.setattr(engine, "cover_geometry_compact", wrapped_compact)
    monkeypatch.setattr(engine, "cover_geometry", wrapped_full)

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
    assert calls["compact"] == 1
    assert calls["full"] == 0
