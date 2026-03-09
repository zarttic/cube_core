from __future__ import annotations

from shapely.geometry import Point, Polygon, box, shape

from grid_core.app.core.exceptions import ValidationError


GEOMETRY_TYPES = {"Point", "LineString", "Polygon", "MultiPolygon"}


def to_shapely(geometry: dict):
    if not isinstance(geometry, dict) or "type" not in geometry:
        raise ValidationError("Geometry must be a GeoJSON-like object")

    gtype = geometry["type"]
    if gtype not in GEOMETRY_TYPES:
        raise ValidationError(f"Unsupported geometry type: {gtype}")
    return shape(geometry)


def point_from_coords(point: list[float]) -> Point:
    if len(point) != 2:
        raise ValidationError("Point must be [lon, lat]")
    return Point(point[0], point[1])


def bbox_to_polygon(bbox: list[float]) -> Polygon:
    if len(bbox) != 4:
        raise ValidationError("BBox must be [min_lon, min_lat, max_lon, max_lat]")
    return box(*bbox)
