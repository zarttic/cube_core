from __future__ import annotations

from shapely.geometry import MultiPolygon, Point, Polygon, box, shape

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


def bbox_to_polygon(bbox: list[float]) -> Polygon | MultiPolygon:
    if len(bbox) != 4:
        raise ValidationError("BBox must be [min_lon, min_lat, max_lon, max_lat]")
    min_lon, min_lat, max_lon, max_lat = bbox
    if min_lat > max_lat:
        raise ValidationError("BBox min_lat must be <= max_lat")
    if min_lat < -90.0 or max_lat > 90.0:
        raise ValidationError("BBox latitude must be in [-90, 90]")
    if min_lon < -180.0 or min_lon > 180.0 or max_lon < -180.0 or max_lon > 180.0:
        raise ValidationError("BBox longitude must be in [-180, 180]")

    if min_lon <= max_lon:
        return box(min_lon, min_lat, max_lon, max_lat)

    # Dateline-crossing bbox (e.g. [170, -10, -170, 10]):
    # split into two polygons in [-180, 180] longitude space.
    left = box(min_lon, min_lat, 180.0, max_lat)
    right = box(-180.0, min_lat, max_lon, max_lat)
    return MultiPolygon([left, right])
