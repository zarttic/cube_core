from enum import Enum


class GridType(str, Enum):
    S2 = "s2"
    MGRS = "mgrs"
    ISEA4H = "isea4h"
    TILE_MATRIX = "tile_matrix"
    PLANE_GRID = "plane_grid"


class CoverMode(str, Enum):
    INTERSECT = "intersect"
    CONTAIN = "contain"
    MINIMAL = "minimal"


class BoundaryType(str, Enum):
    BBOX = "bbox"
    POLYGON = "polygon"


class TimeGranularity(str, Enum):
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    MONTH = "month"
