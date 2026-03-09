from enum import Enum


class GridType(str, Enum):
    GEOHASH = "geohash"
    MGRS = "mgrs"
    ISEA4H = "isea4h"


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
