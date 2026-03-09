from __future__ import annotations

from grid_core.app.core.enums import GridType
from grid_core.app.core.exceptions import ValidationError
from grid_core.app.engines.geohash_engine import GeohashEngine
from grid_core.app.engines.isea4h_engine import ISEA4HEngine
from grid_core.app.engines.mgrs_engine import MGRSEngine


class GridEngineRegistry:
    def __init__(self) -> None:
        self._engines = {
            GridType.GEOHASH: GeohashEngine(),
            GridType.MGRS: MGRSEngine(),
            GridType.ISEA4H: ISEA4HEngine(),
        }

    def get_engine(self, grid_type: GridType):
        engine = self._engines.get(grid_type)
        if engine is None:
            raise ValidationError(f"Unsupported grid_type: {grid_type}")
        return engine
