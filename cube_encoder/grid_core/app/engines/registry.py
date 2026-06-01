from __future__ import annotations

from grid_core.app.core.enums import GridType
from grid_core.app.core.exceptions import ValidationError
from grid_core.app.engines.geohash_engine import GeohashEngine
from grid_core.app.engines.mgrs_engine import MGRSEngine
from grid_core.app.engines.tile_matrix_engine import TileMatrixEngine


class GridEngineRegistry:
    def __init__(self) -> None:
        self._engines = {
            GridType.GEOHASH: GeohashEngine(),
            GridType.MGRS: MGRSEngine(),
            GridType.TILE_MATRIX: TileMatrixEngine(),
        }

    def get_engine(self, grid_type: GridType):
        if grid_type == GridType.ISEA4H and grid_type not in self._engines:
            from grid_core.app.engines.isea4h_engine import ISEA4HEngine

            self._engines[grid_type] = ISEA4HEngine()
        engine = self._engines.get(grid_type)
        if engine is None:
            raise ValidationError(f"Unsupported grid_type: {grid_type}")
        return engine
