from __future__ import annotations

from grid_core.app.core.enums import GridType
from grid_core.app.core.exceptions import ValidationError
from grid_core.app.engines.base import BaseGridEngine

# Lazy factories so importing the registry never pays for heavy engine
# module imports (shapely, mgrs, pyproj, ...) until an engine is actually
# requested.
_ENGINE_FACTORIES = {
    GridType.GEOHASH: lambda: __import__(
        "grid_core.app.engines.geohash_engine", fromlist=["GeohashEngine"]
    ).GeohashEngine(),
    GridType.MGRS: lambda: __import__(
        "grid_core.app.engines.mgrs_engine", fromlist=["MGRSEngine"]
    ).MGRSEngine(),
    GridType.ISEA4H: lambda: __import__(
        "grid_core.app.engines.isea4h_engine", fromlist=["ISEA4HEngine"]
    ).ISEA4HEngine(),
}


class GridEngineRegistry:
    """Registry mapping GridType to its singleton production engine instance.

    Engines are lazily imported and instantiated on first lookup so simply
    constructing a registry (or importing this module) never incurs the
    import cost of shapely/mgrs/pyproj unless an engine is actually used.
    """

    def __init__(self) -> None:
        self._engines: dict[GridType, BaseGridEngine] = {}

    def get_engine(self, grid_type: GridType) -> BaseGridEngine:
        if grid_type not in _ENGINE_FACTORIES:
            raise ValidationError(f"Unsupported grid_type: {grid_type}")
        engine = self._engines.get(grid_type)
        if engine is None:
            engine = _ENGINE_FACTORIES[grid_type]()
            self._engines[grid_type] = engine
        return engine
