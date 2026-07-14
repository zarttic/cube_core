from __future__ import annotations

from grid_core.app.core.enums import GridType
from grid_core.app.core.exceptions import ValidationError


class GridEngineRegistry:
    """Engine registry stub for M1 contract freeze.

    Real engine registrations are wired in Task 8 after replacement engines
    (Tasks 2–7) pass their replacement gates.  Until then every engine lookup
    raises ValidationError so callers get a clear message instead of a crash.
    """

    def __init__(self) -> None:
        self._engines: dict[GridType, object] = {}

    def get_engine(self, grid_type: GridType) -> object:
        engine = self._engines.get(grid_type)
        if engine is None:
            raise ValidationError(f"Engine not yet implemented for grid_type: {grid_type}")
        return engine
