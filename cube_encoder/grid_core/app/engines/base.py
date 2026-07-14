from __future__ import annotations

from abc import ABC, abstractmethod

from grid_core.app.models.compact_grid_cell import CompactGridCell
from grid_core.app.models.grid_address import GridAddress
from grid_core.app.models.grid_cell import GridCell


class BaseGridEngine(ABC):
    """Abstract base protocol for all production grid engines.

    All topology and geometry methods consume GridAddress objects so that:
    - ISEA4H sequence numbers are always paired with their resolution.
    - MGRS topology results retain both space_code and topology_code identities.

    Implementations raise NotImplementedError until replaced in Tasks 2-7.
    """

    @abstractmethod
    def locate_point(self, lon: float, lat: float, requested_grid_level: int) -> GridCell:
        raise NotImplementedError

    @abstractmethod
    def locate_space_code(self, lon: float, lat: float, requested_grid_level: int) -> GridAddress:
        raise NotImplementedError

    @abstractmethod
    def cover_geometry(
        self, geometry: dict[str, object], requested_grid_level: int, cover_mode: str
    ) -> list[GridCell]:
        raise NotImplementedError

    @abstractmethod
    def cover_geometry_compact(
        self, geometry: dict[str, object], requested_grid_level: int, cover_mode: str
    ) -> list[CompactGridCell]:
        raise NotImplementedError

    @abstractmethod
    def code_to_geometry(self, address: GridAddress) -> dict[str, object]:
        raise NotImplementedError

    @abstractmethod
    def code_to_center(self, address: GridAddress) -> list[float]:
        raise NotImplementedError

    @abstractmethod
    def code_to_bbox(self, address: GridAddress) -> list[float]:
        raise NotImplementedError

    @abstractmethod
    def neighbors(self, address: GridAddress, k: int = 1) -> list[GridAddress]:
        raise NotImplementedError

    @abstractmethod
    def parent(self, address: GridAddress) -> GridAddress:
        raise NotImplementedError

    @abstractmethod
    def children(self, address: GridAddress, target_grid_level: int) -> list[GridAddress]:
        raise NotImplementedError
