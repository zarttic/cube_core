from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import Field

from grid_core.app.models.grid_address import GridAddress


class GridCell(GridAddress):
    """A fully resolved grid cell with geometry and metadata.

    Inherits grid_type, grid_level, space_code, topology_code from GridAddress.
    Adds spatial geometry fields and arbitrary metadata.
    """

    center: List[float]
    bbox: List[float]
    geometry: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
