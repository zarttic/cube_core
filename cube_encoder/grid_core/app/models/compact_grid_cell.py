from __future__ import annotations

from typing import List

from grid_core.app.models.grid_address import GridAddress


class CompactGridCell(GridAddress):
    """Compact grid cell for bbox-only cover responses.

    Inherits grid_type, grid_level, space_code, topology_code from GridAddress.
    Adds only the bounding box for lightweight spatial queries.
    """

    bbox: List[float]
