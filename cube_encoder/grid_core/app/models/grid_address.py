from __future__ import annotations

from pydantic import BaseModel


class GridAddress(BaseModel):
    """Canonical address for any grid cell across all production grid types.

    - grid_type: one of "geohash", "mgrs", "isea4h"
    - grid_level: the actual level/resolution of this specific cell
    - space_code: standard publicly exchangeable code (Geohash base32, MGRS, ISEA4H SEQNUM)
    - topology_code: optional legacy extension; standard grid engines return None
    """

    grid_type: str
    grid_level: int
    space_code: str
    topology_code: str | None = None
