from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class GridCell(BaseModel):
    grid_type: str
    level: int
    cell_id: str
    space_code: str
    center: list[float]
    bbox: list[float]
    geometry: dict[str, Any] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
