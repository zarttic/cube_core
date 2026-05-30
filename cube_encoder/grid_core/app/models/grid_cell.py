from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class GridCell(BaseModel):
    grid_type: str
    level: int
    cell_id: str
    space_code: str
    center: List[float]
    bbox: List[float]
    geometry: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
