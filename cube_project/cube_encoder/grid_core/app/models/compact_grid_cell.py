from __future__ import annotations

from pydantic import BaseModel


class CompactGridCell(BaseModel):
    space_code: str
    level: int
    bbox: list[float]
