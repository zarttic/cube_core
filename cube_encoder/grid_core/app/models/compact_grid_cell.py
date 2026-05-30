from __future__ import annotations

from typing import List

from pydantic import BaseModel


class CompactGridCell(BaseModel):
    space_code: str
    level: int
    bbox: List[float]
