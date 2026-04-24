from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class PartitionResult:
    data_type: str
    rows_path: Path
    total_rows: int


class PartitionService(Protocol):
    data_type: str
