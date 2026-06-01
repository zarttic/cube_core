from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RadarPartitionService:
    """Service marker for Sentinel-1 radar window partitioning."""

    data_type: str = "radar"
    supported_families: tuple[str, ...] = ("sentinel1",)

    def job_module(self) -> str:
        return "cube_split.jobs.ray_logical_partition_job"

    def default_rows_path(self, run_dir: Path) -> Path:
        return run_dir / "index_rows.jsonl"
