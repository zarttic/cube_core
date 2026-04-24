from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class OpticalPartitionService:
    """Service marker for image/COG window partitioning.

    The current optical implementation remains in
    `cube_split.jobs.ray_logical_partition_job` and
    `cube_split.jobs.ray_partition_core`. This wrapper makes the data-type
    boundary explicit without changing the proven optical pipeline.
    """

    data_type: str = "optical"
    supported_families: tuple[str, ...] = ("landsat", "sentinel_optical")

    def job_module(self) -> str:
        return "cube_split.jobs.ray_logical_partition_job"

    def default_rows_path(self, run_dir: Path) -> Path:
        return run_dir / "index_rows.jsonl"
