from __future__ import annotations

from cube_split.partition.base import PartitionService
from cube_split.partition.carbon import CarbonSatellitePartitionService
from cube_split.partition.optical import OpticalPartitionService


def get_partition_service(data_type: str) -> PartitionService:
    normalized = data_type.strip().lower().replace("-", "_")
    if normalized == "optical":
        return OpticalPartitionService()
    if normalized in {"carbon", "carbon_satellite"}:
        return CarbonSatellitePartitionService()
    raise ValueError(f"Unsupported partition data_type: {data_type}")
