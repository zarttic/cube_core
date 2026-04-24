"""Partition services for different remote-sensing data families."""

from cube_split.partition.carbon import (
    CarbonPartitionConfig,
    CarbonSatelliteObservation,
    CarbonSatellitePartitionService,
)
from cube_split.partition.optical import OpticalPartitionService
from cube_split.partition.registry import get_partition_service

__all__ = [
    "CarbonPartitionConfig",
    "CarbonSatelliteObservation",
    "CarbonSatellitePartitionService",
    "OpticalPartitionService",
    "get_partition_service",
]
