from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class OpticalAssetMetadata:
    scene_id: str
    band: str
    acq_time: datetime
    product_family: str
    sensor: str


class OpticalProductAdapter(Protocol):
    family: str
    aliases: tuple[str, ...]
    sensor: str

    def match(self, path: Path) -> bool:
        ...

    def parse(self, path: Path) -> OpticalAssetMetadata:
        ...


class LandsatCollectionAdapter:
    family = "landsat"
    aliases = ("landsat", "landsat_collection", "landsat8", "landsat9")
    sensor = "landsat_oli_tirs"

    def match(self, path: Path) -> bool:
        return len(path.stem.split("_")) >= 7 and path.stem.upper().startswith(("LC08_", "LC09_", "LE07_", "LT05_"))

    def parse(self, path: Path) -> OpticalAssetMetadata:
        parts = path.stem.split("_")
        if len(parts) < 7:
            raise ValueError(f"Invalid Landsat Collection filename: {path.name}")
        scene_id = "_".join(parts[:7])
        date_text = parts[3]
        acq_time = datetime.strptime(date_text, "%Y%m%d").replace(tzinfo=timezone.utc)
        band = path.stem[len(scene_id) + 1 :].lower() if path.stem.startswith(scene_id + "_") else path.stem.lower()
        return OpticalAssetMetadata(
            scene_id=scene_id,
            band=band,
            acq_time=acq_time,
            product_family=self.family,
            sensor=self._sensor_from_scene_id(scene_id),
        )

    def _sensor_from_scene_id(self, scene_id: str) -> str:
        if scene_id.startswith("LC09_"):
            return "landsat9_oli_tirs"
        if scene_id.startswith("LC08_"):
            return "landsat8_oli_tirs"
        if scene_id.startswith("LE07_"):
            return "landsat7_etm"
        if scene_id.startswith("LT05_"):
            return "landsat5_tm"
        return self.sensor


class Sentinel2TifAdapter:
    family = "sentinel2"
    aliases = ("sentinel2", "sentinel_2", "sentinel_optical", "s2")
    sensor = "sentinel2_msi"
    _NAME_RE = re.compile(r"^(T\d{2}[A-Z]{3})_(\d{8}T\d{6})_(B\d{2,3}A?)_(\d+m)$", re.IGNORECASE)

    def match(self, path: Path) -> bool:
        return bool(self._NAME_RE.match(path.stem))

    def parse(self, path: Path) -> OpticalAssetMetadata:
        match = self._NAME_RE.match(path.stem)
        if not match:
            raise ValueError(f"Invalid Sentinel-2 optical TIF filename: {path.name}")
        tile, acq_time_text, band, resolution = match.groups()
        acq_time = datetime.strptime(acq_time_text, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
        return OpticalAssetMetadata(
            scene_id=f"S2_{tile.upper()}_{acq_time_text.upper()}",
            band=f"{band}_{resolution}".lower(),
            acq_time=acq_time,
            product_family=self.family,
            sensor=self.sensor,
        )


class GenericTifAdapter:
    family = "generic_tif"
    aliases: tuple[str, ...] = ()
    sensor = "unknown"

    def match(self, path: Path) -> bool:
        return path.suffix.lower() in {".tif", ".tiff"}

    def parse(self, path: Path) -> OpticalAssetMetadata:
        acq_time = datetime(1970, 1, 1, tzinfo=timezone.utc)
        return OpticalAssetMetadata(
            scene_id=path.stem,
            band=path.stem.lower(),
            acq_time=acq_time,
            product_family=self.family,
            sensor=self.sensor,
        )


_ADAPTERS: tuple[OpticalProductAdapter, ...] = (
    LandsatCollectionAdapter(),
    Sentinel2TifAdapter(),
)
_GENERIC_TIF_ADAPTER = GenericTifAdapter()


def supported_optical_product_families() -> tuple[str, ...]:
    return tuple(adapter.family for adapter in _ADAPTERS)


def get_optical_product_adapter(product_family: str) -> OpticalProductAdapter:
    normalized = product_family.strip().lower().replace("-", "_")
    for adapter in _ADAPTERS:
        if normalized == adapter.family or normalized in adapter.aliases:
            return adapter
    raise ValueError(f"Unsupported optical product_family: {product_family}")


def detect_optical_product_adapter(path: Path) -> OpticalProductAdapter | None:
    for adapter in _ADAPTERS:
        if adapter.match(path):
            return adapter
    return None


def parse_optical_asset(path: Path, product_family: str = "auto") -> OpticalAssetMetadata:
    if product_family == "auto":
        adapter = detect_optical_product_adapter(path)
        if adapter is None:
            adapter = _GENERIC_TIF_ADAPTER
    else:
        adapter = get_optical_product_adapter(product_family)
    return adapter.parse(path)
