from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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


class OtherOpticalMosaicAdapter:
    family = "other"
    aliases = ("generic_optical", "shandong", "shandong_optical", "optical_mosaic")
    sensor = "optical_mosaic"
    _NAME_RE = re.compile(
        r"^(Shandong_mosaic)_(\d{4}(?:Q[1-4]|\d{2}))_(sr_band\d+)(?:_cut)?$",
        re.IGNORECASE,
    )

    def match(self, path: Path) -> bool:
        return bool(self._NAME_RE.match(path.stem))

    def parse(self, path: Path) -> OpticalAssetMetadata:
        match = self._NAME_RE.match(path.stem)
        if not match:
            raise ValueError(f"Invalid Shandong mosaic filename: {path.name}")
        prefix, period, band = match.groups()
        acq_time = self._parse_period_start(period)
        return OpticalAssetMetadata(
            scene_id=f"{prefix}_{period}",
            band=band.lower(),
            acq_time=acq_time,
            product_family=self.family,
            sensor=self.sensor,
        )

    def _parse_period_start(self, period: str) -> datetime:
        if "Q" in period.upper():
            year_text, quarter_text = period.upper().split("Q", maxsplit=1)
            month = (int(quarter_text) - 1) * 3 + 1
            return datetime(int(year_text), month, 1, tzinfo=timezone.utc)
        return datetime.strptime(period, "%Y%m").replace(day=1, tzinfo=timezone.utc)


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


class LandsatL1TPAdapter:
    """Adapter for Landsat L1TP / WRS-2 scene IDs (old-style naming).

    Old-style format: L[OCTE][5789][path 3d][row 3d][YYYYDDD][station]_[band]
    Example: LO81200292021293BJC00_B1.TIF
    """

    family = "landsat"
    aliases = ("landsat_l1tp", "landsat_wrs2", "l1tp")
    sensor = "landsat_oli_tirs"
    _SCENE_RE = re.compile(
        r"^L[OCTE][5789]\d{3}\d{3}\d{7}[A-Z0-9]+_[A-Za-z0-9]+$", re.IGNORECASE
    )

    def match(self, path: Path) -> bool:
        return bool(self._SCENE_RE.match(path.stem))

    def parse(self, path: Path) -> OpticalAssetMetadata:
        stem = path.stem
        parts = stem.split("_")
        if len(parts) != 2:
            raise ValueError(f"Invalid Landsat L1TP filename: {path.name}")
        scene_id, band_raw = parts
        band = band_raw.lower()
        year_day = scene_id[9:16]  # YYYYDDD after L+sensor(3)+path(3)+row(3)
        acq_time = (
            datetime(int(year_day[:4]), 1, 1, tzinfo=timezone.utc)
            + timedelta(days=int(year_day[4:7]) - 1)
        )
        return OpticalAssetMetadata(
            scene_id=scene_id,
            band=band,
            acq_time=acq_time,
            product_family=self.family,
            sensor=self._sensor_from_prefix(scene_id[:3]),
        )

    def _sensor_from_prefix(self, prefix: str) -> str:
        p = prefix.upper()
        if p in ("LO8", "LC8"):
            return "landsat8_oli_tirs"
        if p in ("LO9", "LC9"):
            return "landsat9_oli_tirs"
        if p == "LE7":
            return "landsat7_etm"
        if p == "LT5":
            return "landsat5_tm"
        return self.sensor


_ADAPTERS: tuple[OpticalProductAdapter, ...] = (
    LandsatL1TPAdapter(),
    LandsatCollectionAdapter(),
    Sentinel2TifAdapter(),
    OtherOpticalMosaicAdapter(),
)
_GENERIC_TIF_ADAPTER = GenericTifAdapter()


def supported_optical_product_families() -> tuple[str, ...]:
    seen: set[str] = set()
    families: list[str] = []
    for adapter in _ADAPTERS:
        if adapter.family not in seen:
            seen.add(adapter.family)
            families.append(adapter.family)
    return tuple(families)


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
