from __future__ import annotations

import csv
import json
import math
import re
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from grid_core.sdk import CubeEncoderSDK

from cube_split.partition.base import PartitionResult


@dataclass(frozen=True)
class CarbonPartitionConfig:
    grid_type: str = "geohash"
    grid_level: int = 7
    time_granularity: str = "day"
    product_type: str = "xco2"
    max_observations: int | None = None


@dataclass(frozen=True)
class CarbonSatelliteObservation:
    satellite: str
    observation_id: str
    acq_time: str
    lon: float
    lat: float
    xco2: float
    quality_flag: str | None = None
    footprint: list[list[float]] | None = None
    source_uri: str = ""
    source_index: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _time_bucket(value: str, granularity: str) -> str:
    dt = _parse_time(value)
    formats = {
        "year": "%Y",
        "month": "%Y%m",
        "day": "%Y%m%d",
        "hour": "%Y%m%d%H",
        "minute": "%Y%m%d%H%M",
    }
    if granularity not in formats:
        raise ValueError(f"Unsupported time_granularity: {granularity}")
    return dt.strftime(formats[granularity])


def _footprint_geojson(observation: CarbonSatelliteObservation) -> dict[str, Any]:
    if observation.footprint:
        coords = observation.footprint
        if coords[0] != coords[-1]:
            coords = [*coords, coords[0]]
        return {"type": "Polygon", "coordinates": [coords]}
    return {"type": "Point", "coordinates": [observation.lon, observation.lat]}


def partition_observation(
    observation: CarbonSatelliteObservation,
    config: CarbonPartitionConfig,
    sdk: CubeEncoderSDK | None = None,
) -> dict[str, Any]:
    encoder = sdk or CubeEncoderSDK()
    cell = encoder.locate(
        grid_type=config.grid_type,
        level=config.grid_level,
        point=[observation.lon, observation.lat],
    )
    st_code = encoder.generate_st_code(
        grid_type=config.grid_type,
        level=int(cell.level),
        space_code=cell.space_code,
        timestamp=_parse_time(observation.acq_time),
        time_granularity=config.time_granularity,
        version="v1",
    ).st_code
    return {
        "data_type": "carbon_satellite",
        "satellite": observation.satellite,
        "product_type": config.product_type,
        "observation_id": observation.observation_id,
        "acq_time": observation.acq_time,
        "time_bucket": _time_bucket(observation.acq_time, config.time_granularity),
        "grid_type": config.grid_type,
        "grid_level": int(cell.level),
        "space_code": cell.space_code,
        "st_code": st_code,
        "xco2": float(observation.xco2),
        "quality_flag": observation.quality_flag,
        "center_lon": float(observation.lon),
        "center_lat": float(observation.lat),
        "footprint_geojson": _footprint_geojson(observation),
        "source_uri": observation.source_uri,
        "source_index": observation.source_index,
        "metadata_json": json.dumps(observation.metadata, ensure_ascii=False),
    }


def _metadata_from_raw(value: Any) -> dict[str, Any]:
    if value is None or value == "":
        return {}
    if isinstance(value, str):
        parsed = json.loads(value)
        if not isinstance(parsed, dict):
            raise ValueError("carbon observation metadata must decode to an object")
        return parsed
    if isinstance(value, dict):
        return value
    raise ValueError("carbon observation metadata must be an object or JSON object string")


def _observation_from_mapping(raw: dict[str, Any], source_uri: str, fallback_index: int) -> CarbonSatelliteObservation:
    footprint = raw.get("footprint")
    if isinstance(footprint, str) and footprint:
        footprint = json.loads(footprint)
    return CarbonSatelliteObservation(
        satellite=str(raw["satellite"]),
        observation_id=str(raw.get("observation_id") or raw.get("sounding_id") or fallback_index),
        acq_time=str(raw["acq_time"]),
        lon=float(raw["lon"]),
        lat=float(raw["lat"]),
        xco2=float(raw["xco2"]),
        quality_flag=(None if raw.get("quality_flag") is None else str(raw.get("quality_flag"))),
        footprint=footprint,
        source_uri=str(raw.get("source_uri") or source_uri),
        source_index=(int(raw["source_index"]) if raw.get("source_index") not in {None, ""} else fallback_index),
        metadata=_metadata_from_raw(raw.get("metadata")),
    )


def load_observations(path: Path) -> list[CarbonSatelliteObservation]:
    return load_observations_from_file(path)


def load_observations_from_file(
    path: Path,
    max_observations: int | None = None,
) -> list[CarbonSatelliteObservation]:
    observations: list[CarbonSatelliteObservation] = []
    if path.suffix.lower() == ".jsonl":
        with path.open("r", encoding="utf-8") as fh:
            for idx, line in enumerate(fh):
                if max_observations is not None and len(observations) >= max_observations:
                    break
                text = line.strip()
                if not text:
                    continue
                observations.append(_observation_from_mapping(json.loads(text), str(path), idx))
        return observations
    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8", newline="") as fh:
            for idx, row in enumerate(csv.DictReader(fh)):
                if max_observations is not None and len(observations) >= max_observations:
                    break
                observations.append(_observation_from_mapping(dict(row), str(path), idx))
        return observations
    if path.suffix.lower() in {".nc", ".nc4", ".h5", ".hdf"}:
        return load_oco2_lite_observations(path, max_observations=max_observations)
    raise ValueError(f"Unsupported carbon observation input file: {path}")


def _is_valid_measurement(*values: float) -> bool:
    return all(math.isfinite(value) and value > -999000 for value in values)


def _format_oco_sounding_time(sounding_id: str) -> str:
    text = sounding_id.strip()
    if len(text) < 14:
        raise ValueError(f"Invalid OCO sounding_id: {sounding_id}")
    dt = datetime.strptime(text[:14], "%Y%m%d%H%M%S").replace(tzinfo=UTC)
    if len(text) >= 15 and text[14].isdigit():
        dt = dt.replace(microsecond=int(text[14]) * 100000)
    return dt.isoformat().replace("+00:00", "Z")


def _load_oco2_lite_with_netcdf4(
    path: Path,
    max_observations: int | None,
) -> list[CarbonSatelliteObservation] | None:
    try:
        from netCDF4 import Dataset  # type: ignore
    except ModuleNotFoundError:
        return None

    observations: list[CarbonSatelliteObservation] = []
    with Dataset(path, "r") as ds:
        sounding_ids = ds.variables["sounding_id"]
        latitudes = ds.variables["latitude"]
        longitudes = ds.variables["longitude"]
        times = ds.variables["time"]
        xco2_values = ds.variables["xco2"]
        quality_flags = ds.variables["xco2_quality_flag"]
        vertex_latitudes = ds.variables["vertex_latitude"]
        vertex_longitudes = ds.variables["vertex_longitude"]
        count = len(sounding_ids) if max_observations is None else min(max_observations, len(sounding_ids))
        for idx in range(count):
            lat = float(latitudes[idx])
            lon = float(longitudes[idx])
            xco2 = float(xco2_values[idx])
            epoch_seconds = float(times[idx])
            if not _is_valid_measurement(lat, lon, xco2, epoch_seconds):
                continue
            footprint = [
                [float(vertex_longitudes[idx][vertex_idx]), float(vertex_latitudes[idx][vertex_idx])]
                for vertex_idx in range(4)
            ]
            observations.append(
                CarbonSatelliteObservation(
                    satellite="OCO2",
                    observation_id=str(int(sounding_ids[idx])),
                    acq_time=_format_oco_sounding_time(str(int(sounding_ids[idx]))),
                    lon=lon,
                    lat=lat,
                    xco2=xco2,
                    quality_flag=str(int(quality_flags[idx])),
                    footprint=footprint,
                    source_uri=str(path),
                    source_index=idx,
                    metadata={"source_format": "oco2_lite_nc4"},
                )
            )
    return observations


_NUMBER_RE = re.compile(r"[-+]?(?:\d+\.\d*|\.\d+|\d+)(?:[eE][-+]?\d+)?")


def _h5dump_dataset_numbers(
    path: Path,
    dataset: str,
    max_rows: int | None,
    width: int = 1,
) -> list[float]:
    h5dump = shutil.which("h5dump")
    if not h5dump:
        raise RuntimeError("Reading OCO-2 .nc4 requires either Python netCDF4 or the h5dump CLI")

    command = [h5dump, "-d", f"/{dataset}"]
    if max_rows is not None:
        command.extend(["-s", "0" if width == 1 else "0,0", "-c", str(max_rows) if width == 1 else f"{max_rows},{width}"])
    command.append(str(path))

    completed = subprocess.run(command, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    numbers: list[float] = []
    in_data = False
    for line in completed.stdout.splitlines():
        stripped = line.strip()
        if stripped == "DATA {":
            in_data = True
            continue
        if in_data and stripped == "}":
            break
        if not in_data:
            continue
        payload = line.split("):", 1)[1] if "):" in line else line
        numbers.extend(float(match.group(0)) for match in _NUMBER_RE.finditer(payload))
    return numbers


def _load_oco2_lite_with_h5dump(
    path: Path,
    max_observations: int | None,
) -> list[CarbonSatelliteObservation]:
    sounding_ids = _h5dump_dataset_numbers(path, "sounding_id", max_observations)
    latitudes = _h5dump_dataset_numbers(path, "latitude", max_observations)
    longitudes = _h5dump_dataset_numbers(path, "longitude", max_observations)
    times = _h5dump_dataset_numbers(path, "time", max_observations)
    xco2_values = _h5dump_dataset_numbers(path, "xco2", max_observations)
    quality_flags = _h5dump_dataset_numbers(path, "xco2_quality_flag", max_observations)
    vertex_latitudes = _h5dump_dataset_numbers(path, "vertex_latitude", max_observations, width=4)
    vertex_longitudes = _h5dump_dataset_numbers(path, "vertex_longitude", max_observations, width=4)

    count = min(
        len(sounding_ids),
        len(latitudes),
        len(longitudes),
        len(times),
        len(xco2_values),
        len(quality_flags),
        len(vertex_latitudes) // 4,
        len(vertex_longitudes) // 4,
    )
    observations: list[CarbonSatelliteObservation] = []
    for idx in range(count):
        lat = float(latitudes[idx])
        lon = float(longitudes[idx])
        xco2 = float(xco2_values[idx])
        epoch_seconds = float(times[idx])
        if not _is_valid_measurement(lat, lon, xco2, epoch_seconds):
            continue
        vertex_start = idx * 4
        footprint = [
            [float(vertex_longitudes[vertex_start + vertex_idx]), float(vertex_latitudes[vertex_start + vertex_idx])]
            for vertex_idx in range(4)
        ]
        observations.append(
            CarbonSatelliteObservation(
                satellite="OCO2",
                observation_id=str(int(sounding_ids[idx])),
                acq_time=_format_oco_sounding_time(str(int(sounding_ids[idx]))),
                lon=lon,
                lat=lat,
                xco2=xco2,
                quality_flag=str(int(quality_flags[idx])),
                footprint=footprint,
                source_uri=str(path),
                source_index=idx,
                metadata={"source_format": "oco2_lite_nc4"},
            )
        )
    return observations


def load_oco2_lite_observations(
    path: Path,
    max_observations: int | None = None,
) -> list[CarbonSatelliteObservation]:
    observations = _load_oco2_lite_with_netcdf4(path, max_observations)
    if observations is not None:
        return observations
    return _load_oco2_lite_with_h5dump(path, max_observations)


def _iter_input_files(input_dir: Path) -> list[Path]:
    return sorted(
        path
        for path in input_dir.rglob("*")
        if path.is_file() and path.suffix.lower() in {".jsonl", ".csv", ".nc", ".nc4", ".h5", ".hdf"}
    )


class CarbonSatellitePartitionService:
    data_type = "carbon_satellite"

    def run(
        self,
        input_dir: Path,
        output_dir: Path,
        config: CarbonPartitionConfig | None = None,
        workers: int = 1,
    ) -> PartitionResult:
        cfg = config or CarbonPartitionConfig()
        files = _iter_input_files(input_dir)
        if not files:
            raise RuntimeError(f"No carbon observation .jsonl/.csv files found under: {input_dir}")

        worker_count = max(1, workers)
        sdk = CubeEncoderSDK()

        def process_file(path: Path) -> list[dict[str, Any]]:
            return [
                partition_observation(obs, cfg, sdk=sdk)
                for obs in load_observations_from_file(path, max_observations=cfg.max_observations)
            ]

        rows: list[dict[str, Any]] = []
        if worker_count == 1:
            for path in files:
                rows.extend(process_file(path))
        else:
            with ThreadPoolExecutor(max_workers=worker_count) as pool:
                for part in pool.map(process_file, files):
                    rows.extend(part)

        output_dir.mkdir(parents=True, exist_ok=True)
        rows_path = output_dir / "carbon_observation_rows.jsonl"
        with rows_path.open("w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")

        return PartitionResult(data_type=self.data_type, rows_path=rows_path, total_rows=len(rows))
