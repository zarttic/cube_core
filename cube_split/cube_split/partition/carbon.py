from __future__ import annotations

import csv
import json
import math
import os
import re
import shutil
import subprocess
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from itertools import repeat
from pathlib import Path
from typing import Any

from grid_core.sdk import CubeEncoderSDK

from cube_split import runtime_config
from cube_split.jobs.cancellation import PartitionCancelledError, cancel_ray_refs
from cube_split.partition.base import PartitionResult
from cube_split.partition.carbon_products import (
    get_carbon_product_adapter,
    normalize_carbon_product_type,
    supported_carbon_product_types,
)

UTC = timezone.utc


@dataclass(frozen=True)
class CarbonPartitionConfig:
    grid_type: str = "isea4h"
    grid_level: int = 5
    time_granularity: str = "day"
    product_type: str = "xco2"
    max_observations: int | None = None
    selected_source_indexes: tuple[int, ...] | None = None
    partition_chunk_size: int = 1000
    partition_backend: str = "process"
    ray_address: str = ""
    source_uris: tuple[str, ...] | None = None
    cancellation_check: Any | None = None


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


@dataclass(frozen=True)
class CarbonObservationSourceSlice:
    source_uri: str
    start_index: int
    stop_index: int


CarbonPartitionChunk = list[CarbonSatelliteObservation] | CarbonObservationSourceSlice


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
    ).st_code
    return {
        "data_type": "carbon",
        "satellite": observation.satellite,
        "product_type": normalize_carbon_product_type(config.product_type),
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
    product_type: str = "xco2",
) -> list[CarbonSatelliteObservation]:
    adapter = get_carbon_product_adapter(product_type)
    if not adapter.supports_file(path):
        raise ValueError(f"Unsupported carbon {adapter.product_type} input file: {path}")
    return adapter.load_observations(path, max_observations=max_observations)


def _load_xco2_observations_from_file(
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


def _netcdf4_dataset_class():
    try:
        from netCDF4 import Dataset  # type: ignore
    except ModuleNotFoundError:
        return None
    return Dataset


def _dataset_variable(ds: Any, *candidates: str) -> Any | None:
    for name in candidates:
        if name in ds.variables:
            return ds.variables[name]
    return None


def _dataset_has_oco2_lite_schema(ds: Any) -> bool:
    return all(
        name in ds.variables
        for name in (
            "sounding_id",
            "latitude",
            "longitude",
            "time",
            "xco2",
            "xco2_quality_flag",
            "vertex_latitude",
            "vertex_longitude",
        )
    )


def _scalar_text(value: Any) -> str:
    raw = value
    if hasattr(raw, "tolist"):
        raw = raw.tolist()
    if isinstance(raw, (list, tuple)):
        return "".join(_scalar_text(item) for item in raw).strip()
    if hasattr(raw, "item"):
        try:
            raw = raw.item()
        except Exception:
            pass
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="ignore").strip()
    return str(raw).strip()


def _scalar_number(value: Any) -> float:
    raw = value
    if hasattr(raw, "item"):
        try:
            raw = raw.item()
        except Exception:
            pass
    return float(raw)


def _dataset_satellite_name(ds: Any, path: Path) -> str:
    for attr_name in ("satellite", "platform", "platform_name", "mission_name", "sensor"):
        text = _scalar_text(getattr(ds, attr_name, ""))
        if not text:
            continue
        lowered = text.lower()
        if "tansat" in lowered:
            return "TanSat"
        if "oco" in lowered:
            return "OCO2"
        return text
    lowered_name = path.name.lower()
    if "tansat" in lowered_name:
        return "TanSat"
    if "oco" in lowered_name:
        return "OCO2"
    if _dataset_variable(ds, "exposure_id", "exposureID") is not None:
        return "TanSat"
    return "OCO2"


def _dataset_time_iso(value: Any, time_var: Any) -> str:
    raw = value
    if hasattr(raw, "item"):
        try:
            raw = raw.item()
        except Exception:
            pass

    units = str(getattr(time_var, "units", "") or "").strip()
    if units and isinstance(raw, (int, float)):
        from netCDF4 import num2date  # type: ignore

        dt = num2date(
            float(raw),
            units=units,
            calendar=str(getattr(time_var, "calendar", "standard") or "standard"),
            only_use_cftime_datetimes=False,
        )
        if hasattr(dt, "to_pydatetime"):
            dt = dt.to_pydatetime()
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")

    text = _scalar_text(raw)
    if text:
        normalized = text.replace(" ", "T")
        if normalized.endswith("Z"):
            return normalized
        try:
            dt = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
        except ValueError:
            if normalized.isdigit() and len(normalized) == 14:
                dt = datetime.strptime(normalized, "%Y%m%d%H%M%S").replace(tzinfo=UTC)
            else:
                raise ValueError(f"Unsupported time value: {text}")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")

    dt = datetime.fromtimestamp(_scalar_number(raw), tz=UTC)
    return dt.isoformat().replace("+00:00", "Z")


def _dataset_observation_id(id_var: Any | None, offset: int, start_index: int) -> str:
    if id_var is None:
        return str(start_index + offset)
    text = _scalar_text(id_var[offset])
    if text:
        return text
    return str(start_index + offset)


def _dataset_footprint(
    footprint_lon_var: Any | None,
    footprint_lat_var: Any | None,
    offset: int,
) -> list[list[float]] | None:
    if footprint_lon_var is None or footprint_lat_var is None:
        return None
    longitudes = footprint_lon_var[offset]
    latitudes = footprint_lat_var[offset]
    if len(longitudes) < 4 or len(latitudes) < 4:
        return None
    return [
        [float(longitudes[vertex_idx]), float(latitudes[vertex_idx])]
        for vertex_idx in range(4)
    ]


def _dataset_observation_count(ds: Any) -> int:
    for variable in (
        _dataset_variable(ds, "sounding_id", "exposure_id", "exposureID", "observation_id"),
        _dataset_variable(ds, "latitude", "lat"),
        _dataset_variable(ds, "longitude", "lon"),
        _dataset_variable(ds, "xco2", "xco2_no_bias_correction"),
        _dataset_variable(ds, "time"),
    ):
        if variable is not None:
            return len(variable)
    raise ValueError("Unsupported carbon netCDF/HDF dataset: missing observation dimension")


def _build_oco2_lite_observations(
    *,
    source_uri: str,
    start_index: int,
    sounding_ids: Any,
    latitudes: Any,
    longitudes: Any,
    times: Any,
    xco2_values: Any,
    quality_flags: Any,
    vertex_latitudes: Any,
    vertex_longitudes: Any,
) -> list[CarbonSatelliteObservation]:
    observations: list[CarbonSatelliteObservation] = []
    count = len(sounding_ids)
    for offset in range(count):
        lat = float(latitudes[offset])
        lon = float(longitudes[offset])
        xco2 = float(xco2_values[offset])
        epoch_seconds = float(times[offset])
        if not _is_valid_measurement(lat, lon, xco2, epoch_seconds):
            continue
        footprint = [
            [float(vertex_longitudes[offset][vertex_idx]), float(vertex_latitudes[offset][vertex_idx])]
            for vertex_idx in range(4)
        ]
        source_index = start_index + offset
        observations.append(
            CarbonSatelliteObservation(
                satellite="OCO2",
                observation_id=str(int(sounding_ids[offset])),
                acq_time=_format_oco_sounding_time(str(int(sounding_ids[offset]))),
                lon=lon,
                lat=lat,
                xco2=xco2,
                quality_flag=str(int(quality_flags[offset])),
                footprint=footprint,
                source_uri=source_uri,
                source_index=source_index,
                metadata={"source_format": "oco2_lite_nc4"},
            )
        )
    return observations


def _build_generic_xco2_observations(
    *,
    ds: Any,
    path: Path,
    source_uri: str,
    start_index: int,
    stop_index: int,
) -> list[CarbonSatelliteObservation]:
    satellite = _dataset_satellite_name(ds, path)
    latitude_var = _dataset_variable(ds, "latitude", "lat")
    longitude_var = _dataset_variable(ds, "longitude", "lon")
    time_var = _dataset_variable(ds, "time")
    xco2_var = _dataset_variable(ds, "xco2", "xco2_no_bias_correction")
    quality_var = _dataset_variable(ds, "xco2_quality_flag", "quality_flag", "retr_flag", "qa_value")
    observation_id_var = _dataset_variable(ds, "sounding_id", "exposure_id", "exposureID", "observation_id")
    footprint_lat_var = _dataset_variable(ds, "vertex_latitude", "footprint_latitude", "latitude_corners")
    footprint_lon_var = _dataset_variable(ds, "vertex_longitude", "footprint_longitude", "longitude_corners")

    if latitude_var is None or longitude_var is None or time_var is None or xco2_var is None:
        raise ValueError("Unsupported carbon netCDF/HDF dataset: missing latitude/longitude/time/xco2")

    selection = slice(start_index, stop_index)
    latitudes = latitude_var[selection]
    longitudes = longitude_var[selection]
    times = time_var[selection]
    xco2_values = xco2_var[selection]
    quality_flags = quality_var[selection] if quality_var is not None else None

    observations: list[CarbonSatelliteObservation] = []
    count = len(latitudes)
    for offset in range(count):
        lat = float(latitudes[offset])
        lon = float(longitudes[offset])
        xco2 = float(xco2_values[offset])
        if not _is_valid_measurement(lat, lon, xco2):
            continue
        source_index = start_index + offset
        quality_flag = None
        if quality_flags is not None:
            quality_flag = _scalar_text(quality_flags[offset]) or None
        observations.append(
            CarbonSatelliteObservation(
                satellite=satellite,
                observation_id=_dataset_observation_id(observation_id_var, source_index, 0),
                acq_time=_dataset_time_iso(times[offset], time_var),
                lon=lon,
                lat=lat,
                xco2=xco2,
                quality_flag=quality_flag,
                footprint=_dataset_footprint(footprint_lon_var, footprint_lat_var, source_index),
                source_uri=source_uri,
                source_index=source_index,
                metadata={"source_format": path.suffix.lower().lstrip("."), "schema_kind": "generic_xco2_netcdf"},
            )
        )
    return observations


def _load_oco2_lite_with_netcdf4(
    path: Path,
    max_observations: int | None,
) -> list[CarbonSatelliteObservation] | None:
    Dataset = _netcdf4_dataset_class()
    if Dataset is None:
        return None

    with Dataset(path, "r") as ds:
        count = _dataset_observation_count(ds)
        limit = count if max_observations is None else min(max_observations, count)
        if _dataset_has_oco2_lite_schema(ds):
            return _build_oco2_lite_observations(
                source_uri=str(path),
                start_index=0,
                sounding_ids=ds.variables["sounding_id"][:limit],
                latitudes=ds.variables["latitude"][:limit],
                longitudes=ds.variables["longitude"][:limit],
                times=ds.variables["time"][:limit],
                xco2_values=ds.variables["xco2"][:limit],
                quality_flags=ds.variables["xco2_quality_flag"][:limit],
                vertex_latitudes=ds.variables["vertex_latitude"][:limit, :],
                vertex_longitudes=ds.variables["vertex_longitude"][:limit, :],
            )
        return _build_generic_xco2_observations(
            ds=ds,
            path=path,
            source_uri=str(path),
            start_index=0,
            stop_index=limit,
        )


def _oco2_lite_observation_count(path: Path) -> int | None:
    Dataset = _netcdf4_dataset_class()
    if Dataset is None:
        return None
    with Dataset(path, "r") as ds:
        return _dataset_observation_count(ds)


def _load_oco2_lite_observation_slice(
    path: Path,
    start_index: int,
    stop_index: int,
    *,
    source_uri: str | None = None,
) -> list[CarbonSatelliteObservation]:
    Dataset = _netcdf4_dataset_class()
    if Dataset is None:
        raise RuntimeError("Reading carbon netCDF/HDF slices on Ray workers requires Python netCDF4")
    if start_index < 0 or stop_index < start_index:
        raise ValueError("invalid OCO-2 observation slice range")
    with Dataset(path, "r") as ds:
        end_index = min(stop_index, _dataset_observation_count(ds))
        if start_index >= end_index:
            return []
        if _dataset_has_oco2_lite_schema(ds):
            selection = slice(start_index, end_index)
            return _build_oco2_lite_observations(
                source_uri=source_uri or str(path),
                start_index=start_index,
                sounding_ids=ds.variables["sounding_id"][selection],
                latitudes=ds.variables["latitude"][selection],
                longitudes=ds.variables["longitude"][selection],
                times=ds.variables["time"][selection],
                xco2_values=ds.variables["xco2"][selection],
                quality_flags=ds.variables["xco2_quality_flag"][selection],
                vertex_latitudes=ds.variables["vertex_latitude"][selection, :],
                vertex_longitudes=ds.variables["vertex_longitude"][selection, :],
            )
        return _build_generic_xco2_observations(
            ds=ds,
            path=path,
            source_uri=source_uri or str(path),
            start_index=start_index,
            stop_index=end_index,
        )


_NUMBER_RE = re.compile(r"[-+]?(?:\d+\.\d*|\.\d+|\d+)(?:[eE][-+]?\d+)?")


def _h5dump_dataset_numbers(
    path: Path,
    dataset: str,
    max_rows: int | None,
    width: int = 1,
) -> list[float]:
    h5dump = shutil.which("h5dump")
    if not h5dump:
        raise RuntimeError("Reading carbon netCDF/HDF requires either Python netCDF4 or the h5dump CLI")

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
        if path.is_file()
        and path.suffix.lower() in {".jsonl", ".csv", ".nc", ".nc4", ".h5", ".hdf"}
        and path.name not in {"carbon_observation_rows.jsonl", "index_rows.jsonl"}
    )


def _is_oco2_lite_netcdf_source(source_uri: str) -> bool:
    return Path(str(source_uri).strip()).suffix.lower() in {".nc", ".nc4", ".h5", ".hdf"}


def _resolve_oco2_lite_source_path(source_uri: str) -> Path:
    from cube_split.jobs.ray_partition_core import resolve_asset_source_path

    return Path(resolve_asset_source_path(source_uri))


def _oco2_lite_observation_count_for_source(source_uri: str) -> int | None:
    return _oco2_lite_observation_count(_resolve_oco2_lite_source_path(source_uri))


def _plan_oco2_lite_source_slices(
    source_uris: list[str],
    config: CarbonPartitionConfig,
) -> list[CarbonObservationSourceSlice] | None:
    if config.partition_backend != "ray":
        return None
    if normalize_carbon_product_type(config.product_type) != "xco2":
        return None
    if config.selected_source_indexes:
        return None
    if not source_uris or any(not _is_oco2_lite_netcdf_source(source_uri) for source_uri in source_uris):
        return None
    if _netcdf4_dataset_class() is None:
        return None

    slices: list[CarbonObservationSourceSlice] = []
    remaining = config.max_observations
    for source_uri in source_uris:
        if remaining is not None and remaining <= 0:
            break
        count = _oco2_lite_observation_count_for_source(source_uri)
        if count is None:
            return None
        limit = count if remaining is None else min(count, remaining)
        for start_index in range(0, limit, config.partition_chunk_size):
            stop_index = min(start_index + config.partition_chunk_size, limit)
            slices.append(
                CarbonObservationSourceSlice(
                    source_uri=source_uri,
                    start_index=start_index,
                    stop_index=stop_index,
                )
            )
        if remaining is not None:
            remaining -= limit
    return slices


def _chunk_observations(
    observations: list[CarbonSatelliteObservation],
    chunk_size: int,
) -> list[list[CarbonSatelliteObservation]]:
    if chunk_size <= 0:
        raise ValueError("partition_chunk_size must be > 0")
    return [observations[idx : idx + chunk_size] for idx in range(0, len(observations), chunk_size)]


def _partition_observation_chunk(
    observations: list[CarbonSatelliteObservation],
    config: CarbonPartitionConfig,
) -> list[dict[str, Any]]:
    sdk = CubeEncoderSDK()
    located = sdk.batch_locate_st_codes(
        grid_type=config.grid_type,
        level=config.grid_level,
        items=[
            {
                "point": [obs.lon, obs.lat],
                "timestamp": _parse_time(obs.acq_time),
            }
            for obs in observations
        ],
        time_granularity=config.time_granularity,
    )
    if len(located) != len(observations):
        raise RuntimeError(
            "batch locate returned "
            f"{len(located)} cells for {len(observations)} carbon observations"
        )
    rows: list[dict[str, Any]] = []
    for observation, cell in zip(observations, located):
        rows.append(
            {
                "data_type": "carbon",
                "satellite": observation.satellite,
                "product_type": normalize_carbon_product_type(config.product_type),
                "observation_id": observation.observation_id,
                "acq_time": observation.acq_time,
                "time_bucket": cell["time_code"],
                "grid_type": config.grid_type,
                "grid_level": int(cell["grid_level"]),
                "space_code": cell["space_code"],
                "st_code": cell["st_code"],
                "xco2": float(observation.xco2),
                "quality_flag": observation.quality_flag,
                "center_lon": float(observation.lon),
                "center_lat": float(observation.lat),
                "footprint_geojson": _footprint_geojson(observation),
                "source_uri": observation.source_uri,
                "source_index": observation.source_index,
                "metadata_json": json.dumps(observation.metadata, ensure_ascii=False),
            }
        )
    return rows


def _partition_source_slice_chunk(
    chunk: CarbonObservationSourceSlice,
    config: CarbonPartitionConfig,
    *,
    resolved_source_path: str | None = None,
) -> list[dict[str, Any]]:
    observations = _load_oco2_lite_observation_slice(
        Path(resolved_source_path or _resolve_oco2_lite_source_path(chunk.source_uri)),
        chunk.start_index,
        chunk.stop_index,
        source_uri=chunk.source_uri,
    )
    return _partition_observation_chunk(observations, config)


def _partition_chunk(
    chunk: CarbonPartitionChunk,
    config: CarbonPartitionConfig,
) -> list[dict[str, Any]]:
    if isinstance(chunk, CarbonObservationSourceSlice):
        return _partition_source_slice_chunk(chunk, config)
    return _partition_observation_chunk(chunk, config)


def _load_observation_chunks(
    files: list[Path],
    config: CarbonPartitionConfig,
    worker_count: int,
) -> list[list[CarbonSatelliteObservation]]:
    chunks: list[list[CarbonSatelliteObservation]] = []
    normalized_product_type = normalize_carbon_product_type(config.product_type)
    selected_source_indexes = set(config.selected_source_indexes or ())

    def load_one(path: Path, max_observations: int | None = None) -> list[CarbonSatelliteObservation]:
        if normalized_product_type == "xco2":
            return load_observations_from_file(path, max_observations=max_observations)
        return load_observations_from_file(path, max_observations=max_observations, product_type=config.product_type)

    def select_observations(observations: list[CarbonSatelliteObservation]) -> list[CarbonSatelliteObservation]:
        if not selected_source_indexes:
            return observations
        return [obs for obs in observations if obs.source_index in selected_source_indexes]

    if config.max_observations is None and worker_count > 1 and len(files) > 1:
        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            for observations in pool.map(load_one, files):
                observations = select_observations(observations)
                chunks.extend(_chunk_observations(observations, config.partition_chunk_size))
        return chunks

    remaining = config.max_observations
    for path in files:
        if remaining is not None and remaining <= 0:
            break
        observations = load_one(path, max_observations=remaining)
        observations = select_observations(observations)
        if remaining is not None:
            observations = observations[:remaining]
            remaining -= len(observations)
        chunks.extend(_chunk_observations(observations, config.partition_chunk_size))
    return chunks


def _partition_chunks(
    chunks: list[CarbonPartitionChunk],
    config: CarbonPartitionConfig,
    worker_count: int,
) -> list[dict[str, Any]]:
    if not chunks:
        return []
    worker_config = replace(config, cancellation_check=None)
    if config.partition_backend == "ray":
        return _partition_chunks_with_ray(chunks, config, worker_count)

    rows: list[dict[str, Any]] = []
    if worker_count == 1 or len(chunks) <= 1:
        for chunk in chunks:
            if config.cancellation_check is not None and config.cancellation_check():
                raise PartitionCancelledError("Partition task cancelled")
            rows.extend(_partition_chunk(chunk, worker_config))
        return rows

    if config.partition_backend == "process":
        executor_cls = ProcessPoolExecutor
    elif config.partition_backend == "thread":
        executor_cls = ThreadPoolExecutor
    else:
        raise ValueError("partition_backend must be 'process', 'thread', or 'ray'")

    with executor_cls(max_workers=worker_count) as pool:
        for part in pool.map(_partition_chunk, chunks, repeat(worker_config)):
            if config.cancellation_check is not None and config.cancellation_check():
                raise PartitionCancelledError("Partition task cancelled")
            rows.extend(part)
    return rows


def _load_ray():
    try:
        import ray  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError("Ray is not installed. Install `ray` before running carbon partition with Ray.") from exc
    return ray


def _ray_runtime_env_from_env() -> dict[str, Any] | None:
    raw = os.environ.get("RAY_RUNTIME_ENV_JSON", "").strip()
    if raw:
        loaded = json.loads(raw)
        if not isinstance(loaded, dict):
            raise ValueError("RAY_RUNTIME_ENV_JSON must decode to an object")
        return loaded

    project_root = Path(__file__).resolve().parents[3]
    minio = runtime_config.minio_settings()
    return {
        "working_dir": str(project_root),
        "excludes": [
            ".git/**",
            ".agents/**",
            ".cache/**",
            ".codegraph/**",
            ".codex/**",
            ".mypy_cache/**",
            ".ruff_cache/**",
            ".tmp/**",
            ".venv/**",
            "**/__pycache__/**",
            "**/.pytest_cache/**",
            "cube_split/*.gz",
            "cube_split/*.nc4",
            "cube_split/data/**",
            "cube_split/data_tmp/**",
            "cube_split/test_output/**",
            "cube_split/results/**",
            "cube_web/frontend/node_modules/**",
            "cube_web/frontend/dist/**",
        ],
        "env_vars": {
            "CUBE_CARBON_RUNTIME_ENV_REV": "20260620",
            "CUBE_PROJECT_ROOT": ".",
            "CUBE_WEB_MINIO_ENDPOINT": minio.endpoint,
            "CUBE_WEB_MINIO_ACCESS_KEY": minio.access_key,
            "CUBE_WEB_MINIO_SECRET_KEY": minio.secret_key,
            "CUBE_WEB_MINIO_BUCKET": minio.bucket,
            "PYTHONPATH": ".:./cube_encoder:./cube_split:./cube_web",
        },
    }


def _ray_actor_options_from_env() -> dict[str, Any]:
    node_resource = os.environ.get("RAY_ACTOR_NODE_RESOURCE", "").strip()
    if not node_resource:
        return {}
    return {"resources": {node_resource: 0.001}}


def _init_ray(ray: Any, ray_address: str) -> None:
    runtime_env = _ray_runtime_env_from_env()
    init_kwargs = {
        "ignore_reinit_error": True,
        "include_dashboard": False,
        "logging_level": "ERROR",
        "runtime_env": runtime_env,
    }
    if ray_address:
        try:
            ray.init(address=ray_address, **init_kwargs)
        except Exception:
            if ray_address != "auto":
                raise
            ray.init(**init_kwargs)
    else:
        ray.init(**init_kwargs)


def _ray_head_node_id(ray: Any) -> str | None:
    for node in ray.nodes():
        if not bool(node.get("Alive", node.get("alive", False))):
            continue
        resources = node.get("Resources", {})
        if "node:__internal_head__" in resources:
            node_id = str(node.get("NodeID") or "").strip()
            if node_id:
                return node_id
    return None


def _ray_actor_options(ray: Any, prefer_head: bool = False) -> dict[str, Any]:
    actor_options = _ray_actor_options_from_env()
    if actor_options or not prefer_head:
        return actor_options
    head_node_id = _ray_head_node_id(ray)
    if not head_node_id:
        return actor_options
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    return {
        "scheduling_strategy": NodeAffinitySchedulingStrategy(head_node_id, soft=False),
    }


def _should_retry_runtime_env_on_head(ray: Any, exc: Exception) -> bool:
    if _ray_actor_options_from_env():
        return False
    if exc.__class__.__name__ != "RuntimeEnvSetupError":
        return False
    if "No space left on device" not in str(exc):
        return False
    return _ray_head_node_id(ray) is not None


def _partition_chunks_with_ray_once(
    ray: Any,
    chunks: list[CarbonPartitionChunk],
    config: CarbonPartitionConfig,
    worker_count: int,
    *,
    prefer_head: bool = False,
) -> list[dict[str, Any]]:
    parallelism = max(1, min(worker_count, len(chunks)))
    worker_config = replace(config, cancellation_check=None)

    @ray.remote
    class CarbonChunkProcessor:
        def __init__(self):
            self._resolved_source_paths: dict[str, str] = {}

        def _resolved_source_path(self, source_uri: str) -> str:
            resolved = self._resolved_source_paths.get(source_uri)
            if resolved:
                return resolved
            resolved = str(_resolve_oco2_lite_source_path(source_uri))
            self._resolved_source_paths[source_uri] = resolved
            return resolved

        def process_chunk(
            self,
            chunk: CarbonPartitionChunk,
            cfg: CarbonPartitionConfig,
        ) -> list[dict[str, Any]]:
            import os
            import sys

            project_roots = [
                root
                for root in (os.environ.get("CUBE_PROJECT_ROOT", ""), os.getcwd(), "/tmp/cube_project_ray_code")
                if root
            ]
            package_paths = [
                os.path.abspath(os.path.join(project_root, rel_path))
                for project_root in project_roots
                for rel_path in ("", "cube_encoder", "cube_split", "cube_web")
            ]
            for package_path in reversed(package_paths):
                if os.path.isdir(package_path) and package_path not in sys.path:
                    sys.path.insert(0, package_path)

            from cube_split.partition.carbon import CarbonObservationSourceSlice, _partition_chunk, _partition_source_slice_chunk

            if isinstance(chunk, CarbonObservationSourceSlice):
                return _partition_source_slice_chunk(
                    chunk,
                    cfg,
                    resolved_source_path=self._resolved_source_path(chunk.source_uri),
                )
            return _partition_chunk(chunk, cfg)

    rows: list[dict[str, Any]] = []
    pending: list[Any] = []
    try:
        actor_cls = CarbonChunkProcessor.options(**_ray_actor_options(ray, prefer_head=prefer_head))
        actors = [actor_cls.remote() for _ in range(parallelism)]
        if not hasattr(ray, "wait"):
            futures = [
                actors[idx % parallelism].process_chunk.remote(chunk, worker_config)
                for idx, chunk in enumerate(chunks)
            ]
            for part in ray.get(futures):
                rows.extend(part)
            return rows
        next_idx = 0
        while next_idx < len(chunks) and len(pending) < parallelism:
            if config.cancellation_check is not None and config.cancellation_check():
                raise PartitionCancelledError("Partition task cancelled")
            pending.append(actors[next_idx % parallelism].process_chunk.remote(chunks[next_idx], worker_config))
            next_idx += 1
        while pending:
            if config.cancellation_check is not None and config.cancellation_check():
                raise PartitionCancelledError("Partition task cancelled")
            ready, pending = ray.wait(pending, num_returns=1, timeout=1.0)
            if not ready:
                continue
            rows.extend(ray.get(ready[0]))
            while next_idx < len(chunks) and len(pending) < parallelism:
                if config.cancellation_check is not None and config.cancellation_check():
                    raise PartitionCancelledError("Partition task cancelled")
                pending.append(actors[next_idx % parallelism].process_chunk.remote(chunks[next_idx], worker_config))
                next_idx += 1
    except PartitionCancelledError:
        cancel_ray_refs(ray, pending)
        raise
    except Exception:
        cancel_ray_refs(ray, pending)
        raise
    return rows


def _partition_chunks_with_ray(
    chunks: list[CarbonPartitionChunk],
    config: CarbonPartitionConfig,
    worker_count: int,
) -> list[dict[str, Any]]:
    ray = _load_ray()
    last_exc: Exception | None = None
    for prefer_head in (False, True):
        if prefer_head and last_exc is None:
            break
        _init_ray(ray, config.ray_address)
        try:
            return _partition_chunks_with_ray_once(
                ray,
                chunks,
                config,
                worker_count,
                prefer_head=prefer_head,
            )
        except PartitionCancelledError:
            raise
        except Exception as exc:
            last_exc = exc
            if not prefer_head and _should_retry_runtime_env_on_head(ray, exc):
                continue
            raise
        finally:
            ray.shutdown()
    assert last_exc is not None
    raise last_exc


class CarbonSatellitePartitionService:
    data_type = "carbon"
    supported_product_types = supported_carbon_product_types()

    def run(
        self,
        input_dir: Path,
        output_dir: Path,
        config: CarbonPartitionConfig | None = None,
        workers: int = 1,
    ) -> PartitionResult:
        cfg = config or CarbonPartitionConfig()
        normalize_carbon_product_type(cfg.product_type)
        files = _iter_input_files(input_dir)
        source_uris = [str(path) for path in files]
        if cfg.source_uris:
            source_uris.extend(str(source_uri) for source_uri in cfg.source_uris if str(source_uri).strip())
        if not files and not source_uris:
            raise RuntimeError(f"No carbon observation input or source_uri found under: {input_dir}")

        worker_count = max(1, workers)
        if cfg.cancellation_check is not None and cfg.cancellation_check():
            raise PartitionCancelledError("Partition task cancelled")
        ray_source_slices = _plan_oco2_lite_source_slices(source_uris, cfg)
        if ray_source_slices is not None:
            chunks: list[CarbonPartitionChunk] = ray_source_slices
        else:
            chunks = _load_observation_chunks(files, cfg, worker_count)
        if cfg.cancellation_check is not None and cfg.cancellation_check():
            raise PartitionCancelledError("Partition task cancelled")
        rows = _partition_chunks(chunks, cfg, worker_count)
        if cfg.cancellation_check is not None and cfg.cancellation_check():
            raise PartitionCancelledError("Partition task cancelled")

        output_dir.mkdir(parents=True, exist_ok=True)
        rows_path = output_dir / "carbon_observation_rows.jsonl"
        with rows_path.open("w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")

        return PartitionResult(data_type=self.data_type, rows_path=rows_path, total_rows=len(rows))
