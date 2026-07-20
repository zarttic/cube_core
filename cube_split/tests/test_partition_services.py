from __future__ import annotations

import builtins
import json
import shutil
import threading
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from grid_core.app.models.grid_address import GridAddress
from grid_core.sdk import CubeEncoderSDK

from cube_split.jobs.cancellation import cancel_ray_refs
from cube_split.partition import CarbonSatellitePartitionService, OpticalPartitionService, RadarPartitionService, get_partition_service
from cube_split.partition.carbon import (
    CarbonObservationSourceSlice,
    CarbonObservationSourceSliceGroup,
    CarbonPartitionConfig,
    CarbonSatelliteObservation,
    _load_observation_chunks,
    _load_oco2_lite_observation_slice,
    _dataset_time_iso,
    _partition_chunk,
    _partition_chunks,
    _resolve_partition_chunk_size,
    _partition_observation_chunk,
    _partition_source_slice_chunk,
    _plan_oco2_lite_source_slices,
    _ray_runtime_env_from_env,
    _time_bucket,
    load_oco2_lite_observations,
    load_observations_from_file,
    partition_observation,
)
from cube_split.partition.carbon_products import get_carbon_product_adapter, supported_carbon_product_types
from cube_split.partition.radar_products import parse_radar_asset


def test_partition_registry_separates_optical_and_carbon_services():
    assert isinstance(get_partition_service("optical"), OpticalPartitionService)
    assert isinstance(get_partition_service("carbon"), CarbonSatellitePartitionService)
    assert isinstance(get_partition_service("carbon_satellite"), CarbonSatellitePartitionService)
    assert isinstance(get_partition_service("radar"), RadarPartitionService)


def test_carbon_dataset_time_accepts_unix_seconds_without_units():
    assert _dataset_time_iso(1522210622.4899948, SimpleNamespace()) == "2018-03-28T04:17:02.489995Z"


def test_carbon_auto_partition_chunk_size_scales_with_workload_and_workers():
    assert _resolve_partition_chunk_size(0, total_observations=4_000, worker_count=4) == 1_000
    assert _resolve_partition_chunk_size(0, total_observations=400_000, worker_count=4) == 5_000
    assert _resolve_partition_chunk_size(250, total_observations=400_000, worker_count=4) == 250
    with pytest.raises(ValueError, match="must be >= 0"):
        _resolve_partition_chunk_size(-1, total_observations=4_000, worker_count=4)


def test_carbon_auto_chunking_splits_one_loaded_source_for_all_workers(monkeypatch, tmp_path: Path):
    observations = [
        CarbonSatelliteObservation(
            satellite="OCO2",
            observation_id=f"snd-{index}",
            acq_time="2026-04-24T00:00:00Z",
            lon=116.391,
            lat=39.907,
            xco2=420.5,
        )
        for index in range(4_000)
    ]
    monkeypatch.setattr("cube_split.partition.carbon.load_observations_from_file", lambda *_args, **_kwargs: observations)

    chunks = _load_observation_chunks(
        [tmp_path / "oco2.jsonl"],
        CarbonPartitionConfig(partition_chunk_size=0),
        worker_count=4,
    )

    assert [len(chunk) for chunk in chunks] == [1_000, 1_000, 1_000, 1_000]


def test_carbon_selected_indexes_apply_before_max_observations(monkeypatch, tmp_path: Path):
    observations = [
        CarbonSatelliteObservation(
            satellite="OCO2",
            observation_id=f"snd-{index}",
            acq_time="2026-04-24T00:00:00Z",
            lon=116.391,
            lat=39.907,
            xco2=420.5,
            source_index=index,
        )
        for index in range(9)
    ]
    monkeypatch.setattr("cube_split.partition.carbon.load_observations_from_file", lambda *_args, **_kwargs: observations)

    chunks = _load_observation_chunks(
        [tmp_path / "oco2.jsonl"],
        CarbonPartitionConfig(partition_chunk_size=0, selected_source_indexes=(7, 8), max_observations=2),
        worker_count=4,
    )

    assert [observation.observation_id for chunk in chunks for observation in chunk] == ["snd-7", "snd-8"]


def test_cancel_ray_refs_force_cancels_all_refs():
    class FakeRay:
        def __init__(self):
            self.cancelled = []

        def cancel(self, ref, *, force):
            self.cancelled.append((ref, force))

    ray = FakeRay()

    cancel_ray_refs(ray, ["ref-a", "ref-b"])

    assert ray.cancelled == [("ref-a", True), ("ref-b", True)]


def test_optical_service_declares_landsat_and_sentinel2_families():
    service = OpticalPartitionService()

    assert service.supported_families == ("landsat", "sentinel2", "other")


def test_radar_asset_parser_extracts_sentinel1_date_and_polarization():
    metadata = parse_radar_asset(Path("20180615_VV.dat"))

    assert metadata.scene_id == "S1_20180615"
    assert metadata.band == "vv"
    assert metadata.acq_time.isoformat() == "2018-06-15T00:00:00+00:00"
    assert metadata.product_family == "sentinel1"
    assert metadata.sensor == "sentinel1_sar"


def test_carbon_observation_partition_outputs_observation_fact():
    observation = CarbonSatelliteObservation(
        satellite="OCO2",
        observation_id="snd-1",
        acq_time="2026-04-24T03:04:05Z",
        lon=116.391,
        lat=39.907,
        xco2=421.25,
        quality_flag="0",
        footprint=[
            [116.38, 39.91],
            [116.40, 39.91],
            [116.40, 39.89],
            [116.38, 39.89],
            [116.38, 39.91],
        ],
        source_uri="s3://bucket/oco2.nc4",
        source_index=7,
        metadata={"orbit": 42},
    )

    row = partition_observation(observation, CarbonPartitionConfig())

    assert row["data_type"] == "carbon"
    assert row["satellite"] == "OCO2"
    assert row["observation_id"] == "snd-1"
    assert row["xco2"] == 421.25
    assert row["time_bucket"] == "20260424"
    assert row["grid_type"] == "isea4h"
    assert row["grid_level"] == 5
    assert row["space_code"]
    assert row["st_code"].startswith("i4h:5:")
    assert row["footprint_geojson"]["type"] == "Polygon"
    assert row["source_uri"] == "s3://bucket/oco2.nc4"
    assert row["source_index"] == 7
    assert json.loads(row["metadata_json"]) == {"orbit": 42}


@pytest.mark.parametrize(
    ("grid_type", "grid_level"),
    [("geohash", 6), ("mgrs", 3), ("isea4h", 5)],
)
@pytest.mark.parametrize("time_granularity", ["month", "day", "hour", "minute", "second"])
def test_carbon_partition_chunk_preserves_full_lookup_address_and_st_code(
    grid_type: str,
    grid_level: int,
    time_granularity: str,
) -> None:
    observation = CarbonSatelliteObservation(
        satellite="OCO2",
        observation_id="snd-compat",
        acq_time="2026-04-24T03:04:05Z",
        lon=116.391,
        lat=39.907,
        xco2=421.25,
    )
    config = CarbonPartitionConfig(
        grid_type=grid_type,
        grid_level=grid_level,
        time_granularity=time_granularity,
    )

    row = _partition_observation_chunk([observation], config)[0]
    full_cell = CubeEncoderSDK().locate(grid_type, grid_level, [observation.lon, observation.lat])
    full_address = GridAddress(
        grid_type=full_cell.grid_type,
        grid_level=full_cell.grid_level,
        space_code=full_cell.space_code,
        topology_code=full_cell.topology_code,
    )
    expected_st_code = CubeEncoderSDK().generate_st_code(
        full_address,
        datetime.fromisoformat(observation.acq_time.replace("Z", "+00:00")),
        time_granularity=time_granularity,
    ).st_code

    assert (row["grid_type"], row["grid_level"], row["space_code"]) == (
        full_address.grid_type,
        full_address.grid_level,
        full_address.space_code,
    )
    assert row["st_code"] == expected_st_code
    assert row["time_bucket"] == {
        "month": "202604",
        "day": "20260424",
        "hour": "2026042403",
        "minute": "202604240304",
        "second": "20260424030405",
    }[time_granularity]


def test_carbon_time_bucket_matches_frozen_sdk_granularities():
    value = "2026-04-24T03:04:05Z"

    assert _time_bucket(value, "second") == "20260424030405"
    with pytest.raises(ValueError, match="Unsupported time_granularity: year"):
        _time_bucket(value, "year")


def test_carbon_service_partitions_jsonl_to_jsonl_output(tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "out"
    input_dir.mkdir()
    (input_dir / "oco2.jsonl").write_text(
        json.dumps(
            {
                "satellite": "OCO2",
                "observation_id": "snd-1",
                "acq_time": "2026-04-24T00:00:00Z",
                "lon": 116.391,
                "lat": 39.907,
                "xco2": 420.5,
                "quality_flag": "0",
                "source_uri": "file:///oco2.nc4",
                "source_index": 0,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = CarbonSatellitePartitionService().run(
        input_dir=input_dir,
        output_dir=output_dir,
        config=CarbonPartitionConfig(grid_type="isea4h", grid_level=7),
        workers=1,
    )

    assert result.total_rows == 1
    rows_path = output_dir / "carbon_observation_rows.jsonl"
    assert result.rows_path == rows_path
    row = json.loads(rows_path.read_text(encoding="utf-8"))
    assert row["data_type"] == "carbon"
    assert row["satellite"] == "OCO2"


def test_carbon_service_removes_partial_rows_file_when_chunk_processing_fails(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "out"
    input_dir.mkdir()
    (input_dir / "oco2.jsonl").write_text(
        json.dumps(
            {
                "satellite": "OCO2",
                "observation_id": "snd-1",
                "acq_time": "2026-04-24T00:00:00Z",
                "lon": 116.391,
                "lat": 39.907,
                "xco2": 420.5,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    def fail_after_first_chunk(chunks, config, worker_count, *, on_chunk=None):
        _ = chunks, config, worker_count
        assert on_chunk is not None
        on_chunk(0, [{"data_type": "carbon", "observation_id": "partial"}])
        raise RuntimeError("chunk failed")

    monkeypatch.setattr("cube_split.partition.carbon._partition_chunks", fail_after_first_chunk)

    with pytest.raises(RuntimeError, match="chunk failed"):
        CarbonSatellitePartitionService().run(input_dir, output_dir)

    assert not (output_dir / "carbon_observation_rows.jsonl").exists()
    assert not (output_dir / "carbon_observation_rows.jsonl.part").exists()


def test_carbon_service_filters_selected_source_indexes(tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "out"
    input_dir.mkdir()
    rows = []
    for idx in range(4):
        rows.append(
            json.dumps(
                {
                    "satellite": "OCO2",
                    "observation_id": f"snd-{idx}",
                    "acq_time": "2026-04-24T00:00:00Z",
                    "lon": 116.391 + idx * 0.001,
                    "lat": 39.907 + idx * 0.001,
                    "xco2": 420.5 + idx,
                    "quality_flag": "0",
                    "source_uri": "file:///oco2.nc4",
                    "source_index": idx,
                }
            )
        )
    (input_dir / "oco2.jsonl").write_text("\n".join(rows) + "\n", encoding="utf-8")

    result = CarbonSatellitePartitionService().run(
        input_dir=input_dir,
        output_dir=output_dir,
        config=CarbonPartitionConfig(
            grid_type="isea4h",
            grid_level=7,
            selected_source_indexes=(1, 3),
        ),
        workers=1,
    )

    output_rows = [json.loads(line) for line in result.rows_path.read_text(encoding="utf-8").splitlines()]

    assert result.total_rows == 2
    assert [row["source_index"] for row in output_rows] == [1, 3]
    assert [row["observation_id"] for row in output_rows] == ["snd-1", "snd-3"]


def test_supported_carbon_product_types_are_registered():
    service = CarbonSatellitePartitionService()

    assert service.supported_product_types == ("xco2", "tansat")
    assert supported_carbon_product_types() == ("xco2", "tansat")
    assert get_carbon_product_adapter("oco2_lite").product_type == "xco2"
    assert get_carbon_product_adapter("tansat_xco2").product_type == "tansat"


def test_carbon_service_can_use_explicit_product_type_for_standard_rows(tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "out"
    input_dir.mkdir()
    (input_dir / "standard.jsonl").write_text(
        json.dumps(
            {
                "satellite": "OCO2",
                "observation_id": "snd-1",
                "acq_time": "2026-04-24T00:00:00Z",
                "lon": 116.391,
                "lat": 39.907,
                "xco2": 420.5,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = CarbonSatellitePartitionService().run(
        input_dir=input_dir,
        output_dir=output_dir,
        config=CarbonPartitionConfig(product_type="oco2_lite", grid_type="isea4h", grid_level=7),
        workers=1,
    )

    row = json.loads(result.rows_path.read_text(encoding="utf-8"))
    assert row["product_type"] == "xco2"
    assert row["satellite"] == "OCO2"


def test_carbon_service_partitions_tansat_with_its_own_product_type(tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "out"
    input_dir.mkdir()
    (input_dir / "tansat.jsonl").write_text(
        json.dumps(
            {
                "satellite": "TanSat",
                "observation_id": "exposure-1",
                "acq_time": "2026-04-24T00:00:00Z",
                "lon": 116.391,
                "lat": 39.907,
                "xco2": 420.5,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = CarbonSatellitePartitionService().run(
        input_dir=input_dir,
        output_dir=output_dir,
        config=CarbonPartitionConfig(product_type="tansat_xco2", grid_type="isea4h", grid_level=7),
        workers=1,
    )

    row = json.loads(result.rows_path.read_text(encoding="utf-8"))
    assert row["product_type"] == "tansat"
    assert row["satellite"] == "TanSat"


def test_tansat_product_rejects_non_tansat_observations(tmp_path: Path):
    path = tmp_path / "oco2.jsonl"
    path.write_text(
        json.dumps(
            {
                "satellite": "OCO2",
                "observation_id": "snd-1",
                "acq_time": "2026-04-24T00:00:00Z",
                "lon": 116.391,
                "lat": 39.907,
                "xco2": 420.5,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="TanSat product requires TanSat observations"):
        load_observations_from_file(path, product_type="tansat")


def test_carbon_service_rejects_unknown_product_type(tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "out"
    input_dir.mkdir()
    (input_dir / "standard.jsonl").write_text(
        json.dumps(
            {
                "satellite": "OCO2",
                "observation_id": "snd-1",
                "acq_time": "2026-04-24T00:00:00Z",
                "lon": 116.391,
                "lat": 39.907,
                "xco2": 420.5,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unsupported carbon product_type"):
        CarbonSatellitePartitionService().run(
            input_dir=input_dir,
            output_dir=output_dir,
            config=CarbonPartitionConfig(product_type="unknown-product"),
            workers=1,
        )


def test_carbon_service_parallelizes_single_file_by_observation_chunks(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "out"
    input_dir.mkdir()
    rows = [
        {
            "satellite": "OCO2",
            "observation_id": f"snd-{idx}",
            "acq_time": "2026-04-24T00:00:00Z",
            "lon": 116.391 + idx * 0.001,
            "lat": 39.907,
            "xco2": 420.5,
        }
        for idx in range(6)
    ]
    (input_dir / "oco2.jsonl").write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )
    thread_ids: set[int] = set()

    class FakeSDK:
        def locate_space_code(self, **kwargs):
            thread_ids.add(threading.get_ident())
            time.sleep(0.02)
            return SimpleNamespace(
                grid_type="isea4h",
                grid_level=kwargs["requested_grid_level"],
                space_code=str(kwargs["point"][0]),
                topology_code=None,
            )

        def generate_st_code(self, *, address, **_kwargs):
            return SimpleNamespace(st_code=f"i4h:{address.grid_level}:{address.space_code}:20260424")

    monkeypatch.setattr("cube_split.partition.carbon.CubeEncoderSDK", FakeSDK)

    result = CarbonSatellitePartitionService().run(
        input_dir=input_dir,
        output_dir=output_dir,
        config=CarbonPartitionConfig(
            grid_type="isea4h",
            grid_level=7,
            partition_chunk_size=1,
            partition_backend="thread",
        ),
        workers=3,
    )

    assert result.total_rows == 6
    assert len(thread_ids) > 1


def test_carbon_service_applies_max_observations_across_whole_run(tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "out"
    input_dir.mkdir()
    for file_idx in range(2):
        rows = [
            {
                "satellite": "OCO2",
                "observation_id": f"snd-{file_idx}-{idx}",
                "acq_time": "2026-04-24T00:00:00Z",
                "lon": 116.391 + idx * 0.001,
                "lat": 39.907,
                "xco2": 420.5,
            }
            for idx in range(3)
        ]
        (input_dir / f"oco2_{file_idx}.jsonl").write_text(
            "\n".join(json.dumps(row) for row in rows) + "\n",
            encoding="utf-8",
        )

    result = CarbonSatellitePartitionService().run(
        input_dir=input_dir,
        output_dir=output_dir,
        config=CarbonPartitionConfig(grid_type="isea4h", grid_level=7, max_observations=4),
        workers=1,
    )

    output_rows = result.rows_path.read_text(encoding="utf-8").splitlines()
    assert result.total_rows == 4
    assert len(output_rows) == 4


def test_carbon_service_parallelizes_multiple_input_files(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "out"
    input_dir.mkdir()
    for idx in range(4):
        (input_dir / f"oco2_{idx}.jsonl").write_text(
            json.dumps(
                {
                    "satellite": "OCO2",
                    "observation_id": f"snd-{idx}",
                    "acq_time": "2026-04-24T00:00:00Z",
                    "lon": 116.391 + idx * 0.001,
                    "lat": 39.907,
                    "xco2": 420.5,
                }
            )
            + "\n",
            encoding="utf-8",
    )
    thread_ids: set[int] = set()

    class FakeSDK:
        def locate_space_code(self, **kwargs):
            thread_ids.add(threading.get_ident())
            time.sleep(0.02)
            return SimpleNamespace(
                grid_type="isea4h",
                grid_level=kwargs["requested_grid_level"],
                space_code=str(kwargs["point"][0]),
                topology_code=None,
            )

        def generate_st_code(self, *, address, **_kwargs):
            return SimpleNamespace(st_code=f"i4h:{address.grid_level}:{address.space_code}:20260424")

    monkeypatch.setattr("cube_split.partition.carbon.CubeEncoderSDK", FakeSDK)

    result = CarbonSatellitePartitionService().run(
        input_dir=input_dir,
        output_dir=output_dir,
        config=CarbonPartitionConfig(
            grid_type="isea4h",
            grid_level=7,
            partition_chunk_size=1,
            partition_backend="thread",
        ),
        workers=4,
    )

    assert result.total_rows == 4
    assert len(thread_ids) > 1


def test_carbon_service_parallelizes_observation_loading_across_files(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "out"
    input_dir.mkdir()
    for idx in range(4):
        (input_dir / f"oco2_{idx}.nc4").write_text("sample", encoding="utf-8")
    load_thread_ids: set[int] = set()

    def fake_load_observations_from_file(path, max_observations=None):
        _ = max_observations
        load_thread_ids.add(threading.get_ident())
        time.sleep(0.02)
        idx = path.stem.rsplit("_", 1)[1]
        return [
            CarbonSatelliteObservation(
                satellite="OCO2",
                observation_id=f"snd-{idx}",
                acq_time="2026-04-24T00:00:00Z",
                lon=116.391,
                lat=39.907,
                xco2=420.5,
                source_uri=str(path),
            )
        ]

    def fake_partition_observation(observation, config, sdk=None):
        _ = sdk
        return {
            "data_type": "carbon",
            "satellite": observation.satellite,
            "observation_id": observation.observation_id,
            "space_code": observation.observation_id,
        }

    monkeypatch.setattr("cube_split.partition.carbon.load_observations_from_file", fake_load_observations_from_file)
    monkeypatch.setattr("cube_split.partition.carbon.partition_observation", fake_partition_observation)

    result = CarbonSatellitePartitionService().run(
        input_dir=input_dir,
        output_dir=output_dir,
        config=CarbonPartitionConfig(grid_type="isea4h", grid_level=7, partition_chunk_size=1),
        workers=4,
    )

    assert result.total_rows == 4
    assert len(load_thread_ids) > 1


def test_carbon_partition_chunk_uses_frozen_sdk_address_calls(monkeypatch):
    calls: list[dict] = []
    generated: list[tuple[object, object, object]] = []

    class FakeSDK:
        def locate_space_code(self, grid_type, requested_grid_level, point):
            calls.append(
                {
                    "grid_type": grid_type,
                    "requested_grid_level": requested_grid_level,
                    "point": point,
                }
            )
            return SimpleNamespace(
                grid_type="isea4h",
                grid_level=requested_grid_level,
                space_code=f"cell-{len(calls)}",
                topology_code=None,
            )

        def generate_st_code(self, address, timestamp, time_granularity):
            generated.append((address, timestamp, time_granularity))
            return SimpleNamespace(st_code=f"i4h:{address.grid_level}:{address.space_code}:20260424")

    monkeypatch.setattr("cube_split.partition.carbon.CubeEncoderSDK", FakeSDK)
    observations = [
        CarbonSatelliteObservation(
            satellite="OCO2",
            observation_id="snd-a",
            acq_time="2026-04-24T00:00:00Z",
            lon=116.391,
            lat=39.907,
            xco2=420.5,
        ),
        CarbonSatelliteObservation(
            satellite="OCO2",
            observation_id="snd-b",
            acq_time="2026-04-24T00:00:00Z",
            lon=116.392,
            lat=39.908,
            xco2=421.5,
        ),
    ]

    rows = _partition_observation_chunk(
        observations,
        CarbonPartitionConfig(grid_type="isea4h", grid_level=7),
    )

    assert [row["space_code"] for row in rows] == ["cell-1", "cell-2"]
    assert calls[0]["point"] == [116.391, 39.907]
    assert calls[1]["point"] == [116.392, 39.908]
    assert [call[0].space_code for call in generated] == ["cell-1", "cell-2"]
    assert [call[2] for call in generated] == ["day", "day"]


def test_carbon_partition_uses_process_backend_by_default(monkeypatch):
    executor_calls: list[int] = []

    class FakeProcessPoolExecutor:
        def __init__(self, max_workers):
            executor_calls.append(max_workers)

        def __enter__(self):
            return self

        def __exit__(self, *_exc_info):
            return False

        def map(self, fn, chunks, configs):
            return [fn(chunk, config) for chunk, config in zip(chunks, configs)]

    monkeypatch.setattr("cube_split.partition.carbon.ProcessPoolExecutor", FakeProcessPoolExecutor)
    chunks = [
        [
            CarbonSatelliteObservation(
                satellite="OCO2",
                observation_id="snd-a",
                acq_time="2026-04-24T00:00:00Z",
                lon=116.391,
                lat=39.907,
                xco2=420.5,
            )
        ],
        [
            CarbonSatelliteObservation(
                satellite="OCO2",
                observation_id="snd-b",
                acq_time="2026-04-24T00:00:00Z",
                lon=116.392,
                lat=39.908,
                xco2=421.5,
            )
        ],
    ]

    rows = _partition_chunks(chunks, CarbonPartitionConfig(grid_type="isea4h", grid_level=7), worker_count=2)

    assert len(rows) == 2
    assert executor_calls == [2]


def test_carbon_partition_can_use_ray_backend(monkeypatch):
    remote_calls: list[str] = []
    processed_chunks: list[int] = []

    class FakeRemoteMethod:
        def __init__(self, fn):
            self._fn = fn

        def remote(self, *args, **kwargs):
            return self._fn(*args, **kwargs)

    class FakeActorHandle:
        def __init__(self, cls):
            self._instance = cls()

        @property
        def process_chunk(self):
            return FakeRemoteMethod(self._instance.process_chunk)

    class FakeActorClass:
        def __init__(self, cls):
            self._cls = cls

        def options(self, **kwargs):
            return self

        def remote(self):
            remote_calls.append("actor")
            return FakeActorHandle(self._cls)

    class FakeRay:
        def remote(self, cls):
            return FakeActorClass(cls)

        def init(self, **kwargs):
            remote_calls.append("init")

        def get(self, futures):
            return futures

        def shutdown(self):
            remote_calls.append("shutdown")

    def fake_partition_observation_chunk(chunk, config):
        processed_chunks.append(len(chunk))
        return [
            {
                "data_type": "carbon",
                "satellite": observation.satellite,
                "observation_id": observation.observation_id,
                "grid_type": config.grid_type,
                "grid_level": config.grid_level,
                "space_code": observation.observation_id,
            }
            for observation in chunk
        ]

    monkeypatch.setattr("cube_split.partition.carbon._load_ray", lambda: FakeRay())
    monkeypatch.setattr("cube_split.partition.carbon._partition_observation_chunk", fake_partition_observation_chunk)
    chunks = [
        [
            CarbonSatelliteObservation(
                satellite="OCO2",
                observation_id="snd-a",
                acq_time="2026-04-24T00:00:00Z",
                lon=116.391,
                lat=39.907,
                xco2=420.5,
            )
        ],
        [
            CarbonSatelliteObservation(
                satellite="OCO2",
                observation_id="snd-b",
                acq_time="2026-04-24T00:00:00Z",
                lon=116.392,
                lat=39.908,
                xco2=421.5,
            )
        ],
    ]

    rows = _partition_chunks(
        chunks,
        CarbonPartitionConfig(grid_type="isea4h", grid_level=5, partition_backend="ray"),
        worker_count=2,
    )

    assert [row["observation_id"] for row in rows] == ["snd-a", "snd-b"]
    assert processed_chunks == [1, 1]
    assert remote_calls == ["init", "actor", "actor", "shutdown"]


def test_carbon_partition_source_slice_uses_resolved_path_but_keeps_source_uri(monkeypatch):
    calls: dict[str, object] = {}

    def fake_load(path: Path, start_index: int, stop_index: int, *, source_uri: str | None = None):
        calls["path"] = path
        calls["start_index"] = start_index
        calls["stop_index"] = stop_index
        calls["source_uri"] = source_uri
        return [
            CarbonSatelliteObservation(
                satellite="OCO2",
                observation_id="snd-a",
                acq_time="2026-04-24T00:00:00Z",
                lon=116.391,
                lat=39.907,
                xco2=420.5,
                source_uri=str(source_uri or path),
                source_index=start_index,
            )
        ]

    monkeypatch.setattr("cube_split.partition.carbon._load_oco2_lite_observation_slice", fake_load)
    monkeypatch.setattr(
        "cube_split.partition.carbon._partition_observation_chunk",
        lambda observations, cfg: [{"source_uri": observations[0].source_uri, "grid_type": cfg.grid_type}],
    )

    rows = _partition_source_slice_chunk(
        CarbonObservationSourceSlice("s3://cube/cube/source/carbon/oco2.nc4", 2, 3),
        CarbonPartitionConfig(grid_type="isea4h", grid_level=5, partition_backend="ray"),
        resolved_source_path="/tmp/worker-cache/oco2.nc4",
    )

    assert calls["path"] == Path("/tmp/worker-cache/oco2.nc4")
    assert calls["start_index"] == 2
    assert calls["stop_index"] == 3
    assert calls["source_uri"] == "s3://cube/cube/source/carbon/oco2.nc4"
    assert rows == [{"source_uri": "s3://cube/cube/source/carbon/oco2.nc4", "grid_type": "isea4h"}]


def test_carbon_ray_source_slice_planning_uses_source_uris(monkeypatch):
    counts = {
        "s3://cube/cube/source/carbon/a.nc4": 5,
        "s3://cube/cube/source/carbon/b.nc4": 3,
    }

    monkeypatch.setattr(
        "cube_split.partition.carbon._oco2_lite_observation_count_for_source",
        lambda source_uri: counts[source_uri],
    )

    slices = _plan_oco2_lite_source_slices(
        ["s3://cube/cube/source/carbon/a.nc4", "s3://cube/cube/source/carbon/b.nc4"],
        CarbonPartitionConfig(partition_backend="ray", partition_chunk_size=2, max_observations=6),
    )

    assert slices == [
        CarbonObservationSourceSlice("s3://cube/cube/source/carbon/a.nc4", 0, 2),
        CarbonObservationSourceSlice("s3://cube/cube/source/carbon/a.nc4", 2, 4),
        CarbonObservationSourceSlice("s3://cube/cube/source/carbon/a.nc4", 4, 5),
        CarbonObservationSourceSlice("s3://cube/cube/source/carbon/b.nc4", 0, 1),
    ]


def test_carbon_ray_source_slice_planning_auto_chunks_one_source_for_workers(monkeypatch):
    monkeypatch.setattr(
        "cube_split.partition.carbon._oco2_lite_observation_count_for_source",
        lambda _source_uri: 4_000,
    )

    slices = _plan_oco2_lite_source_slices(
        ["s3://cube/cube/source/carbon/a.nc4"],
        CarbonPartitionConfig(partition_backend="ray", partition_chunk_size=0),
        worker_count=4,
    )

    assert slices == [
        CarbonObservationSourceSlice("s3://cube/cube/source/carbon/a.nc4", 0, 1_000),
        CarbonObservationSourceSlice("s3://cube/cube/source/carbon/a.nc4", 1_000, 2_000),
        CarbonObservationSourceSlice("s3://cube/cube/source/carbon/a.nc4", 2_000, 3_000),
        CarbonObservationSourceSlice("s3://cube/cube/source/carbon/a.nc4", 3_000, 4_000),
    ]


def test_carbon_ray_auto_chunking_groups_small_sources(monkeypatch):
    source_uris = [f"s3://cube/cube/source/carbon/{index}.nc4" for index in range(100)]
    monkeypatch.setattr(
        "cube_split.partition.carbon._oco2_lite_observation_count_for_source",
        lambda _source_uri: 100,
    )

    chunks = _plan_oco2_lite_source_slices(
        source_uris,
        CarbonPartitionConfig(partition_backend="ray", partition_chunk_size=0),
        worker_count=4,
    )

    assert len(chunks) == 10
    assert all(isinstance(chunk, CarbonObservationSourceSliceGroup) for chunk in chunks)
    assert [sum(item.stop_index - item.start_index for item in chunk.slices) for chunk in chunks] == [1_000] * 10


def test_carbon_source_slice_group_processes_its_slices(monkeypatch):
    monkeypatch.setattr(
        "cube_split.partition.carbon._partition_source_slice_chunk",
        lambda source_slice, _config: [{"source_uri": source_slice.source_uri}],
    )
    group = CarbonObservationSourceSliceGroup(
        (
            CarbonObservationSourceSlice("s3://cube/cube/source/carbon/a.nc4", 0, 100),
            CarbonObservationSourceSlice("s3://cube/cube/source/carbon/b.nc4", 0, 100),
        )
    )

    rows = _partition_chunk(group, CarbonPartitionConfig())

    assert rows == [
        {"source_uri": "s3://cube/cube/source/carbon/a.nc4"},
        {"source_uri": "s3://cube/cube/source/carbon/b.nc4"},
    ]


def test_carbon_partition_ray_retries_on_head_when_runtime_env_disk_is_full(monkeypatch):
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    init_calls: list[dict] = []
    option_calls: list[dict] = []
    shutdown_calls: list[str] = []
    processed: list[str] = []

    class RuntimeEnvSetupError(RuntimeError):
        pass

    class FakeRemoteMethod:
        def __init__(self, fn):
            self._fn = fn

        def remote(self, *args, **kwargs):
            return self._fn(*args, **kwargs)

    class FakeActorHandle:
        def __init__(self, prefer_head: bool):
            self._prefer_head = prefer_head

        @property
        def process_chunk(self):
            return FakeRemoteMethod(lambda chunk, cfg: ("future", self._prefer_head, chunk, cfg))

    class FakeActorClass:
        def __init__(self, cls):
            self._cls = cls
            self._prefer_head = False

        def options(self, **kwargs):
            option_calls.append(kwargs)
            self._prefer_head = "scheduling_strategy" in kwargs
            return self

        def remote(self):
            return FakeActorHandle(self._prefer_head)

    class FakeRay:
        def remote(self, cls):
            return FakeActorClass(cls)

        def init(self, **kwargs):
            init_calls.append(kwargs)

        def get(self, ref):
            if isinstance(ref, list):
                return [self.get(item) for item in ref]
            _, prefer_head, chunk, cfg = ref
            if not prefer_head and chunk[0].observation_id == "snd-b":
                raise RuntimeEnvSetupError("No space left on device")
            processed.extend(observation.observation_id for observation in chunk)
            return [
                {
                    "data_type": "carbon",
                    "satellite": observation.satellite,
                    "observation_id": observation.observation_id,
                    "grid_type": cfg.grid_type,
                    "grid_level": cfg.grid_level,
                    "space_code": observation.observation_id,
                }
                for observation in chunk
            ]

        def wait(self, pending, num_returns=1, timeout=1.0):
            return [pending[0]], pending[1:]

        def cancel(self, ref, *, force):
            _ = (ref, force)

        def shutdown(self):
            shutdown_calls.append("shutdown")

        def nodes(self):
            return [
                {
                    "Alive": True,
                    "NodeID": "a2362ca71987bdd4036e21f1e2b9b55318489a42741adb914f6ea46f",
                    "Resources": {"node:__internal_head__": 1.0},
                }
            ]

    monkeypatch.setattr("cube_split.partition.carbon._load_ray", lambda: FakeRay())
    chunks = [
        [
            CarbonSatelliteObservation(
                satellite="OCO2",
                observation_id="snd-a",
                acq_time="2026-04-24T00:00:00Z",
                lon=116.391,
                lat=39.907,
                xco2=420.5,
            )
        ],
        [
            CarbonSatelliteObservation(
                satellite="OCO2",
                observation_id="snd-b",
                acq_time="2026-04-24T00:00:00Z",
                lon=116.392,
                lat=39.908,
                xco2=421.5,
            )
        ]
    ]
    streamed: list[dict] = []

    rows = _partition_chunks(
        chunks,
        CarbonPartitionConfig(grid_type="isea4h", grid_level=5, partition_backend="ray"),
        worker_count=1,
        on_chunk=lambda _chunk_index, part: streamed.extend(part),
    )

    assert rows == []
    assert [row["observation_id"] for row in streamed] == ["snd-a", "snd-b"]
    assert processed == ["snd-a", "snd-a", "snd-b"]
    assert len(init_calls) == 2
    assert len(shutdown_calls) == 2
    assert "scheduling_strategy" not in option_calls[0]
    assert isinstance(option_calls[1]["scheduling_strategy"], NodeAffinitySchedulingStrategy)
    assert getattr(option_calls[1]["scheduling_strategy"], "node_id", "") == "a2362ca71987bdd4036e21f1e2b9b55318489a42741adb914f6ea46f"


def test_carbon_ray_runtime_env_ships_project_code(monkeypatch):
    monkeypatch.delenv("RAY_RUNTIME_ENV_JSON", raising=False)
    monkeypatch.setattr(
        "cube_split.partition.carbon.runtime_config.minio_settings",
        lambda: type(
            "MinioSettings",
            (),
            {
                "endpoint": "10.3.100.179:9000",
                "access_key": "minio-access",
                "secret_key": "minio-secret",
                "bucket": "cube",
            },
        )(),
    )

    runtime_env = _ray_runtime_env_from_env()

    assert runtime_env is not None
    assert Path(runtime_env["working_dir"]).resolve() == Path(__file__).resolve().parents[2]
    assert runtime_env["env_vars"]["CUBE_CARBON_RUNTIME_ENV_REV"] == "20260720-grid-v8"
    assert runtime_env["env_vars"]["CUBE_WEB_MINIO_ENDPOINT"] == "10.3.100.179:9000"
    assert runtime_env["env_vars"]["CUBE_WEB_MINIO_ACCESS_KEY"] == "minio-access"
    assert runtime_env["env_vars"]["CUBE_WEB_MINIO_SECRET_KEY"] == "minio-secret"
    assert runtime_env["env_vars"]["PYTHONPATH"] == ".:./cube_encoder:./cube_split:./cube_web"
    assert "cube_split/data/**" in runtime_env["excludes"]
    assert ".codegraph/**" in runtime_env["excludes"]
    assert ".mypy_cache/**" in runtime_env["excludes"]
    assert ".tmp/**" in runtime_env["excludes"]


def test_carbon_service_can_use_ray_source_uri_without_local_input_files(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "out"
    input_dir.mkdir()
    planned = [CarbonObservationSourceSlice("s3://cube/cube/source/carbon/oco2.nc4", 0, 1)]

    monkeypatch.setattr(
        "cube_split.partition.carbon._plan_oco2_lite_source_slices",
        lambda source_uris, config, worker_count: planned,
    )
    monkeypatch.setattr(
        "cube_split.partition.carbon._load_observation_chunks",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected local load")),
    )
    def fake_partition_chunks(chunks, config, worker_count, *, on_chunk=None):
        rows = [
            {
                "data_type": "carbon",
                "satellite": "OCO2",
                "observation_id": "snd-a",
                "grid_type": config.grid_type,
                "grid_level": config.grid_level,
                "space_code": "cell-a",
                "st_code": "i4h:5:cell-a:20260424",
                "source_uri": "s3://cube/cube/source/carbon/oco2.nc4",
            }
        ]
        assert on_chunk is not None
        on_chunk(0, rows)
        return []

    monkeypatch.setattr("cube_split.partition.carbon._partition_chunks", fake_partition_chunks)

    result = CarbonSatellitePartitionService().run(
        input_dir=input_dir,
        output_dir=output_dir,
        config=CarbonPartitionConfig(
            grid_type="isea4h",
            grid_level=5,
            partition_backend="ray",
            source_uris=("s3://cube/cube/source/carbon/oco2.nc4",),
        ),
        workers=4,
    )

    assert result.total_rows == 1
    row = json.loads(result.rows_path.read_text(encoding="utf-8"))
    assert row["source_uri"] == "s3://cube/cube/source/carbon/oco2.nc4"


def test_carbon_service_loads_source_uri_when_ray_slice_fast_path_is_unavailable(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "out"
    input_dir.mkdir()
    loaded_sources: list[str] = []

    monkeypatch.setattr(
        "cube_split.partition.carbon._plan_oco2_lite_source_slices",
        lambda source_uris, config, worker_count: None,
    )
    monkeypatch.setattr("cube_split.partition.carbon._resolve_oco2_lite_source_path", lambda source_uri: Path("/resolved") / Path(source_uri).name)

    def fake_load_oco2_lite_observations(path: Path, max_observations=None):
        loaded_sources.append(str(path))
        return [
            CarbonSatelliteObservation(
                satellite="OCO2",
                observation_id="snd-a",
                acq_time="2026-04-24T00:00:00Z",
                lon=116.391,
                lat=39.907,
                xco2=420.5,
                source_uri="s3://cube/cube/source/carbon/oco2.nc4",
                source_index=0,
            )
        ]

    monkeypatch.setattr("cube_split.partition.carbon.load_oco2_lite_observations", fake_load_oco2_lite_observations)
    def fake_partition_chunks(chunks, config, worker_count, *, on_chunk=None):
        rows = [
            {
                "data_type": "carbon",
                "satellite": "OCO2",
                "observation_id": chunks[0][0].observation_id,
                "grid_type": config.grid_type,
                "grid_level": config.grid_level,
                "space_code": "cell-a",
                "st_code": "i4h:5:cell-a:20260424",
                "source_uri": chunks[0][0].source_uri,
            }
        ]
        assert on_chunk is not None
        on_chunk(0, rows)
        return []

    monkeypatch.setattr("cube_split.partition.carbon._partition_chunks", fake_partition_chunks)

    result = CarbonSatellitePartitionService().run(
        input_dir=input_dir,
        output_dir=output_dir,
        config=CarbonPartitionConfig(
            grid_type="isea4h",
            grid_level=5,
            partition_backend="thread",
            source_uris=("s3://cube/cube/source/carbon/oco2.nc4",),
        ),
        workers=1,
    )

    assert result.total_rows == 1
    assert loaded_sources == ["/resolved/oco2.nc4"]


def test_carbon_netcdf4_loader_only_falls_back_when_module_missing(monkeypatch, tmp_path: Path):
    original_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "netCDF4":
            raise RuntimeWarning("numpy.ndarray size changed")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(RuntimeWarning, match="numpy.ndarray size changed"):
        load_oco2_lite_observations(tmp_path / "sample.nc4", max_observations=1)


def test_carbon_loader_falls_back_to_h5dump_when_netcdf4_is_unavailable(monkeypatch, tmp_path: Path):
    sentinel = [
        CarbonSatelliteObservation(
            satellite="OCO2",
            observation_id="snd-1",
            acq_time="2026-04-24T00:00:00Z",
            lon=116.391,
            lat=39.907,
            xco2=420.5,
        )
    ]

    monkeypatch.setattr("cube_split.partition.carbon._load_oco2_lite_with_netcdf4", lambda *args, **kwargs: None)
    monkeypatch.setattr("cube_split.partition.carbon._load_oco2_lite_with_h5dump", lambda *args, **kwargs: sentinel)

    assert load_oco2_lite_observations(tmp_path / "sample.nc4", max_observations=1) == sentinel


def test_carbon_loader_reads_tansat_style_h5_via_generic_netcdf4_schema(monkeypatch, tmp_path: Path):
    class FakeVar:
        def __init__(self, values, **attrs):
            self._values = values
            for key, value in attrs.items():
                setattr(self, key, value)

        def __len__(self):
            return len(self._values)

        def __getitem__(self, item):
            return self._values[item]

    class FakeDataset:
        platform = "TanSat"

        def __init__(self, path, mode):
            _ = (path, mode)
            self.variables = {
                "exposure_id": FakeVar([9001, 9002]),
                "latitude": FakeVar([10.5, 11.5]),
                "longitude": FakeVar([100.5, 101.5]),
                "time": FakeVar([0.0, 60.0], units="seconds since 2020-01-01 00:00:00"),
                "xco2": FakeVar([411.2, 412.3]),
                "xco2_quality_flag": FakeVar([0, 1]),
            }

        def __enter__(self):
            return self

        def __exit__(self, *_exc_info):
            return False

    monkeypatch.setattr("cube_split.partition.carbon._netcdf4_dataset_class", lambda: FakeDataset)

    observations = load_observations_from_file(
        tmp_path / "TanSat_demo.h5",
        max_observations=2,
        product_type="tansat",
    )

    assert len(observations) == 2
    assert observations[0].satellite == "TanSat"
    assert observations[0].observation_id == "9001"
    assert observations[0].acq_time == "2020-01-01T00:00:00Z"
    assert observations[0].quality_flag == "0"
    assert observations[0].source_index == 0
    assert observations[0].footprint is None
    assert observations[1].acq_time == "2020-01-01T00:01:00Z"
    assert observations[1].metadata["schema_kind"] == "generic_xco2_netcdf"


def test_carbon_loader_reads_tansat_style_h5_slice(monkeypatch, tmp_path: Path):
    class FakeVar:
        def __init__(self, values, **attrs):
            self._values = values
            for key, value in attrs.items():
                setattr(self, key, value)

        def __len__(self):
            return len(self._values)

        def __getitem__(self, item):
            return self._values[item]

    class FakeDataset:
        platform = "TanSat"

        def __init__(self, path, mode):
            _ = (path, mode)
            self.variables = {
                "exposure_id": FakeVar([9001, 9002, 9003]),
                "latitude": FakeVar([10.5, 11.5, 12.5]),
                "longitude": FakeVar([100.5, 101.5, 102.5]),
                "time": FakeVar([0.0, 60.0, 120.0], units="seconds since 2020-01-01 00:00:00"),
                "xco2": FakeVar([411.2, 412.3, 413.4]),
                "xco2_quality_flag": FakeVar([0, 1, 1]),
            }

        def __enter__(self):
            return self

        def __exit__(self, *_exc_info):
            return False

    monkeypatch.setattr("cube_split.partition.carbon._netcdf4_dataset_class", lambda: FakeDataset)

    observations = _load_oco2_lite_observation_slice(
        tmp_path / "TanSat_demo.h5",
        1,
        3,
        source_uri="s3://cube/cube/source/carbon/TanSat_demo.h5",
    )

    assert [obs.observation_id for obs in observations] == ["9002", "9003"]
    assert [obs.source_index for obs in observations] == [1, 2]
    assert observations[0].source_uri == "s3://cube/cube/source/carbon/TanSat_demo.h5"
    assert observations[1].acq_time == "2020-01-01T00:02:00Z"


def test_carbon_ray_source_slice_planning_supports_tansat(monkeypatch):
    monkeypatch.setattr(
        "cube_split.partition.carbon._oco2_lite_observation_count_for_source",
        lambda source_uri: 3 if source_uri.endswith("tansat.h5") else None,
    )

    slices = _plan_oco2_lite_source_slices(
        ["s3://cube/cube/source/carbon/tansat.h5"],
        CarbonPartitionConfig(product_type="tansat", partition_backend="ray", partition_chunk_size=2),
    )

    assert slices == [
        CarbonObservationSourceSlice("s3://cube/cube/source/carbon/tansat.h5", 0, 2),
        CarbonObservationSourceSlice("s3://cube/cube/source/carbon/tansat.h5", 2, 3),
    ]


def test_carbon_ray_source_slice_rejects_non_tansat_observations(monkeypatch):
    monkeypatch.setattr(
        "cube_split.partition.carbon._load_oco2_lite_observation_slice",
        lambda *args, **kwargs: [
            CarbonSatelliteObservation(
                satellite="OCO2", observation_id="snd-1", acq_time="2026-04-24T00:00:00Z",
                lon=116.391, lat=39.907, xco2=420.5,
            )
        ],
    )

    with pytest.raises(ValueError, match="TanSat product requires TanSat observations"):
        _partition_source_slice_chunk(
            CarbonObservationSourceSlice("s3://cube/cube/source/carbon/tansat.h5", 0, 1),
            CarbonPartitionConfig(product_type="tansat", partition_backend="ray"),
            resolved_source_path="/tmp/tansat.h5",
        )


@pytest.mark.filterwarnings("ignore:numpy.ndarray size changed.*:RuntimeWarning")
def test_carbon_service_reads_uploaded_oco2_lite_nc4_sample(tmp_path: Path):
    sample_path = Path(__file__).parents[1] / "oco2_LtCO2_201231_B11014Ar_220729012824s(1).nc4"
    if not sample_path.exists():
        pytest.skip("uploaded OCO-2 Lite sample is not present")
    if not shutil.which("h5dump"):
        pytest.importorskip("netCDF4")

    observations = load_oco2_lite_observations(sample_path, max_observations=3)

    assert len(observations) == 3
    assert observations[0].satellite == "OCO2"
    assert observations[0].observation_id == "2020123100010671"
    assert observations[0].acq_time.startswith("2020-12-31T00:01:06")
    assert observations[0].lon == pytest.approx(-167.413, abs=0.001)
    assert observations[0].lat == pytest.approx(41.1686, abs=0.001)
    assert observations[0].xco2 == pytest.approx(417.384, abs=0.001)
    assert observations[0].quality_flag == "1"
    assert len(observations[0].footprint or []) == 4

    result = CarbonSatellitePartitionService().run(
        input_dir=sample_path.parent,
        output_dir=tmp_path / "out",
        config=CarbonPartitionConfig(grid_type="isea4h", grid_level=7, max_observations=3),
        workers=1,
    )

    assert result.total_rows == 3
    first_row = json.loads(result.rows_path.read_text(encoding="utf-8").splitlines()[0])
    assert first_row["data_type"] == "carbon"
    assert first_row["satellite"] == "OCO2"
    assert first_row["observation_id"] == "2020123100010671"
    assert first_row["footprint_geojson"]["type"] == "Polygon"
