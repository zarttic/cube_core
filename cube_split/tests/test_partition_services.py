import json
import shutil
import threading
import time
from pathlib import Path

import pytest

from cube_split.partition import CarbonSatellitePartitionService, OpticalPartitionService, get_partition_service
from cube_split.partition.carbon import (
    CarbonPartitionConfig,
    CarbonSatelliteObservation,
    _partition_chunks,
    _partition_observation_chunk,
    load_oco2_lite_observations,
    partition_observation,
)
from cube_split.partition.carbon_products import get_carbon_product_adapter, supported_carbon_product_types


def test_partition_registry_separates_optical_and_carbon_services():
    assert isinstance(get_partition_service("optical"), OpticalPartitionService)
    assert isinstance(get_partition_service("carbon_satellite"), CarbonSatellitePartitionService)


def test_optical_service_declares_landsat_and_sentinel2_families():
    service = OpticalPartitionService()

    assert service.supported_families == ("landsat", "sentinel2", "other")


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

    assert row["data_type"] == "carbon_satellite"
    assert row["satellite"] == "OCO2"
    assert row["observation_id"] == "snd-1"
    assert row["xco2"] == 421.25
    assert row["time_bucket"] == "20260424"
    assert row["grid_type"] == "isea4h"
    assert row["grid_level"] == 5
    assert row["space_code"]
    assert row["st_code"].startswith("hx:5:")
    assert row["footprint_geojson"]["type"] == "Polygon"
    assert row["source_uri"] == "s3://bucket/oco2.nc4"
    assert row["source_index"] == 7
    assert json.loads(row["metadata_json"]) == {"orbit": 42}


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
        config=CarbonPartitionConfig(grid_type="geohash", grid_level=7),
        workers=1,
    )

    assert result.total_rows == 1
    rows_path = output_dir / "carbon_observation_rows.jsonl"
    assert result.rows_path == rows_path
    row = json.loads(rows_path.read_text(encoding="utf-8"))
    assert row["data_type"] == "carbon_satellite"
    assert row["satellite"] == "OCO2"


def test_supported_carbon_product_types_are_registered():
    service = CarbonSatellitePartitionService()

    assert service.supported_product_types == ("xco2",)
    assert supported_carbon_product_types() == ("xco2",)
    assert get_carbon_product_adapter("oco2_lite").product_type == "xco2"


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
        config=CarbonPartitionConfig(product_type="oco2_lite", grid_type="geohash", grid_level=7),
        workers=1,
    )

    row = json.loads(result.rows_path.read_text(encoding="utf-8"))
    assert row["product_type"] == "xco2"
    assert row["satellite"] == "OCO2"


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
        def batch_locate_st_codes(self, **kwargs):
            thread_ids.add(threading.get_ident())
            time.sleep(0.02)
            return [
                {
                    "grid_level": kwargs["level"],
                    "space_code": item["point"][0],
                    "st_code": f"gh:7:{item['point'][0]}:20260424:v1",
                    "time_code": "20260424",
                }
                for item in kwargs["items"]
            ]

    monkeypatch.setattr("cube_split.partition.carbon.CubeEncoderSDK", FakeSDK)

    result = CarbonSatellitePartitionService().run(
        input_dir=input_dir,
        output_dir=output_dir,
        config=CarbonPartitionConfig(
            grid_type="geohash",
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
        config=CarbonPartitionConfig(grid_type="geohash", grid_level=7, max_observations=4),
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
        def batch_locate_st_codes(self, **kwargs):
            thread_ids.add(threading.get_ident())
            time.sleep(0.02)
            return [
                {
                    "grid_level": kwargs["level"],
                    "space_code": item["point"][0],
                    "st_code": f"gh:7:{item['point'][0]}:20260424:v1",
                    "time_code": "20260424",
                }
                for item in kwargs["items"]
            ]

    monkeypatch.setattr("cube_split.partition.carbon.CubeEncoderSDK", FakeSDK)

    result = CarbonSatellitePartitionService().run(
        input_dir=input_dir,
        output_dir=output_dir,
        config=CarbonPartitionConfig(
            grid_type="geohash",
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
        return {
            "data_type": "carbon_satellite",
            "satellite": observation.satellite,
            "observation_id": observation.observation_id,
            "space_code": observation.observation_id,
        }

    monkeypatch.setattr("cube_split.partition.carbon.load_observations_from_file", fake_load_observations_from_file)
    monkeypatch.setattr("cube_split.partition.carbon.partition_observation", fake_partition_observation)

    result = CarbonSatellitePartitionService().run(
        input_dir=input_dir,
        output_dir=output_dir,
        config=CarbonPartitionConfig(grid_type="geohash", grid_level=7, partition_chunk_size=1),
        workers=4,
    )

    assert result.total_rows == 4
    assert len(load_thread_ids) > 1


def test_carbon_partition_chunk_uses_sdk_batch_locate_st_codes(monkeypatch):
    calls: list[dict] = []

    class FakeSDK:
        def batch_locate_st_codes(self, **kwargs):
            calls.append(kwargs)
            return [
                {
                    "grid_level": kwargs["level"],
                    "space_code": "cell-a",
                    "st_code": "gh:7:cell-a:20260424:v1",
                    "time_code": "20260424",
                },
                {
                    "grid_level": kwargs["level"],
                    "space_code": "cell-b",
                    "st_code": "gh:7:cell-b:20260424:v1",
                    "time_code": "20260424",
                },
            ]

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
        CarbonPartitionConfig(grid_type="geohash", grid_level=7),
    )

    assert [row["space_code"] for row in rows] == ["cell-a", "cell-b"]
    assert calls[0]["items"][0]["point"] == [116.391, 39.907]
    assert calls[0]["items"][1]["point"] == [116.392, 39.908]


def test_carbon_partition_uses_process_backend_by_default(monkeypatch):
    executor_calls: list[int] = []

    class FakeProcessPoolExecutor:
        def __init__(self, max_workers):
            executor_calls.append(max_workers)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
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

    rows = _partition_chunks(chunks, CarbonPartitionConfig(grid_type="geohash", grid_level=7), worker_count=2)

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
                "data_type": "carbon_satellite",
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
        config=CarbonPartitionConfig(grid_type="geohash", grid_level=7, max_observations=3),
        workers=1,
    )

    assert result.total_rows == 3
    first_row = json.loads(result.rows_path.read_text(encoding="utf-8").splitlines()[0])
    assert first_row["data_type"] == "carbon_satellite"
    assert first_row["satellite"] == "OCO2"
    assert first_row["observation_id"] == "2020123100010671"
    assert first_row["footprint_geojson"]["type"] == "Polygon"
