import json
import shutil
from pathlib import Path

import pytest

from cube_split.partition import CarbonSatellitePartitionService, OpticalPartitionService, get_partition_service
from cube_split.partition.carbon import (
    CarbonPartitionConfig,
    CarbonSatelliteObservation,
    load_oco2_lite_observations,
    partition_observation,
)


def test_partition_registry_separates_optical_and_carbon_services():
    assert isinstance(get_partition_service("optical"), OpticalPartitionService)
    assert isinstance(get_partition_service("carbon_satellite"), CarbonSatellitePartitionService)


def test_optical_service_declares_landsat_and_sentinel_optical_families():
    service = OpticalPartitionService()

    assert service.supported_families == ("landsat", "sentinel_optical")


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

    row = partition_observation(observation, CarbonPartitionConfig(grid_type="geohash", grid_level=7))

    assert row["data_type"] == "carbon_satellite"
    assert row["satellite"] == "OCO2"
    assert row["observation_id"] == "snd-1"
    assert row["xco2"] == 421.25
    assert row["time_bucket"] == "20260424"
    assert row["grid_type"] == "geohash"
    assert row["grid_level"] == 7
    assert row["space_code"]
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
