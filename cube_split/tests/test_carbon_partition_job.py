from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from netCDF4 import Dataset

from cube_split.jobs.carbon_partition_job import (
    _default_ray_parallelism,
    _resolve_backend,
    _resolve_worker_count,
    parse_args,
    run_carbon_partition,
)
from cube_split.partition.carbon import CarbonPartitionConfig


def test_carbon_partition_config_defaults_to_isea4h_level6():
    config = CarbonPartitionConfig()

    assert config.grid_type == "isea4h"
    assert config.grid_level == 6
    assert config.partition_chunk_size == 0


def test_carbon_partition_job_defaults_to_automatic_chunking(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["carbon_partition_job"])

    assert parse_args().partition_chunk_size == 0


def test_carbon_partition_job_auto_backend_selects_ray_when_address_is_set():
    assert _resolve_backend("auto", "") == "process"
    assert _resolve_backend("auto", "auto") == "ray"
    assert _resolve_backend("thread", "auto") == "thread"


def test_carbon_partition_job_resolves_ray_worker_count(monkeypatch):
    monkeypatch.delenv("CUBE_CARBON_RAY_PARALLELISM", raising=False)
    monkeypatch.setattr("cube_split.jobs.carbon_partition_job.os.cpu_count", lambda: 2)
    monkeypatch.setitem(sys.modules, "ray", SimpleNamespace(is_initialized=lambda: False))

    assert _resolve_worker_count(partition_workers=0, ray_parallelism=0, backend="ray") == 4
    assert _resolve_worker_count(partition_workers=2, ray_parallelism=0, backend="ray") == 2
    assert _resolve_worker_count(partition_workers=2, ray_parallelism=6, backend="ray") == 6
    assert _resolve_worker_count(partition_workers=0, ray_parallelism=6, backend="process") == 1


def test_carbon_partition_job_default_parallelism_prefers_env_override(monkeypatch):
    monkeypatch.setenv("CUBE_CARBON_RAY_PARALLELISM", "12")

    assert _default_ray_parallelism() == 12
    assert _resolve_worker_count(partition_workers=0, ray_parallelism=0, backend="ray") == 12
    assert _resolve_worker_count(partition_workers=0, ray_parallelism=6, backend="ray") == 6
    assert _resolve_worker_count(partition_workers=0, ray_parallelism=0, backend="process") == 1

    monkeypatch.setenv("CUBE_CARBON_RAY_PARALLELISM", "0")
    monkeypatch.setattr("cube_split.jobs.carbon_partition_job.os.cpu_count", lambda: 2)
    monkeypatch.setitem(sys.modules, "ray", SimpleNamespace(is_initialized=lambda: False))
    assert _default_ray_parallelism() == 1

    monkeypatch.setenv("CUBE_CARBON_RAY_PARALLELISM", "not-a-number")
    assert _default_ray_parallelism() == 4


def test_carbon_partition_job_default_parallelism_uses_cluster_cpus(monkeypatch):
    monkeypatch.delenv("CUBE_CARBON_RAY_PARALLELISM", raising=False)
    fake_ray = SimpleNamespace(is_initialized=lambda: True, cluster_resources=lambda: {"CPU": 24.0})
    monkeypatch.setitem(sys.modules, "ray", fake_ray)

    assert _default_ray_parallelism() == 24


def test_carbon_partition_job_accepts_only_frozen_sdk_time_granularities(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["carbon_partition_job", "--time-granularity", "second"])

    assert parse_args().time_granularity == "second"

    monkeypatch.setattr(sys, "argv", ["carbon_partition_job", "--time-granularity", "year"])
    with pytest.raises(SystemExit):
        parse_args()


def test_run_carbon_partition_writes_standard_run_dir(tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "ray_output" / "carbon"
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
            }
        )
        + "\n",
        encoding="utf-8",
    )

    summary = run_carbon_partition(
        SimpleNamespace(
            input_dir=str(input_dir),
            output_dir=str(output_dir),
            grid_type="isea4h",
            grid_level=6,
            time_granularity="day",
            product_type="xco2",
            max_observations=0,
            partition_chunk_size=1000,
            partition_workers=1,
            partition_backend="process",
            ray_address="",
            ray_parallelism=0,
        )
    )

    run_dir = Path(summary["run_dir"])
    rows_path = run_dir / "carbon_observation_rows.jsonl"
    report_path = run_dir / "job_report.json"
    row = json.loads(rows_path.read_text(encoding="utf-8"))
    report = json.loads(report_path.read_text(encoding="utf-8"))

    assert summary["status"] == "completed"
    assert summary["data_type"] == "carbon"
    assert summary["grid_type"] == "isea4h"
    assert summary["grid_level"] == 6
    assert summary["rows"] == 1
    assert report["rows_path"] == str(rows_path.resolve())
    assert row["grid_type"] == "isea4h"
    assert row["grid_level"] == 6
    assert row["st_code"].startswith("i4h:6:")


def test_run_carbon_partition_normalizes_tansat_alias_in_rows_and_report(tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
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
                "quality_flag": "0",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    summary = run_carbon_partition(
        SimpleNamespace(
            input_dir=str(input_dir), output_dir=str(output_dir), grid_type="isea4h", grid_level=6,
            time_granularity="day", product_type="tansat_xco2", max_observations=0,
            partition_chunk_size=1000, partition_workers=1, partition_backend="process",
            ray_address="", ray_parallelism=0,
        )
    )

    run_dir = Path(summary["run_dir"])
    row = json.loads((run_dir / "carbon_observation_rows.jsonl").read_text(encoding="utf-8"))
    report = json.loads((run_dir / "job_report.json").read_text(encoding="utf-8"))
    assert row["product_type"] == "tansat"
    assert summary["product_type"] == "tansat"
    assert report["product_type"] == "tansat"


def test_run_carbon_partition_writes_both_tansat_sif_measurements(tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    source = input_dir / "20260725_192050_TanSat_SIF_L2_20170209_ACGS_ND_V01.nc4"
    with Dataset(source, mode="w") as dataset:
        dataset.createDimension("sounding_dim", 1)
        dataset.createDimension("vertex_dim", 4)
        latitude = dataset.createVariable("Latitude", "f4", ("sounding_dim",))
        longitude = dataset.createVariable("Longitude", "f4", ("sounding_dim",))
        time_var = dataset.createVariable("Time", "f8", ("sounding_dim",))
        sif_758 = dataset.createVariable("SIF_758nm", "f4", ("sounding_dim",))
        sif_771 = dataset.createVariable("SIF_771nm", "f4", ("sounding_dim",))
        lat_vertex = dataset.createVariable("LatVertex", "f4", ("vertex_dim", "sounding_dim"))
        lon_vertex = dataset.createVariable("LongVertex", "f4", ("vertex_dim", "sounding_dim"))
        latitude[:] = [39.9]
        longitude[:] = [116.4]
        time_var[:] = [0]
        sif_758[:] = [1.25]
        sif_771[:] = [0.25]
        lat_vertex[:] = [[39.8], [39.8], [40.0], [40.0]]
        lon_vertex[:] = [[116.3], [116.5], [116.5], [116.3]]

    summary = run_carbon_partition(
        SimpleNamespace(
            input_dir=str(input_dir), output_dir=str(output_dir), grid_type="isea4h", grid_level=6,
            time_granularity="day", product_type="sif", max_observations=0,
            partition_chunk_size=1000, partition_workers=1, partition_backend="process",
            ray_address="", ray_parallelism=0,
        )
    )

    rows = [json.loads(line) for line in (Path(summary["run_dir"]) / "carbon_observation_rows.jsonl").read_text(encoding="utf-8").splitlines()]
    assert summary["rows"] == 2
    assert {item["observation_id"] for item in rows} == {"20170209-00000000:SIF_758nm", "20170209-00000000:SIF_771nm"}
    assert {json.loads(item["metadata_json"])["measurement_name"] for item in rows} == {"SIF_758nm", "SIF_771nm"}
