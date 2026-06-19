from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from cube_split.jobs.carbon_partition_job import _resolve_backend, _resolve_worker_count, run_carbon_partition
from cube_split.partition.carbon import CarbonPartitionConfig


def test_carbon_partition_config_defaults_to_isea4h_level5():
    config = CarbonPartitionConfig()

    assert config.grid_type == "isea4h"
    assert config.grid_level == 5


def test_carbon_partition_job_auto_backend_selects_ray_when_address_is_set():
    assert _resolve_backend("auto", "") == "process"
    assert _resolve_backend("auto", "auto") == "ray"
    assert _resolve_backend("thread", "auto") == "thread"


def test_carbon_partition_job_resolves_ray_worker_count():
    assert _resolve_worker_count(partition_workers=0, ray_parallelism=0, backend="ray") == 4
    assert _resolve_worker_count(partition_workers=2, ray_parallelism=0, backend="ray") == 2
    assert _resolve_worker_count(partition_workers=2, ray_parallelism=6, backend="ray") == 6
    assert _resolve_worker_count(partition_workers=0, ray_parallelism=6, backend="process") == 1


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
            grid_level=5,
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
    assert summary["grid_level"] == 5
    assert summary["rows"] == 1
    assert report["rows_path"] == str(rows_path.resolve())
    assert row["grid_type"] == "isea4h"
    assert row["grid_level"] == 5
    assert row["st_code"].startswith("hx:5:")
