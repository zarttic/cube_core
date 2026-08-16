from __future__ import annotations

import csv
import importlib.util
import json
import sys
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).parents[1] / "scripts" / "aggregate_single_well_performance.py"


def load_aggregate_script():
    spec = importlib.util.spec_from_file_location("single_well_performance_aggregate", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("aggregate script could not be imported")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def write_run(
    root: Path,
    *,
    cold_start: bool = True,
    checksum_verified: bool = True,
    threshold_scope: str = "both",
    worker_timing: bool = True,
    case_ids: list[str] | None = None,
) -> Path:
    run_dir = root / "round_1" / "run"
    run_dir.mkdir(parents=True)
    case_ids = case_ids or ["optical_geohash"]
    summary_fields = [
        "case_id", "data_type", "grid_type", "grid_level", "partition_execution_sec",
        "end_to_end_sec", "ray_job_sec", "queue_wait_sec", "final_status", "attempt_status",
        "ray_job_status", "ray_job_available", "threshold_scope", "passed",
    ]
    with (run_dir / "summary.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=summary_fields)
        writer.writeheader()
        for case_id in case_ids:
            data_type, grid_type = case_id.split("_", 1)
            writer.writerow({
                "case_id": case_id,
                "data_type": data_type,
                "grid_type": grid_type,
                "grid_level": 4,
                "partition_execution_sec": 2.0,
                "end_to_end_sec": 4.0,
                "ray_job_sec": 3.0,
                "queue_wait_sec": 0.1,
                "final_status": "completed",
                "attempt_status": "succeeded",
                "ray_job_status": "SUCCEEDED",
                "ray_job_available": "true",
                "threshold_scope": threshold_scope,
                "passed": "true",
            })
    raw_cases = [
        {
            "case_id": case_id,
            "cold_start_gate": (
                {"enabled": True, "observations": [{"active_partition_job_count": 0}]}
                if cold_start else {"enabled": False, "observations": []}
            ),
        }
        for case_id in case_ids
    ]
    (run_dir / "raw_cases.json").write_text(json.dumps(raw_cases), encoding="utf-8")
    phase_fields = [
        "case_id", "path", "scope", "phase", "phase_count", "phase_elapsed_sec",
        "elapsed_sec", "started_at", "finished_at",
    ]
    with (run_dir / "phase_timings.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=phase_fields)
        writer.writeheader()
        for case_id in case_ids:
            if worker_timing:
                writer.writerow({
                    "case_id": case_id,
                    "path": "datasets[0]",
                    "scope": "raster_worker",
                    "phase": "grid.cover",
                    "phase_count": 1,
                    "phase_elapsed_sec": 0.5,
                    "elapsed_sec": 1.0,
                    "started_at": "2026-01-01T00:00:00Z",
                    "finished_at": "2026-01-01T00:00:01Z",
                })
    metadata = {
        "cold_start_gate": cold_start,
        "source_verification": {
            "checksum_skipped": not checksum_verified,
            "objects": [{"source_uri": "s3://cube/example.tif", "checksum_verified": checksum_verified}],
        },
    }
    (run_dir / "run_metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    return run_dir


def test_aggregate_rejects_missing_worker_timing(tmp_path: Path) -> None:
    module = load_aggregate_script()
    root = tmp_path / "root"
    run_dir = write_run(root, worker_timing=False)

    rows = module.load_run(run_dir, root)

    assert rows[0]["valid_sample"] is False
    assert rows[0]["excluded_reason"] == "missing_worker_timing"


def test_aggregate_rejects_warm_start_and_unverified_source(tmp_path: Path) -> None:
    module = load_aggregate_script()
    root = tmp_path / "root"
    warm_run = write_run(root / "warm", cold_start=False)
    checksum_run = write_run(root / "checksum", checksum_verified=False)

    assert module.load_run(warm_run, root / "warm")[0]["excluded_reason"] == "invalid_cold_start_gate"
    assert module.load_run(checksum_run, root / "checksum")[0]["excluded_reason"] == "source_checksum_not_verified"


def test_aggregate_requires_both_threshold_scope(tmp_path: Path) -> None:
    module = load_aggregate_script()
    root = tmp_path / "root"
    write_run(root, threshold_scope="partition")

    with pytest.raises(RuntimeError, match="threshold_scope=both"):
        module.main(["--root", str(root)])


def test_aggregate_requires_complete_fixed_case_set(tmp_path: Path) -> None:
    module = load_aggregate_script()
    root = tmp_path / "root"
    write_run(root, case_ids=["optical_geohash"])

    with pytest.raises(RuntimeError, match="exactly the 10 fixed cases"):
        module.main(["--root", str(root)])


def test_public_overview_is_single_scene_oriented_and_metadata_has_no_calendar_fields() -> None:
    module = load_aggregate_script()

    overview = module.scene_overview_rows(
        [{
            "data_type": "optical",
            "grid_type": "geohash",
            "round_count": 5,
            "normal_mean_end_to_end_sec": 7.5,
            "normal_pass_rate": 1.0,
        }],
        {"input_profile": {
            "optical": {
                "assets": [{"width": 7595, "height": 6337, "raster_band_count": 4}],
            },
        }},
    )

    assert overview[0]["data_type"] == "光学遥感"
    assert overview[0]["grid_type"] == "逻辑格网（Geohash）"
    assert overview[0]["scene_spec"] == "7595x6337/4 band"
    assert overview[0]["conclusion"] == "通过"

    sanitized = module.sanitize_minio_metadata({
        "source_root": "/tmp/dated-root",
        "started_at": "2026-01-01T00:00:00Z",
        "samples": {"source_uri": "s3://cube/source.tif", "size_bytes": 12},
    })

    assert sanitized == {"samples": {"size_bytes": 12}}
