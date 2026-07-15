from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest


def _load_smoke_module() -> ModuleType:
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_all_partition_flows_smoke.py"
    spec = importlib.util.spec_from_file_location("run_all_partition_flows_smoke", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load smoke script: {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _smoke_python_executable() -> str:
    configured = os.environ.get("CUBE_PARTITION_E2E_PYTHON")
    if configured:
        return configured
    return shutil.which("python3.11") or sys.executable


def test_smoke_result_summary_includes_quality_metadata(tmp_path: Path) -> None:
    smoke = _load_smoke_module()
    rows_path = tmp_path / "index_rows.jsonl"
    rows_path.write_text(json.dumps({"asset_path": "s3://cube/cog/a.tif"}) + "\n", encoding="utf-8")

    item = smoke._validate_result(
        "optical:geohash",
        "geohash",
        "demo",
        {
            "rows": 1,
            "rows_path": str(rows_path),
            "execution_engine": "ray",
            "grid_type": "geohash",
            "grid_level": 5,
            "ingest_enabled": True,
            "metadata_backend": "postgres",
            "asset_storage_backend": "minio",
            "ingest_stats": {"rows": 1},
            "quality_status": "PASS",
            "quality_report_id": "quality-smoke-report",
        },
        keep_quality=True,
        require_quality=True,
    )

    assert item["status"] == "pass"
    assert item["quality_status"] == "PASS"
    assert item["quality_report_id"] == "quality-smoke-report"


def test_smoke_acceptance_cases_are_fixed() -> None:
    smoke = _load_smoke_module()

    assert [case.case_id for case in smoke.ACCEPTANCE_CASES] == [
        "optical_geohash",
        "optical_mgrs",
        "optical_isea4h_level1",
        "radar_geohash",
        "product_geohash",
        "carbon_satellite",
    ]
    assert smoke.ACCEPTANCE_CASES[1].grid_type == "mgrs"
    assert smoke.ACCEPTANCE_CASES[2].grid_level == 1
    assert smoke.ACCEPTANCE_CASES[3].data_type == "radar"


@pytest.mark.e2e
def test_run_all_partition_flows_smoke(tmp_path: Path) -> None:
    if os.environ.get("CUBE_RUN_PARTITION_E2E_SMOKE") != "1":
        pytest.skip("set CUBE_RUN_PARTITION_E2E_SMOKE=1 to run the Ray/MinIO/Postgres partition smoke")

    repo_root = Path(__file__).resolve().parents[2]
    work_dir = tmp_path / "partition-flow-smoke"
    summary_path = tmp_path / "partition-flow-smoke-summary.json"
    env = dict(os.environ)
    env["PYTHONPATH"] = "cube_encoder:cube_split:cube_web"

    subprocess.run(
        [
            _smoke_python_executable(),
            "cube_split/scripts/run_all_partition_flows_smoke.py",
            "--work-dir",
            str(work_dir),
            "--summary-path",
            str(summary_path),
            "--mode",
            "demo",
            "--ray-parallelism",
            "2",
            "--chunk-size",
            "1",
            "--max-cells-per-asset",
            "50",
            "--keep-quality",
        ],
        cwd=repo_root,
        env=env,
        check=True,
    )

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    ids = {item["id"] for item in summary["results"]}
    assert {
        "optical_geohash",
        "optical_mgrs",
        "optical_isea4h_level1",
        "radar_geohash",
        "product_geohash",
        "carbon_satellite",
        "quality_checks",
        "aoi_readback",
    } <= ids
    assert summary["status"] == "pass"
    assert all(item["status"] == "pass" for item in summary["results"])
    quality_results = [item for item in summary["results"] if item["id"] in {"optical_geohash", "product_geohash", "carbon_satellite"}]
    assert quality_results
    assert all(item["quality_status"] for item in quality_results)
    assert all(item["quality_report_id"] for item in quality_results)
