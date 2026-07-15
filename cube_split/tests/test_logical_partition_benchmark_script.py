from __future__ import annotations

import importlib.util
import json
from argparse import Namespace
from pathlib import Path


def _load_script_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_logical_partition_benchmark.py"
    spec = importlib.util.spec_from_file_location("run_logical_partition_benchmark", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_logical_benchmark_default_manifest_uses_shandong_minio_sources(tmp_path: Path):
    module = _load_script_module()
    manifest_path = module._write_default_manifest(tmp_path / "manifest.jsonl")

    rows = [json.loads(line) for line in manifest_path.read_text(encoding="utf-8").splitlines()]

    assert len(rows) == 2
    assert {row["band"] for row in rows} == {"sr_band2", "sr_band3"}
    assert {row["scene_id"] for row in rows} == {"Shandong_mosaic_2020Q3"}
    assert all(row["source_uri"].startswith("s3://cube/cube/source/optocal/") for row in rows)
    assert all(row["sensor"] == "optical_mosaic" for row in rows)
    assert all(row["resolution"] == 30 for row in rows)


def test_logical_benchmark_case_matrix_is_limited_to_logical_grids():
    module = _load_script_module()

    cases = module._grid_cases(Namespace(grid_types="geohash,mgrs", geohash_level=5, mgrs_level=3))

    assert cases == [("geohash", 5), ("mgrs", 3)]


def test_logical_benchmark_redacts_sensitive_cli_values():
    module = _load_script_module()

    command = module._redacted_argv(
        [
            "run_logical_partition_benchmark.py",
            "--minio-access-key",
            "access",
            "--minio-secret-key=secret",
            "--ray-address",
            "10.3.100.182:6379",
        ]
    )

    tokens = command.split()
    assert "access" not in tokens
    assert "secret" not in tokens
    assert "--minio-secret-key=***" in command
    assert "10.3.100.182:6379" in command
