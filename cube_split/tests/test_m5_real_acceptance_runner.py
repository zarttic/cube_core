from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from cube_split.scripts import run_m5_real_acceptance as runner


def _request(case_id: str, *, grid_type: str, data_type: str = "optical", datasets: int = 1) -> dict:
    level = {"geohash": 7, "mgrs": 2, "isea4h": 1}[grid_type]
    method = "entity" if grid_type == "isea4h" else "logical"
    return {
        "batch_id": f"m5-{case_id}",
        "grid_type": grid_type,
        "requested_grid_level": level,
        "partition_method": method,
        "cover_mode": "minimal" if grid_type == "mgrs" else "intersect",
        "time_granularity": "day",
        "max_cells_per_asset": 20,
        "datasets": [
            {
                "dataset_id": f"{case_id}-{number}",
                "dataset_code": f"M5-{number}",
                "dataset_title": f"M5 {case_id} {number}",
                "data_type": data_type,
                "assets": [
                    {
                        "source_asset_id": f"asset-{number}",
                        **(
                            {
                                "source_uri": f"s3://cube/cube/source/carbon/{case_id}-{number}.nc4",
                                "source_kind": "raw",
                                "source_format": "netcdf",
                            }
                            if data_type == "carbon"
                            else {"cog_uri": f"s3://cube/cube/source/{case_id}-{number}.tif"}
                        ),
                        "checksum": "a" * 64,
                        "bbox": [100.0, 20.0, 101.0, 21.0],
                        "crs": "EPSG:4326",
                        "time_start": "2026-07-01T00:00:00Z",
                        "time_end": "2026-07-01T00:01:00Z",
                    }
                ],
                "bands": [
                    {
                        "source_asset_id": f"asset-{number}",
                        "band_code": "B01",
                        "band_name": "B01",
                        "band_type": "variable" if data_type == "carbon" else "spectral",
                        "display_order": 0,
                    }
                ],
            }
            for number in range(datasets)
        ],
    }


def _manifest_payloads() -> tuple[dict, dict]:
    requests = {
        "geohash_logical_single_dataset": _request("geohash_logical_single_dataset", grid_type="geohash"),
        "mgrs_cross_zone_boundary_logical": _request("mgrs_cross_zone_boundary_logical", grid_type="mgrs"),
        "isea4h_low_resolution_entity": _request("isea4h_low_resolution_entity", grid_type="isea4h"),
        "batch_two_datasets_sibling_partial_failure": _request(
            "batch_two_datasets_sibling_partial_failure", grid_type="geohash", datasets=2
        ),
        "quality_fail_complete_exports": _request("quality_fail_complete_exports", grid_type="geohash", data_type="carbon"),
        "pass_warn_publish_withdraw_reconciliation": _request(
            "pass_warn_publish_withdraw_reconciliation", grid_type="geohash", datasets=2
        ),
    }
    requests["mgrs_cross_zone_boundary_logical"]["datasets"][0]["attributes"] = {
        "m5_expected_mgrs_domains": ["utm-31n", "utm-32n"]
    }
    partial = requests["batch_two_datasets_sibling_partial_failure"]["datasets"][1]
    partial["data_type"] = "radar"
    partial["bands"][0].update({"band_code": "VV", "band_name": "VV", "band_type": "polarization"})
    requests["batch_two_datasets_sibling_partial_failure"]["datasets"][0]["partition"] = {
        "grid_type": "geohash", "requested_grid_level": 7, "partition_method": "logical"
    }
    partial["partition"] = {"grid_type": "isea4h", "requested_grid_level": 1, "partition_method": "entity"}
    requests["quality_fail_complete_exports"]["datasets"][0]["partition"] = {"max_observations": 100}
    requests["pass_warn_publish_withdraw_reconciliation"]["datasets"][1]["assets"][0]["attributes"] = {
        "quality_metadata_defects": [{"error_code": "m5_warn", "message": "reviewed loader warning"}]
    }
    input_payload = {"cases": [{"id": case_id, "request": requests[case_id]} for case_id in runner.CASE_IDS]}
    defects = {
        case_id: requests[case_id]
        for case_id in sorted(runner.DEFECT_CASE_IDS)
    }
    defect_payload = {"cases": [{"id": case_id, "request": defects[case_id]} for case_id in sorted(defects)]}
    return input_payload, defect_payload


class _Minio:
    def __init__(self) -> None:
        self.keys: list[str] = []

    def bucket_exists(self, bucket: str) -> bool:
        return bucket == "cube"

    def stat_object(self, bucket: str, key: str):
        assert bucket == "cube"
        self.keys.append(key)
        return SimpleNamespace(size=1024)


@pytest.fixture
def manifests(tmp_path: Path) -> tuple[Path, Path]:
    source, defects = _manifest_payloads()
    input_path = tmp_path / "m5-input.json"
    defect_path = tmp_path / "m5-defects.json"
    input_path.write_text(json.dumps(source), encoding="utf-8")
    defect_path.write_text(json.dumps(defects), encoding="utf-8")
    return input_path, defect_path


@pytest.fixture
def environment() -> dict[str, str]:
    return {
        "CUBE_WEB_POSTGRES_DSN": "postgresql://redacted",
        "CUBE_WEB_RAY_ADDRESS": "ray.example:6379",
        "CUBE_WEB_MINIO_ENDPOINT": "minio.example:9000",
        "CUBE_WEB_MINIO_ACCESS_KEY": "test-key",
        "CUBE_WEB_MINIO_SECRET_KEY": "test-secret",
        "CUBE_WEB_MINIO_BUCKET": "cube",
    }


def test_real_acceptance_case_ids_are_immutable() -> None:
    assert [case.id for case in runner.REAL_ACCEPTANCE_CASES] == [
        "geohash_logical_single_dataset",
        "mgrs_cross_zone_boundary_logical",
        "isea4h_low_resolution_entity",
        "batch_two_datasets_sibling_partial_failure",
        "quality_fail_complete_exports",
        "pass_warn_publish_withdraw_reconciliation",
    ]
    assert len(runner.REAL_ACCEPTANCE_CASES) == 6


def test_manifest_validation_requires_exact_cases_strict_requests_and_stattable_sources(
    manifests: tuple[Path, Path], environment: dict[str, str]
) -> None:
    result = runner.validate_real_manifests(
        input_manifest=manifests[0], defect_manifest=manifests[1], environment=environment, minio_client=_Minio()
    )
    assert result["case_ids"] == list(runner.CASE_IDS)
    assert len(result["sources"]) == 8
    assert all(value.startswith("s3://cube/<source:") for value in result["sources"])
    for request in result["input_requests"].values():
        assert "grid_level" not in request and "grid_level_mode" not in request


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda payload: payload["cases"].pop(), "case IDs"),
        (lambda payload: payload["cases"][0]["request"].update({"grid_level": 7}), "StrictPartitionRequest"),
        (lambda payload: payload["cases"][0]["request"].update({"cog_workers": 1}), "forbidden"),
    ],
)
def test_manifest_validation_rejects_missing_extra_or_retired_fields(
    manifests: tuple[Path, Path], environment: dict[str, str], mutate, message: str
) -> None:
    payload = json.loads(manifests[0].read_text(encoding="utf-8"))
    mutate(payload)
    manifests[0].write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(runner.ManifestError, match=message):
        runner.validate_real_manifests(
            input_manifest=manifests[0], defect_manifest=manifests[1], environment=environment, minio_client=_Minio()
        )


def test_gate_invokes_only_the_six_real_marker_tests_and_requires_passing_summary(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, manifests: tuple[Path, Path], environment: dict[str, str]
) -> None:
    monkeypatch.setattr(
        runner,
        "validate_real_manifests",
        lambda **_: {"input_manifest_digest": "a" * 64, "defect_manifest_digest": "b" * 64, "sources": ["s3://cube/<source:1>"]},
    )

    def fake_run(command, *, env, text, capture_output, check):
        assert command == runner._gate_command()
        assert text and capture_output and not check
        scenario_path = Path(env["CUBE_M5_REAL_SCENARIO_SUMMARY"])
        scenario_path.write_text(
            json.dumps({"scenarios": [{"id": case_id, "status": "passed"} for case_id in runner.CASE_IDS]}), encoding="utf-8"
        )
        return subprocess.CompletedProcess(command, 0, "collected 6 items\n6 passed", "")

    monkeypatch.setattr(runner.subprocess, "run", fake_run)
    summary_path = tmp_path / "summary.json"
    assert runner.run_gate(summary_path=summary_path, input_manifest=manifests[0], defect_manifest=manifests[1], environment=environment) == 0
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["m5_gate_status"] == "PASS"
    assert (summary["scenario_count"], summary["passed"], summary["failed"], summary["skipped"], summary["deselected"]) == (6, 6, 0, 0, 0)


def test_gate_rejects_skips_and_nonpassing_scenarios(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, manifests, environment) -> None:
    monkeypatch.setattr(
        runner,
        "validate_real_manifests",
        lambda **_: {"input_manifest_digest": "a" * 64, "defect_manifest_digest": "b" * 64, "sources": []},
    )
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 0, "1 skipped, 5 passed", ""),
    )
    with pytest.raises(RuntimeError, match="forbidden state"):
        runner.run_gate(
            summary_path=tmp_path / "summary.json", input_manifest=manifests[0], defect_manifest=manifests[1], environment=environment
        )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload["cases"][0]["request"]["datasets"][0]["assets"][0].update({"cog_uri": "s3://cube/key.tif?X-Amz-Credential=leak"}),
        lambda payload: payload["cases"][0]["request"]["datasets"][0]["assets"][0].update({"cog_uri": "s3://cube/cube/source/carbon/input.nc4"}),
        lambda payload: payload["cases"][0]["request"]["datasets"][0].update({"attributes": {"note": "synthetic source"}}),
    ],
)
def test_manifest_validation_rejects_sensitive_uri_and_synthetic_values(manifests, environment, mutate) -> None:
    payload = json.loads(manifests[0].read_text(encoding="utf-8"))
    mutate(payload)
    manifests[0].write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(runner.ManifestError):
        runner.validate_real_manifests(
            input_manifest=manifests[0], defect_manifest=manifests[1], environment=environment, minio_client=_Minio()
        )


def test_runner_redacts_dsn_sensitive_mapping_and_absolute_paths() -> None:
    payload = runner.redact({"CUBE_WEB_POSTGRES_DSN": "postgresql://user:password@host/db", "path": "/tmp/private/input.json"})
    assert payload["CUBE_WEB_POSTGRES_DSN"] == "<redacted>"
    assert "password" not in str(payload)
    assert "<absolute-path>" in payload["path"]
    assert runner.redact("s3://cube/<source:0123456789abcdef>") == "s3://cube/<source:0123456789abcdef>"


def test_gate_rejects_any_collection_count_other_than_six(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, manifests, environment) -> None:
    monkeypatch.setattr(
        runner,
        "validate_real_manifests",
        lambda **_: {"input_manifest_digest": "a" * 64, "defect_manifest_digest": "b" * 64, "sources": []},
    )

    def fake_run(command, *, env, **kwargs):
        Path(env["CUBE_M5_REAL_SCENARIO_SUMMARY"]).write_text(
            json.dumps({"scenarios": [{"id": case_id, "status": "passed"} for case_id in runner.CASE_IDS]}), encoding="utf-8"
        )
        return subprocess.CompletedProcess(command, 0, "collected 7 items\n7 passed", "")

    monkeypatch.setattr(runner.subprocess, "run", fake_run)
    with pytest.raises(RuntimeError, match="exactly six"):
        runner.run_gate(summary_path=tmp_path / "summary.json", input_manifest=manifests[0], defect_manifest=manifests[1], environment=environment)
