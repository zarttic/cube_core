from __future__ import annotations

import json
import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from cube_web.acceptance.m6_mock_data import (
    build_mock_manifest,
    classify_source_key,
    collect_source_snapshot,
    ensure_tmp_path,
    source_key,
)


class _Minio:
    def __init__(self, keys: list[str], *, empty_key: str | None = None) -> None:
        self.keys = keys
        self.empty_key = empty_key
        self.list_calls: list[tuple[str, str, bool]] = []
        self.stat_calls: list[tuple[str, str]] = []

    def list_objects(self, bucket: str, *, prefix: str, recursive: bool):
        self.list_calls.append((bucket, prefix, recursive))
        return [SimpleNamespace(object_name=key) for key in self.keys]

    def stat_object(self, bucket: str, key: str):
        self.stat_calls.append((bucket, key))
        return SimpleNamespace(
            size=0 if key == self.empty_key else 2048,
            etag=f"etag-{key}",
            last_modified=datetime(2026, 6, 1, tzinfo=timezone.utc),
        )


def _keys() -> list[str]:
    return [
        "cube/source/optocal/landsat/red.tif",
        "cube/source/optocal/landsat/nir.tif",
        "cube/source/radar/sentinel-1/vv.tif",
        "cube/source/product/ecology/value.tif",
        "cube/source/carbon/orbit.nc",
        "partition/output/not-source.tif",
    ]


def test_source_classification_and_uri_validation() -> None:
    assert classify_source_key("cube/source/optocal/a.tif") == "optical"
    assert classify_source_key("cube/source/radar/a.tif") == "radar"
    assert classify_source_key("cube/source/product/a.tif") == "product"
    assert classify_source_key("cube/source/carbon/a.nc") == "carbon"
    assert classify_source_key("cube/source/product/not-carbon.nc") == "product"
    assert classify_source_key("cube/source/radar/not-carbon.h5") == "radar"
    assert classify_source_key("cube/source/vendor/unclassified.nc") is None
    assert classify_source_key("partition/a.tif") is None
    assert source_key("s3://cube/cube/source/product/a%20b.tif", bucket="cube") == "cube/source/product/a b.tif"
    with pytest.raises(ValueError):
        source_key("s3://other/cube/source/product/a.tif", bucket="cube")


def test_runner_snapshot_is_read_only_stat_checked_and_contains_no_secrets() -> None:
    client = _Minio(_keys())
    snapshot = collect_source_snapshot(client, bucket="cube")
    manifest = build_mock_manifest(snapshot)

    assert client.list_calls == [("cube", "cube/source/", True)]
    assert len(client.stat_calls) == 5
    assert {row["source_uri"] for rows in snapshot.values() for row in rows} == {
        f"s3://cube/{key}" for key in _keys()[:5]
    }
    serialized = json.dumps(manifest)
    for forbidden in ("access_key", "secret_key", "password", "endpoint", "dsn"):
        assert forbidden not in serialized.casefold()


def test_runner_fails_closed_for_missing_type_or_empty_object() -> None:
    without_radar = [key for key in _keys() if "/radar/" not in key]
    with pytest.raises(RuntimeError, match="radar"):
        collect_source_snapshot(_Minio(without_radar), bucket="cube")
    with pytest.raises(RuntimeError, match="empty"):
        collect_source_snapshot(
            _Minio(_keys(), empty_key="cube/source/carbon/orbit.nc"),
            bucket="cube",
        )


def test_explicit_source_uri_supports_unclassified_real_layout() -> None:
    keys = [key for key in _keys() if "/radar/" not in key]
    keys.append("cube/source/vendor/s1_scene.tif")
    client = _Minio(keys)
    snapshot = collect_source_snapshot(
        client,
        bucket="cube",
        explicit_uris={"radar": ["s3://cube/cube/source/vendor/s1_scene.tif"]},
    )
    assert snapshot["radar"][0]["source_uri"].endswith("/vendor/s1_scene.tif")


def test_mock_paths_are_restricted_to_tmp() -> None:
    assert ensure_tmp_path(Path("/tmp/m6/manifest.json")) == Path("/tmp/m6/manifest.json")
    with pytest.raises(ValueError, match="/tmp"):
        ensure_tmp_path(Path("/var/lib/cube/manifest.json"))


def test_prepare_runner_does_not_print_runtime_credentials(monkeypatch, capsys, tmp_path: Path) -> None:
    script = Path(__file__).parents[1] / "scripts" / "run_m6_mock_acceptance.py"
    spec = importlib.util.spec_from_file_location("m6_mock_runner_under_test", script)
    assert spec is not None and spec.loader is not None
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)

    settings = SimpleNamespace(
        endpoint="sentinel-endpoint:9000",
        access_key="sentinel-access-key",
        secret_key="sentinel-secret-key",
        bucket="cube",
        secure=False,
    )
    monkeypatch.setattr(runner.runtime_config, "minio_settings", lambda: settings)
    monkeypatch.setitem(sys.modules, "minio", SimpleNamespace(Minio=lambda *_args, **_kwargs: _Minio(_keys())))
    output = tmp_path / "manifest.json"

    assert runner.main(["--prepare-only", "--output", str(output)]) == 0
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "sentinel-endpoint" not in combined
    assert "sentinel-access-key" not in combined
    assert "sentinel-secret-key" not in combined
    assert output.exists()
