from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from cube_web.services import partition_dataset_runner as runner_module
from cube_web.services.dataset_management import DatasetManagementService, InMemoryDatasetManagementRepository
from cube_web.services.partition_contracts import StrictPartitionRequest
from cube_web.services.partition_service import PartitionService
from cube_web.services.partition_workflow import PartitionWorkflowService


class _NoSuchKey(Exception):
    code = "NoSuchKey"


class _FakeMinio:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def stat_object(self, bucket: str, key: str) -> Any:
        if key.endswith("nope.tif"):
            raise _NoSuchKey()
        if key.endswith("empty.tif"):
            return SimpleNamespace(size=0)
        return SimpleNamespace(size=1024)


def _payload(uri: str) -> list[dict[str, Any]]:
    return [{
        "dataset": {"assets": [{"source_asset_id": "a1", "source_uri": uri, "cog_uri": uri}]},
    }]


def test_preflight_rejects_missing_source(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runner_module.runtime_config,
        "minio_settings",
        lambda **_: SimpleNamespace(endpoint="minio:9000", access_key="k", secret_key="s", bucket="cube", secure=False),
    )
    monkeypatch.setattr("minio.Minio", _FakeMinio)
    with pytest.raises(runner_module.SourceObjectMissingError, match="source_missing:s3://cube/nope.tif"):
        runner_module.NormalizedPartitionDatasetRunner._verify_assets_exist(_payload("s3://cube/nope.tif"))


def test_preflight_rejects_empty_source(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runner_module.runtime_config,
        "minio_settings",
        lambda **_: SimpleNamespace(endpoint="minio:9000", access_key="k", secret_key="s", bucket="cube", secure=False),
    )
    monkeypatch.setattr("minio.Minio", _FakeMinio)
    with pytest.raises(runner_module.SourceObjectMissingError, match="source_empty:s3://cube/empty.tif"):
        runner_module.NormalizedPartitionDatasetRunner._verify_assets_exist(_payload("s3://cube/empty.tif"))


def test_preflight_passes_when_sources_exist(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runner_module.runtime_config,
        "minio_settings",
        lambda **_: SimpleNamespace(endpoint="minio:9000", access_key="k", secret_key="s", bucket="cube", secure=False),
    )
    monkeypatch.setattr("minio.Minio", _FakeMinio)
    runner_module.NormalizedPartitionDatasetRunner._verify_assets_exist(_payload("s3://cube/good.tif"))


class _FakeSceneRepository:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.normalized_rows: list[dict[str, Any]] = []

    def read_scene_assets_by_ids(self, asset_ids: list[str]) -> list[dict[str, Any]]:
        return self.rows

    def read_partition_dataset_asset_uris(self, dataset_id: str, asset_ids: list[str]) -> list[dict[str, Any]]:
        return self.normalized_rows


def _strict_request(source_uri: str) -> StrictPartitionRequest:
    return StrictPartitionRequest.model_validate({
        "batch_id": "batch-refresh",
        "grid_type": "geohash",
        "requested_grid_level": 1,
        "partition_method": "logical",
        "datasets": [{
            "dataset_id": "dataset-a",
            "dataset_code": "DS-A",
            "dataset_title": "Optical A",
            "data_type": "optical",
            "assets": [{
                "source_asset_id": "a1", "source_uri": None, "cog_uri": source_uri,
                "source_kind": "cog", "source_format": "cog", "checksum": "0" * 64,
                "bbox": [116.0, 35.0, 119.0, 37.0], "crs": "EPSG:4326",
                "time_start": "2020-01-01T00:00:00Z", "time_end": "2020-01-02T00:00:00Z",
            }],
            "bands": [{"source_asset_id": "a1", "band_code": "B1", "band_name": "b1", "band_type": "spectral", "display_order": 0}],
        }],
    })


def test_retry_asset_refresh_replaces_stale_source_uri() -> None:
    workflow = PartitionWorkflowService(
        PartitionService(),
        scene_repository=_FakeSceneRepository([{
            "scene_id": "scene-a1", "asset_id": "a1", "source_uri": "s3://cube/new.tif",
            "cog_uri": "s3://cube/new.tif", "checksum": "1" * 64,
        }]),
    )
    refreshed = workflow._refresh_retry_assets(_strict_request("s3://cube/old.tif"))
    asset = refreshed.datasets[0].assets[0]
    assert str(asset.cog_uri) == "s3://cube/new.tif"
    assert asset.source_uri is None
    assert asset.checksum == "1" * 64


def test_retry_asset_refresh_without_repository_keeps_payload() -> None:
    workflow = PartitionWorkflowService(PartitionService(), scene_repository=None)
    refreshed = workflow._refresh_retry_assets(_strict_request("s3://cube/old.tif"))
    assert str(refreshed.datasets[0].assets[0].cog_uri) == "s3://cube/old.tif"


def test_retry_asset_refresh_warns_on_divergent_definitions(caplog: pytest.LogCaptureFixture) -> None:
    repo = _FakeSceneRepository([{
        "scene_id": "scene-a1", "asset_id": "a1", "source_uri": "s3://cube/new.tif",
        "cog_uri": "s3://cube/new.tif", "checksum": "1" * 64,
    }])
    repo.normalized_rows = [{
        "source_asset_id": "a1", "source_uri": "s3://cube/old.tif", "cog_uri": "s3://cube/old.tif", "checksum": "0" * 64,
    }]
    workflow = PartitionWorkflowService(PartitionService(), scene_repository=repo)
    with caplog.at_level("WARNING", logger="cube_web.services.partition_workflow"):
        workflow._refresh_retry_assets(_strict_request("s3://cube/old.tif"))
    assert any("diverges" in record.message for record in caplog.records)


def test_update_dataset_asset_syncs_authoritative_asset_definition() -> None:
    repository = InMemoryDatasetManagementRepository(
        datasets={
            "dataset-a": {
                "dataset_id": "dataset-a", "dataset_code": "DS-A", "dataset_title": "Optical A",
                "data_type": "optical", "status": "active",
                "created_at": "2026-07-01T00:00:00+00:00", "updated_at": "2026-07-15T00:00:00+00:00",
            },
        },
        details={"dataset-a": {"assets": [{
            "scene_id": "scene-a1", "asset_id": "a1", "source_uri": "s3://cube/old.tif",
            "cog_uri": "s3://cube/old.tif", "checksum": "0" * 64,
        }]}},
    )
    service = DatasetManagementService(repository)
    result = service.update_dataset_asset(
        "dataset-a", "a1", {"source_uri": "s3://cube/new.tif", "checksum": "1" * 64}, actor="admin"
    )
    assert result["source_uri"] == "s3://cube/new.tif"
    assert result["checksum"] == "1" * 64
    stored = repository.details["dataset-a"]["assets"][0]
    assert stored["source_uri"] == "s3://cube/new.tif"
    assert stored["checksum"] == "1" * 64
