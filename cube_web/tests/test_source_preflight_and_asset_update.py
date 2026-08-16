from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from cube_web.services import partition_dataset_runner as runner_module
from cube_web.services.dataset_management import (
    DatasetManagementConflict,
    DatasetManagementService,
    InMemoryDatasetManagementRepository,
    OpenGaussDatasetManagementRepository,
)
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


class _CountingMinio(_FakeMinio):
    calls: list[tuple[str, str]] = []

    def stat_object(self, bucket: str, key: str) -> Any:
        self.calls.append((bucket, key))
        return super().stat_object(bucket, key)


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


def test_preflight_deduplicates_same_source_object(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runner_module.runtime_config,
        "minio_settings",
        lambda **_: SimpleNamespace(endpoint="minio:9000", access_key="k", secret_key="s", bucket="cube", secure=False),
    )
    _CountingMinio.calls = []
    monkeypatch.setattr("minio.Minio", _CountingMinio)
    uri = "s3://cube/source/scene%20full.tif"
    payloads = [{
        "dataset": {
            "assets": [
                {"source_asset_id": "a1", "cog_uri": uri},
                {"source_asset_id": "a2", "cog_uri": uri},
            ],
        },
    }]

    runner_module.NormalizedPartitionDatasetRunner._verify_assets_exist(payloads)

    assert _CountingMinio.calls == [("cube", "source/scene full.tif")]


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


def test_update_dataset_asset_does_not_mutate_same_asset_id_in_another_dataset() -> None:
    repository = InMemoryDatasetManagementRepository(
        datasets={
            dataset_id: {
                "dataset_id": dataset_id, "dataset_code": dataset_id.upper(),
                "dataset_title": dataset_id, "data_type": "optical", "status": "active",
            }
            for dataset_id in ("dataset-a", "dataset-b")
        },
        details={
            "dataset-a": {"assets": [{"asset_id": "a1", "source_uri": "s3://cube/a.tif", "cog_uri": "s3://cube/a.tif"}]},
            "dataset-b": {"assets": [{"asset_id": "a1", "source_uri": "s3://cube/b.tif", "cog_uri": "s3://cube/b.tif"}]},
        },
    )
    service = DatasetManagementService(repository)

    service.update_dataset_asset("dataset-a", "a1", {"source_uri": "s3://cube/a-new.tif"}, actor="admin")

    assert repository.details["dataset-a"]["assets"][0]["source_uri"] == "s3://cube/a-new.tif"
    assert repository.details["dataset-b"]["assets"][0]["source_uri"] == "s3://cube/b.tif"


def test_update_dataset_asset_rejects_non_s3_source_uri() -> None:
    repository = InMemoryDatasetManagementRepository(
        datasets={"dataset-a": {"dataset_id": "dataset-a", "dataset_code": "DS-A", "dataset_title": "A", "data_type": "optical", "status": "active"}},
    )
    service = DatasetManagementService(repository)

    with pytest.raises(DatasetManagementConflict, match="valid s3:// URI"):
        service.update_dataset_asset("dataset-a", "a1", {"source_uri": "/tmp/source.tif"}, actor="admin")
    with pytest.raises(DatasetManagementConflict, match="valid s3:// URI"):
        service.update_dataset_asset("dataset-a", "a1", {"source_uri": "s3://cube/source.tif?versionId=1"}, actor="admin")


def test_opengauss_asset_update_scopes_authoritative_sql_to_dataset() -> None:
    class Cursor:
        def __init__(self) -> None:
            self.statements: list[tuple[str, tuple[Any, ...]]] = []
            self.rows = [
                {"dataset_id": "dataset-a"},
                {"scene_id": "scene-a"},
                {"source_asset_id": "a1", "source_uri": "s3://cube/new.tif", "cog_uri": None, "checksum": None},
            ]

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, sql: str, params=()):
            self.statements.append((sql, tuple(params)))

        def fetchone(self):
            return self.rows.pop(0)

    cursor = Cursor()

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def cursor(self, **_kwargs):
            return cursor

    repository = OpenGaussDatasetManagementRepository(None, connection_factory=lambda: Connection())
    result = repository.update_dataset_asset(
        "dataset-a", "a1", {"source_uri": "s3://cube/new.tif"}, actor="admin"
    )

    assert result["source_asset_id"] == "a1"
    scene_lookup = next(sql for sql, _params in cursor.statements if "FROM scene_assets sa JOIN scenes s" in sql)
    assert "s.dataset_id=%s" in scene_lookup
    scene_lookup_params = next(params for sql, params in cursor.statements if "FROM scene_assets sa JOIN scenes s" in sql)
    assert scene_lookup_params == ("dataset-a", "a1")
    scene_update, scene_update_params = next(
        (sql, params) for sql, params in cursor.statements if "UPDATE scene_assets SET" in sql
    )
    assert "SELECT scene_id FROM scenes WHERE dataset_id=%s" in scene_update
    assert scene_update_params[-2:] == ("dataset-a", "a1")


class _ObjectCleanupRepository(InMemoryDatasetManagementRepository):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.completed_cleanup_ids: list[str] = []

    def delete_band_grid(self, dataset_id: str, band_unit_id: str, grid_type: str, *, actor: str) -> dict[str, Any]:
        return {
            "dataset_id": dataset_id,
            "object_uris": [f"s3://cube/partition/{dataset_id}/versions/v1/tile.tif"],
            "object_cleanup_id": "cleanup-1",
        }

    def mark_object_cleanup_complete(self, dataset_id: str, cleanup_id: str, *, actor: str) -> None:
        self.completed_cleanup_ids.append(cleanup_id)


def test_failed_object_cleanup_is_recorded_for_retry() -> None:
    repository = _ObjectCleanupRepository(
        datasets={"dataset-a": {"dataset_id": "dataset-a", "dataset_code": "DS-A", "dataset_title": "A", "data_type": "optical", "status": "active"}},
    )

    def fail_cleanup(_object_uris: tuple[str, ...]) -> dict[str, Any]:
        raise RuntimeError("storage unavailable")

    service = DatasetManagementService(repository, grid_object_cleanup=fail_cleanup)
    result = service.delete_band_grid("dataset-a", "band-1", "geohash", actor="admin")

    assert result["object_cleanup"] == {
        "status": "pending",
        "error": "RuntimeError",
        "object_count": 1,
        "object_uris": ["s3://cube/partition/dataset-a/versions/v1/tile.tif"],
        "cleanup_id": "cleanup-1",
        "durable": True,
    }
    assert repository.completed_cleanup_ids == []


def test_successful_object_cleanup_acknowledges_transactional_outbox() -> None:
    repository = _ObjectCleanupRepository(
        datasets={"dataset-a": {"dataset_id": "dataset-a", "dataset_code": "DS-A", "dataset_title": "A", "data_type": "optical", "status": "active"}},
    )
    service = DatasetManagementService(
        repository,
        grid_object_cleanup=lambda _object_uris: {"status": "completed", "object_count": 1},
    )

    result = service.delete_band_grid("dataset-a", "band-1", "geohash", actor="admin")

    assert result["object_cleanup"]["status"] == "completed"
    assert repository.completed_cleanup_ids == ["cleanup-1"]
