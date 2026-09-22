"""Load-batch physical deletion: service semantics + HTTP contract.

Covers the ARD contract ``POST /v1/partition/load-batches/{id}/delete`` that the
loader calls before falling back to (deprecated) archiving.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from cube_web.routes.auth import Actor
from cube_web.routes.scene_partition import create_scene_partition_router
from cube_web.services import load_batch_deletion
from cube_web.services.load_batch_deletion import (
    LoadBatchDeletionConflict,
    LoadBatchNotFound,
    delete_load_batch,
)


class _Cursor:
    def __init__(self, connection: "_Connection") -> None:
        self._connection = connection
        self._rows: list[dict[str, Any]] = []
        self.rowcount = 0

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, statement: str, params: tuple[Any, ...] = ()) -> None:
        normalized = " ".join(statement.split())
        self._connection.executed.append((normalized, params))
        self._rows = self._connection.results_for(normalized)
        self.rowcount = self._connection.rowcount_for(normalized)

    def fetchall(self) -> list[dict[str, Any]]:
        return list(self._rows)

    def fetchone(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None


class _Connection:
    def __init__(
        self,
        *,
        batch: dict[str, Any] | None = None,
        scenes: tuple[dict[str, Any], ...] = (),
        shared: tuple[dict[str, Any], ...] = (),
    ) -> None:
        self.batch = batch
        self.scenes = scenes
        self.shared = shared
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.batch_present = batch is not None
        self.keep_row_after_delete = False

    def cursor(self, **_kwargs: object) -> _Cursor:
        return _Cursor(self)

    def results_for(self, statement: str) -> list[dict[str, Any]]:
        if statement.startswith("SELECT status, source_type FROM load_batches"):
            return [self.batch] if self.batch else []
        if statement == "SELECT 1 FROM load_batches WHERE load_batch_id = %s":
            return [{"?column?": 1}] if self.batch_present else []
        if "count(DISTINCT lbs.load_batch_id)" in statement:
            return list(self.shared)
        if "SELECT DISTINCT s.scene_id, s.dataset_id" in statement:
            return list(self.scenes)
        return []

    def rowcount_for(self, statement: str) -> int:
        if statement.startswith("DELETE FROM load_batches"):
            if self.keep_row_after_delete:
                return 0
            removed = 1 if self.batch_present else 0
            self.batch_present = False
            return removed
        return 1 if statement.startswith(("DELETE", "UPDATE")) else 0

    def deletes(self) -> list[tuple[str, tuple[Any, ...]]]:
        return [entry for entry in self.executed if entry[0].startswith("DELETE")]


class _FakeDatasetAdmin:
    def __init__(self, *, error: Exception | None = None, on_delete: Any = None) -> None:
        self.error = error
        self.on_delete = on_delete
        self.calls: list[tuple[str, str]] = []

    def delete_dataset(self, dataset_id: str, *, actor: str) -> dict[str, Any]:
        if self.error is not None:
            raise self.error
        self.calls.append((dataset_id, actor))
        if self.on_delete is not None:
            self.on_delete(dataset_id)
        return {"dataset_id": dataset_id, "deleted": True}


def _batch(status: str = "succeeded", source_type: str = "subsystem_import") -> dict[str, str]:
    return {"status": status, "source_type": source_type}


# ── 服务语义 ────────────────────────────────────────────────────────────────


def test_missing_batch_raises_not_found() -> None:
    connection = _Connection(batch=None)
    with pytest.raises(LoadBatchNotFound):
        delete_load_batch(connection, load_batch_id="ard-load-x", actor="admin", reason="r", dataset_admin=_FakeDatasetAdmin())
    assert connection.deletes() == []


def test_active_batch_is_rejected() -> None:
    connection = _Connection(batch=_batch(status="running"))
    with pytest.raises(LoadBatchDeletionConflict, match="进行中"):
        delete_load_batch(connection, load_batch_id="ard-load-x", actor="admin", reason="r", dataset_admin=_FakeDatasetAdmin())
    assert connection.deletes() == []


def test_exclusive_dataset_is_deleted_together_with_the_batch() -> None:
    connection = _Connection(
        batch=_batch(),
        scenes=({"scene_id": "scene-1", "dataset_id": "ard-optical-ard-load-aaa"},),
    )
    admin = _FakeDatasetAdmin()

    result = delete_load_batch(
        connection, load_batch_id="ard-load-aaa", actor="admin", reason="cleanup", dataset_admin=admin
    )

    assert admin.calls == [("ard-optical-ard-load-aaa", "admin")]
    assert result["deletable_dataset_ids"] == ["ard-optical-ard-load-aaa"]
    assert result["retained_dataset_ids"] == []
    assert result["deleted"]["load_batches"] == 1
    deleted_tables = [statement.split()[2] for statement, _ in connection.deletes()]
    assert deleted_tables == ["load_batch_sources", "load_batch_sources", "load_batch_scenes", "load_batches"]


def test_shared_dataset_is_retained_and_only_lineage_is_removed() -> None:
    connection = _Connection(
        batch=_batch(),
        scenes=({"scene_id": "scene-1", "dataset_id": "ard-optical-higlass-landsat-01"},),
        shared=({"dataset_id": "ard-optical-higlass-landsat-01", "batch_count": 2},),
    )
    admin = _FakeDatasetAdmin()

    result = delete_load_batch(
        connection, load_batch_id="ard-load-aaa", actor="admin", reason="cleanup", dataset_admin=admin
    )

    assert admin.calls == []
    assert result["deletable_dataset_ids"] == []
    assert result["retained_dataset_ids"] == ["ard-optical-higlass-landsat-01"]
    assert result["deleted"]["load_batches"] == 1


def test_reload_batch_never_deletes_its_dataset() -> None:
    connection = _Connection(
        batch=_batch(source_type="dataset_reload"),
        scenes=({"scene_id": "scene-1", "dataset_id": "ard-optical-ard-load-aaa"},),
    )
    admin = _FakeDatasetAdmin()

    result = delete_load_batch(
        connection, load_batch_id="dataset-reload-001", actor="admin", reason="cleanup", dataset_admin=admin
    )

    assert admin.calls == []
    assert result["deletable_dataset_ids"] == []
    assert result["retained_dataset_ids"] == ["ard-optical-ard-load-aaa"]


def test_dry_run_returns_the_plan_without_writing() -> None:
    connection = _Connection(
        batch=_batch(),
        scenes=(
            {"scene_id": "scene-1", "dataset_id": "ard-optical-ard-load-aaa"},
            {"scene_id": "scene-2", "dataset_id": "ard-optical-ard-load-aaa"},
        ),
        shared=({"dataset_id": "ard-optical-ard-load-aaa", "batch_count": 2},),
    )
    admin = _FakeDatasetAdmin()

    result = delete_load_batch(
        connection, load_batch_id="ard-load-aaa", actor="admin", reason="preview", dataset_admin=admin, dry_run=True
    )

    assert result["dry_run"] is True
    assert result["scene_ids"] == ["scene-1", "scene-2"]
    assert result["retained_dataset_ids"] == ["ard-optical-ard-load-aaa"]
    assert admin.calls == []
    assert connection.deletes() == []


def test_batch_row_already_removed_by_dataset_deletion_is_not_an_error() -> None:
    """delete_dataset removes the load-batch rows it owns; the helper must accept that."""
    connection = _Connection(batch=_batch(), scenes=({"scene_id": "scene-1", "dataset_id": "ard-optical-ard-load-aaa"},))
    admin = _FakeDatasetAdmin(on_delete=lambda _dataset_id: setattr(connection, "batch_present", False))

    result = delete_load_batch(
        connection, load_batch_id="ard-load-aaa", actor="admin", reason="cleanup", dataset_admin=admin
    )

    assert result["deleted"]["load_batches"] == 0  # already gone
    assert admin.calls == [("ard-optical-ard-load-aaa", "admin")]


def test_leftover_batch_row_after_writes_is_a_conflict() -> None:
    connection = _Connection(batch=_batch(), scenes=())
    connection.keep_row_after_delete = True
    admin = _FakeDatasetAdmin()

    with pytest.raises(LoadBatchDeletionConflict, match="批次行仍存在"):
        delete_load_batch(connection, load_batch_id="ard-load-aaa", actor="admin", reason="r", dataset_admin=admin)


def test_dataset_guard_failure_maps_to_conflict() -> None:
    connection = _Connection(batch=_batch(), scenes=({"scene_id": "scene-1", "dataset_id": "ard-optical-ard-load-aaa"},))
    admin = _FakeDatasetAdmin(error=RuntimeError("数据集存在运行中的剖分任务"))

    with pytest.raises(LoadBatchDeletionConflict, match="数据集删除失败"):
        delete_load_batch(connection, load_batch_id="ard-load-aaa", actor="admin", reason="r", dataset_admin=admin)
    # The batch row must survive a refused dataset deletion.
    assert all("load_batches" not in statement for statement, _ in connection.deletes())


def test_plan_does_not_lock_the_batch_row() -> None:
    """Regression: a row lock here deadlocks against the dataset deletion's own connection.

    The dataset deletion runs in a separate transaction, so holding ``FOR UPDATE`` on
    the load_batches row made both transactions wait on each other (observed as a
    30s request timeout in the cross-system acceptance run).
    """
    connection = _Connection(batch=_batch(), scenes=())
    delete_load_batch(connection, load_batch_id="ard-load-aaa", actor="admin", reason="r", dataset_admin=_FakeDatasetAdmin())

    plan_statements = [statement for statement, _ in connection.executed if "FROM load_batches" in statement]
    assert plan_statements, "批次查询未执行"
    assert not any("FOR UPDATE" in statement for statement in plan_statements)


def test_referencing_reload_bookkeeping_is_dropped_and_counted() -> None:
    connection = _Connection(batch=_batch(), scenes=())
    result = delete_load_batch(
        connection, load_batch_id="ard-load-aaa", actor="admin", reason="r", dataset_admin=_FakeDatasetAdmin()
    )
    assert result["deleted"]["referencing_load_batch_sources"] == 1
    statements = [statement for statement, _ in connection.deletes()]
    assert any("source_load_batch_id = %s" in statement for statement in statements)


# ── HTTP 契约 ───────────────────────────────────────────────────────────────


class _Pool:
    @classmethod
    def for_dsn(cls, _dsn: str) -> "_Pool":
        return cls()

    @contextmanager
    def connection(self) -> Any:
        yield _Connection(
            batch=_batch(),
            scenes=({"scene_id": "scene-1", "dataset_id": "ard-optical-ard-load-aaa"},),
        )


class _Service:
    """Minimal SceneDomainService stand-in for router construction."""

    def get_load_batch(self, load_batch_id: str) -> dict[str, Any]:
        return {"load_batch_id": load_batch_id}


@pytest.fixture
def api(monkeypatch):
    from cube_web.routes import scene_partition

    admin = _FakeDatasetAdmin()
    monkeypatch.setattr(scene_partition.runtime_config, "postgres_dsn", lambda: "postgresql://stub")
    monkeypatch.setattr(scene_partition, "_PostgresPool", _Pool)

    app = FastAPI()

    @app.middleware("http")
    async def actor(request: Request, call_next):
        role = request.headers.get("x-test-role", "admin")
        request.state.actor = Actor(username=role, role=role)
        return await call_next(request)

    app.include_router(
        create_scene_partition_router(_Service(), dataset_service_factory=lambda: admin),  # type: ignore[arg-type]
        prefix="/v1",
    )
    return TestClient(app), admin


def test_delete_endpoint_returns_the_deletion_summary(api) -> None:
    client, admin = api

    response = client.post("/v1/partition/load-batches/ard-load-aaa/delete", json={"reason": "ARD 批次删除"})

    assert response.status_code == 200
    body = response.json()
    assert body["load_batch_id"] == "ard-load-aaa"
    assert body["deleted"]["load_batches"] == 1
    assert body["deletable_dataset_ids"] == ["ard-optical-ard-load-aaa"]
    assert admin.calls == [("ard-optical-ard-load-aaa", "admin")]


def test_delete_endpoint_supports_dry_run(api) -> None:
    client, admin = api

    response = client.post(
        "/v1/partition/load-batches/ard-load-aaa/delete", json={"reason": "预览", "dry_run": True}
    )

    assert response.status_code == 200
    assert response.json()["dry_run"] is True
    assert admin.calls == []


def test_delete_endpoint_requires_admin(api) -> None:
    client, _ = api

    response = client.post(
        "/v1/partition/load-batches/ard-load-aaa/delete",
        json={"reason": "x"},
        headers={"x-test-role": "viewer"},
    )

    assert response.status_code == 403


def test_delete_endpoint_maps_not_found_to_404(api, monkeypatch) -> None:
    client, _ = api

    def raise_not_found(*_args: object, **_kwargs: object) -> dict[str, Any]:
        raise LoadBatchNotFound("ard-load-missing")

    monkeypatch.setattr(load_batch_deletion, "delete_load_batch", raise_not_found)
    response = client.post("/v1/partition/load-batches/ard-load-missing/delete", json={"reason": "x"})

    assert response.status_code == 404


def test_delete_endpoint_maps_conflict_to_409(api, monkeypatch) -> None:
    client, _ = api

    def raise_conflict(*_args: object, **_kwargs: object) -> dict[str, Any]:
        raise LoadBatchDeletionConflict("批次仍有进行中的任务")

    monkeypatch.setattr(load_batch_deletion, "delete_load_batch", raise_conflict)
    response = client.post("/v1/partition/load-batches/ard-load-aaa/delete", json={"reason": "x"})

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "load_batch_delete_conflict"
