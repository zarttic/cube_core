"""Dataset identity resolution (name mode) and scene content refresh.

``legacy`` mode must stay byte-identical to the old behaviour; ``name`` mode makes
a re-imported product land in the dataset it already has, with the batch kept as
lineage.
"""

from __future__ import annotations

import json

import pytest

from cube_web.services import dataset_identity
from cube_web.services.dataset_identity import (
    identity_mode,
    identity_product_whitelist,
    match_dataset_by_name,
    record_identity_audit,
    record_scene_content_refresh,
    uses_name_identity,
)


class _Cursor:
    def __init__(self, rows: list[dict[str, object]] | None = None) -> None:
        self.rows = rows or []
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, statement: str, params: tuple[object, ...] = ()) -> None:
        self.executed.append((" ".join(statement.split()), params))

    def fetchall(self) -> list[dict[str, object]]:
        return list(self.rows)


@pytest.fixture
def env(monkeypatch):
    values: dict[str, str] = {}

    def env_text(key: str) -> str:
        return values.get(key, "")

    monkeypatch.setattr(dataset_identity.runtime_config, "env_text", env_text)
    return values


def test_default_mode_is_legacy(env) -> None:
    assert identity_mode() == dataset_identity.LEGACY_MODE
    assert uses_name_identity({"dataset_title": "任意产品"}) is False
    assert identity_product_whitelist() == ()


def test_name_mode_applies_to_every_product_without_a_whitelist(env) -> None:
    env[dataset_identity.DATASET_IDENTITY_ENV] = "name"

    assert identity_mode() == dataset_identity.NAME_MODE
    assert uses_name_identity({"dataset_title": "任意产品"}) is True


def test_whitelist_narrows_name_mode(env) -> None:
    env[dataset_identity.DATASET_IDENTITY_ENV] = "name"
    env[dataset_identity.IDENTITY_PRODUCTS_ENV] = "产品甲|产品乙"

    assert identity_product_whitelist() == ("产品甲", "产品乙")
    assert uses_name_identity({"dataset_title": "产品甲"}) is True
    assert uses_name_identity({"dataset_title": "产品丙"}) is False


def test_unknown_mode_value_falls_back_to_legacy(env) -> None:
    env[dataset_identity.DATASET_IDENTITY_ENV] = "garbage"

    assert identity_mode() == dataset_identity.LEGACY_MODE


def test_match_by_name_ignores_archived_and_orders_candidates() -> None:
    cursor = _Cursor(rows=[{"dataset_id": "ard-optical-x", "dataset_code": "X"}])
    chosen, candidates = match_dataset_by_name(cursor, dataset_title="产品甲", data_type="optical")

    statement, params = cursor.executed[0]
    assert "dataset_title = %s" in statement and "data_type = %s" in statement
    assert "status <> 'archived'" in statement
    # 选择依据：最近有剖分产物的优先，其次最新创建
    assert "last_output_at DESC NULLS LAST" in statement and "created_at DESC" in statement
    assert params == ("产品甲", "optical")
    assert chosen == {"dataset_id": "ard-optical-x", "dataset_code": "X"}
    assert candidates == [chosen]


def test_match_by_name_picks_the_first_of_several_live_copies() -> None:
    """历史遗留多个活跃副本时必须选一个（不能让导入失败），并把它作为首选返回。"""
    cursor = _Cursor(rows=[
        {"dataset_id": "ard-optical-in-use", "dataset_code": "IN-USE"},
        {"dataset_id": "ard-optical-stale", "dataset_code": "STALE"},
    ])

    chosen, candidates = match_dataset_by_name(cursor, dataset_title="产品甲", data_type="optical")

    assert chosen == {"dataset_id": "ard-optical-in-use", "dataset_code": "IN-USE"}
    assert [row["dataset_id"] for row in candidates] == ["ard-optical-in-use", "ard-optical-stale"]


def test_match_by_name_returns_none_when_absent() -> None:
    chosen, candidates = match_dataset_by_name(_Cursor(rows=[]), dataset_title="产品甲", data_type="optical")
    assert chosen is None and candidates == []


def test_identity_audit_appends_to_jsonb_attributes() -> None:
    cursor = _Cursor()
    record_identity_audit(
        cursor,
        dataset_id="ard-optical-x",
        load_batch_id="ard-load-1",
        action="reused",
        previous_dataset_id="ard-optical-batch-derived",
    )

    statement, params = cursor.executed[0]
    assert "jsonb_set" in statement and "{identity_audit}" in statement
    assert params[1] == "ard-optical-x"
    entry = json.loads(params[0])[0]
    assert entry["action"] == "reused"
    assert entry["previous_dataset_id"] == "ard-optical-batch-derived"
    assert entry["load_batch_id"] == "ard-load-1"


def test_content_refresh_audit_records_both_checksums() -> None:
    cursor = _Cursor()
    record_scene_content_refresh(
        cursor,
        scene_id="scene-1",
        load_batch_id="ard-load-2",
        previous_checksum="a" * 64,
        incoming_checksum="b" * 64,
        refreshed_in_place=True,
    )

    statement, params = cursor.executed[0]
    assert "{content_history}" in statement
    entry = json.loads(params[0])[0]
    assert entry["previous_checksum"] == "a" * 64
    assert entry["checksum"] == "b" * 64
    assert entry["assets_refreshed_in_place"] is True


# ── 导入路径集成（CI 级，不依赖数据库）────────────────────────────────────


class _RepoCursor:
    """Answers the identity/scene lookups and records every statement."""

    def __init__(
        self,
        *,
        existing_dataset: tuple[str, str] | None,
        existing_scene: tuple[str, str] | None,
        existing_scene_checksum: str = "b" * 64,
    ) -> None:
        self.existing_dataset = existing_dataset
        self.existing_scene = existing_scene
        self.existing_scene_checksum = existing_scene_checksum
        self.statements: list[str] = []
        self._rows: list[tuple[object, ...]] = []
        self.rowcount = 1
        self.merged_scene: tuple[object, ...] | None = None

    def __enter__(self) -> "_RepoCursor":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, statement: str, params: tuple[object, ...] = ()) -> None:
        normalized = " ".join(statement.split())
        self.statements.append(normalized)
        if "FROM datasets" in normalized and "dataset_title = %s" in normalized:
            self._rows = [self.existing_dataset] if self.existing_dataset else []
        elif "SELECT scene_id,dataset_id,checksum FROM scenes" in normalized:
            self._rows = (
                [(self.existing_scene[0], self.existing_scene[1], self.existing_scene_checksum)]
                if self.existing_scene
                else []
            )
        elif normalized.startswith("SELECT scene_id,dataset_id FROM scenes"):
            # post-merge identity lookup: echo what the scene MERGE just wrote
            self._rows = [self.merged_scene] if self.merged_scene else []
        elif normalized.startswith("MERGE INTO scenes"):
            self.merged_scene = (params[0], params[1])
            self._rows = []
        elif "FROM scene_assets" in normalized:
            self._rows = [("asset-1", "b1")]  # one existing data asset with the same band set
        else:
            self._rows = []

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._rows)

    def fetchone(self) -> tuple[object, ...] | None:
        return self._rows[0] if self._rows else None


class _RepoConnection:
    def __init__(self, cursor: _RepoCursor) -> None:
        self._cursor = cursor
        self.commits = 0

    def cursor(self, **_kwargs: object) -> _RepoCursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1


def _import_payload(*, load_batch_id: str, dataset_id: str, title: str, checksum: str) -> dict[str, object]:
    return {
        "load_batch_id": load_batch_id,
        "source_system": "ard_loader",
        "datasets": [
            {
                "dataset_id": dataset_id,
                "dataset_code": f"CODE-{load_batch_id}",
                "dataset_title": title,
                "data_type": "optical",
                "scenes": [
                    {
                        "scene_key": "scene-key-1",
                        "source_namespace": "product-namespace",
                        "asset_id": "asset-1",
                        "source_uri": "s3://cube/source/1.tif",
                        "checksum": checksum,
                        "acquisition_time": "2024-06-15T00:00:00Z",
                        "bbox": [120.0, 40.0, 121.0, 41.0],
                        "crs": "EPSG:4326",
                        "bands": [{"band_code": "b1", "band_name": "b1"}],
                    }
                ],
            }
        ],
    }


def test_name_mode_reuses_the_existing_dataset_id(env, monkeypatch) -> None:
    env[dataset_identity.DATASET_IDENTITY_ENV] = "name"
    from cube_web.services.scene_repository import OpenGaussSceneRepository

    cursor = _RepoCursor(
        existing_dataset=("ard-optical-canonical", "CODE-CANONICAL"),
        existing_scene=("scene-1", "ard-optical-canonical"),
        existing_scene_checksum="a" * 64,  # 旧修订 → 触发内容刷新
    )
    connection = _RepoConnection(cursor)
    repository = OpenGaussSceneRepository(None, connection_factory=lambda: connection)

    result = repository.upsert_load_schema(
        _import_payload(load_batch_id="ard-load-2", dataset_id="ard-optical-ard-load-2",
                        title="产品甲", checksum="b" * 64)
    )

    dataset_merge = next(s for s in cursor.statements if s.startswith("MERGE INTO datasets"))
    assert "MERGE INTO datasets" in dataset_merge
    assert any("MERGE INTO load_batch_sources" in s for s in cursor.statements)
    assert any("{identity_audit}" in s for s in cursor.statements)
    assert any("{content_history}" in s for s in cursor.statements)
    identity = result["dataset_identity"]
    assert identity["mode"] == "name"
    assert len(identity["resolutions"]) == 1
    resolution = identity["resolutions"][0]
    assert resolution["dataset_id"] == "ard-optical-canonical"
    assert resolution["previous_dataset_id"] == "ard-optical-ard-load-2"
    assert resolution["action"] == "reused"
    # 只有一个活跃副本 → 不歧义；候选清单仍会回传，便于运营核对
    assert resolution["candidate_dataset_ids"] == ["ard-optical-canonical"]
    assert resolution["ambiguous"] is False
    assert result["content_refreshes"][0]["refreshed_in_place"] is True
    assert connection.commits == 1
    # 就地刷新时不应再插入新的资产行
    assert not any(s.startswith("MERGE INTO scene_assets") for s in cursor.statements)


def test_legacy_mode_keeps_the_payload_dataset_id(env) -> None:
    env[dataset_identity.DATASET_IDENTITY_ENV] = "legacy"
    from cube_web.services.scene_repository import OpenGaussSceneRepository

    cursor = _RepoCursor(existing_dataset=("ard-optical-canonical", "CODE-CANONICAL"), existing_scene=None)
    connection = _RepoConnection(cursor)
    repository = OpenGaussSceneRepository(None, connection_factory=lambda: connection)

    result = repository.upsert_load_schema(
        _import_payload(load_batch_id="ard-load-3", dataset_id="ard-optical-ard-load-3",
                        title="产品甲", checksum="c" * 64)
    )

    assert result["dataset_identity"]["mode"] == "legacy"
    assert result["dataset_identity"]["resolutions"] == []
    assert not any("FROM datasets WHERE dataset_title = %s" in s for s in cursor.statements)
