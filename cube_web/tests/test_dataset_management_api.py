from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from cube_web.routes.auth import Actor
from cube_web.routes.datasets import create_datasets_router
from cube_web.services.dataset_management import (
    DatasetManagementService,
    InMemoryDatasetManagementRepository,
    ManagedDatasetQuery,
    OpenGaussDatasetManagementRepository,
)


def _fixture() -> tuple[TestClient, InMemoryDatasetManagementRepository, dict[str, list]]:
    repository = InMemoryDatasetManagementRepository(
        datasets={
            "dataset-a": {
                "dataset_id": "dataset-a", "dataset_code": "DS-A", "dataset_title": "Optical A",
                "data_type": "optical", "product_type": "L2A", "status": "active",
                "current_output_version": "out-a", "description": "first", "keywords": ["cloud"],
                "created_at": "2026-07-01T00:00:00+00:00", "updated_at": "2026-07-15T00:00:00+00:00",
            },
            "dataset-b": {
                "dataset_id": "dataset-b", "dataset_code": "DS-B", "dataset_title": "Radar B",
                "data_type": "radar", "product_type": "GRD", "status": "active",
                "current_output_version": None, "description": "second", "keywords": [],
                "created_at": "2026-07-02T00:00:00+00:00", "updated_at": "2026-07-16T00:00:00+00:00",
            },
        },
        details={
            "dataset-a": {
                "scenes": [
                    {"scene_id": "scene-a1", "dataset_id": "dataset-a", "scene_key": "A1", "status": "available", "acquisition_time": "2026-06-01T00:00:00+00:00", "source_uri": "s3://cube/a1.tif"},
                    {"scene_id": "scene-a2", "dataset_id": "dataset-a", "scene_key": "A2", "status": "failed", "acquisition_time": "2026-06-02T00:00:00+00:00", "source_uri": "s3://cube/a2.tif"},
                ],
                "assets": [{"scene_id": "scene-a1", "asset_id": "asset-a1", "source_uri": "s3://cube/a1.tif"}],
                "bands": [{"scene_id": "scene-a1", "asset_id": "asset-a1", "band_unit_id": "band-a1-b01", "band_code": "B01"}],
                "outputs": [{"output_version": "out-a", "status": "completed"}],
                "grid": [{"output_id": "grid-a", "space_code": "wx4"}],
                "tiles": [{"output_id": "tile-a", "status": "ready"}],
                "indexes": [{"output_id": "index-a", "st_code": "wx4-2026"}],
                "ingest-records": [{"ingest_run_id": "ingest-a", "scene_id": "scene-a2", "band_unit_ids": ["band-a2-b01"], "status": "failed", "error_message": "bad tile"}],
                "quality": [{"quality_run_id": "quality-a", "status": "pass"}],
                "publications": [{"publication_id": "11111111-1111-1111-1111-111111111111", "status": "active"}],
                "provenance": [{"relation_type": "load_batch", "load_batch_id": "load-1", "status": "succeeded"}],
            },
            "dataset-b": {"scenes": []},
        },
    )
    hooks: dict[str, list] = {"quality": [], "publish": [], "withdraw": []}

    def quality(dataset_id, actor):
        hooks["quality"].append((dataset_id, actor.username))
        return {"quality_run_id": "quality-new", "dataset_id": dataset_id, "status": "pending"}

    def publish(dataset_id, actor, targets=()):
        hooks["publish"].append((dataset_id, actor.username))
        return {"publication_id": "publication-new", "dataset_id": dataset_id, "status": "active"}

    def withdraw(dataset_id, publication_id, reason, actor):
        hooks["withdraw"].append((dataset_id, publication_id, reason, actor.username))
        return {"publication_id": publication_id, "dataset_id": dataset_id, "status": "withdrawn"}

    service = DatasetManagementService(repository, quality_hook=quality, publish_hook=publish, withdraw_hook=withdraw)
    app = FastAPI()

    @app.middleware("http")
    async def actor(request: Request, call_next):
        role = request.headers.get("x-test-role", "admin")
        request.state.actor = Actor(username=role, role=role)
        return await call_next(request)

    app.include_router(create_datasets_router(service), prefix="/v1")
    return TestClient(app), repository, hooks


def test_list_exposes_scene_aggregates_filters_and_stable_paging() -> None:
    client, _, _ = _fixture()
    response = client.get("/v1/datasets", params={"keyword": "Optical", "data_type": "optical", "page_size": 1})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert {
        "dataset_id": "dataset-a", "scene_count": 2, "ready_scene_count": 1,
        "failed_scene_count": 1, "ingest_status": "failed", "quality_status": "pass",
        "publish_status": "active", "archived": False,
    }.items() <= body["items"][0].items()
    assert body["summary"] == {
        "dataset_count": 2, "scene_count": 2, "ready_scene_count": 1, "failed_scene_count": 1,
    }
    assert client.get("/v1/datasets", params={"ingest_status": "failed"}).json()["total"] == 1
    assert client.get("/v1/datasets", params={"archived": "true"}).json()["total"] == 0
    invalid = client.get("/v1/datasets", params={"sort_by": "raw_sql"})
    assert invalid.status_code == 422
    assert invalid.json()["detail"]["code"] == "invalid_dataset_query"


def test_all_management_detail_domains_are_paginated() -> None:
    client, _, _ = _fixture()
    details = (
        "scenes", "assets", "bands", "outputs", "grid", "tiles", "indexes",
        "ingest-records", "quality", "publications", "provenance",
    )
    for detail in details:
        response = client.get(f"/v1/datasets/dataset-a/{detail}", params={"page": 1, "page_size": 1})
        assert response.status_code == 200, (detail, response.text)
        assert set(response.json()) == {"items", "total", "page", "page_size"}
        assert response.json()["total"] >= 1
    missing = client.get("/v1/datasets/missing/scenes")
    assert missing.status_code == 404
    assert missing.json()["detail"]["code"] == "dataset_not_found"


def test_dataset_visibility_restrictions_filter_list_detail_and_tiles() -> None:
    client, repository, _ = _fixture()

    repository.replace_hidden_roles("dataset-a", ("NORMAL", "SCIENTIST"), actor="admin")

    normal_headers = {"x-test-role": "NORMAL"}
    assert [row["dataset_id"] for row in client.get("/v1/datasets", headers=normal_headers).json()["items"]] == ["dataset-b"]
    assert client.get("/v1/datasets/dataset-a", headers=normal_headers).status_code == 404
    assert client.get("/v1/datasets/dataset-a/tiles", headers=normal_headers).status_code == 404
    assert client.get("/v1/datasets/dataset-a", headers={"x-test-role": "ADVANCED"}).status_code == 200


def test_admin_replaces_dataset_role_restrictions() -> None:
    client, _, _ = _fixture()

    saved = client.put("/v1/datasets/dataset-a/role-restrictions", json={"hidden_roles": ["普通用户", "SCIENTIST"]})
    assert saved.status_code == 200
    assert saved.json() == {"hidden_roles": ["NORMAL", "SCIENTIST"]}
    assert client.get("/v1/datasets/dataset-a/role-restrictions").json() == saved.json()
    invalid = client.put("/v1/datasets/dataset-a/role-restrictions", json={"hidden_roles": ["guest"]})
    assert invalid.status_code == 422
    assert client.put("/v1/datasets/dataset-a/role-restrictions", json={"hidden_roles": ["ADMIN"]}).status_code == 422
    assert client.get("/v1/datasets/dataset-a/role-restrictions", headers={"x-test-role": "NORMAL"}).status_code == 403


def test_opengauss_role_visibility_filter_uses_dataset_restrictions() -> None:
    condition, params = OpenGaussDatasetManagementRepository._visibility_filter("普通用户")

    assert "dataset_role_restrictions" in condition
    assert params == ("NORMAL",)
    assert OpenGaussDatasetManagementRepository._visibility_filter("ADMIN") == ("", ())


def test_scene_detail_embeds_band_units_for_three_level_management() -> None:
    client, _, _ = _fixture()

    body = client.get("/v1/datasets/dataset-a/scenes", params={"page": 1, "page_size": 20}).json()

    assert body["items"][0]["scene_id"] == "scene-a1"
    assert body["items"][0]["bands"] == [{
        "scene_id": "scene-a1", "asset_id": "asset-a1",
        "band_unit_id": "band-a1-b01", "band_code": "B01",
    }]


def test_metadata_and_scene_reassignment_are_audited() -> None:
    client, repository, _ = _fixture()
    updated = client.patch("/v1/datasets/dataset-a", json={
        "dataset_title": "Optical A revised", "description": "reviewed", "keywords": ["cloud", "2026"],
    })
    assert updated.status_code == 200
    assert updated.json()["dataset_title"] == "Optical A revised"
    assert repository.metadata_audit[-1]["changed_by"] == "admin"
    empty = client.patch("/v1/datasets/dataset-a", json={})
    assert empty.status_code == 422

    moved = client.post("/v1/datasets/dataset-a/scenes/scene-a1/reassign", json={
        "target_dataset_id": "dataset-b", "reason": "metadata correction",
    })
    assert moved.status_code == 200
    assert {
        "scene_id": "scene-a1", "previous_dataset_id": "dataset-a", "dataset_id": "dataset-b",
        "reason": "metadata correction", "changed_by": "admin",
    }.items() <= moved.json().items()
    assert client.get("/v1/datasets/dataset-a/scenes").json()["total"] == 1
    assert client.get("/v1/datasets/dataset-b/scenes").json()["total"] == 1


def test_failed_band_retry_and_actions_use_injected_domain_hooks() -> None:
    client, repository, hooks = _fixture()
    retry = client.post("/v1/datasets/dataset-a/bands/band-a2-b01/ingest-retry", json={})
    assert retry.status_code == 202
    assert retry.json()["status"] == "queued"
    conflict = client.post("/v1/datasets/dataset-a/bands/band-a2-b01/ingest-retry", json={})
    assert conflict.status_code == 409

    assert client.post("/v1/datasets/dataset-a/quality-runs", json={}).status_code == 202
    blocked = client.post("/v1/datasets/dataset-a/publish", json={})
    assert blocked.status_code == 409
    assert blocked.json()["detail"]["message"] == "publication requires completed ingest"
    repository.details["dataset-a"]["ingest-records"].append({
        "ingest_run_id": "ingest-a-completed", "scene_id": "scene-a2", "band_unit_ids": ["band-a2-b02"], "status": "completed"
    })
    assert client.post("/v1/datasets/dataset-a/publish", json={}).status_code == 201
    publication_id = "11111111-1111-1111-1111-111111111111"
    withdrawn = client.post(f"/v1/datasets/dataset-a/publications/{publication_id}/withdraw", json={})
    assert withdrawn.status_code == 200
    assert client.post("/v1/datasets/dataset-a/publications/not-a-uuid/withdraw", json={}).status_code == 422
    assert hooks == {
        "quality": [("dataset-a", "admin")],
        "publish": [("dataset-a", "admin")],
        "withdraw": [("dataset-a", publication_id, "数据管理页面撤回", "admin")],
    }
    unsupported = client.post("/v1/datasets/dataset-b/quality-runs", json={})
    assert unsupported.status_code == 409
    assert unsupported.json()["detail"]["code"] == "dataset_action_conflict"


def test_publish_requires_passed_quality_even_after_ingest_completed() -> None:
    client, repository, hooks = _fixture()
    repository.details["dataset-a"]["ingest-records"].append({
        "ingest_run_id": "ingest-completed", "scene_id": "scene-a1", "band_unit_ids": ["band-a1-b01"], "status": "completed"
    })
    repository.details["dataset-a"]["quality"].append({
        "quality_run_id": "quality-failed", "status": "fail"
    })

    response = client.post("/v1/datasets/dataset-a/publish", json={})

    assert response.status_code == 409
    assert response.json()["detail"]["message"] == "publication requires a passing quality decision"
    assert hooks["publish"] == []


def test_archive_preserves_scenes_and_source_objects() -> None:
    client, repository, _ = _fixture()
    before = repository.list_detail("dataset-a", "assets", limit=20, offset=0)[0]
    response = client.post("/v1/datasets/dataset-a/archive", json={"reason": "retention policy"})
    assert response.status_code == 200
    assert response.json()["archived"] is True
    assert repository.list_detail("dataset-a", "assets", limit=20, offset=0)[0] == before
    assert {
        "action": "archive", "reason": "retention policy", "physical_data_deleted": False,
    }.items() <= repository.metadata_audit[-1].items()
    audit_count = len(repository.metadata_audit)
    assert client.post("/v1/datasets/dataset-a/archive", json={"reason": "repeated request"}).status_code == 200
    assert len(repository.metadata_audit) == audit_count


def test_dataset_mutations_require_admin() -> None:
    client, repository, hooks = _fixture()
    headers = {"x-test-role": "user"}
    publication_id = "11111111-1111-1111-1111-111111111111"
    requests = (
        client.patch(
            "/v1/datasets/dataset-a",
            json={"dataset_title": "unauthorized"},
            headers=headers,
        ),
        client.post(
            "/v1/datasets/dataset-a/scenes/scene-a1/reassign",
            json={"target_dataset_id": "dataset-b", "reason": "unauthorized"},
            headers=headers,
        ),
        client.post("/v1/datasets/dataset-a/quality-runs", json={}, headers=headers),
        client.post("/v1/datasets/dataset-a/bands/band-a2-b01/ingest-retry", json={}, headers=headers),
        client.post("/v1/datasets/dataset-a/publish", json={}, headers=headers),
        client.post(
            f"/v1/datasets/dataset-a/publications/{publication_id}/withdraw",
            json={},
            headers=headers,
        ),
        client.post(
            "/v1/datasets/dataset-a/archive",
            json={"reason": "unauthorized"},
            headers=headers,
        ),
    )

    assert [response.status_code for response in requests] == [403] * len(requests)
    assert repository.datasets["dataset-a"]["dataset_title"] == "Optical A"
    assert repository.list_detail("dataset-a", "scenes", limit=20, offset=0)[1] == 2
    assert repository.details["dataset-a"]["ingest-records"][0]["status"] == "failed"
    assert hooks == {"quality": [], "publish": [], "withdraw": []}


def test_opengauss_provenance_uses_formal_domain_relations() -> None:
    sql, params = OpenGaussDatasetManagementRepository._detail_sql("provenance", "dataset-a")
    assert "migration_lineage" not in sql
    assert "load_batch_scenes" in sql
    assert "partition_run_scenes" in sql
    assert "ingest_run_scenes" in sql
    assert "scene_dataset_audit" in sql
    assert len(params) == 6
    tile_sql, _ = OpenGaussDatasetManagementRepository._detail_sql("tiles", "dataset-a")
    index_sql, _ = OpenGaussDatasetManagementRepository._detail_sql("indexes", "dataset-a")
    output_sql, _ = OpenGaussDatasetManagementRepository._detail_sql("outputs", "dataset-a")
    assert "'executor-' || md5(o.task_id)" in output_sql
    assert "migration_lineage" not in tile_sql
    assert "st.st_code" in tile_sql
    assert "i.time_bucket=t.time_bucket" in tile_sql
    assert "migration_lineage" not in index_sql


def test_opengauss_dataset_list_has_no_migration_shadow_filter() -> None:
    sql = OpenGaussDatasetManagementRepository._overview_sql("")
    assert "rs-dataset" not in sql
    assert "legacy_dataset" not in sql


def test_opengauss_native_dataset_status_uses_its_own_partition_identity() -> None:
    overview = OpenGaussDatasetManagementRepository._overview_sql("")
    filters, _ = OpenGaussDatasetManagementRepository._filters(
        ManagedDatasetQuery(quality_status="pass", publish_status="active")
    )
    quality, _ = OpenGaussDatasetManagementRepository._detail_sql("quality", "dataset-a")
    publications, _ = OpenGaussDatasetManagementRepository._detail_sql("publications", "dataset-a")
    assert "pd.dataset_id=d.dataset_id" in overview
    assert "pp.dataset_id=d.dataset_id" in overview
    assert "pd.dataset_id=d.dataset_id" in filters
    assert "pp.dataset_id=d.dataset_id" in filters
    assert "d.dataset_id=q.dataset_id" in quality
    assert "d.dataset_id=p.dataset_id" in publications


def test_opengauss_native_dataset_details_use_own_partition_identity() -> None:
    for detail in ("outputs", "grid", "tiles", "indexes", "quality", "publications"):
        sql, _ = OpenGaussDatasetManagementRepository._detail_sql(detail, "dataset-native")
        assert "legacy_partition_dataset_id" not in sql


def test_opengauss_dataset_summary_uses_same_canonical_filter() -> None:
    filter_sql = OpenGaussDatasetManagementRepository._canonical_dataset_filter("d")
    assert filter_sql == "TRUE"
