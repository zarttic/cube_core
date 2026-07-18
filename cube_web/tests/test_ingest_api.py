from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from cube_web.routes.auth import Actor
from cube_web.routes.ingest_runs import create_ingest_runs_router
from cube_web.services.ingest_contracts import CreateIngestRun, IngestSceneInput
from cube_web.services.ingest_repository import InMemoryIngestRepository
from cube_web.services.ingest_service import IngestRunService


def _request(scene_id: str) -> CreateIngestRun:
    return CreateIngestRun(
        partition_run_id="partition-run-api",
        dataset_id="dataset-a",
        scenes=(
            IngestSceneInput(
                scene_id=scene_id,
                output_version="output-v1",
                quality_run_id=f"quality-{scene_id}",
                source_load_batch_ids=("load-batch-a",),
            ),
        ),
    )


def _client() -> tuple[TestClient, IngestRunService, str, str]:
    service = IngestRunService(InMemoryIngestRepository({"failed-scene": "dataset-a", "queued-scene": "dataset-a"}))
    failed = service.schedule_after_quality(_request("failed-scene"), quality_passed=True)
    service.start_scene(failed.ingest_run_id, "failed-scene")
    service.fail_scene(failed.ingest_run_id, "failed-scene", "index write failed")
    queued = service.schedule_after_quality(_request("queued-scene"), quality_passed=True)
    app = FastAPI()

    @app.middleware("http")
    async def actor(request: Request, call_next):
        role = request.headers.get("x-test-role", "admin")
        request.state.actor = Actor(username=role, role=role)
        return await call_next(request)

    app.include_router(create_ingest_runs_router(service), prefix="/v1")
    return TestClient(app), service, failed.ingest_run_id, queued.ingest_run_id


def test_list_and_get_ingest_runs_match_frontend_contract() -> None:
    client, _service, failed_id, queued_id = _client()
    response = client.get("/v1/ingest-runs", params={"dataset_id": "dataset-a", "page": 1, "page_size": 1})
    assert response.status_code == 200
    assert set(response.json()) == {"items", "total", "page", "page_size", "summary"}
    assert response.json()["total"] == 2
    assert response.json()["summary"]["scene_count"] == 2
    assert response.json()["items"][0]["ingest_run_id"] == queued_id
    detail = client.get(f"/v1/ingest-runs/{failed_id}")
    assert detail.status_code == 200
    assert detail.json()["failed_scene_count"] == 1
    assert detail.json()["scenes"][0]["source_load_batch_ids"] == ["load-batch-a"]


def test_retry_requires_explicit_failed_scene_ids() -> None:
    client, _service, failed_id, _queued_id = _client()
    assert client.post(f"/v1/ingest-runs/{failed_id}/retry", json={"scene_ids": []}).status_code == 422
    response = client.post(f"/v1/ingest-runs/{failed_id}/retry", json={"scene_ids": ["failed-scene"]})
    assert response.status_code == 200
    assert response.json()["scenes"][0]["status"] == "queued"
    assert client.post(f"/v1/ingest-runs/{failed_id}/retry", json={"scene_ids": ["failed-scene"]}).status_code == 409


def test_cancel_records_reason_without_changing_completed_scenes() -> None:
    client, _service, _failed_id, queued_id = _client()
    response = client.post(f"/v1/ingest-runs/{queued_id}/cancel", json={"reason": "operator requested"})
    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"
    assert response.json()["cancel_reason"] == "operator requested"
    assert client.post(f"/v1/ingest-runs/{queued_id}/cancel", json={"reason": "again"}).status_code == 409


def test_ingest_run_mutations_require_admin() -> None:
    client, service, failed_id, queued_id = _client()
    headers = {"x-test-role": "user"}

    retry = client.post(
        f"/v1/ingest-runs/{failed_id}/retry",
        json={"scene_ids": ["failed-scene"]},
        headers=headers,
    )
    cancel = client.post(
        f"/v1/ingest-runs/{queued_id}/cancel",
        json={"reason": "operator requested"},
        headers=headers,
    )

    assert retry.status_code == 403
    assert cancel.status_code == 403
    assert service.get(failed_id).status == "failed"
    assert service.get(queued_id).status == "queued"


def test_missing_and_invalid_query_errors_are_stable() -> None:
    client, _service, _failed_id, _queued_id = _client()
    missing = client.get("/v1/ingest-runs/missing")
    assert missing.status_code == 404
    assert missing.json()["detail"]["code"] == "ingest_run_not_found"
    invalid = client.get("/v1/ingest-runs", params={"sort_by": "raw_sql"})
    assert invalid.status_code == 422
    assert invalid.json()["detail"]["code"] == "invalid_ingest_query"
    paths = client.get("/openapi.json").json()["paths"]
    assert not any("preview" in path or "confirm" in path for path in paths)
