from fastapi import FastAPI
from fastapi.testclient import TestClient

from cube_web.routes.partition_datasets import create_partition_datasets_router
from cube_web.services.dataset_service import DatasetService
from cube_web.services.partition_domain_store import InMemoryPartitionDomainStore


def _client() -> TestClient:
    store = InMemoryPartitionDomainStore()
    store.datasets.update(
        {
            "dataset-a": {
                "dataset_id": "dataset-a",
                "dataset_code": "DATASET-A",
                "dataset_title": "Dataset A",
                "updated_at": "2026-07-15T01:00:00+00:00",
                "created_at": "2026-07-15T00:00:00+00:00",
                "partition_completed_at": "2026-07-15T01:00:00+00:00",
                "partition_status": "completed",
                "quality_status": "pass",
                "current_output_version": "ov-a-current",
            },
            "dataset-b": {
                "dataset_id": "dataset-b",
                "dataset_code": "DATASET-B",
                "dataset_title": "Dataset B",
                "updated_at": "2026-07-15T02:00:00+00:00",
                "created_at": "2026-07-15T00:00:00+00:00",
                "partition_completed_at": "2026-07-15T02:00:00+00:00",
                "partition_status": "completed",
                "quality_status": "warn",
                "current_output_version": "ov-b-current",
            },
        }
    )
    store.outputs.update(
        {
            ("dataset-a", "ov-a-current"): {"dataset_id": "dataset-a", "output_version": "ov-a-current", "status": "completed"},
            ("dataset-b", "ov-b-current"): {"dataset_id": "dataset-b", "output_version": "ov-b-current", "status": "completed"},
        }
    )
    store.tiles.update(
        {
            "tile-a": {
                "output_id": "tile-a",
                "dataset_id": "dataset-a",
                "output_version": "ov-a-current",
                "created_at": "2026-07-15T00:00:00+00:00",
            },
            "tile-b": {
                "output_id": "tile-b",
                "dataset_id": "dataset-b",
                "output_version": "ov-b-current",
                "created_at": "2026-07-15T00:00:00+00:00",
            },
        }
    )
    store.publications.append(
        {
            "publication_id": "publication-a",
            "dataset_id": "dataset-a",
            "output_version": "ov-a-current",
            "status": "active",
            "requested_at": "2026-07-15T01:00:00+00:00",
        }
    )
    app = FastAPI()
    app.include_router(create_partition_datasets_router(DatasetService(store)), prefix="/v1")
    return TestClient(app)


def test_dataset_list_is_stably_paginated_and_uses_active_status() -> None:
    response = _client().get(
        "/v1/partition/datasets",
        params={"page": 2, "page_size": 1, "sort_by": "updated_at", "sort_order": "desc"},
    )
    assert response.status_code == 200
    assert set(response.json()) == {"items", "total", "page", "page_size"}
    assert response.json()["items"][0]["dataset_id"] == "dataset-a"

    active = _client().get("/v1/partition/datasets", params={"publish_status": "active"})
    assert active.status_code == 200
    assert [item["dataset_id"] for item in active.json()["items"]] == ["dataset-a"]
    assert _client().get("/v1/partition/datasets", params={"publish_status": "published"}).json()["total"] == 0


def test_tiles_default_to_current_and_reject_foreign_version() -> None:
    client = _client()
    current = client.get("/v1/partition/datasets/dataset-a/tiles")
    assert current.status_code == 200
    assert {item["output_version"] for item in current.json()["items"]} == {"ov-a-current"}
    foreign = client.get("/v1/partition/datasets/dataset-a/tiles", params={"output_version": "ov-b-current"})
    assert foreign.status_code == 404
    assert foreign.json()["detail"]["code"] == "partition_output_version_not_found"


def test_dataset_and_sort_errors_have_stable_codes() -> None:
    client = _client()
    assert client.get("/v1/partition/datasets/missing").json()["detail"]["code"] == "partition_dataset_not_found"
    response = client.get("/v1/partition/datasets", params={"sort_by": "raw_sql"})
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "invalid_sort"


def test_dataset_quality_collection_is_part_of_the_normalized_api() -> None:
    schema = _client().get("/openapi.json").json()["paths"]

    assert "/v1/partition/datasets/{dataset_id}/quality" in schema
