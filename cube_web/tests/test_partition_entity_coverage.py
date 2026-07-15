from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

from cube_web.routes.partition import create_partition_router
from cube_web.services import partition_runners
from cube_web.services.config_store import normalized_config
from cube_web.services.partition_contracts import StrictPartitionRequest
from cube_web.services.partition_job_store import InMemoryPartitionJobStore
from cube_web.services.partition_service import PartitionBackend, PartitionService, PartitionTask
from cube_web.services.partition_workflow import PartitionWorkflowService


class _SyncTaskStore:
    def __init__(self) -> None:
        self.tasks: dict[str, PartitionTask] = {}

    def submit(
        self,
        data_type: str,
        operation: str,
        runner,
        task_id: str | None = None,
        on_started=None,
        on_succeeded=None,
        on_failed=None,
        cancellation_check=None,
    ) -> PartitionTask:
        del cancellation_check
        now = time.time()
        task = PartitionTask(
            task_id=task_id or "partition-sync",
            status="running",
            data_type=data_type,
            operation=operation,
            created_at=now,
            updated_at=now,
        )
        self.tasks[task.task_id] = task
        if on_started is not None:
            on_started(task.task_id)
        try:
            result = runner()
        except Exception as exc:
            task.status = "failed"
            task.error = str(exc)
            task.updated_at = time.time()
            if on_failed is not None:
                on_failed(task.task_id, str(exc))
            return task
        task.status = "completed"
        task.result = result
        task.updated_at = time.time()
        if on_succeeded is not None:
            on_succeeded(task.task_id, result)
        return task

    def get(self, task_id: str) -> PartitionTask | None:
        return self.tasks.get(task_id)

    def cancel(self, task_id: str) -> PartitionTask | None:
        task = self.tasks.get(task_id)
        if task is not None:
            task.status = "cancelled"
            task.updated_at = time.time()
        return task


def _asset(source_uri: str, scene_id: str, data_type: str) -> dict[str, Any]:
    band = "product_value" if data_type == "product" else "vv"
    asset = {
        "source_uri": source_uri,
        "scene_id": scene_id,
        "acq_time": "2026-05-30T00:00:00Z",
        "bands": [band],
        "band": band,
        "corners": [[100.0, 27.0], [105.0, 27.0], [105.0, 23.0], [100.0, 23.0]],
        "resolution": 30 if data_type == "product" else 10,
        "sensor": "data_product" if data_type == "product" else "sentinel1_sar",
        "product_family": "product" if data_type == "product" else "sentinel1",
    }
    if data_type == "product":
        asset["product_name"] = "test_product"
        asset["product_year"] = 2026
    if data_type == "radar":
        asset["polarization"] = band
    return asset


def _runtime_payload(data_type: str, tmp_path: Path) -> dict[str, Any]:
    suffix = "tif" if data_type == "product" else "dat"
    payload = {
        "input_dir": str(tmp_path / "missing"),
        "selected_assets": [_asset(f"s3://cube/cube/source/{data_type}/entity-{data_type}.{suffix}", f"entity-{data_type}", data_type)],
        "grid_type": "isea4h",
        "partition_method": "entity",
        "grid_level": 2,
        "max_cells_per_asset": 9,
        "ray_parallelism": 3,
        "partition_backend": "thread",
        "ray_address": "local-ray:6379",
        "metadata_backend": "none",
        "postgres_dsn": "postgresql://example",
        "asset_storage_backend": "local",
        "minio_endpoint": "127.0.0.1:9000",
        "minio_access_key": "access",
        "minio_secret_key": "secret",
        "minio_bucket": "cube",
    }
    return payload


def test_web_time_granularity_matches_frozen_sdk_contract():
    payload = {
        "batch_id": "time-contract",
        "grid_type": "geohash",
        "requested_grid_level": 5,
        "partition_method": "logical",
        "time_granularity": "second",
        "datasets": [
            {
                "dataset_id": "dataset-time",
                "dataset_code": "dataset-time",
                "dataset_title": "Dataset time",
                "data_type": "optical",
                "assets": [
                    {
                        "source_asset_id": "asset-time",
                        "cog_uri": "s3://cube/loader/time.tif",
                        "checksum": "a" * 64,
                        "bbox": [100.0, 20.0, 101.0, 21.0],
                        "crs": "EPSG:4326",
                        "time_start": "2026-05-30T00:00:00Z",
                        "time_end": "2026-05-30T00:01:00Z",
                    }
                ],
                "bands": [
                    {
                        "source_asset_id": "asset-time",
                        "band_code": "B01",
                        "band_name": "Band 1",
                        "band_type": "spectral",
                        "display_order": 0,
                    }
                ],
            }
        ],
    }
    assert StrictPartitionRequest.model_validate(payload).time_granularity == "second"
    payload["time_granularity"] = "year"
    with pytest.raises(ValidationError):
        StrictPartitionRequest.model_validate(payload)

    config = normalized_config({"partition": {"optical": {"time_granularity": "second"}}})
    assert config["partition"]["optical"]["time_granularity"] == "second"
    with pytest.raises(ValueError, match="time_granularity"):
        normalized_config({"partition": {"optical": {"time_granularity": "year"}}})


def test_carbon_runner_rejects_annual_sdk_time_granularity(tmp_path: Path):
    with pytest.raises(ValueError, match="'year' is not a valid TimeGranularity"):
        partition_runners._run_carbon_partition_demo(
            payload={
                "input_dir": str(tmp_path),
                "grid_type": "isea4h",
                "grid_level": 5,
                "time_granularity": "year",
            }
        )


@pytest.mark.parametrize(
    ("data_type", "runner_name", "logical_target"),
    [
        ("product", "_run_product_partition_demo", "cube_split.jobs.product_partition_job.run_product_partition"),
        ("radar", "_run_radar_partition_demo", "cube_split.jobs.ray_logical_partition_job.run_logical_partition"),
    ],
)
def test_product_and_radar_run_payloads_dispatch_isea4h_to_entity_partition(
    monkeypatch,
    tmp_path: Path,
    data_type: str,
    runner_name: str,
    logical_target: str,
) -> None:
    captured: dict[str, Any] = {}

    def fake_run_entity_partition(args):
        captured.update(vars(args))
        run_dir = tmp_path / f"{data_type}-entity-run"
        run_dir.mkdir()
        rows_path = run_dir / "entity_index_rows.jsonl"
        rows_path.write_text("", encoding="utf-8")
        return {
            "status": "completed",
            "data_type": data_type,
            "partition_type": "entity",
            "partition_method": "entity",
            "run_dir": str(run_dir),
            "rows_path": str(rows_path),
            "total_index_rows": 0,
            "grid_type": args.grid_type,
            "grid_level": args.grid_level,
            "partition_backend_used": args.partition_backend,
            "execution_engine": args.partition_backend,
            "ray_parallelism": args.ray_parallelism,
            "ingest_enabled": False,
        }

    def fail_logical_partition(_args):
        raise AssertionError(f"{data_type} isea4h entity run should not use logical partition")

    monkeypatch.setattr("cube_split.jobs.entity_partition_job.run_entity_partition", fake_run_entity_partition)
    monkeypatch.setattr(logical_target, fail_logical_partition)
    monkeypatch.setattr(partition_runners, "optical_partition_defaults", lambda: {})

    result = getattr(partition_runners, runner_name)(_runtime_payload(data_type, tmp_path), mode="partition_run")

    assert result["mode"] == "partition_run"
    assert result["data_type"] == data_type
    assert result["partition_type"] == "entity"
    assert captured["data_type"] == data_type
    assert captured["grid_type"] == "isea4h"
    assert captured["grid_level"] == 2
    assert "target_pixels_per_hex_edge" not in captured
    assert captured["max_cells_per_asset"] == 9
    assert captured["ray_parallelism"] == 3
    assert captured["partition_backend"] == "thread"
    assert Path(captured["input_dir"]).name == "input"
    assert Path(captured["manifest_path"]).exists()


def test_partition_run_route_preserves_isea4h_entity_payload_options() -> None:
    captured: dict[str, Any] = {}

    def run_product(payload=None):
        captured.update(payload or {})
        return {
            "status": "completed",
            "data_type": "product",
            "partition_method": "entity",
            "partition_type": "entity",
            "grid_type": captured["grid_type"],
            "grid_level": captured["grid_level"],
            "rows": 0,
        }

    service = PartitionService(
        {"product": PartitionBackend(data_type="product", run=run_product)},
        task_store=_SyncTaskStore(),
    )
    workflow = PartitionWorkflowService(service, store=InMemoryPartitionJobStore())
    route_app = FastAPI()
    route_app.include_router(create_partition_router(service=service, workflow=workflow, legacy_service=service))
    client = TestClient(route_app)

    response = client.post(
        "/partition/product/run",
        json={
            "grid_type": "isea4h",
            "partition_method": "entity",
            "grid_level": 3,
            "max_cells_per_asset": 11,
            "ray_parallelism": 2,
            "partition_backend": "thread",
        },
    )

    assert response.status_code == 200
    assert response.json()["partition_type"] == "entity"
    assert captured["grid_type"] == "isea4h"
    assert captured["partition_method"] == "entity"
    assert captured["grid_level"] == 3
    assert captured["max_cells_per_asset"] == 11
    assert captured["ray_parallelism"] == 2
