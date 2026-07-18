from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from cube_web.services.ingest_contracts import CreateIngestRun, IngestSceneInput
from cube_web.services.ingest_repository import (
    IngestConflict,
    InMemoryIngestRepository,
    InvalidIngestTransition,
    OpenGaussIngestRepository,
    scene_idempotency_key,
)
from cube_web.services.ingest_service import IngestRunService, QualityGateRejected


def request(*scene_ids: str) -> CreateIngestRun:
    return CreateIngestRun(
        partition_run_id="partition-run-1",
        dataset_id="dataset-a",
        requested_by="tester",
        scenes=tuple(
            IngestSceneInput(
                scene_id=scene_id,
                output_version="output-v1",
                quality_run_id=f"quality-{scene_id}",
                source_load_batch_ids=("load-batch-1",),
            )
            for scene_id in scene_ids
        ),
    )


def service(*scene_ids: str) -> IngestRunService:
    return IngestRunService(InMemoryIngestRepository({scene_id: "dataset-a" for scene_id in scene_ids}))


def test_quality_gate_and_generated_run_id() -> None:
    svc = service("scene-a")
    with pytest.raises(QualityGateRejected):
        svc.schedule_after_quality(request("scene-a"), quality_passed=False)
    run = svc.schedule_after_quality(request("scene-a"), quality_passed=True)
    assert run.ingest_run_id.startswith("ingest-run-")
    assert run.status == "queued"
    assert run.scenes[0].source_load_batch_ids == ("load-batch-1",)


def test_scene_level_partial_failure_retry_and_idempotent_completion() -> None:
    repository = InMemoryIngestRepository({"scene-a": "dataset-a", "scene-b": "dataset-a"})
    svc = IngestRunService(repository)
    run = svc.schedule_after_quality(request("scene-a", "scene-b"), quality_passed=True)
    svc.start_scene(run.ingest_run_id, "scene-a")
    svc.complete_scene(run.ingest_run_id, "scene-a")
    svc.start_scene(run.ingest_run_id, "scene-b")
    failed = svc.fail_scene(run.ingest_run_id, "scene-b", "tile index unavailable")
    assert failed.status == "partial_failure"
    retried = svc.retry_failed(run.ingest_run_id)
    assert retried.status == "queued"
    assert {scene.status for scene in retried.scenes} == {"completed", "queued"}
    svc.start_scene(run.ingest_run_id, "scene-b")
    completed = svc.complete_scene(run.ingest_run_id, "scene-b")
    assert completed.status == "completed"
    assert repository.scene_statuses == {"scene-a": "available", "scene-b": "available"}
    assert svc.complete_scene(run.ingest_run_id, "scene-b").status == "completed"


def test_retry_can_target_one_failed_scene_and_list_by_dataset() -> None:
    svc = service("scene-a", "scene-b")
    run = svc.schedule_after_quality(request("scene-a", "scene-b"), quality_passed=True)
    for scene_id in ("scene-a", "scene-b"):
        svc.start_scene(run.ingest_run_id, scene_id)
        svc.fail_scene(run.ingest_run_id, scene_id, f"failed {scene_id}")
    retried = svc.retry_failed(run.ingest_run_id, ("scene-a",))
    assert {scene.scene_id: scene.status for scene in retried.scenes} == {"scene-a": "queued", "scene-b": "failed"}
    assert svc.list_runs(dataset_id="dataset-a").items[0].ingest_run_id == run.ingest_run_id
    assert svc.list_runs(dataset_id="dataset-other").items == ()
    scene_a = next(scene for scene in retried.scenes if scene.scene_id == "scene-a")
    assert scene_a.retry_history[0].error_message == "failed scene-a"
    assert scene_a.retry_history[0].retried_by == "system"


def test_failed_scene_without_error_message_can_be_retried_and_read() -> None:
    repository = InMemoryIngestRepository({"scene-a": "dataset-a"})
    svc = IngestRunService(repository)
    run = svc.schedule_after_quality(request("scene-a"), quality_passed=True)
    repository.runs[run.ingest_run_id]["scenes"]["scene-a"].update(status="failed", error_message=None)
    repository.runs[run.ingest_run_id]["status"] = "failed"
    retried = svc.retry_failed(run.ingest_run_id, ("scene-a",))
    assert retried.scenes[0].retry_history[0].error_message is None
    assert svc.get(run.ingest_run_id).scenes[0].status == "queued"


def test_redelivered_scene_output_returns_original_run_without_mutation() -> None:
    svc = service("scene-a")
    first = svc.schedule_after_quality(request("scene-a"), quality_passed=True)
    redelivered = svc.schedule_after_quality(request("scene-a"), quality_passed=True)
    assert redelivered.ingest_run_id == first.ingest_run_id
    assert svc.get(first.ingest_run_id).scenes[0].status == "queued"
    assert scene_idempotency_key("dataset-a", "scene-a", "output-v1")


def test_partially_overlapping_scene_outputs_are_rejected() -> None:
    svc = service("scene-a", "scene-b")
    svc.schedule_after_quality(request("scene-a"), quality_passed=True)
    with pytest.raises(IngestConflict):
        svc.schedule_after_quality(request("scene-a", "scene-b"), quality_passed=True)


def test_cancel_preserves_completed_scene_and_rejects_terminal_cancel() -> None:
    svc = service("scene-a", "scene-b")
    run = svc.schedule_after_quality(request("scene-a", "scene-b"), quality_passed=True)
    svc.start_scene(run.ingest_run_id, "scene-a")
    svc.complete_scene(run.ingest_run_id, "scene-a")
    cancelled = svc.cancel(run.ingest_run_id)
    assert cancelled.status == "cancelled"
    assert {scene.scene_id: scene.status for scene in cancelled.scenes} == {"scene-a": "completed", "scene-b": "cancelled"}
    with pytest.raises(InvalidIngestTransition):
        svc.cancel(run.ingest_run_id)


def test_scene_must_belong_to_dataset() -> None:
    svc = service("scene-a")
    bad = IngestRunService(InMemoryIngestRepository({"scene-a": "dataset-other"}))
    with pytest.raises(IngestConflict):
        bad.schedule_after_quality(request("scene-a"), quality_passed=True)
    with pytest.raises(IngestConflict):
        svc.schedule_after_quality(request("scene-a", "scene-a"), quality_passed=True)


def test_runtime_credentials_are_not_part_of_ingest_contract() -> None:
    payload = request("scene-a").model_dump()
    payload["minio_secret_key"] = "must-not-enter-domain-payload"
    with pytest.raises(ValueError):
        CreateIngestRun.model_validate(payload)


def test_concurrent_scene_completion_reaches_completed_run() -> None:
    svc = service("scene-a", "scene-b")
    run = svc.schedule_after_quality(request("scene-a", "scene-b"), quality_passed=True)
    for scene_id in ("scene-a", "scene-b"):
        svc.start_scene(run.ingest_run_id, scene_id)
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda scene_id: svc.complete_scene(run.ingest_run_id, scene_id), ("scene-a", "scene-b")))
    assert svc.get(run.ingest_run_id).status == "completed"


class _RecordingCursor:
    def __init__(self, *, scene_status: str = "running", update_count: int = 1) -> None:
        self.scene_status = scene_status
        self.update_count = update_count
        self.statements: list[str] = []
        self.rowcount = 0
        self._one = None
        self._all = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, sql, _params=()):
        self.statements.append(sql)
        self.rowcount = 0
        self._one = None
        self._all = []
        if sql.startswith("SELECT status FROM ingest_runs"):
            self._one = ("running",)
        elif sql.startswith("UPDATE ingest_run_scenes"):
            self.rowcount = self.update_count
        elif sql.startswith("SELECT status FROM ingest_run_scenes"):
            self._one = (self.scene_status,)
        elif sql.startswith("SELECT status,error_message"):
            self._all = [("completed", None)]

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all


class _RecordingConnection:
    def __init__(self, cursor: _RecordingCursor) -> None:
        self.recording_cursor = cursor

    def cursor(self, **_kwargs):
        return self.recording_cursor


class _RecordingContext:
    def __init__(self, connection: _RecordingConnection) -> None:
        self.connection_value = connection

    def __enter__(self):
        return self.connection_value

    def __exit__(self, *_args):
        return None


class _RecordingPool:
    def __init__(self, cursor: _RecordingCursor) -> None:
        self.connection_value = _RecordingConnection(cursor)

    def connection(self):
        return _RecordingContext(self.connection_value)


def _sql_repository(cursor: _RecordingCursor) -> OpenGaussIngestRepository:
    repository = object.__new__(OpenGaussIngestRepository)
    repository.pool = _RecordingPool(cursor)
    repository.get = lambda _run_id: "persisted-run"  # type: ignore[method-assign]
    return repository


def test_open_gauss_mutations_lock_run_before_scene_and_replay_completed_start() -> None:
    finish_cursor = _RecordingCursor()
    assert _sql_repository(finish_cursor).complete_scene("run-a", "scene-a") == "persisted-run"
    assert finish_cursor.statements[0].startswith("SELECT status FROM ingest_runs")
    assert finish_cursor.statements[1].startswith("UPDATE ingest_run_scenes")

    replay_cursor = _RecordingCursor(scene_status="completed", update_count=0)
    assert _sql_repository(replay_cursor).start_scene("run-a", "scene-a") == "persisted-run"
    assert replay_cursor.statements[0].startswith("SELECT status FROM ingest_runs")
    assert not any(sql.startswith("UPDATE ingest_runs") for sql in replay_cursor.statements)
