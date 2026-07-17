from contextlib import nullcontext

import pytest
from psycopg.errors import UniqueViolation

from cube_web.services.m6_ingest_contracts import CreateIngestRun, IngestSceneInput
from cube_web.services import m6_quality_ingest_bridge as bridge_module
from cube_web.services.m6_quality_ingest_bridge import (
    AutoIngestPolicy,
    DatasetAutoIngestState,
    InMemoryQualityIngestBridge,
    PartitionSceneOutput,
    _ensure_ingest_scene,
    _ensure_ingest_run,
    _insert_request,
    _require_m6_tables,
    plan_ingest_requests,
)
from cube_web.services.quality_worker import _m6_auto_ingest_enabled


def _dataset(
    *,
    status: str = "active",
    allowed: bool = True,
    output: str | None = "output-v2",
    allow_warn: bool = False,
):
    return DatasetAutoIngestState("dataset-a", status, allowed, output, allow_warn)


def _scene(
    scene_id: str,
    *,
    run: str = "partition-run-a",
    dataset_id: str = "dataset-a",
    output: str | None = "output-v2",
    status: str = "completed",
    batches: tuple[str, ...] = ("load-a",),
):
    return PartitionSceneOutput(run, scene_id, dataset_id, output, status, batches)


def _plan(quality_status: str, *, dataset=None, scenes=None, allow_warn: bool = False):
    return plan_ingest_requests(
        quality_run_id="quality-a",
        quality_status=quality_status,
        dataset=dataset or _dataset(),
        partition_scenes=scenes or (_scene("scene-a"),),
        policy=AutoIngestPolicy(allow_warn=allow_warn),
    )


def test_pass_creates_scene_level_requests_for_completed_current_output_only() -> None:
    requests = _plan(
        "pass",
        scenes=(
            _scene("scene-a", batches=("load-b", "load-a", "load-a")),
            _scene("scene-b", status="failed"),
            _scene("scene-c", output="old-output"),
            _scene("scene-d", dataset_id="dataset-other"),
            _scene("scene-e", run="partition-run-b", batches=("load-c",)),
        ),
    )

    assert [request.partition_run_id for request in requests] == ["partition-run-a", "partition-run-b"]
    assert [scene.scene_id for scene in requests[0].scenes] == ["scene-a"]
    assert requests[0].scenes[0].source_load_batch_ids == ("load-a", "load-b")
    assert requests[0].scenes[0].quality_run_id == "quality-a"


def test_terminal_gate_defaults_to_pass_and_warn_requires_explicit_policy() -> None:
    assert _plan("pass")
    assert _plan("warn") == ()
    assert _plan("warn", allow_warn=True)
    assert _plan("warn", dataset=_dataset(allow_warn=True))
    for status in ("fail", "error", "cancelled", "running", "pending"):
        assert _plan(status, allow_warn=True) == ()


def test_draft_archived_and_auto_ingest_disabled_datasets_are_blocked() -> None:
    assert _plan("pass", dataset=_dataset(status="draft")) == ()
    assert _plan("pass", dataset=_dataset(status="archived")) == ()
    assert _plan("pass", dataset=_dataset(allowed=False)) == ()
    assert _plan("pass", dataset=_dataset(output=None)) == ()


def test_repeated_dispatch_is_idempotent() -> None:
    bridge = InMemoryQualityIngestBridge()
    planned = _plan("pass", scenes=(_scene("scene-a"), _scene("scene-b")))

    first = bridge.dispatch(planned)
    second = bridge.dispatch(planned)

    assert second == first
    assert len(second) == 1
    assert len(second[0].scenes) == 2
    assert second[0].requested_by == "system:quality-gate"


class _FakeConnection:
    def transaction(self):
        return nullcontext()


class _DuplicateSceneCursor:
    connection = _FakeConnection()

    def __init__(self) -> None:
        self.owner_query = False

    def execute(self, sql, params) -> None:
        if sql.startswith("INSERT INTO ingest_run_scenes"):
            raise UniqueViolation("duplicate idempotency key")
        self.owner_query = True

    def fetchone(self):
        assert self.owner_query
        return {
            "ingest_run_id": "concurrent-run",
            "scene_id": "scene-a",
            "output_version": "output-v2",
            "dataset_id": "dataset-a",
        }


def test_concurrent_scene_owner_is_accepted_without_escaping_savepoint() -> None:
    request = CreateIngestRun(
        partition_run_id="partition-run-a",
        dataset_id="dataset-a",
        scenes=(IngestSceneInput(scene_id="scene-a", output_version="output-v2", quality_run_id="quality-a"),),
    )
    cursor = _DuplicateSceneCursor()

    inserted = _ensure_ingest_scene(
        cursor,
        ingest_run_id="ingest-run-a",
        request=request,
        scene=request.scenes[0],
        idempotency_key="scene-key-a",
    )

    assert inserted == "concurrent-run"


class _RequestCursor:
    connection = _FakeConnection()

    def execute(self, _sql, _params) -> None:
        return None

    def fetchall(self):
        return []


def test_all_concurrently_owned_scenes_roll_back_new_empty_parent(monkeypatch) -> None:
    request = CreateIngestRun(
        partition_run_id="partition-run-a",
        dataset_id="dataset-a",
        scenes=(IngestSceneInput(scene_id="scene-a", output_version="output-v2", quality_run_id="quality-a"),),
    )
    monkeypatch.setattr(bridge_module, "_ensure_ingest_run", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(bridge_module, "_ensure_ingest_scene", lambda *_args, **_kwargs: "concurrent-run")

    assert _insert_request(_RequestCursor(), request) == 0


class _TerminalRunCursor:
    connection = _FakeConnection()

    def __init__(self) -> None:
        self.owner_query = False

    def execute(self, sql, _params) -> None:
        if sql.startswith("INSERT INTO ingest_runs"):
            raise UniqueViolation("duplicate run")
        self.owner_query = True

    def fetchone(self):
        assert self.owner_query
        return {"partition_run_id": "partition-run-a", "dataset_id": "dataset-a", "status": "completed"}


def test_terminal_parent_cannot_receive_new_queued_scenes() -> None:
    request = CreateIngestRun(
        partition_run_id="partition-run-a",
        dataset_id="dataset-a",
        scenes=(IngestSceneInput(scene_id="scene-a", output_version="output-v2", quality_run_id="quality-a"),),
    )

    with pytest.raises(RuntimeError, match="cannot append scenes"):
        _ensure_ingest_run(_TerminalRunCursor(), ingest_run_id="ingest-run-a", request=request)


class _SchemaCursor:
    def __init__(self, rows) -> None:
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, _sql, _params) -> None:
        return None

    def fetchall(self):
        return self.rows


class _SchemaTransaction:
    def __init__(self, rows) -> None:
        self.rows = rows

    def cursor(self):
        return _SchemaCursor(self.rows)


def test_m6_primary_schema_gate_fails_closed_when_a_required_table_is_missing() -> None:
    transaction = _SchemaTransaction(
        [(name,)]
        for name in (
            "datasets",
            "load_batch_scenes",
            "partition_runs",
            "partition_run_scenes",
            "ingest_runs",
        )
    )

    try:
        _require_m6_tables(transaction)
    except RuntimeError as exc:
        assert "ingest_run_scenes" in str(exc)
    else:
        raise AssertionError("missing M6 tables must fail closed")


def test_quality_worker_only_enables_bridge_in_m6_primary(monkeypatch) -> None:
    for mode in ("legacy", "shadow", "m6-read"):
        monkeypatch.setenv("CUBE_WEB_M6_MODE", mode)
        assert _m6_auto_ingest_enabled() is False
    monkeypatch.setenv("CUBE_WEB_M6_MODE", "m6-primary")
    assert _m6_auto_ingest_enabled() is True
