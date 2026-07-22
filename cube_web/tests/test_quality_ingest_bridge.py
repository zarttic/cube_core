from contextlib import nullcontext
from uuid import UUID

import pytest
from psycopg.errors import UniqueViolation

from cube_web.services import quality_ingest_bridge as bridge_module
from cube_web.services.ingest_contracts import CreateIngestRun, IngestSceneInput
from cube_web.services.quality_ingest_bridge import (
    AutoIngestPolicy,
    DatasetAutoIngestState,
    InMemoryQualityIngestBridge,
    PartitionSceneOutput,
    _ensure_ingest_run,
    _ensure_ingest_scene,
    _insert_request,
    _require_domain_tables,
    create_ingest_runs_after_quality,
    plan_ingest_requests,
)


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
    bands: tuple[str, ...] | None = None,
):
    return PartitionSceneOutput(run, scene_id, dataset_id, output, status, batches, bands or (f"band-{scene_id}",))


def _plan(quality_status: str, *, dataset=None, scenes=None, allow_warn: bool = False):
    return plan_ingest_requests(
        quality_run_id="quality-a",
        quality_status=quality_status,
        dataset=dataset or _dataset(),
        partition_scenes=scenes or (_scene("scene-a"),),
        policy=AutoIngestPolicy(allow_warn=allow_warn),
    )


def test_pass_creates_band_level_requests_for_completed_current_output_only() -> None:
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


def test_manual_ingest_accepts_a_quality_approved_noncurrent_grid_output() -> None:
    requests = plan_ingest_requests(
        quality_run_id="quality-a",
        quality_status="pass",
        dataset=_dataset(output="mgrs-output"),
        partition_scenes=(_scene("scene-geohash", output="geohash-output"),),
        manual=True,
    )

    assert len(requests) == 1
    assert requests[0].scenes[0].output_version == "geohash-output"


def test_ingest_request_creates_one_request_per_band_unit() -> None:
    requests = _plan(
        "pass",
        scenes=(
            PartitionSceneOutput(
                "partition-run-a", "scene-a", "dataset-a", "output-v2", "completed",
                ("load-a",), ("band-a-b01", "band-a-b02"),
            ),
        ),
    )

    assert len(requests) == 2
    assert [request.scenes[0].band_unit_ids for request in requests] == [("band-a-b01",), ("band-a-b02",)]


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
    assert len(second) == 2
    assert all(len(request.scenes) == 1 for request in second)
    assert all(request.requested_by == "system:quality-gate" for request in second)


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
        scenes=(IngestSceneInput(scene_id="scene-a", output_version="output-v2", quality_run_id="quality-a", band_unit_ids=("band-scene-a",)),),
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
        scenes=(IngestSceneInput(scene_id="scene-a", output_version="output-v2", quality_run_id="quality-a", band_unit_ids=("band-scene-a",)),),
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
        scenes=(IngestSceneInput(scene_id="scene-a", output_version="output-v2", quality_run_id="quality-a", band_unit_ids=("band-scene-a",)),),
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


def test_domain_schema_gate_fails_closed_when_a_required_table_is_missing() -> None:
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
        _require_domain_tables(transaction)
    except RuntimeError as exc:
        assert "ingest_run_scenes" in str(exc)
    else:
        raise AssertionError("missing domain tables must fail closed")


class _CompletedPartitionGateCursor:
    rowcount = 0

    def __init__(self) -> None:
        self.rows = []
        self.row = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, sql, _params) -> None:
        normalized = " ".join(sql.split())
        if "information_schema.tables" in normalized:
            self.rows = [
                (name,)
                for name in (
                    "datasets", "load_batch_scenes", "partition_runs",
                    "partition_run_scenes", "ingest_runs", "ingest_run_scenes",
                )
            ]
            self.row = None
        elif normalized.startswith("SELECT dataset_id,status"):
            self.row = {
                "dataset_id": "dataset-a",
                "status": "active",
                "auto_ingest_allowed": True,
                "current_output_version": "output-v2",
                "attributes": {},
            }
            self.rows = []
        elif "FROM partition_run_scenes prs JOIN partition_runs pr" in normalized:
            assert "pr.status='completed'" in normalized
            assert "partial_failure" not in normalized
            self.rows = []
            self.row = None
        else:
            self.rows = []
            self.row = None

    def fetchone(self):
        return self.row

    def fetchall(self):
        return self.rows


class _CompletedPartitionGateTransaction:
    def __init__(self) -> None:
        self.gate_cursor = _CompletedPartitionGateCursor()

    def cursor(self, **_kwargs):
        return self.gate_cursor


def test_auto_ingest_requires_a_fully_completed_partition_run() -> None:
    created = create_ingest_runs_after_quality(
        _CompletedPartitionGateTransaction(),
        quality_run_id=UUID("00000000-0000-0000-0000-000000000001"),
        dataset_id="dataset-a",
        output_version="output-v2",
        quality_status="pass",
    )

    assert created == 0
