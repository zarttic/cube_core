from threading import Event, Lock, Thread
from types import SimpleNamespace
from uuid import uuid4

from cube_web.services.config_store import default_config
from cube_web.services.quality_rules import QualityFinding
from cube_web.services.quality_worker import QualityRuntime, _errors_from_findings, _safe_execution_error, execute_quality_run
from cube_web.services.quality_repository import QualityLease, StaleQualityLease


def test_safe_execution_error_does_not_persist_exception_message() -> None:
    error = OSError("s3://access:secret@minio/private.tif")

    persisted = _safe_execution_error(error, "quality rule execution failed")

    assert persisted == "quality rule execution failed (OSError)"
    assert "secret" not in persisted
    assert "s3://" not in persisted


def test_auto_ingest_after_quality_is_disabled_by_default() -> None:
    assert default_config()["ingest"]["optical"]["auto_after_quality"] is False


def test_worker_preserves_all_finding_identity_fields_when_persisting_errors() -> None:
    run_id = uuid4()
    finding = QualityFinding(
        error_code="invalid_bbox",
        message="bounds are invalid",
        source_asset_id="asset-1",
        tile_id="tile-1",
        index_id="index-1",
        output_id="output-1",
        row_number=7,
        field="bbox",
        context={"west": 181},
    )

    (error,) = _errors_from_findings(run_id, "cell_bbox_validity", (finding,))

    assert error.quality_run_id == run_id
    assert error.rule_code == "cell_bbox_validity"
    assert error.error_code == "invalid_bbox"
    assert error.source_asset_id == "asset-1"
    assert error.tile_id == "tile-1"
    assert error.index_id == "index-1"
    assert error.output_id == "output-1"
    assert error.row_number == 7
    assert error.field == "bbox"
    assert error.context == {"west": 181}


class _QualityTransaction:
    def cursor(self, **_kwargs):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, _sql, _params=None) -> None:
        return None

    def fetchone(self):
        return {"data_type": "optical", "product_type": None}


class _QualityStore:
    def __init__(self, tx: _QualityTransaction) -> None:
        self.tx = tx

    def transaction(self):
        return self.tx


def test_current_quality_completion_enqueues_auto_ingest_only_when_enabled(monkeypatch) -> None:
    tx = _QualityTransaction()
    lease = QualityLease(uuid4(), "quality-worker", 1)
    snapshot = SimpleNamespace(code="rule", implementation_version="v1", mandatory=True)
    run = SimpleNamespace(dataset_id="dataset-a", output_version="output-v1", rule_snapshot=(snapshot,))
    rule = SimpleNamespace(implementation_version="v1", evaluate=lambda _context: ())
    created: list[dict] = []

    monkeypatch.setattr("cube_web.services.quality_worker.require_open_gauss_domain_store", lambda: _QualityStore(tx))
    monkeypatch.setattr("cube_web.services.quality_worker.start_quality_run", lambda *_args, **_kwargs: run)
    monkeypatch.setattr("cube_web.services.quality_worker.default_rule_registry", lambda: SimpleNamespace(get=lambda _code: rule))
    monkeypatch.setattr("cube_web.services.quality_worker.quality_object_reader", lambda: object())
    monkeypatch.setattr("cube_web.services.quality_worker._write_findings", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("cube_web.services.quality_worker.finish_quality_result", lambda _tx, *, result: result)
    monkeypatch.setattr("cube_web.services.quality_worker.assert_quality_result_totals", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("cube_web.services.quality_worker.reduce_quality_status", lambda *_args: "pass")
    monkeypatch.setattr("cube_web.services.quality_worker.complete_quality_run_if_current", lambda *_args, **_kwargs: True)
    monkeypatch.setattr("cube_web.services.quality_worker.auto_ingest_after_quality_enabled", lambda: True)
    monkeypatch.setattr(
        "cube_web.services.quality_worker.create_ingest_runs_after_quality",
        lambda _tx, **kwargs: created.append(kwargs) or 1,
    )

    execute_quality_run(lease)

    assert created == [{
        "quality_run_id": lease.quality_run_id,
        "dataset_id": "dataset-a",
        "output_version": "output-v1",
        "quality_status": "pass",
    }]


def test_current_quality_completion_does_not_enqueue_ingest_by_default(monkeypatch) -> None:
    tx = _QualityTransaction()
    lease = QualityLease(uuid4(), "quality-worker", 1)
    snapshot = SimpleNamespace(code="rule", implementation_version="v1", mandatory=True)
    run = SimpleNamespace(dataset_id="dataset-a", output_version="output-v1", rule_snapshot=(snapshot,))
    rule = SimpleNamespace(implementation_version="v1", evaluate=lambda _context: ())

    monkeypatch.setattr("cube_web.services.quality_worker.require_open_gauss_domain_store", lambda: _QualityStore(tx))
    monkeypatch.setattr("cube_web.services.quality_worker.start_quality_run", lambda *_args, **_kwargs: run)
    monkeypatch.setattr("cube_web.services.quality_worker.default_rule_registry", lambda: SimpleNamespace(get=lambda _code: rule))
    monkeypatch.setattr("cube_web.services.quality_worker.quality_object_reader", lambda: object())
    monkeypatch.setattr("cube_web.services.quality_worker._write_findings", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("cube_web.services.quality_worker.finish_quality_result", lambda _tx, *, result: result)
    monkeypatch.setattr("cube_web.services.quality_worker.assert_quality_result_totals", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("cube_web.services.quality_worker.reduce_quality_status", lambda *_args: "pass")
    monkeypatch.setattr("cube_web.services.quality_worker.complete_quality_run_if_current", lambda *_args, **_kwargs: True)
    monkeypatch.setattr("cube_web.services.quality_worker.auto_ingest_after_quality_enabled", lambda: False)
    monkeypatch.setattr(
        "cube_web.services.quality_worker.create_ingest_runs_after_quality",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("quality completion queued ingest")),
    )

    execute_quality_run(lease)


def test_stale_quality_completion_does_not_enqueue_auto_ingest(monkeypatch) -> None:
    tx = _QualityTransaction()
    lease = QualityLease(uuid4(), "quality-worker", 1)
    snapshot = SimpleNamespace(code="rule", implementation_version="v1", mandatory=True)
    run = SimpleNamespace(dataset_id="dataset-a", output_version="output-v1", rule_snapshot=(snapshot,))
    rule = SimpleNamespace(implementation_version="v1", evaluate=lambda _context: ())

    monkeypatch.setattr("cube_web.services.quality_worker.require_open_gauss_domain_store", lambda: _QualityStore(tx))
    monkeypatch.setattr("cube_web.services.quality_worker.start_quality_run", lambda *_args, **_kwargs: run)
    monkeypatch.setattr("cube_web.services.quality_worker.default_rule_registry", lambda: SimpleNamespace(get=lambda _code: rule))
    monkeypatch.setattr("cube_web.services.quality_worker.quality_object_reader", lambda: object())
    monkeypatch.setattr("cube_web.services.quality_worker._write_findings", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("cube_web.services.quality_worker.finish_quality_result", lambda _tx, *, result: result)
    monkeypatch.setattr("cube_web.services.quality_worker.assert_quality_result_totals", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("cube_web.services.quality_worker.reduce_quality_status", lambda *_args: "pass")
    monkeypatch.setattr("cube_web.services.quality_worker.complete_quality_run_if_current", lambda *_args, **_kwargs: False)
    monkeypatch.setattr("cube_web.services.quality_worker.auto_ingest_after_quality_enabled", lambda: True)
    monkeypatch.setattr(
        "cube_web.services.quality_worker.create_ingest_runs_after_quality",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("stale run queued ingest")),
    )

    execute_quality_run(lease)


def test_runtime_continues_after_a_stale_lease(monkeypatch) -> None:
    runtime = QualityRuntime()
    first = QualityLease(uuid4(), "quality-worker", 1)
    second = QualityLease(uuid4(), "quality-worker", 1)
    seen = []

    def execute(lease):
        seen.append(lease.quality_run_id)
        if lease == first:
            raise StaleQualityLease(str(lease.quality_run_id))

    monkeypatch.setattr("cube_web.services.quality_worker.execute_quality_run", execute)

    runtime._execute_claimed_runs([first, second])

    assert seen == [first.quality_run_id, second.quality_run_id]


def test_runtime_executes_independent_leases_with_bounded_parallelism(monkeypatch) -> None:
    runtime = QualityRuntime(execution_workers=2)
    leases = [QualityLease(uuid4(), "quality-worker", 1) for _ in range(3)]
    first_batch_started = Event()
    release_first_batch = Event()
    lock = Lock()
    active = 0
    peak = 0
    seen = []

    def execute(lease):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            seen.append(lease.quality_run_id)
            if len(seen) == 2:
                first_batch_started.set()
        if len(seen) <= 2:
            assert release_first_batch.wait(timeout=1)
        with lock:
            active -= 1

    monkeypatch.setattr("cube_web.services.quality_worker.execute_quality_run", execute)
    thread = Thread(target=runtime._execute_claimed_runs, args=(leases,))
    thread.start()
    assert first_batch_started.wait(timeout=1)
    release_first_batch.set()
    thread.join(timeout=1)

    assert not thread.is_alive()
    assert peak == 2
    assert set(seen) == {lease.quality_run_id for lease in leases}
