from __future__ import annotations

from cube_web.services.ingest_worker import _verify_partition_output, process_queued_ingest_scenes


class _Repository:
    def __init__(self) -> None:
        self.completed = []
        self.failed = []

    def claim_queued_outputs(self, *, limit):
        assert limit == 10
        return ({
            "dataset_id": "dataset-a",
            "output_version": "v1",
            "items": (
                {"ingest_run_id": "run-a", "scene_id": "scene-ok", "dataset_id": "dataset-a", "output_version": "v1", "band_unit_ids": ["band-ok"]},
                {"ingest_run_id": "run-a", "scene_id": "scene-bad", "dataset_id": "dataset-a", "output_version": "v1", "band_unit_ids": ["band-bad"]},
            ),
        },)

    def complete_scene(self, ingest_run_id, scene_id):
        self.completed.append((ingest_run_id, scene_id))

    def fail_scene(self, ingest_run_id, scene_id, error_message):
        self.failed.append((ingest_run_id, scene_id, error_message))


def test_worker_fails_only_the_invalid_band_unit() -> None:
    repository = _Repository()

    def verify(_repository, item):
        if item["scene_id"] == "scene-bad":
            raise RuntimeError("output is stale")
        return "dataset-a"

    completed = process_queued_ingest_scenes(
        repository=repository,
        verifier=verify,
        executor=lambda *_args, **_kwargs: None,
    )

    assert completed == 1
    assert repository.completed == [("run-a", "scene-ok")]
    assert repository.failed == [
        ("run-a", "scene-bad", "output is stale"),
    ]


def test_worker_executes_each_band_unit_independently() -> None:
    repository = _Repository()
    repository.claim_queued_outputs = lambda **_kwargs: ({
        "dataset_id": "dataset-a",
        "output_version": "v1",
        "items": (
            {"ingest_run_id": "run-a", "scene_id": "scene-a", "dataset_id": "dataset-a", "output_version": "v1", "band_unit_ids": ["band-a"]},
            {"ingest_run_id": "run-a", "scene_id": "scene-b", "dataset_id": "dataset-a", "output_version": "v1", "band_unit_ids": ["band-b"]},
        ),
    },)
    calls = []
    completed = process_queued_ingest_scenes(
        repository=repository,
        verifier=lambda *_args: "partition-dataset-a",
        executor=lambda *_args, **kwargs: calls.append(kwargs),
    )
    assert completed == 2
    assert len(calls) == 2
    assert calls[0]["scene_ids"] == ("scene-a",)
    assert calls[0]["band_unit_ids"] == ("band-a",)
    assert calls[1]["scene_ids"] == ("scene-b",)
    assert calls[1]["band_unit_ids"] == ("band-b",)
    assert repository.completed == [("run-a", "scene-a"), ("run-a", "scene-b")]


def test_worker_never_completes_scenes_when_rs_merge_fails() -> None:
    repository = _Repository()
    repository.claim_queued_outputs = lambda **_kwargs: ({
        "dataset_id": "dataset-a", "output_version": "v1",
        "items": ({"ingest_run_id": "run-a", "scene_id": "scene-a", "dataset_id": "dataset-a", "output_version": "v1", "band_unit_ids": ["band-a"]},),
    },)

    def fail(*_args, **_kwargs):
        raise RuntimeError("RS MERGE failed")

    assert process_queued_ingest_scenes(repository=repository, verifier=lambda *_args: "partition-dataset-a", executor=fail) == 0
    assert repository.completed == []
    assert repository.failed == [("run-a", "scene-a", "RS MERGE failed")]


class _VerifyCursor:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.rows = iter((
            {
                "output_dataset_id": "dataset-a",
                "scene_partition_status": "completed",
                "scene_output_version": "geohash-output",
            },
            {"status": "completed"},
            {"index_count": 1, "tile_count": 1, "grid_cell_count": 1},
        ))

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, sql, _params):
        self.queries.append(sql)

    def fetchone(self):
        return next(self.rows)


class _VerifyConnection:
    def __init__(self) -> None:
        self.cursor_value = _VerifyCursor()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def cursor(self, **_kwargs):
        return self.cursor_value


class _VerifyPool:
    def __init__(self) -> None:
        self.connection_value = _VerifyConnection()

    def connection(self):
        return self.connection_value


class _VerifyRepository:
    def __init__(self) -> None:
        self.pool = _VerifyPool()


def test_worker_accepts_completed_output_from_another_grid() -> None:
    repository = _VerifyRepository()

    output_dataset_id = _verify_partition_output(repository, {
        "dataset_id": "dataset-a",
        "scene_id": "scene-a",
        "output_version": "geohash-output",
    })

    assert output_dataset_id == "dataset-a"
    assert "current_output_version" not in repository.pool.connection_value.cursor_value.queries[0]
