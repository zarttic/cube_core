from __future__ import annotations

from cube_web.services.ingest_repository import OpenGaussIngestRepository
from cube_web.services.partition_domain_schema import (
    backfill_partition_tile_publication_status,
    count_partition_tile_publication_backfill_candidates,
)


class _Cursor:
    def __init__(self, *, count: int = 0, rowcount: int = 0) -> None:
        self.count = count
        self.rowcount = rowcount
        self.calls: list[tuple[str, tuple[object, ...] | None]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def execute(self, statement: str, params: tuple[object, ...] | None = None) -> None:
        self.calls.append((statement, params))

    def fetchone(self):
        return ("running", "claim-token") if "SELECT status,provenance" in self.calls[-1][0] else (self.count,)


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self.cursor_value = cursor
        self.committed = False

    def cursor(self):
        return self.cursor_value

    def commit(self) -> None:
        self.committed = True


def test_completed_manual_ingest_publishes_only_matching_ready_tiles() -> None:
    cursor = _Cursor()
    connection = _Connection(cursor)
    repository = object.__new__(OpenGaussIngestRepository)
    repository._refresh = lambda *_args: None

    repository.complete_claimed_output(
        connection,
        ({
            "ingest_run_id": "ingest-a",
            "scene_id": "scene-a",
            "dataset_id": "dataset-a",
            "output_version": "version-a",
            "band_unit_ids": ["band-unit-a"],
        },),
        "claim-token",
    )

    publication_call = next(call for call in cursor.calls if "SET publication_status='published'" in call[0])
    assert "tile.status='ready'" in publication_call[0]
    assert "band.scene_id=%s AND band.band_unit_id=ANY(%s)" in publication_call[0]
    assert "unit.quality_status IN ('pass','warn')" in publication_call[0]
    assert "unit.ingest_status='completed'" in publication_call[0]
    assert publication_call[1] == ("dataset-a", "version-a", "scene-a", ["band-unit-a"])


def test_publication_backfill_only_selects_quality_approved_completed_ingest() -> None:
    cursor = _Cursor(count=4, rowcount=3)
    connection = _Connection(cursor)

    assert count_partition_tile_publication_backfill_candidates(connection) == 4
    assert backfill_partition_tile_publication_status(connection) == 3
    assert connection.committed is True

    candidate_sql = cursor.calls[0][0]
    update_sql = cursor.calls[1][0]
    assert "unit.quality_status IN ('pass','warn')" in candidate_sql
    assert "unit.ingest_status='completed'" in candidate_sql
    assert "tile.publication_status='pending'" in candidate_sql
    assert "tile.publication_status='pending'" in update_sql
    assert "SET publication_status='published'" in update_sql
