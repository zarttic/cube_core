"""Unit tests for the direct logical chunk row writer (no database required)."""

from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest
from psycopg.errors import UniqueViolation

from cube_split.jobs.logical_row_writer import (
    TARGET_COLUMNS,
    WRITE_ORDER,
    group_chunk_rows,
    merge_temp_table,
    row_values,
    write_chunk_rows,
)


def _cell_row(output_id: str = "cell-1") -> dict:
    return {
        "output_id": output_id,
        "grid_type": "geohash",
        "grid_level": 4,
        "space_code": "u4pr",
        "topology_code": "geohash-topo-v1:u4pr",
        "bbox": [1.0, 2.0, 3.0, 4.0],
        "geometry": {"type": "Polygon", "coordinates": [[[1.0, 2.0]]]},
    }


def _tile_row(output_id: str = "tile-1") -> dict:
    return {
        "output_id": output_id,
        "source_asset_id": "asset-a",
        "band_code": "B04",
        "grid_type": "geohash",
        "grid_level": 4,
        "space_code": "u4pr",
        "topology_code": None,
        "time_bucket": "20260701",
        "tile_uri": "s3://cube/source/a.tif",
        "tile_kind": "logical_reference",
        "bbox": [1.0, 2.0, 3.0, 4.0],
    }


def _index_row(output_id: str = "index-1") -> dict:
    return {
        "output_id": output_id,
        "tile_output_id": None,
        "source_asset_id": "asset-a",
        "band_code": "B04",
        "acquisition_time": "2026-07-01T00:00:00Z",
        "time_bucket": "20260701",
        "grid_type": "geohash",
        "grid_level": 4,
        "space_code": "u4pr",
        "topology_code": None,
        "st_code": "u4pr-1",
        "value_ref_uri": "s3://cube/source/a.tif",
        "attributes": {"band_unit_id": "unit-a"},
    }


def test_group_chunk_rows_keeps_first_row_per_output_id() -> None:
    rows = [
        {"kind": "grid_cells", "row": _cell_row("cell-1")},
        {"kind": "grid_cells", "row": dict(_cell_row("cell-1"), space_code="later")},
        {"kind": "tiles", "row": _tile_row()},
    ]

    grouped = group_chunk_rows(rows)

    assert list(grouped) == ["grid_cells", "tiles"]
    assert list(grouped["grid_cells"]) == ["cell-1"]
    assert grouped["grid_cells"]["cell-1"]["space_code"] == "u4pr"


def test_group_chunk_rows_rejects_unknown_kind() -> None:
    with pytest.raises(ValueError, match="unsupported logical chunk row kind"):
        group_chunk_rows([{"kind": "entities", "row": {"output_id": "x"}}])


@pytest.mark.parametrize(
    ("kind", "row"),
    [("grid_cells", _cell_row()), ("tiles", _tile_row()), ("indexes", _index_row())],
)
def test_row_values_match_declared_columns_and_derive_level_name(kind: str, row: dict) -> None:
    _table, columns = TARGET_COLUMNS[kind]

    values = row_values(kind, row, "dataset-a", "version-a")

    assert len(values) == len(columns)
    mapped = dict(zip(columns, values))
    assert mapped["dataset_id"] == "dataset-a"
    assert mapped["output_version"] == "version-a"
    assert mapped["grid_level_name"] == "4"


def test_row_values_serialize_json_columns() -> None:
    _table, columns = TARGET_COLUMNS["grid_cells"]

    mapped = dict(zip(columns, row_values("grid_cells", _cell_row(), "dataset-a", "version-a")))

    assert mapped["bbox"] == "[1.0,2.0,3.0,4.0]"
    assert mapped["geometry"] == '{"type":"Polygon","coordinates":[[[1.0,2.0]]]}'


class _FakeCopy:
    def __init__(self, log: list) -> None:
        self.log = log
        self.rows: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def write_row(self, value: tuple) -> None:
        self.rows.append(value)

    def close(self) -> None:
        self.log.append(("copy", tuple(self.rows)))


class _FakeCursor:
    def __init__(self, log: list, unique_violations: int = 0) -> None:
        self.log = log
        self.rowcount = 0
        self._unique_violations = unique_violations

    def execute(self, sql: str, params=None) -> None:
        self.log.append(("execute", sql))
        if sql.startswith("INSERT INTO partition_"):
            if self._unique_violations > 0:
                self._unique_violations -= 1
                raise UniqueViolation("duplicate key value violates unique constraint")
            self.rowcount = 2

    def copy(self, sql: str) -> _FakeCopy:
        self.log.append(("copy_start", sql))
        return _FakeCopy(self.log)

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> bool:
        return False


class _FakeConnection:
    def __init__(self, log: list, unique_violations: int = 0) -> None:
        self.log = log
        self.autocommit = False
        self.rollbacks = 0
        self._cursor = _FakeCursor(log, unique_violations)

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def rollback(self) -> None:
        self.rollbacks += 1
        self.log.append(("rollback", ""))

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> bool:
        return False


def _install_fake_psycopg(monkeypatch, connection: _FakeConnection) -> None:
    module = SimpleNamespace(connect=lambda *_args, **_kwargs: connection)
    monkeypatch.setitem(sys.modules, "psycopg", module)


def test_write_chunk_rows_merges_in_foreign_key_order(monkeypatch) -> None:
    log: list = []
    connection = _FakeConnection(log)
    _install_fake_psycopg(monkeypatch, connection)

    inserted = write_chunk_rows(
        dsn="postgresql://ignored",
        dataset_id="dataset-a",
        output_version="version-a",
        rows=[
            {"kind": "indexes", "row": _index_row()},
            {"kind": "tiles", "row": _tile_row()},
            {"kind": "grid_cells", "row": _cell_row()},
        ],
    )

    assert connection.autocommit is True
    inserts = [sql for action, sql in log if action == "execute" and sql.startswith("INSERT INTO partition_")]
    assert [sql.split()[2] for sql in inserts] == ["partition_grid_cells", "partition_tiles", "partition_indexes"]
    assert inserted == {"grid_cells": 2, "tiles": 2, "indexes": 2}
    # rows are copied into a session TEMP table, never straight into the target table
    copies = [sql for action, sql in log if action == "copy_start"]
    assert [sql.split()[1] for sql in copies] == [
        "tmp_chunk_grid_cells", "tmp_chunk_tiles", "tmp_chunk_indexes",
    ]


def test_write_chunk_rows_skips_kinds_without_rows(monkeypatch) -> None:
    log: list = []
    _install_fake_psycopg(monkeypatch, _FakeConnection(log))

    inserted = write_chunk_rows(
        dsn="postgresql://ignored",
        dataset_id="dataset-a",
        output_version="version-a",
        rows=[{"kind": "tiles", "row": _tile_row()}],
    )

    assert inserted == {"grid_cells": 0, "tiles": 2, "indexes": 0}
    inserts = [sql for action, sql in log if action == "execute" and sql.startswith("INSERT INTO partition_")]
    assert [sql.split()[2] for sql in inserts] == ["partition_tiles"]


def test_write_chunk_rows_retries_on_unique_violation(monkeypatch) -> None:
    log: list = []
    connection = _FakeConnection(log, unique_violations=1)
    _install_fake_psycopg(monkeypatch, connection)

    inserted = write_chunk_rows(
        dsn="postgresql://ignored",
        dataset_id="dataset-a",
        output_version="version-a",
        rows=[{"kind": "tiles", "row": _tile_row()}],
    )

    assert connection.rollbacks == 1
    assert inserted["tiles"] == 2
    assert len([sql for action, sql in log if action == "execute" and sql.startswith("INSERT INTO partition_tiles")]) == 2


def test_merge_temp_table_gives_up_after_max_attempts() -> None:
    log: list = []
    connection = _FakeConnection(log, unique_violations=5)
    cursor = _FakeCursor(log, unique_violations=5)

    with pytest.raises(UniqueViolation):
        merge_temp_table(
            cursor, connection,
            table="partition_tiles", columns=("output_id",), temp_table="tmp_chunk_tiles",
            max_attempts=2,
        )

    assert connection.rollbacks == 1


def test_write_order_matches_target_tables() -> None:
    assert WRITE_ORDER == ("grid_cells", "tiles", "indexes")
    assert set(WRITE_ORDER) == set(TARGET_COLUMNS)
