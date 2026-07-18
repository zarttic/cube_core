from __future__ import annotations

import json
import sys
from types import SimpleNamespace

import pytest
from grid_core.sdk import CubeEncoderSDK

from cube_split.scripts import migrate_cube_cell_geom as migration


class FakeCursor:
    def __init__(self, *, fetchone=None, fetchall=None, rowcount: int = 0):
        self.calls: list[tuple[str, tuple | None]] = []
        self._fetchone = fetchone
        self._fetchall = fetchall or []
        self.rowcount = rowcount

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.calls.append((sql, tuple(params) if params is not None else None))

    def fetchone(self):
        return self._fetchone

    def fetchall(self):
        return self._fetchall


class FakeConnection:
    def __init__(self, cursors: list[FakeCursor]):
        self.cursors = list(cursors)
        self.used: list[FakeCursor] = []
        self.commits = 0
        self.rollbacks = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        cursor = self.cursors.pop(0)
        self.used.append(cursor)
        return cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_backfill_reconstructs_historical_mgrs_without_topology_code():
    cell = CubeEncoderSDK().locate(grid_type="mgrs", requested_grid_level=3, point=[116.3, 39.9])
    cursor = FakeCursor(rowcount=2)
    conn = FakeConnection([cursor])

    updated = migration.backfill(conn, [("mgrs", cell.grid_level, cell.space_code)])

    assert updated == 2
    sql, params = cursor.calls[0]
    geometry = json.loads(params[0])
    assert "ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)" in sql
    assert params[1:] == ("mgrs", cell.grid_level, cell.space_code)
    assert len(geometry["coordinates"][0]) == 5
    assert geometry["coordinates"][0][0] == geometry["coordinates"][0][-1]


def test_preview_reads_and_rolls_back_without_schema_or_data_writes(monkeypatch):
    column_cursor = FakeCursor(fetchone=(False,))
    cells_cursor = FakeCursor(fetchall=[("geohash", 5, "wx4dy")])
    conn = FakeConnection([column_cursor, cells_cursor])
    monkeypatch.setattr(migration, "parse_args", lambda: SimpleNamespace(postgres_dsn="configured", execute=False))
    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=lambda *args, **kwargs: conn))

    assert migration.main() == 0

    all_sql = "\n".join(sql for cursor in conn.used for sql, _ in cursor.calls)
    assert "ALTER TABLE" not in all_sql
    assert "UPDATE rs_cube_cell_fact" not in all_sql
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_execute_rolls_back_additive_alter_when_backfill_fails(monkeypatch):
    alter_cursor = FakeCursor()
    conn = FakeConnection([alter_cursor])
    monkeypatch.setattr(migration, "parse_args", lambda: SimpleNamespace(postgres_dsn="configured", execute=True))
    monkeypatch.setattr(migration, "_column_exists", lambda _conn: False)
    monkeypatch.setattr(migration, "_missing_cells", lambda _conn, column_exists: [("isea4h", 2, "39")])
    monkeypatch.setattr(migration, "backfill", lambda _conn, _cells: (_ for _ in ()).throw(ValueError("bad cell")))
    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=lambda *args, **kwargs: conn))

    with pytest.raises(ValueError, match="bad cell"):
        migration.main()

    assert "ADD COLUMN IF NOT EXISTS cell_geom geometry(Polygon, 4326)" in alter_cursor.calls[0][0]
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_validate_rejects_wrong_polygon_point_count():
    conn = FakeConnection([FakeCursor(fetchone=(0, 0, 1))])

    with pytest.raises(RuntimeError, match="wrong_point_count.*1"):
        migration.validate(conn)


def test_validate_allows_variable_mgrs_boundary_point_count():
    cursor = FakeCursor(fetchone=(0, 0, 0))
    conn = FakeConnection([cursor])

    assert migration.validate(conn) == {"missing": 0, "invalid": 0, "wrong_point_count": 0}
    assert "grid_type = 'mgrs' AND ST_NPoints(ST_ExteriorRing(cell_geom)) < 4" in cursor.calls[0][0]
