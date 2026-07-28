from __future__ import annotations

import pytest

from cube_web.services.db_pool import _acquire_timeout_seconds, _default_max_size, _PostgresPool


class _FakeConn:
    closed = False

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _isolated_pools(monkeypatch):
    monkeypatch.setattr(_PostgresPool, "_pools", {})
    monkeypatch.setattr(_PostgresPool, "_new_conn", lambda self: _FakeConn())


def test_pool_default_max_size_env_override(monkeypatch):
    monkeypatch.delenv("CUBE_WEB_PG_POOL_SIZE", raising=False)
    assert _default_max_size() == 8

    monkeypatch.setenv("CUBE_WEB_PG_POOL_SIZE", "3")
    assert _default_max_size() == 3
    assert _PostgresPool.for_dsn("dsn-a")._max_size == 3

    monkeypatch.setenv("CUBE_WEB_PG_POOL_SIZE", "0")
    assert _default_max_size() == 1

    monkeypatch.setenv("CUBE_WEB_PG_POOL_SIZE", "not-a-number")
    assert _default_max_size() == 8


def test_pool_configuration_reads_runtime_env_file(monkeypatch, tmp_path):
    env_file = tmp_path / "cube_web.env"
    env_file.write_text("CUBE_WEB_PG_POOL_SIZE=3\nCUBE_WEB_PG_POOL_ACQUIRE_TIMEOUT_SECONDS=11\n", encoding="utf-8")
    monkeypatch.delenv("CUBE_WEB_PG_POOL_SIZE", raising=False)
    monkeypatch.delenv("CUBE_WEB_PG_POOL_ACQUIRE_TIMEOUT_SECONDS", raising=False)
    monkeypatch.setenv("CUBE_WEB_ENV_FILE", str(env_file))

    assert _default_max_size() == 3
    assert _acquire_timeout_seconds() == 11.0


def test_pool_exhaustion_raises_runtime_error_after_timeout(monkeypatch):
    monkeypatch.setenv("CUBE_WEB_PG_POOL_ACQUIRE_TIMEOUT_SECONDS", "1")
    assert _acquire_timeout_seconds() == 1.0
    # Speed the test up beyond the env-enforced minimum of one second.
    monkeypatch.setattr("cube_web.services.db_pool._acquire_timeout_seconds", lambda: 0.05)

    pool = _PostgresPool("dsn-b", min_size=0, max_size=2)
    first = pool._acquire()
    second = pool._acquire()
    assert isinstance(first, _FakeConn) and isinstance(second, _FakeConn)

    with pytest.raises(RuntimeError, match="max_size=2"):
        pool._acquire()

    pool._release(first)
    assert pool._acquire() is first


def test_pool_timeout_retries_creation_when_capacity_freed(monkeypatch):
    monkeypatch.setattr("cube_web.services.db_pool._acquire_timeout_seconds", lambda: 0.05)

    pool = _PostgresPool("dsn-c", min_size=0, max_size=2)
    pool._acquire()
    pool._acquire()
    # Simulate a failed connection decrementing the counter while a waiter blocks.
    with pool._lock:
        pool._created -= 1

    conn = pool._acquire()
    assert isinstance(conn, _FakeConn)
    assert pool._created == 2


def test_pool_context_returns_connection_on_success():
    pool = _PostgresPool("dsn-d", min_size=1, max_size=2)
    with pool.connection() as conn:
        assert isinstance(conn, _FakeConn)
    assert pool._queue.qsize() == 1
    assert pool._created == 1
