from __future__ import annotations

import queue
import threading
from typing import Any

from cube_split import runtime_config

# Chunk staging writers use multiple independent transactions. Keep enough
# connections for their bounded concurrency while preserving a small warm pool.
_DEFAULT_MIN_SIZE = 1


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = runtime_config.env_text(name)
    if raw:
        try:
            return max(minimum, int(raw))
        except ValueError:
            pass
    return default


def _default_max_size() -> int:
    return _env_int("CUBE_WEB_PG_POOL_SIZE", 8)


def _acquire_timeout_seconds() -> float:
    return float(_env_int("CUBE_WEB_PG_POOL_ACQUIRE_TIMEOUT_SECONDS", 30))


class _PostgresPool:
    """Minimal thread-safe PostgreSQL/OpenGauss connection pool.

    Lazily initialized — the pool is created on first connection request.
    All stores with the same DSN share one pool.
    """

    _pools: dict[str, "_PostgresPool"] = {}
    _pools_lock = threading.Lock()

    def __init__(self, dsn: str, min_size: int = 1, max_size: int = 8):
        if min_size < 0:
            raise ValueError("min_size must be non-negative")
        if max_size < 1:
            raise ValueError("max_size must be positive")
        if min_size > max_size:
            raise ValueError("min_size must not exceed max_size")
        self._dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self._queue: queue.Queue[Any] = queue.Queue()
        self._created = 0
        self._lock = threading.Lock()

    def _new_conn(self):
        import psycopg

        return psycopg.connect(self._dsn, client_encoding="UTF8")

    def _reserve_slot(self) -> bool:
        with self._lock:
            if self._created >= self._max_size:
                return False
            self._created += 1
            return True

    def _decrement_created(self) -> None:
        with self._lock:
            self._created -= 1

    def _create_tracked(self):
        """Create a connection after reserving a slot, rolling it back on failure."""
        try:
            conn = self._new_conn()
        except BaseException:
            self._decrement_created()
            raise
        if getattr(conn, "closed", False):
            self._discard(conn)
            raise RuntimeError("database connection was closed during creation")
        return conn

    def _ensure_min_size(self) -> None:
        """Lazily warm the configured minimum without holding the DSN registry lock."""
        while True:
            with self._lock:
                if self._created >= self._min_size:
                    return
                self._created += 1
            conn = self._create_tracked()
            self._queue.put(conn)

    def _discard(self, conn) -> None:
        try:
            conn.close()
        except BaseException:
            pass
        self._decrement_created()

    def _acquire(self):
        self._ensure_min_size()
        while True:
            try:
                conn = self._queue.get_nowait()
            except queue.Empty:
                if self._reserve_slot():
                    return self._create_tracked()
                timeout = _acquire_timeout_seconds()
                try:
                    conn = self._queue.get(timeout=timeout)
                except queue.Empty:
                    # A slot may have been discarded while this borrower waited.
                    if self._reserve_slot():
                        return self._create_tracked()
                    try:
                        conn = self._queue.get_nowait()
                    except queue.Empty:
                        raise RuntimeError(
                            f"Timed out acquiring a database connection after {timeout:.0f}s "
                            f"(pool max_size={self._max_size}, dsn pool exhausted)"
                        ) from None
            if getattr(conn, "closed", False):
                self._discard(conn)
                continue
            return conn

    def _release(self, conn):
        if getattr(conn, "closed", False):
            self._discard(conn)
            return
        self._queue.put(conn)

    def connection(self):
        return _PoolContext(self)

    @classmethod
    def for_dsn(cls, dsn: str, min_size: int = _DEFAULT_MIN_SIZE, max_size: int | None = None) -> "_PostgresPool":
        with cls._pools_lock:
            pool = cls._pools.get(dsn)
            if pool is None:
                pool = cls(dsn, min_size=min_size, max_size=max_size if max_size is not None else _default_max_size())
                cls._pools[dsn] = pool
            return pool


class _PoolContext:
    __slots__ = ("_pool", "_conn")

    def __init__(self, pool: _PostgresPool):
        self._pool = pool
        self._conn = None

    def __enter__(self):
        self._conn = self._pool._acquire()
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        conn = self._conn
        self._conn = None
        if conn is None:
            return False
        if exc_type is not None:
            try:
                conn.rollback()
            except BaseException:
                self._pool._discard(conn)
            else:
                self._pool._release(conn)
            return False
        try:
            conn.commit()
        except BaseException:
            try:
                conn.rollback()
            except BaseException:
                pass
            self._pool._discard(conn)
            raise
        self._pool._release(conn)
        return False
