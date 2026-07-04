from __future__ import annotations

import queue
import threading
from typing import Any

# 经真实 OpenGauss 4 节点集群压测验证的最优参数：
#   max_size=2 在 1~64 并发下均持平或优于其他配置
#   min_size=1 保证无冷启动延迟
_DEFAULT_MIN_SIZE = 1
_DEFAULT_MAX_SIZE = 2


class _PostgresPool:
    """Minimal thread-safe PostgreSQL/OpenGauss connection pool.

    Lazily initialized — the pool is created on first connection request.
    All stores with the same DSN share one pool.
    """

    _pools: dict[str, "_PostgresPool"] = {}
    _pools_lock = threading.Lock()

    def __init__(self, dsn: str, min_size: int = 1, max_size: int = 8):
        self._dsn = dsn
        self._max_size = max_size
        self._queue: queue.Queue[Any] = queue.Queue()
        self._created = 0
        self._lock = threading.Lock()

        for _ in range(min_size):
            self._queue.put(self._new_conn())
            self._created += 1

    def _new_conn(self):
        import psycopg

        return psycopg.connect(self._dsn, client_encoding="UTF8")

    def _acquire(self):
        try:
            conn = self._queue.get_nowait()
        except queue.Empty:
            with self._lock:
                if self._created < self._max_size:
                    self._created += 1
                    return self._new_conn()
            return self._queue.get()
        if getattr(conn, "closed", False):
            with self._lock:
                self._created -= 1
            try:
                conn.close()
            except Exception:
                pass
            return self._acquire()
        return conn

    def _release(self, conn):
        self._queue.put(conn)

    def connection(self):
        return _PoolContext(self)

    @classmethod
    def for_dsn(cls, dsn: str, min_size: int = _DEFAULT_MIN_SIZE, max_size: int = _DEFAULT_MAX_SIZE) -> "_PostgresPool":
        with cls._pools_lock:
            pool = cls._pools.get(dsn)
            if pool is None:
                pool = cls(dsn, min_size=min_size, max_size=max_size)
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
        if self._conn is not None:
            if exc_type is not None:
                try:
                    self._conn.rollback()
                except Exception:
                    pass
                if not getattr(self._conn, "closed", False):
                    self._pool._release(self._conn)
                else:
                    with self._pool._lock:
                        self._pool._created -= 1
            else:
                try:
                    self._conn.commit()
                except Exception:
                    try:
                        self._conn.rollback()
                    except Exception:
                        pass
                    try:
                        self._conn.close()
                    except Exception:
                        pass
                    with self._pool._lock:
                        self._pool._created -= 1
                else:
                    self._pool._release(self._conn)
            self._conn = None
