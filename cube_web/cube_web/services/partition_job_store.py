from __future__ import annotations

import copy
from datetime import datetime, timezone
from typing import Any

from cube_split.runtime_config import postgres_dsn

from cube_web.services.partition_defaults import apply_resolution_grid_defaults

BATCH_ACTIVE_STATUSES = {"pending", "queued", "running", "retrying", "cancel_requested"}
BATCH_VISIBLE_STATUSES = BATCH_ACTIVE_STATUSES | {"failed", "manual_required", "cancelled"}


class PartitionJobStore:
    def ensure_schema(self) -> None:
        raise NotImplementedError

    def upsert_schema(self, schema: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    def list_batches(
        self,
        *,
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
        include_succeeded: bool = False,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def list_assets(self, batch_id: str, status: str | None = None) -> list[dict[str, Any]]:
        raise NotImplementedError

    def create_attempt(
        self,
        *,
        task_id: str,
        batch_id: str,
        operation: str,
        payload: dict[str, Any],
        asset_ids: list[str] | None = None,
        requested_by: str = "system",
    ) -> dict[str, Any]:
        raise NotImplementedError

    def start_attempt(self, task_id: str) -> None:
        raise NotImplementedError

    def succeed_attempt(self, task_id: str, result: dict[str, Any]) -> None:
        raise NotImplementedError

    def fail_attempt(
        self,
        task_id: str,
        error: str,
        *,
        manual_required: bool = False,
        error_type: str | None = None,
    ) -> None:
        raise NotImplementedError

    def mark_batch_queued(self, batch_id: str, task_id: str, *, operation: str) -> None:
        raise NotImplementedError

    def request_cancel(self, task_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def mark_cancelled(self, task_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def is_cancel_requested(self, task_id: str) -> bool:
        raise NotImplementedError

    def get_attempt(self, task_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def list_attempts(self, batch_id: str) -> list[dict[str, Any]]:
        raise NotImplementedError


class InMemoryPartitionJobStore(PartitionJobStore):
    def __init__(self) -> None:
        self.batches: dict[str, dict[str, Any]] = {}
        self.assets: dict[str, dict[str, Any]] = {}
        self.attempts: dict[str, dict[str, Any]] = {}

    def ensure_schema(self) -> None:
        return None

    def upsert_schema(self, schema: dict[str, Any]) -> dict[str, Any]:
        record = _normalized_schema_record(schema)
        now = _utc_now_iso()
        existing = self.batches.get(record["batch_id"], {})
        batch = {
            **existing,
            **record,
            "status": existing.get("status") or "pending",
            "attempt_count": int(existing.get("attempt_count") or 0),
            "max_auto_retries": int(record.get("max_auto_retries") or existing.get("max_auto_retries") or 1),
            "last_task_id": existing.get("last_task_id"),
            "last_error": existing.get("last_error"),
            "partitioned_at": existing.get("partitioned_at"),
            "manual_required_at": existing.get("manual_required_at"),
            "created_at": existing.get("created_at") or now,
            "updated_at": now,
        }
        self.batches[batch["batch_id"]] = batch
        for asset in _assets_from_record(batch):
            self.assets[asset["asset_id"]] = {
                **self.assets.get(asset["asset_id"], {}),
                **asset,
                "status": self.assets.get(asset["asset_id"], {}).get("status") or "pending",
                "attempt_count": int(self.assets.get(asset["asset_id"], {}).get("attempt_count") or 0),
                "created_at": self.assets.get(asset["asset_id"], {}).get("created_at") or now,
                "updated_at": now,
            }
        return copy.deepcopy(batch)

    def list_batches(
        self,
        *,
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
        include_succeeded: bool = False,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        rows = list(self.batches.values())
        if status:
            rows = [row for row in rows if row["status"] == status]
        elif not include_succeeded:
            rows = [row for row in rows if row["status"] != "succeeded"]
        if data_type:
            rows = [row for row in rows if row["data_type"] == data_type]
        if keyword:
            needle = keyword.lower()
            rows = [row for row in rows if needle in row["batch_id"].lower() or needle in row["batch_name"].lower()]
        rows.sort(key=lambda row: (int(row.get("priority") or 0), row.get("created_at") or ""), reverse=True)
        return copy.deepcopy(rows[:limit])

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        row = self.batches.get(batch_id)
        return None if row is None else copy.deepcopy(row)

    def list_assets(self, batch_id: str, status: str | None = None) -> list[dict[str, Any]]:
        rows = [row for row in self.assets.values() if row["batch_id"] == batch_id]
        if status:
            rows = [row for row in rows if row["status"] == status]
        rows.sort(key=lambda row: row["asset_id"])
        return copy.deepcopy(rows)

    def create_attempt(
        self,
        *,
        task_id: str,
        batch_id: str,
        operation: str,
        payload: dict[str, Any],
        asset_ids: list[str] | None = None,
        requested_by: str = "system",
    ) -> dict[str, Any]:
        batch = self.batches[batch_id]
        attempt_no = int(batch.get("attempt_count") or 0) + 1
        batch["attempt_count"] = attempt_no
        batch["updated_at"] = _utc_now_iso()
        self._increment_asset_attempts(batch_id, asset_ids)
        attempt = {
            "task_id": task_id,
            "batch_id": batch_id,
            "asset_ids": list(asset_ids or []),
            "operation": operation,
            "status": "queued",
            "attempt_no": attempt_no,
            "payload": copy.deepcopy(payload),
            "runner_result": None,
            "error_type": None,
            "error_message": None,
            "requested_by": requested_by,
            "started_at": None,
            "finished_at": None,
            "created_at": _utc_now_iso(),
            "updated_at": _utc_now_iso(),
        }
        self.attempts[task_id] = attempt
        return copy.deepcopy(attempt)

    def start_attempt(self, task_id: str) -> None:
        attempt = self.attempts[task_id]
        attempt["status"] = "running"
        attempt["started_at"] = attempt["started_at"] or _utc_now_iso()
        attempt["updated_at"] = _utc_now_iso()
        batch = self.batches[attempt["batch_id"]]
        batch["status"] = "running"
        batch["last_task_id"] = task_id
        batch["updated_at"] = _utc_now_iso()
        self._set_assets_status(attempt, "running")

    def succeed_attempt(self, task_id: str, result: dict[str, Any]) -> None:
        attempt = self.attempts[task_id]
        now = _utc_now_iso()
        attempt["status"] = "succeeded"
        attempt["runner_result"] = copy.deepcopy(result)
        attempt["finished_at"] = now
        attempt["updated_at"] = now
        if _asset_results(result):
            self._apply_asset_results(attempt, result, now=now)
        else:
            self._set_assets_status(attempt, "succeeded", partitioned_at=now, last_error=None)
        self._refresh_batch_from_assets(attempt["batch_id"], now=now)

    def fail_attempt(
        self,
        task_id: str,
        error: str,
        *,
        manual_required: bool = False,
        error_type: str | None = None,
    ) -> None:
        attempt = self.attempts[task_id]
        now = _utc_now_iso()
        attempt["status"] = "failed"
        attempt["error_type"] = error_type
        attempt["error_message"] = error
        attempt["finished_at"] = now
        attempt["updated_at"] = now
        batch = self.batches[attempt["batch_id"]]
        batch["status"] = "manual_required" if manual_required else "failed"
        batch["last_error"] = error
        batch["manual_required_at"] = now if manual_required else batch.get("manual_required_at")
        batch["updated_at"] = now
        self._set_assets_status(attempt, "manual_required" if manual_required else "failed", last_error=error)

    def mark_batch_queued(self, batch_id: str, task_id: str, *, operation: str) -> None:
        batch = self.batches[batch_id]
        batch["status"] = "retrying" if "retry" in operation else "queued"
        batch["last_task_id"] = task_id
        batch["updated_at"] = _utc_now_iso()

    def request_cancel(self, task_id: str) -> dict[str, Any] | None:
        attempt = self.attempts.get(task_id)
        if attempt is None:
            return None
        now = _utc_now_iso()
        if attempt["status"] == "queued":
            attempt["status"] = "cancelled"
            attempt["finished_at"] = now
            batch = self.batches[attempt["batch_id"]]
            batch["status"] = "cancelled"
            batch["updated_at"] = now
            self._set_assets_status(attempt, "cancelled")
        elif attempt["status"] == "running":
            attempt["status"] = "cancel_requested"
            batch = self.batches[attempt["batch_id"]]
            batch["status"] = "cancel_requested"
            batch["updated_at"] = now
        attempt["updated_at"] = now
        return copy.deepcopy(attempt)

    def mark_cancelled(self, task_id: str) -> dict[str, Any] | None:
        attempt = self.attempts.get(task_id)
        if attempt is None:
            return None
        now = _utc_now_iso()
        attempt["status"] = "cancelled"
        attempt["finished_at"] = now
        attempt["updated_at"] = now
        batch = self.batches[attempt["batch_id"]]
        batch["status"] = "cancelled"
        batch["updated_at"] = now
        self._set_assets_status(attempt, "cancelled")
        return copy.deepcopy(attempt)

    def is_cancel_requested(self, task_id: str) -> bool:
        attempt = self.attempts.get(task_id)
        return bool(attempt and attempt["status"] in {"cancel_requested", "cancelled"})

    def get_attempt(self, task_id: str) -> dict[str, Any] | None:
        attempt = self.attempts.get(task_id)
        return None if attempt is None else copy.deepcopy(attempt)

    def list_attempts(self, batch_id: str) -> list[dict[str, Any]]:
        rows = [row for row in self.attempts.values() if row["batch_id"] == batch_id]
        rows.sort(key=lambda row: row.get("created_at") or "", reverse=True)
        return copy.deepcopy(rows)

    def _set_assets_status(self, attempt: dict[str, Any], status: str, **updates: Any) -> None:
        asset_ids = attempt.get("asset_ids") or [
            asset_id for asset_id, asset in self.assets.items() if asset["batch_id"] == attempt["batch_id"]
        ]
        now = _utc_now_iso()
        for asset_id in asset_ids:
            asset = self.assets.get(asset_id)
            if asset is None:
                continue
            asset["status"] = status
            asset["updated_at"] = now
            for key, value in updates.items():
                asset[key] = value

    def _increment_asset_attempts(self, batch_id: str, asset_ids: list[str] | None) -> None:
        now = _utc_now_iso()
        for asset_id in self._target_asset_ids(batch_id, asset_ids):
            asset = self.assets.get(asset_id)
            if asset is None:
                continue
            asset["attempt_count"] = int(asset.get("attempt_count") or 0) + 1
            asset["updated_at"] = now

    def _apply_asset_results(self, attempt: dict[str, Any], result: dict[str, Any], *, now: str) -> None:
        attempt_asset_ids = set(attempt.get("asset_ids") or [])
        self._set_assets_status(attempt, "succeeded", partitioned_at=now, last_error=None)
        for item in _asset_results(result):
            asset = self._find_asset_for_result(attempt["batch_id"], item)
            if asset is None:
                continue
            if attempt_asset_ids and asset["asset_id"] not in attempt_asset_ids:
                continue
            status = _normalized_asset_result_status(item)
            asset["status"] = status
            asset["updated_at"] = now
            if status == "succeeded":
                asset["partitioned_at"] = item.get("partitioned_at") or now
                asset["last_error"] = None
            elif item.get("last_error") or item.get("error") or item.get("error_message"):
                asset["last_error"] = str(item.get("last_error") or item.get("error") or item.get("error_message"))
            if item.get("last_run_dir") or result.get("run_dir"):
                asset["last_run_dir"] = str(item.get("last_run_dir") or result.get("run_dir"))

    def _find_asset_for_result(self, batch_id: str, item: dict[str, Any]) -> dict[str, Any] | None:
        asset_id = str(item.get("asset_id") or "").strip()
        if asset_id and self.assets.get(asset_id, {}).get("batch_id") == batch_id:
            return self.assets[asset_id]
        source_uri = str(item.get("source_uri") or "").strip()
        scene_id = str(item.get("scene_id") or "").strip()
        for asset in self.assets.values():
            if asset["batch_id"] != batch_id:
                continue
            if source_uri and str(asset.get("source_uri") or "") == source_uri:
                return asset
            if scene_id and str(asset.get("scene_id") or "") == scene_id:
                return asset
        return None

    def _target_asset_ids(self, batch_id: str, asset_ids: list[str] | None) -> list[str]:
        if asset_ids:
            return list(asset_ids)
        return [asset_id for asset_id, asset in self.assets.items() if asset["batch_id"] == batch_id]

    def _refresh_batch_from_assets(self, batch_id: str, *, now: str) -> None:
        batch = self.batches[batch_id]
        assets = [asset for asset in self.assets.values() if asset["batch_id"] == batch_id]
        if not assets or all(asset["status"] == "succeeded" for asset in assets):
            batch["status"] = "succeeded"
            batch["last_error"] = None
            batch["partitioned_at"] = now
            batch["manual_required_at"] = None
        else:
            status = _summarize_asset_statuses(asset["status"] for asset in assets)
            batch["status"] = status
            failed_asset = next(
                (
                    asset
                    for asset in assets
                    if asset["status"] in {"manual_required", "failed"} and asset.get("last_error")
                ),
                None,
            )
            batch["last_error"] = failed_asset.get("last_error") if failed_asset else batch.get("last_error")
            if status == "manual_required" and not batch.get("manual_required_at"):
                batch["manual_required_at"] = now
            elif status != "manual_required":
                batch["manual_required_at"] = None
        batch["updated_at"] = now


class PostgresPartitionJobStore(PartitionJobStore):
    def __init__(self, dsn: str) -> None:
        if not dsn:
            raise ValueError("PostgreSQL DSN is required")
        self.dsn = dsn

    def ensure_schema(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS partition_batches (
                      batch_id TEXT PRIMARY KEY,
                      batch_name TEXT NOT NULL,
                      data_type TEXT NOT NULL,
                      source_system TEXT,
                      source_schema JSONB NOT NULL,
                      normalized_payload JSONB NOT NULL,
                      status TEXT NOT NULL DEFAULT 'pending',
                      priority INT NOT NULL DEFAULT 0,
                      attempt_count INT NOT NULL DEFAULT 0,
                      max_auto_retries INT NOT NULL DEFAULT 1,
                      last_task_id TEXT,
                      last_error TEXT,
                      partitioned_at TIMESTAMPTZ,
                      manual_required_at TIMESTAMPTZ,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS partition_assets (
                      asset_id TEXT PRIMARY KEY,
                      batch_id TEXT NOT NULL REFERENCES partition_batches(batch_id) ON DELETE CASCADE,
                      data_type TEXT NOT NULL,
                      scene_id TEXT,
                      source_uri TEXT NOT NULL,
                      asset_payload JSONB NOT NULL,
                      status TEXT NOT NULL DEFAULT 'pending',
                      attempt_count INT NOT NULL DEFAULT 0,
                      last_error TEXT,
                      last_run_dir TEXT,
                      partitioned_at TIMESTAMPTZ,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS partition_job_attempts (
                      task_id TEXT PRIMARY KEY,
                      batch_id TEXT NOT NULL REFERENCES partition_batches(batch_id) ON DELETE CASCADE,
                      asset_ids TEXT[] NOT NULL DEFAULT '{}',
                      operation TEXT NOT NULL,
                      status TEXT NOT NULL,
                      attempt_no INT NOT NULL,
                      payload JSONB NOT NULL,
                      runner_result JSONB,
                      error_type TEXT,
                      error_message TEXT,
                      requested_by TEXT NOT NULL DEFAULT 'system',
                      started_at TIMESTAMPTZ,
                      finished_at TIMESTAMPTZ,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute("ALTER TABLE partition_job_attempts ADD COLUMN IF NOT EXISTS error_type TEXT")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_partition_batches_status ON partition_batches(status)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_partition_batches_type_status ON partition_batches(data_type, status)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_partition_assets_batch_status ON partition_assets(batch_id, status)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_partition_attempts_batch ON partition_job_attempts(batch_id, created_at DESC)")
            conn.commit()

    def upsert_schema(self, schema: dict[str, Any]) -> dict[str, Any]:
        self.ensure_schema()
        record = _normalized_schema_record(schema)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO partition_batches (
                      batch_id, batch_name, data_type, source_system, source_schema,
                      normalized_payload, priority, max_auto_retries
                    )
                    VALUES (
                      %(batch_id)s, %(batch_name)s, %(data_type)s, %(source_system)s, %(source_schema)s,
                      %(normalized_payload)s, %(priority)s, %(max_auto_retries)s
                    )
                    ON CONFLICT (batch_id) DO UPDATE SET
                      batch_name = EXCLUDED.batch_name,
                      data_type = EXCLUDED.data_type,
                      source_system = EXCLUDED.source_system,
                      source_schema = EXCLUDED.source_schema,
                      normalized_payload = EXCLUDED.normalized_payload,
                      priority = EXCLUDED.priority,
                      max_auto_retries = EXCLUDED.max_auto_retries,
                      updated_at = now()
                    RETURNING *
                    """,
                    _jsonb_record(record, "source_schema", "normalized_payload"),
                )
                batch = _dict_row(cur)
                for asset in _assets_from_record(record):
                    cur.execute(
                        """
                        INSERT INTO partition_assets (
                          asset_id, batch_id, data_type, scene_id, source_uri, asset_payload
                        )
                        VALUES (
                          %(asset_id)s, %(batch_id)s, %(data_type)s, %(scene_id)s, %(source_uri)s, %(asset_payload)s
                        )
                        ON CONFLICT (asset_id) DO UPDATE SET
                          scene_id = EXCLUDED.scene_id,
                          source_uri = EXCLUDED.source_uri,
                          asset_payload = EXCLUDED.asset_payload,
                          updated_at = now()
                        """,
                        _jsonb_record(asset, "asset_payload"),
                    )
            conn.commit()
        return batch

    def list_batches(
        self,
        *,
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
        include_succeeded: bool = False,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        self.ensure_schema()
        where = []
        params: list[Any] = []
        if status:
            where.append("status = %s")
            params.append(status)
        elif not include_succeeded:
            where.append("status <> 'succeeded'")
        if data_type:
            where.append("data_type = %s")
            params.append(data_type)
        if keyword:
            where.append("(batch_id ILIKE %s OR batch_name ILIKE %s)")
            params.extend([f"%{keyword}%", f"%{keyword}%"])
        sql = "SELECT * FROM partition_batches"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY priority DESC, created_at DESC LIMIT %s"
        params.append(limit)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return [_row_to_dict(cur, row) for row in cur.fetchall()]

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        self.ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM partition_batches WHERE batch_id = %s", (batch_id,))
                row = cur.fetchone()
                return None if row is None else _row_to_dict(cur, row)

    def list_assets(self, batch_id: str, status: str | None = None) -> list[dict[str, Any]]:
        self.ensure_schema()
        params: list[Any] = [batch_id]
        sql = "SELECT * FROM partition_assets WHERE batch_id = %s"
        if status:
            sql += " AND status = %s"
            params.append(status)
        sql += " ORDER BY asset_id"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return [_row_to_dict(cur, row) for row in cur.fetchall()]

    def create_attempt(
        self,
        *,
        task_id: str,
        batch_id: str,
        operation: str,
        payload: dict[str, Any],
        asset_ids: list[str] | None = None,
        requested_by: str = "system",
    ) -> dict[str, Any]:
        self.ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE partition_batches SET attempt_count = attempt_count + 1, updated_at = now() WHERE batch_id = %s RETURNING attempt_count",
                    (batch_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise KeyError(batch_id)
                self._increment_asset_attempts(cur, batch_id, asset_ids)
                cur.execute(
                    """
                    INSERT INTO partition_job_attempts (
                      task_id, batch_id, asset_ids, operation, status, attempt_no, payload, requested_by
                    )
                    VALUES (%s, %s, %s, %s, 'queued', %s, %s, %s)
                    RETURNING *
                    """,
                    (task_id, batch_id, asset_ids or [], operation, row[0], self._jsonb(payload), requested_by),
                )
                attempt = _dict_row(cur)
            conn.commit()
        return attempt

    def start_attempt(self, task_id: str) -> None:
        self._update_attempt_and_batch(task_id, "running", "running", started=True)

    def succeed_attempt(self, task_id: str, result: dict[str, Any]) -> None:
        self.ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE partition_job_attempts SET status = 'succeeded', runner_result = %s, finished_at = now(), updated_at = now() WHERE task_id = %s RETURNING batch_id, asset_ids",
                    (self._jsonb(result), task_id),
                )
                row = cur.fetchone()
                if row is None:
                    return
                batch_id, asset_ids = row
                if _asset_results(result):
                    self._apply_asset_results(cur, batch_id, asset_ids, result)
                else:
                    self._update_assets(cur, batch_id, asset_ids, "succeeded", partitioned=True, last_error=None)
                self._refresh_batch_from_assets(cur, batch_id)
            conn.commit()

    def fail_attempt(
        self,
        task_id: str,
        error: str,
        *,
        manual_required: bool = False,
        error_type: str | None = None,
    ) -> None:
        status = "manual_required" if manual_required else "failed"
        self.ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE partition_job_attempts SET status = 'failed', error_type = %s, error_message = %s, finished_at = now(), updated_at = now() WHERE task_id = %s RETURNING batch_id, asset_ids",
                    (error_type, error, task_id),
                )
                row = cur.fetchone()
                if row is None:
                    return
                batch_id, asset_ids = row
                cur.execute(
                    f"UPDATE partition_batches SET status = %s, last_error = %s, manual_required_at = {'now()' if manual_required else 'manual_required_at'}, updated_at = now() WHERE batch_id = %s",
                    (status, error, batch_id),
                )
                self._update_assets(cur, batch_id, asset_ids, status, last_error=error)
            conn.commit()

    def mark_batch_queued(self, batch_id: str, task_id: str, *, operation: str) -> None:
        self.ensure_schema()
        status = "retrying" if "retry" in operation else "queued"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE partition_batches SET status = %s, last_task_id = %s, updated_at = now() WHERE batch_id = %s",
                    (status, task_id, batch_id),
                )
            conn.commit()

    def request_cancel(self, task_id: str) -> dict[str, Any] | None:
        self.ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT status, batch_id, asset_ids FROM partition_job_attempts WHERE task_id = %s", (task_id,))
                row = cur.fetchone()
                if row is None:
                    return None
                status, batch_id, asset_ids = row
                next_status = "cancelled" if status == "queued" else "cancel_requested"
                finished = ", finished_at = now()" if next_status == "cancelled" else ""
                cur.execute(
                    f"UPDATE partition_job_attempts SET status = %s{finished}, updated_at = now() WHERE task_id = %s RETURNING *",
                    (next_status, task_id),
                )
                attempt = _dict_row(cur)
                cur.execute("UPDATE partition_batches SET status = %s, updated_at = now() WHERE batch_id = %s", (next_status, batch_id))
                if next_status == "cancelled":
                    self._update_assets(cur, batch_id, asset_ids, "cancelled")
            conn.commit()
        return attempt

    def mark_cancelled(self, task_id: str) -> dict[str, Any] | None:
        self.ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE partition_job_attempts SET status = 'cancelled', finished_at = now(), updated_at = now() WHERE task_id = %s RETURNING batch_id, asset_ids",
                    (task_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                batch_id, asset_ids = row
                cur.execute("UPDATE partition_batches SET status = 'cancelled', updated_at = now() WHERE batch_id = %s", (batch_id,))
                self._update_assets(cur, batch_id, asset_ids, "cancelled")
            conn.commit()
        return self.get_attempt(task_id)

    def is_cancel_requested(self, task_id: str) -> bool:
        attempt = self.get_attempt(task_id)
        return bool(attempt and attempt["status"] in {"cancel_requested", "cancelled"})

    def get_attempt(self, task_id: str) -> dict[str, Any] | None:
        self.ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM partition_job_attempts WHERE task_id = %s", (task_id,))
                row = cur.fetchone()
                return None if row is None else _row_to_dict(cur, row)

    def list_attempts(self, batch_id: str) -> list[dict[str, Any]]:
        self.ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM partition_job_attempts WHERE batch_id = %s ORDER BY created_at DESC",
                    (batch_id,),
                )
                return [_row_to_dict(cur, row) for row in cur.fetchall()]

    def _update_attempt_and_batch(self, task_id: str, attempt_status: str, batch_status: str, *, started: bool = False) -> None:
        started_sql = ", started_at = COALESCE(started_at, now())" if started else ""
        self.ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE partition_job_attempts SET status = %s{started_sql}, updated_at = now() WHERE task_id = %s RETURNING batch_id, asset_ids",
                    (attempt_status, task_id),
                )
                row = cur.fetchone()
                if row is None:
                    return
                batch_id, asset_ids = row
                cur.execute(
                    "UPDATE partition_batches SET status = %s, last_task_id = %s, updated_at = now() WHERE batch_id = %s",
                    (batch_status, task_id, batch_id),
                )
                self._update_assets(cur, batch_id, asset_ids, batch_status)
            conn.commit()

    def _update_assets(
        self,
        cur,
        batch_id: str,
        asset_ids: list[str],
        status: str,
        *,
        partitioned: bool = False,
        last_error: str | None | object = ...,
    ) -> None:
        partitioned_sql = ", partitioned_at = now()" if partitioned else ""
        error_sql = "" if last_error is ... else ", last_error = %s"
        params: list[Any] = [status]
        if last_error is not ...:
            params.append(last_error)
        if asset_ids:
            params.append(asset_ids)
            cur.execute(f"UPDATE partition_assets SET status = %s{partitioned_sql}{error_sql}, updated_at = now() WHERE asset_id = ANY(%s)", params)
        else:
            params.append(batch_id)
            cur.execute(f"UPDATE partition_assets SET status = %s{partitioned_sql}{error_sql}, updated_at = now() WHERE batch_id = %s", params)

    def _increment_asset_attempts(self, cur, batch_id: str, asset_ids: list[str] | None) -> None:
        if asset_ids:
            cur.execute(
                "UPDATE partition_assets SET attempt_count = attempt_count + 1, updated_at = now() WHERE asset_id = ANY(%s)",
                (asset_ids,),
            )
        else:
            cur.execute(
                "UPDATE partition_assets SET attempt_count = attempt_count + 1, updated_at = now() WHERE batch_id = %s",
                (batch_id,),
            )

    def _apply_asset_results(self, cur, batch_id: str, asset_ids: list[str], result: dict[str, Any]) -> None:
        self._update_assets(cur, batch_id, asset_ids, "succeeded", partitioned=True, last_error=None)
        for item in _asset_results(result):
            status = _normalized_asset_result_status(item)
            last_error = None if status == "succeeded" else item.get("last_error") or item.get("error") or item.get("error_message")
            partitioned_sql = ", partitioned_at = COALESCE(partitioned_at, now())" if status == "succeeded" else ""
            last_run_dir = item.get("last_run_dir") or result.get("run_dir")
            run_dir_sql = ", last_run_dir = %s" if last_run_dir else ""
            params: list[Any] = [status, None if last_error is None else str(last_error)]
            if last_run_dir:
                params.append(str(last_run_dir))
            where, where_params = _asset_result_where_clause(item, batch_id)
            if where is None:
                continue
            if asset_ids:
                where = f"{where} AND asset_id = ANY(%s)"
                where_params.append(asset_ids)
            params.extend(where_params)
            cur.execute(
                f"UPDATE partition_assets SET status = %s, last_error = %s{partitioned_sql}{run_dir_sql}, updated_at = now() WHERE {where}",
                params,
            )

    def _refresh_batch_from_assets(self, cur, batch_id: str) -> None:
        cur.execute("SELECT status, last_error FROM partition_assets WHERE batch_id = %s", (batch_id,))
        rows = cur.fetchall()
        if not rows or all(row[0] == "succeeded" for row in rows):
            cur.execute(
                "UPDATE partition_batches SET status = 'succeeded', last_error = NULL, partitioned_at = now(), manual_required_at = NULL, updated_at = now() WHERE batch_id = %s",
                (batch_id,),
            )
            return
        status = _summarize_asset_statuses(row[0] for row in rows)
        last_error = next(
            (row[1] for row in rows if row[0] in {"manual_required", "failed"} and row[1]),
            None,
        )
        manual_required_sql = "COALESCE(manual_required_at, now())" if status == "manual_required" else "NULL"
        cur.execute(
            f"UPDATE partition_batches SET status = %s, last_error = COALESCE(%s, last_error), manual_required_at = {manual_required_sql}, updated_at = now() WHERE batch_id = %s",
            (status, last_error, batch_id),
        )

    def _connect(self):
        try:
            import psycopg
        except ModuleNotFoundError as exc:  # pragma: no cover - exercised only in incomplete installs.
            raise RuntimeError("PostgreSQL partition job storage requires `psycopg`") from exc
        return psycopg.connect(self.dsn)

    def _jsonb(self, value: dict[str, Any]):
        from psycopg.types.json import Jsonb

        return Jsonb(value)


_store: PartitionJobStore | None = None


def get_partition_job_store() -> PartitionJobStore:
    global _store
    if _store is None:
        _store = PostgresPartitionJobStore(postgres_dsn())
    return _store


def set_partition_job_store(store: PartitionJobStore | None) -> None:
    global _store
    _store = store


def _normalized_schema_record(schema: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(schema, dict):
        raise ValueError("schema must be an object")
    batch_id = str(schema.get("batch_id") or schema.get("id") or "").strip()
    if not batch_id:
        raise ValueError("batch_id is required")
    data_type = str(schema.get("data_type") or "optical").strip().lower()
    if data_type not in {"optical", "product", "carbon", "radar"}:
        raise ValueError("data_type must be one of: optical, product, carbon, radar")
    batch_name = str(schema.get("batch_name") or schema.get("name") or batch_id).strip()
    payload = copy.deepcopy(schema.get("normalized_payload") or schema.get("payload") or {})
    if not isinstance(payload, dict):
        raise ValueError("normalized_payload must be an object")
    payload.setdefault("batch_id", batch_id)
    payload.setdefault("batch_name", batch_name)
    if data_type in {"optical", "product", "radar"}:
        payload.setdefault("selected_assets", copy.deepcopy(schema.get("assets") or schema.get("selected_assets") or []))
        apply_resolution_grid_defaults(payload, data_type=data_type)
    elif data_type == "carbon":
        payload.setdefault("selected_observations", copy.deepcopy(schema.get("observations") or schema.get("selected_observations") or []))
    return {
        "batch_id": batch_id,
        "batch_name": batch_name,
        "data_type": data_type,
        "source_system": schema.get("source_system"),
        "source_schema": copy.deepcopy(schema),
        "normalized_payload": payload,
        "priority": int(schema.get("priority") or 0),
        "max_auto_retries": int(schema.get("max_auto_retries") if schema.get("max_auto_retries") is not None else 1),
    }


def _assets_from_record(record: dict[str, Any]) -> list[dict[str, Any]]:
    payload = record["normalized_payload"]
    data_type = record["data_type"]
    items = payload.get("selected_observations") if data_type == "carbon" else payload.get("selected_assets")
    if not isinstance(items, list):
        items = []
    assets = []
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        source_uri = str(item.get("source_uri") or item.get("observation_id") or item.get("source_index") or f"{record['batch_id']}:{idx}")
        scene_id = item.get("scene_id") or item.get("product_year") or item.get("observation_id") or item.get("source_index")
        asset_id = str(item.get("asset_id") or f"{record['batch_id']}:{_stable_asset_key(source_uri, idx)}")
        assets.append(
            {
                "asset_id": asset_id,
                "batch_id": record["batch_id"],
                "data_type": data_type,
                "scene_id": None if scene_id is None else str(scene_id),
                "source_uri": source_uri,
                "asset_payload": copy.deepcopy(item),
            }
        )
    return assets


def _stable_asset_key(value: str, idx: int) -> str:
    import hashlib

    digest = hashlib.sha1(f"{idx}:{value}".encode("utf-8")).hexdigest()[:16]
    return digest


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _summarize_asset_statuses(statuses: Any) -> str:
    values = {str(status) for status in statuses if status}
    if not values or values <= {"succeeded"}:
        return "succeeded"
    for status in ("manual_required", "failed", "cancel_requested", "running", "queued", "retrying", "cancelled"):
        if status in values:
            return status
    return "pending"


def _asset_results(result: dict[str, Any]) -> list[dict[str, Any]]:
    items = result.get("asset_results") if isinstance(result, dict) else None
    if items is None and isinstance(result, dict):
        items = result.get("partition_asset_results")
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _normalized_asset_result_status(item: dict[str, Any]) -> str:
    status = str(item.get("status") or "succeeded").strip().lower()
    if status in {"completed", "complete", "success", "ok", "passed"}:
        return "succeeded"
    if status in {"error", "failed", "fail"}:
        return "failed"
    if status in {"manual", "manual_required"}:
        return "manual_required"
    if status in {"queued", "running", "retrying", "cancel_requested", "cancelled", "succeeded"}:
        return status
    return "failed"


def _asset_result_where_clause(item: dict[str, Any], batch_id: str) -> tuple[str | None, list[Any]]:
    asset_id = str(item.get("asset_id") or "").strip()
    if asset_id:
        return "batch_id = %s AND asset_id = %s", [batch_id, asset_id]
    source_uri = str(item.get("source_uri") or "").strip()
    if source_uri:
        return "batch_id = %s AND source_uri = %s", [batch_id, source_uri]
    scene_id = str(item.get("scene_id") or "").strip()
    if scene_id:
        return "batch_id = %s AND scene_id = %s", [batch_id, scene_id]
    return None, []


def _jsonb_record(record: dict[str, Any], *json_keys: str) -> dict[str, Any]:
    from psycopg.types.json import Jsonb

    return {key: (Jsonb(value) if key in json_keys else value) for key, value in record.items()}


def _row_to_dict(cur, row: tuple[Any, ...]) -> dict[str, Any]:
    return {desc.name if hasattr(desc, "name") else desc[0]: value for desc, value in zip(cur.description, row)}


def _dict_row(cur) -> dict[str, Any]:
    row = cur.fetchone()
    return _row_to_dict(cur, row)
