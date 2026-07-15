from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from threading import Lock
from typing import Any

from cube_split import runtime_config

from cube_web.services.partition_defaults import apply_resolution_grid_defaults

BATCH_ACTIVE_STATUSES = {"pending", "queued", "running", "retrying", "cancel_requested"}
BATCH_VISIBLE_STATUSES = BATCH_ACTIVE_STATUSES | {"failed", "manual_required", "cancelled"}
BATCH_RUN_ACTIVE_STATUSES = {"queued", "running", "retrying", "cancel_requested"}
BATCH_HIDDEN_STATUSES = {"succeeded", "archived"}
BATCH_REQUEUEABLE_STATUSES = {"failed", "manual_required", "cancelled"}
INGEST_TRACKED_DATA_TYPES = {"optical", "product", "radar", "entity", "carbon"}


class PartitionBatchAlreadyActiveError(RuntimeError):
    pass


class PartitionBatchArchivedError(RuntimeError):
    pass


class PartitionBatchNotRequeueableError(RuntimeError):
    pass


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

    def get_batch_by_quality_report_id(self, data_type: str, quality_report_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def archive_batch(self, batch_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def requeue_batch(self, batch_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def update_ingest_status(
        self,
        batch_id: str,
        ingest_status: str,
        *,
        job_id: str | None = None,
        error: str | None = None,
        ingested: bool = False,
    ) -> dict[str, Any] | None:
        raise NotImplementedError

    def ensure_runtime_batch(
        self,
        *,
        batch_id: str,
        batch_name: str,
        data_type: str,
        payload: dict[str, Any],
        max_auto_retries: int = 0,
    ) -> dict[str, Any]:
        raise NotImplementedError

    def list_assets(self, batch_id: str, status: str | None = None) -> list[dict[str, Any]]:
        raise NotImplementedError

    def list_assets_by_ids(self, asset_ids: list[str]) -> list[dict[str, Any]]:
        raise NotImplementedError

    def list_received_batches(
        self,
        *,
        batch_ids: list[str] | None = None,
        updated_since: datetime | None = None,
        source_system: str | None = None,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def list_received_assets(
        self,
        *,
        batch_ids: list[str] | None = None,
        asset_ids: list[str] | None = None,
        source_system: str | None = None,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def list_received_observations(
        self,
        *,
        batch_ids: list[str] | None = None,
        observation_ids: list[str] | None = None,
        source_system: str | None = None,
    ) -> list[dict[str, Any]]:
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
        source_task_id: str | None = None,
        retry_strategy: str | None = None,
        failure_reason: str | None = None,
    ) -> dict[str, Any]:
        raise NotImplementedError

    def start_attempt(self, task_id: str) -> bool:
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

    def list_tasks(
        self,
        *,
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def count_tasks(
        self,
        *,
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
    ) -> int:
        raise NotImplementedError


class InMemoryPartitionJobStore(PartitionJobStore):
    def __init__(self) -> None:
        self.batches: dict[str, dict[str, Any]] = {}
        self.assets: dict[str, dict[str, Any]] = {}
        self.attempts: dict[str, dict[str, Any]] = {}
        self.ard_batches: dict[str, dict[str, Any]] = {}
        self.ard_assets: dict[str, dict[str, Any]] = {}
        self.ard_observations: dict[str, dict[str, Any]] = {}
        self._next_ard_batch_id = 1

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
            "max_auto_retries": _max_auto_retries_value(record.get("max_auto_retries"), existing.get("max_auto_retries")),
            "last_task_id": existing.get("last_task_id"),
            "last_error": existing.get("last_error"),
            "quality_status": existing.get("quality_status"),
            "quality_report_id": existing.get("quality_report_id"),
            "quality_failure_reason": existing.get("quality_failure_reason"),
            "ingest_status": existing.get("ingest_status") or _initial_ingest_status(record["data_type"]),
            "ingest_job_id": existing.get("ingest_job_id"),
            "ingest_error": existing.get("ingest_error"),
            "ingested_at": existing.get("ingested_at"),
            "partitioned_at": existing.get("partitioned_at"),
            "manual_required_at": existing.get("manual_required_at"),
            "created_at": existing.get("created_at") or now,
            "updated_at": now,
        }
        self.batches[batch["batch_id"]] = batch
        assets = [
            _dedupe_asset_id_for_batch(asset, existing_batch_id=self.assets.get(asset["asset_id"], {}).get("batch_id"))
            for asset in _assets_from_record(batch)
        ]
        asset_ids = {asset["asset_id"] for asset in assets}
        for asset_id, asset in list(self.assets.items()):
            if asset.get("batch_id") == batch["batch_id"] and asset_id not in asset_ids:
                del self.assets[asset_id]
        for asset in assets:
            existing_asset = self.assets.get(asset["asset_id"], {})
            same_batch = existing_asset.get("batch_id") == asset["batch_id"]
            self.assets[asset["asset_id"]] = {
                **existing_asset,
                **asset,
                "status": existing_asset.get("status") if same_batch else "pending",
                "attempt_count": int(existing_asset.get("attempt_count") or 0) if same_batch else 0,
                "last_error": existing_asset.get("last_error") if same_batch else None,
                "last_run_dir": existing_asset.get("last_run_dir") if same_batch else None,
                "partitioned_at": existing_asset.get("partitioned_at") if same_batch else None,
                "created_at": existing_asset.get("created_at") or now,
                "updated_at": now,
            }
        if _should_sync_ard_loader_schema(record):
            self._sync_ard_loader_schema_in_memory(schema, record)
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
            rows = [row for row in rows if row["status"] not in BATCH_HIDDEN_STATUSES]
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

    def get_batch_by_quality_report_id(self, data_type: str, quality_report_id: str) -> dict[str, Any] | None:
        for batch in self.batches.values():
            if batch.get("data_type") == data_type and batch.get("quality_report_id") == quality_report_id:
                return copy.deepcopy(batch)
        return None

    def archive_batch(self, batch_id: str) -> dict[str, Any] | None:
        batch = self.batches.get(batch_id)
        if batch is None:
            return None
        if str(batch.get("status") or "") in BATCH_RUN_ACTIVE_STATUSES:
            raise PartitionBatchAlreadyActiveError(f"Partition batch already has an active task: {batch_id}")
        now = _utc_now_iso()
        batch["status"] = "archived"
        batch["updated_at"] = now
        return copy.deepcopy(batch)

    def requeue_batch(self, batch_id: str) -> dict[str, Any] | None:
        batch = self.batches.get(batch_id)
        if batch is None:
            return None
        if str(batch.get("status") or "") in BATCH_RUN_ACTIVE_STATUSES:
            raise PartitionBatchAlreadyActiveError(f"Partition batch already has an active task: {batch_id}")
        if str(batch.get("status") or "") not in BATCH_REQUEUEABLE_STATUSES:
            raise PartitionBatchNotRequeueableError(
                f"Partition batch is not requeueable in status {str(batch.get('status') or '-')}: {batch_id}"
            )
        now = _utc_now_iso()
        batch["status"] = "pending"
        batch["last_error"] = None
        batch["manual_required_at"] = None
        batch["updated_at"] = now
        for asset in self.assets.values():
            if asset["batch_id"] != batch_id:
                continue
            asset["status"] = "pending"
            asset["last_error"] = None
            asset["updated_at"] = now
        return copy.deepcopy(batch)

    def update_ingest_status(
        self,
        batch_id: str,
        ingest_status: str,
        *,
        job_id: str | None = None,
        error: str | None = None,
        ingested: bool = False,
    ) -> dict[str, Any] | None:
        batch = self.batches.get(batch_id)
        if batch is None:
            return None
        now = _utc_now_iso()
        batch["ingest_status"] = ingest_status
        batch["ingest_job_id"] = job_id
        batch["ingest_error"] = error
        if ingested:
            batch["ingested_at"] = now
        elif ingest_status != "ingested":
            batch["ingested_at"] = None
        batch["updated_at"] = now
        return copy.deepcopy(batch)

    def ensure_runtime_batch(
        self,
        *,
        batch_id: str,
        batch_name: str,
        data_type: str,
        payload: dict[str, Any],
        max_auto_retries: int = 0,
    ) -> dict[str, Any]:
        now = _utc_now_iso()
        existing = self.batches.get(batch_id, {})
        batch = {
            **existing,
            "batch_id": batch_id,
            "batch_name": batch_name or batch_id,
            "data_type": data_type,
            "source_system": existing.get("source_system") or "runtime",
            "source_schema": existing.get("source_schema") or {
                "batch_id": batch_id,
                "batch_name": batch_name or batch_id,
                "data_type": data_type,
                "source_system": "runtime",
            },
            "normalized_payload": copy.deepcopy(payload),
            "status": existing.get("status") or "pending",
            "priority": int(existing.get("priority") or 0),
            "attempt_count": int(existing.get("attempt_count") or 0),
            "max_auto_retries": _max_auto_retries_value(max_auto_retries, existing.get("max_auto_retries")),
            "last_task_id": existing.get("last_task_id"),
            "last_error": existing.get("last_error"),
            "quality_status": existing.get("quality_status"),
            "quality_report_id": existing.get("quality_report_id"),
            "quality_failure_reason": existing.get("quality_failure_reason"),
            "ingest_status": existing.get("ingest_status") or _initial_ingest_status(data_type),
            "ingest_job_id": existing.get("ingest_job_id"),
            "ingest_error": existing.get("ingest_error"),
            "ingested_at": existing.get("ingested_at"),
            "partitioned_at": existing.get("partitioned_at"),
            "manual_required_at": existing.get("manual_required_at"),
            "created_at": existing.get("created_at") or now,
            "updated_at": now,
        }
        self.batches[batch_id] = batch
        assets = [
            _dedupe_asset_id_for_batch(asset, existing_batch_id=self.assets.get(asset["asset_id"], {}).get("batch_id"))
            for asset in _runtime_assets_from_payload(batch)
        ]
        asset_ids = {asset["asset_id"] for asset in assets}
        for asset_id, asset in list(self.assets.items()):
            if asset.get("batch_id") == batch_id and asset_id not in asset_ids:
                del self.assets[asset_id]
        for asset in assets:
            existing_asset = self.assets.get(asset["asset_id"], {})
            same_batch = existing_asset.get("batch_id") == asset["batch_id"]
            preserve_state = same_batch and _same_runtime_asset(existing_asset, asset)
            self.assets[asset["asset_id"]] = {
                **existing_asset,
                **asset,
                "status": existing_asset.get("status") if preserve_state else "pending",
                "attempt_count": int(existing_asset.get("attempt_count") or 0) if preserve_state else 0,
                "last_error": existing_asset.get("last_error") if preserve_state else None,
                "last_run_dir": existing_asset.get("last_run_dir") if preserve_state else None,
                "partitioned_at": existing_asset.get("partitioned_at") if preserve_state else None,
                "created_at": existing_asset.get("created_at") or now,
                "updated_at": now,
            }
        return copy.deepcopy(batch)

    def list_assets(self, batch_id: str, status: str | None = None) -> list[dict[str, Any]]:
        self._repair_missing_payload_assets(batch_id)
        rows = [row for row in self.assets.values() if row["batch_id"] == batch_id]
        if status:
            rows = [row for row in rows if row["status"] == status]
        rows.sort(key=lambda row: row["asset_id"])
        return copy.deepcopy(rows)

    def list_assets_by_ids(self, asset_ids: list[str]) -> list[dict[str, Any]]:
        wanted = {str(asset_id) for asset_id in asset_ids if str(asset_id).strip()}
        rows = [row for row in self.assets.values() if row["asset_id"] in wanted]
        rows.sort(key=lambda row: row["asset_id"])
        return copy.deepcopy(rows)

    def list_received_batches(
        self,
        *,
        batch_ids: list[str] | None = None,
        updated_since: datetime | None = None,
        source_system: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = list(self.ard_batches.values())
        if batch_ids is not None:
            wanted = {str(batch_id) for batch_id in batch_ids if str(batch_id).strip()}
            rows = [row for row in rows if row["batch_id"] in wanted]
        if source_system:
            rows = [row for row in rows if str(row.get("source_system") or "") == source_system]
        if updated_since is not None:
            rows = [
                row for row in rows
                if (_ard_datetime_or_none(row.get("updated_at")) or _ard_datetime_or_none(row.get("loaded_at")) or datetime.min.replace(tzinfo=timezone.utc)) >= updated_since
            ]
        rows.sort(key=lambda row: (str(row.get("updated_at") or ""), str(row.get("batch_id") or "")))
        return copy.deepcopy(rows)

    def list_received_assets(
        self,
        *,
        batch_ids: list[str] | None = None,
        asset_ids: list[str] | None = None,
        source_system: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = list(self.ard_assets.values())
        if asset_ids is not None:
            wanted = {str(asset_id) for asset_id in asset_ids if str(asset_id).strip()}
            rows = [row for row in rows if row["asset_id"] in wanted]
        rows = self._filter_received_member_rows(rows, batch_ids=batch_ids, source_system=source_system)
        rows.sort(key=lambda row: row["asset_id"])
        return copy.deepcopy(rows)

    def list_received_observations(
        self,
        *,
        batch_ids: list[str] | None = None,
        observation_ids: list[str] | None = None,
        source_system: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = list(self.ard_observations.values())
        if observation_ids is not None:
            wanted = {str(observation_id) for observation_id in observation_ids if str(observation_id).strip()}
            rows = [row for row in rows if row["observation_id"] in wanted]
        rows = self._filter_received_member_rows(rows, batch_ids=batch_ids, source_system=source_system)
        rows.sort(key=lambda row: row["observation_id"])
        return copy.deepcopy(rows)

    def _sync_ard_loader_schema_in_memory(self, schema: dict[str, Any], record: dict[str, Any]) -> None:
        batch_row = _ard_loader_batch_record(schema, record)
        existing = self.ard_batches.get(record["batch_id"])
        batch_pk = int(existing["id"]) if existing is not None else self._next_ard_batch_id
        if existing is None:
            self._next_ard_batch_id += 1
        self.ard_batches[record["batch_id"]] = {
            **(existing or {}),
            **batch_row,
            "id": batch_pk,
        }
        if record["data_type"] == "carbon":
            self.ard_assets = {key: value for key, value in self.ard_assets.items() if int(value.get("batch_id") or -1) != batch_pk}
            observations = _ard_observations_from_record(record, batch_pk)
            keep_ids = {row["observation_id"] for row in observations}
            self.ard_observations = {
                key: value
                for key, value in self.ard_observations.items()
                if not (int(value.get("batch_id") or -1) == batch_pk and key not in keep_ids)
            }
            for row in observations:
                self.ard_observations[row["observation_id"]] = {**self.ard_observations.get(row["observation_id"], {}), **row}
            return
        self.ard_observations = {key: value for key, value in self.ard_observations.items() if int(value.get("batch_id") or -1) != batch_pk}
        assets = _ard_assets_from_record(record, batch_pk)
        keep_ids = {row["asset_id"] for row in assets}
        self.ard_assets = {
            key: value
            for key, value in self.ard_assets.items()
            if not (int(value.get("batch_id") or -1) == batch_pk and key not in keep_ids)
        }
        for row in assets:
            self.ard_assets[row["asset_id"]] = {**self.ard_assets.get(row["asset_id"], {}), **row}

    def _filter_received_member_rows(
        self,
        rows: list[dict[str, Any]],
        *,
        batch_ids: list[str] | None = None,
        source_system: str | None = None,
    ) -> list[dict[str, Any]]:
        wanted_batch_ids = {str(batch_id) for batch_id in batch_ids or [] if str(batch_id).strip()}
        filtered: list[dict[str, Any]] = []
        for row in rows:
            batch = self._received_batch_for_member(int(row.get("batch_id") or -1))
            if batch is None:
                continue
            batch_key = str(batch["batch_id"])
            if wanted_batch_ids and batch_key not in wanted_batch_ids:
                continue
            if source_system and str(batch.get("source_system") or "") != source_system:
                continue
            filtered.append({**row, "batch_key": batch_key, "data_type": batch.get("data_type")})
        return filtered

    def _received_batch_for_member(self, batch_pk: int) -> dict[str, Any] | None:
        for row in self.ard_batches.values():
            if int(row.get("id") or -1) == batch_pk:
                return row
        return None

    def create_attempt(
        self,
        *,
        task_id: str,
        batch_id: str,
        operation: str,
        payload: dict[str, Any],
        asset_ids: list[str] | None = None,
        requested_by: str = "system",
        source_task_id: str | None = None,
        retry_strategy: str | None = None,
        failure_reason: str | None = None,
    ) -> dict[str, Any]:
        batch = self.batches[batch_id]
        status = str(batch.get("status") or "")
        if status in BATCH_RUN_ACTIVE_STATUSES:
            raise PartitionBatchAlreadyActiveError(f"Partition batch already has an active task: {batch_id}")
        if status == "archived":
            raise PartitionBatchArchivedError(f"Partition batch is archived: {batch_id}")
        attempt_no = int(batch.get("attempt_count") or 0) + 1
        batch["attempt_count"] = attempt_no
        batch["status"] = "retrying" if "retry" in operation else "queued"
        batch["last_task_id"] = task_id
        batch["quality_status"] = None
        batch["quality_report_id"] = None
        batch["quality_failure_reason"] = None
        batch["ingest_status"] = _initial_ingest_status(batch.get("data_type"))
        batch["ingest_job_id"] = None
        batch["ingest_error"] = None
        batch["ingested_at"] = None
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
            "source_task_id": source_task_id,
            "retry_strategy": retry_strategy,
            "failure_reason": failure_reason,
            "started_at": None,
            "finished_at": None,
            "created_at": _utc_now_iso(),
            "updated_at": _utc_now_iso(),
        }
        self.attempts[task_id] = attempt
        return copy.deepcopy(attempt)

    def start_attempt(self, task_id: str) -> bool:
        attempt = self.attempts.get(task_id)
        if attempt is None:
            return False
        if str(attempt.get("status") or "") != "queued":
            return False
        attempt["status"] = "running"
        attempt["started_at"] = attempt["started_at"] or _utc_now_iso()
        attempt["updated_at"] = _utc_now_iso()
        batch = self.batches[attempt["batch_id"]]
        batch["status"] = "running"
        batch["last_task_id"] = task_id
        batch["updated_at"] = _utc_now_iso()
        self._set_assets_status(attempt, "running")
        return True

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
        self._apply_quality_result(attempt["batch_id"], result, now=now)
        self._refresh_ingest_readiness(attempt["batch_id"], result, now=now)

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
        failure_status = "manual_required" if manual_required else "failed"
        scoped_asset_ids = self._failed_asset_ids_from_error(attempt["batch_id"], attempt.get("asset_ids") or [], error, error_type=error_type)
        if scoped_asset_ids:
            self._set_assets_status_by_ids(scoped_asset_ids, failure_status, now=now, last_error=error)
            unaffected_asset_ids = [
                asset_id
                for asset_id in self._target_asset_ids(attempt["batch_id"], attempt.get("asset_ids") or None)
                if asset_id not in set(scoped_asset_ids)
            ]
            self._set_assets_status_by_ids(unaffected_asset_ids, "pending", now=now)
            self._refresh_batch_from_assets(attempt["batch_id"], now=now)
            batch = self.batches[attempt["batch_id"]]
            batch["last_error"] = error
            batch["manual_required_at"] = now if batch.get("status") == "manual_required" else None
            batch["updated_at"] = now
        else:
            batch = self.batches[attempt["batch_id"]]
            batch["status"] = failure_status
            batch["last_error"] = error
            batch["manual_required_at"] = now if manual_required else batch.get("manual_required_at")
            batch["updated_at"] = now
            self._set_assets_status(attempt, failure_status, last_error=error)

    def mark_batch_queued(self, batch_id: str, task_id: str, *, operation: str) -> None:
        batch = self.batches[batch_id]
        batch["status"] = "retrying" if "retry" in operation else "queued"
        batch["last_task_id"] = task_id
        batch["quality_status"] = None
        batch["quality_report_id"] = None
        batch["quality_failure_reason"] = None
        batch["ingest_status"] = _initial_ingest_status(batch.get("data_type"))
        batch["ingest_job_id"] = None
        batch["ingest_error"] = None
        batch["ingested_at"] = None
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

    def list_tasks(
        self,
        *,
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        rows = [_task_row_from_attempt(attempt, self.batches.get(attempt["batch_id"], {}), self.assets) for attempt in self.attempts.values()]
        if status:
            rows = [row for row in rows if row["status"] == status]
        if data_type:
            rows = [row for row in rows if row["data_type"] == data_type]
        if keyword:
            needle = keyword.lower()
            rows = [row for row in rows if needle in _task_search_text(row)]
        rows.sort(key=lambda row: row.get("created_at") or "", reverse=True)
        return copy.deepcopy(rows[offset : offset + limit])

    def count_tasks(
        self,
        *,
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
    ) -> int:
        rows = [_task_row_from_attempt(attempt, self.batches.get(attempt["batch_id"], {}), self.assets) for attempt in self.attempts.values()]
        if status:
            rows = [row for row in rows if row["status"] == status]
        if data_type:
            rows = [row for row in rows if row["data_type"] == data_type]
        if keyword:
            needle = keyword.lower()
            rows = [row for row in rows if needle in _task_search_text(row)]
        return len(rows)

    def _set_assets_status(self, attempt: dict[str, Any], status: str, **updates: Any) -> None:
        asset_ids = attempt.get("asset_ids") or [
            asset_id for asset_id, asset in self.assets.items() if asset["batch_id"] == attempt["batch_id"]
        ]
        self._set_assets_status_by_ids(asset_ids, status, now=_utc_now_iso(), **updates)

    def _set_assets_status_by_ids(self, asset_ids: list[str], status: str, *, now: str, **updates: Any) -> None:
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

    def _failed_asset_ids_from_error(
        self,
        batch_id: str,
        asset_ids: list[str],
        error: str,
        *,
        error_type: str | None = None,
    ) -> list[str] | None:
        target_ids = self._target_asset_ids(batch_id, asset_ids or None)
        if not target_ids:
            return None
        normalized_error_type = str(error_type or "").strip().lower()
        if normalized_error_type != "source_missing":
            return None
        text = str(error or "")
        matched: list[str] = []
        for asset_id in target_ids:
            asset = self.assets.get(asset_id)
            if asset is None:
                continue
            source_uri = str(asset.get("source_uri") or "").strip()
            if source_uri and source_uri in text:
                matched.append(asset_id)
                continue
            source_name = source_uri.rsplit("/", 1)[-1]
            if source_name and source_name in text:
                matched.append(asset_id)
        return matched or None

    def _repair_missing_payload_assets(self, batch_id: str) -> None:
        batch = self.batches.get(batch_id)
        if batch is None:
            return
        now = _utc_now_iso()
        for asset in _assets_from_record(batch):
            if _batch_has_matching_asset(self.assets.values(), batch_id, asset):
                continue
            asset = _dedupe_asset_id_for_batch(asset, existing_batch_id=self.assets.get(asset["asset_id"], {}).get("batch_id"))
            self.assets[asset["asset_id"]] = {
                **asset,
                "status": "pending",
                "attempt_count": 0,
                "last_error": None,
                "last_run_dir": None,
                "partitioned_at": None,
                "created_at": now,
                "updated_at": now,
            }

    def _refresh_batch_from_assets(self, batch_id: str, *, now: str) -> None:
        batch = self.batches[batch_id]
        assets = [asset for asset in self.assets.values() if asset["batch_id"] == batch_id]
        if not assets:
            if batch.get("data_type") == "carbon":
                batch["status"] = "succeeded"
                batch["last_error"] = None
                batch["partitioned_at"] = now
                batch["manual_required_at"] = None
                batch["updated_at"] = now
                return
            batch["status"] = "failed"
            batch["last_error"] = "No partition assets found for batch"
            batch["manual_required_at"] = now
            batch["updated_at"] = now
            return
        if all(asset["status"] == "succeeded" for asset in assets):
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

    def _apply_quality_result(self, batch_id: str, result: dict[str, Any], *, now: str) -> None:
        quality = _quality_result_summary(result)
        if quality is None:
            return
        batch = self.batches[batch_id]
        batch["quality_status"] = quality["quality_status"]
        batch["quality_report_id"] = quality.get("quality_report_id")
        batch["quality_failure_reason"] = quality.get("quality_failure_reason")
        if quality["quality_status"] == "FAIL":
            reason = quality.get("quality_failure_reason") or f"Quality status is {quality['quality_status']}"
            batch["status"] = "manual_required"
            batch["last_error"] = reason
            batch["manual_required_at"] = batch.get("manual_required_at") or now
        batch["updated_at"] = now

    def _refresh_ingest_readiness(self, batch_id: str, result: dict[str, Any], *, now: str) -> None:
        batch = self.batches[batch_id]
        batch["ingest_status"] = _ingest_status_for_batch(
            data_type=batch.get("data_type"),
            batch_status=batch.get("status"),
            result=result,
        )
        batch["ingest_job_id"] = None
        batch["ingest_error"] = None
        batch["ingested_at"] = now if batch["ingest_status"] == "ingested" else None
        batch["updated_at"] = now


class PostgresPartitionJobStore(PartitionJobStore):
    def __init__(self, dsn: str) -> None:
        if not dsn:
            raise ValueError("PostgreSQL DSN is required")
        self.dsn = dsn
        self._ard_loader_schema_available: bool | None = None
        self._schema_ensured = False
        self._schema_lock = Lock()

    def ensure_schema(self) -> None:
        if self._schema_ensured:
            return
        with self._schema_lock:
            if self._schema_ensured:
                return
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
                          quality_status TEXT,
                          quality_report_id TEXT,
                          quality_failure_reason TEXT,
                          ingest_status TEXT NOT NULL DEFAULT 'not_ready',
                          ingest_job_id TEXT,
                          ingest_error TEXT,
                          ingested_at TIMESTAMPTZ,
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
                          source_task_id TEXT,
                          retry_strategy TEXT,
                          failure_reason TEXT,
                          started_at TIMESTAMPTZ,
                          finished_at TIMESTAMPTZ,
                          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                        )
                        """
                    )
                    cur.execute("ALTER TABLE partition_job_attempts ADD COLUMN IF NOT EXISTS error_type TEXT")
                    cur.execute("ALTER TABLE partition_job_attempts ADD COLUMN IF NOT EXISTS source_task_id TEXT")
                    cur.execute("ALTER TABLE partition_job_attempts ADD COLUMN IF NOT EXISTS retry_strategy TEXT")
                    cur.execute("ALTER TABLE partition_job_attempts ADD COLUMN IF NOT EXISTS failure_reason TEXT")
                    cur.execute("ALTER TABLE partition_batches ADD COLUMN IF NOT EXISTS quality_status TEXT")
                    cur.execute("ALTER TABLE partition_batches ADD COLUMN IF NOT EXISTS quality_report_id TEXT")
                    cur.execute("ALTER TABLE partition_batches ADD COLUMN IF NOT EXISTS quality_failure_reason TEXT")
                    cur.execute("ALTER TABLE partition_batches ADD COLUMN IF NOT EXISTS ingest_status TEXT DEFAULT 'not_ready'")
                    cur.execute("ALTER TABLE partition_batches ADD COLUMN IF NOT EXISTS ingest_job_id TEXT")
                    cur.execute("ALTER TABLE partition_batches ADD COLUMN IF NOT EXISTS ingest_error TEXT")
                    cur.execute("ALTER TABLE partition_batches ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ")
                    cur.execute(
                        """
                        UPDATE partition_batches
                        SET ingest_status = CASE
                          WHEN data_type = ANY(%s::text[]) AND status = 'succeeded' THEN
                            CASE
                              WHEN COALESCE(LOWER(normalized_payload->>'ingest_enabled'), '') IN ('false', '0', 'no') THEN 'ready'
                              WHEN data_type = 'radar'
                               AND COALESCE(NULLIF(LOWER(normalized_payload->>'metadata_backend'), ''), 'none') IN ('none', 'local')
                              THEN 'ready'
                              WHEN data_type IN ('optical', 'product', 'entity')
                               AND COALESCE(NULLIF(LOWER(normalized_payload->>'metadata_backend'), ''), 'postgres') IN ('none', 'local')
                              THEN 'ready'
                              ELSE 'ingested'
                            END
                          WHEN data_type = ANY(%s::text[]) THEN 'not_ready'
                          ELSE 'not_supported'
                        END
                        WHERE data_type = ANY(%s::text[])
                           OR ingest_status IS NULL
                           OR (
                             NOT (data_type = ANY(%s::text[]))
                             AND ingest_status <> 'not_supported'
                           )
                        """
                        ,
                        (
                            sorted(INGEST_TRACKED_DATA_TYPES),
                            sorted(INGEST_TRACKED_DATA_TYPES),
                            sorted(INGEST_TRACKED_DATA_TYPES),
                            sorted(INGEST_TRACKED_DATA_TYPES),
                        ),
                    )
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_partition_batches_status ON partition_batches(status)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_partition_batches_type_status ON partition_batches(data_type, status)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_partition_assets_batch_status ON partition_assets(batch_id, status)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_partition_attempts_batch ON partition_job_attempts(batch_id, created_at DESC)")
                # M2 keeps scheduling tables intact and installs its versioned
                # dataset domain only after their FK targets are present.
                from cube_web.services.partition_domain_schema import apply_schema

                apply_schema(conn)
                conn.commit()
            self._schema_ensured = True

    def upsert_schema(self, schema: dict[str, Any]) -> dict[str, Any]:
        self.ensure_schema()
        record = _normalized_schema_record(schema)
        batch_params = _jsonb_record(record, "source_schema", "normalized_payload")
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    MERGE INTO partition_batches target
                    USING (
                      SELECT
                        %(batch_id)s::text AS batch_id,
                        %(batch_name)s::text AS batch_name,
                        %(data_type)s::text AS data_type,
                        %(source_system)s::text AS source_system,
                        %(source_schema)s::jsonb AS source_schema,
                        %(normalized_payload)s::jsonb AS normalized_payload,
                        %(priority)s::int AS priority,
                        %(max_auto_retries)s::int AS max_auto_retries,
                        %(ingest_status)s::text AS ingest_status
                    ) source
                    ON (target.batch_id = source.batch_id)
                    WHEN MATCHED THEN UPDATE SET
                      batch_name = source.batch_name,
                      data_type = source.data_type,
                      source_system = source.source_system,
                      source_schema = source.source_schema,
                      normalized_payload = source.normalized_payload,
                      priority = source.priority,
                      max_auto_retries = source.max_auto_retries,
                      updated_at = now()
                        WHEN NOT MATCHED THEN INSERT (
                          batch_id, batch_name, data_type, source_system, source_schema,
                          normalized_payload, priority, max_auto_retries, ingest_status
                        ) VALUES (
                          source.batch_id, source.batch_name, source.data_type, source.source_system, source.source_schema,
                          source.normalized_payload, source.priority, source.max_auto_retries, source.ingest_status
                        )
                    """,
                    batch_params,
                )
                cur.execute("SELECT * FROM partition_batches WHERE batch_id = %s", (record["batch_id"],))
                batch = _dict_row(cur)
                assets = _assets_from_record(record)
                stored_asset_ids: list[str] = []
                for asset in assets:
                    cur.execute("SELECT batch_id FROM partition_assets WHERE asset_id = %s", (asset["asset_id"],))
                    existing_asset = cur.fetchone()
                    existing_batch_id = None if existing_asset is None else str(existing_asset[0] or "")
                    asset = _dedupe_asset_id_for_batch(asset, existing_batch_id=existing_batch_id)
                    stored_asset_ids.append(asset["asset_id"])
                    cur.execute(
                        """
                        MERGE INTO partition_assets target
                        USING (
                          SELECT
                            %(asset_id)s::text AS asset_id,
                            %(batch_id)s::text AS batch_id,
                            %(data_type)s::text AS data_type,
                            %(scene_id)s::text AS scene_id,
                            %(source_uri)s::text AS source_uri,
                            %(asset_payload)s::jsonb AS asset_payload
                        ) source
                        ON (target.asset_id = source.asset_id)
                        WHEN MATCHED THEN UPDATE SET
                          batch_id = source.batch_id,
                          data_type = source.data_type,
                          scene_id = source.scene_id,
                          source_uri = source.source_uri,
                          asset_payload = source.asset_payload,
                          status = CASE
                            WHEN target.batch_id = source.batch_id THEN target.status
                            ELSE 'pending'
                          END,
                          attempt_count = CASE
                            WHEN target.batch_id = source.batch_id THEN target.attempt_count
                            ELSE 0
                          END,
                          last_error = CASE
                            WHEN target.batch_id = source.batch_id THEN target.last_error
                            ELSE NULL
                          END,
                          last_run_dir = CASE
                            WHEN target.batch_id = source.batch_id THEN target.last_run_dir
                            ELSE NULL
                          END,
                          partitioned_at = CASE
                            WHEN target.batch_id = source.batch_id THEN target.partitioned_at
                            ELSE NULL
                          END,
                          updated_at = now()
                        WHEN NOT MATCHED THEN INSERT (
                          asset_id, batch_id, data_type, scene_id, source_uri, asset_payload
                        ) VALUES (
                          source.asset_id, source.batch_id, source.data_type, source.scene_id, source.source_uri, source.asset_payload
                        )
                        """,
                        _jsonb_record(asset, "asset_payload"),
                    )
                if assets:
                    cur.execute(
                        "DELETE FROM partition_assets WHERE batch_id = %s AND NOT (asset_id = ANY(%s::text[]))",
                        (record["batch_id"], stored_asset_ids),
                    )
                else:
                    cur.execute("DELETE FROM partition_assets WHERE batch_id = %s", (record["batch_id"],))
                if _should_sync_ard_loader_schema(record):
                    self._sync_ard_loader_schema(cur, schema, record)
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
            where.append("status NOT IN ('succeeded', 'archived')")
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

    def get_batch_by_quality_report_id(self, data_type: str, quality_report_id: str) -> dict[str, Any] | None:
        self.ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM partition_batches
                    WHERE data_type = %s
                      AND quality_report_id = %s
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    (data_type, quality_report_id),
                )
                row = cur.fetchone()
                return None if row is None else _row_to_dict(cur, row)

    def archive_batch(self, batch_id: str) -> dict[str, Any] | None:
        self.ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE partition_batches
                    SET status = 'archived',
                        updated_at = now()
                    WHERE batch_id = %s
                      AND status NOT IN ('queued', 'running', 'retrying', 'cancel_requested')
                    RETURNING *
                    """,
                    (batch_id,),
                )
                row = cur.fetchone()
                if row is None:
                    cur.execute("SELECT status FROM partition_batches WHERE batch_id = %s", (batch_id,))
                    status_row = cur.fetchone()
                    if status_row is None:
                        return None
                    current_status = str(status_row[0] or "")
                    if current_status in BATCH_RUN_ACTIVE_STATUSES:
                        raise PartitionBatchAlreadyActiveError(f"Partition batch already has an active task: {batch_id}")
                    raise PartitionBatchNotRequeueableError(
                        f"Partition batch is not requeueable in status {current_status or '-'}: {batch_id}"
                    )
                batch = _row_to_dict(cur, row)
            conn.commit()
        return batch

    def requeue_batch(self, batch_id: str) -> dict[str, Any] | None:
        self.ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE partition_batches
                    SET status = 'pending',
                        last_error = NULL,
                        manual_required_at = NULL,
                        updated_at = now()
                    WHERE batch_id = %s
                      AND status NOT IN ('queued', 'running', 'retrying', 'cancel_requested')
                      AND status = ANY(%s::text[])
                    RETURNING *
                    """,
                    (batch_id, sorted(BATCH_REQUEUEABLE_STATUSES)),
                )
                row = cur.fetchone()
                if row is None:
                    cur.execute("SELECT status FROM partition_batches WHERE batch_id = %s", (batch_id,))
                    status_row = cur.fetchone()
                    if status_row is None:
                        return None
                    if str(status_row[0] or "") in BATCH_RUN_ACTIVE_STATUSES:
                        raise PartitionBatchAlreadyActiveError(f"Partition batch already has an active task: {batch_id}")
                    return None
                batch = _row_to_dict(cur, row)
                cur.execute(
                    """
                    UPDATE partition_assets
                    SET status = 'pending',
                        last_error = NULL,
                        updated_at = now()
                    WHERE batch_id = %s
                    """,
                    (batch_id,),
                )
            conn.commit()
        return batch

    def update_ingest_status(
        self,
        batch_id: str,
        ingest_status: str,
        *,
        job_id: str | None = None,
        error: str | None = None,
        ingested: bool = False,
    ) -> dict[str, Any] | None:
        self.ensure_schema()
        ingested_at_sql = "now()" if ingested else "NULL"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE partition_batches
                    SET ingest_status = %s,
                        ingest_job_id = %s,
                        ingest_error = %s,
                        ingested_at = {ingested_at_sql},
                        updated_at = now()
                    WHERE batch_id = %s
                    RETURNING *
                    """,
                    (ingest_status, job_id, error, batch_id),
                )
                row = cur.fetchone()
                batch = None if row is None else _row_to_dict(cur, row)
            conn.commit()
        return batch

    def ensure_runtime_batch(
        self,
        *,
        batch_id: str,
        batch_name: str,
        data_type: str,
        payload: dict[str, Any],
        max_auto_retries: int = 0,
    ) -> dict[str, Any]:
        self.ensure_schema()
        schema = {
            "batch_id": batch_id,
            "batch_name": batch_name or batch_id,
            "data_type": data_type,
            "source_system": "runtime",
            "normalized_payload": copy.deepcopy(payload),
        }
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    MERGE INTO partition_batches target
                    USING (
                      SELECT
                        %(batch_id)s::text AS batch_id,
                        %(batch_name)s::text AS batch_name,
                        %(data_type)s::text AS data_type,
                        %(source_system)s::text AS source_system,
                        %(source_schema)s::jsonb AS source_schema,
                        %(normalized_payload)s::jsonb AS normalized_payload,
                        %(max_auto_retries)s::int AS max_auto_retries,
                        %(ingest_status)s::text AS ingest_status
                    ) source
                    ON (target.batch_id = source.batch_id)
                    WHEN MATCHED THEN UPDATE SET
                      batch_name = source.batch_name,
                      data_type = source.data_type,
                      source_system = COALESCE(target.source_system, source.source_system),
                      source_schema = CASE
                        WHEN target.source_system = 'runtime' THEN source.source_schema
                        ELSE target.source_schema
                      END,
                      normalized_payload = CASE
                        WHEN target.source_system = 'runtime' THEN source.normalized_payload
                        ELSE target.normalized_payload
                      END,
                      max_auto_retries = CASE
                        WHEN target.source_system = 'runtime' THEN source.max_auto_retries
                        ELSE target.max_auto_retries
                      END,
                      updated_at = now()
                        WHEN NOT MATCHED THEN INSERT (
                          batch_id, batch_name, data_type, source_system, source_schema,
                          normalized_payload, max_auto_retries, ingest_status
                        ) VALUES (
                          source.batch_id, source.batch_name, source.data_type, source.source_system,
                          source.source_schema, source.normalized_payload, source.max_auto_retries, source.ingest_status
                        )
                    """,
                    _jsonb_record(
                        {
                            **schema,
                                "source_schema": schema,
                                "max_auto_retries": max_auto_retries,
                                "ingest_status": _initial_ingest_status(data_type),
                            },
                        "source_schema",
                        "normalized_payload",
                    ),
                )
                assets = _runtime_assets_from_payload(
                    {
                        "batch_id": batch_id,
                        "data_type": data_type,
                        "normalized_payload": payload,
                    }
                )
                stored_asset_ids: list[str] = []
                for asset in assets:
                    cur.execute("SELECT batch_id FROM partition_assets WHERE asset_id = %s", (asset["asset_id"],))
                    existing_asset = cur.fetchone()
                    existing_batch_id = None if existing_asset is None else str(existing_asset[0] or "")
                    asset = _dedupe_asset_id_for_batch(asset, existing_batch_id=existing_batch_id)
                    stored_asset_ids.append(asset["asset_id"])
                    cur.execute(
                        """
                        MERGE INTO partition_assets target
                        USING (
                          SELECT
                            %(asset_id)s::text AS asset_id,
                            %(batch_id)s::text AS batch_id,
                            %(data_type)s::text AS data_type,
                            %(scene_id)s::text AS scene_id,
                            %(source_uri)s::text AS source_uri,
                            %(asset_payload)s::jsonb AS asset_payload
                        ) source
                        ON (target.asset_id = source.asset_id)
                        WHEN MATCHED THEN UPDATE SET
                          batch_id = source.batch_id,
                          data_type = source.data_type,
                          scene_id = source.scene_id,
                          source_uri = source.source_uri,
                          asset_payload = source.asset_payload,
                          status = CASE
                            WHEN target.batch_id = source.batch_id
                              AND target.data_type = source.data_type
                              AND target.scene_id IS NOT DISTINCT FROM source.scene_id
                              AND target.source_uri = source.source_uri
                              AND target.asset_payload = source.asset_payload THEN target.status
                            ELSE 'pending'
                          END,
                          attempt_count = CASE
                            WHEN target.batch_id = source.batch_id
                              AND target.data_type = source.data_type
                              AND target.scene_id IS NOT DISTINCT FROM source.scene_id
                              AND target.source_uri = source.source_uri
                              AND target.asset_payload = source.asset_payload THEN target.attempt_count
                            ELSE 0
                          END,
                          last_error = CASE
                            WHEN target.batch_id = source.batch_id
                              AND target.data_type = source.data_type
                              AND target.scene_id IS NOT DISTINCT FROM source.scene_id
                              AND target.source_uri = source.source_uri
                              AND target.asset_payload = source.asset_payload THEN target.last_error
                            ELSE NULL
                          END,
                          last_run_dir = CASE
                            WHEN target.batch_id = source.batch_id
                              AND target.data_type = source.data_type
                              AND target.scene_id IS NOT DISTINCT FROM source.scene_id
                              AND target.source_uri = source.source_uri
                              AND target.asset_payload = source.asset_payload THEN target.last_run_dir
                            ELSE NULL
                          END,
                          partitioned_at = CASE
                            WHEN target.batch_id = source.batch_id
                              AND target.data_type = source.data_type
                              AND target.scene_id IS NOT DISTINCT FROM source.scene_id
                              AND target.source_uri = source.source_uri
                              AND target.asset_payload = source.asset_payload THEN target.partitioned_at
                            ELSE NULL
                          END,
                          updated_at = now()
                        WHEN NOT MATCHED THEN INSERT (
                          asset_id, batch_id, data_type, scene_id, source_uri, asset_payload
                        ) VALUES (
                          source.asset_id, source.batch_id, source.data_type, source.scene_id, source.source_uri, source.asset_payload
                        )
                        """,
                        _jsonb_record(asset, "asset_payload"),
                    )
                if assets:
                    cur.execute(
                        "DELETE FROM partition_assets WHERE batch_id = %s AND NOT (asset_id = ANY(%s::text[]))",
                        (batch_id, stored_asset_ids),
                    )
                else:
                    cur.execute("DELETE FROM partition_assets WHERE batch_id = %s", (batch_id,))
                cur.execute("SELECT * FROM partition_batches WHERE batch_id = %s", (batch_id,))
                batch = _dict_row(cur)
            conn.commit()
        return batch

    def list_assets(self, batch_id: str, status: str | None = None) -> list[dict[str, Any]]:
        self.ensure_schema()
        self._repair_missing_payload_assets(batch_id)
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

    def list_assets_by_ids(self, asset_ids: list[str]) -> list[dict[str, Any]]:
        self.ensure_schema()
        wanted = [str(asset_id) for asset_id in asset_ids if str(asset_id).strip()]
        if not wanted:
            return []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM partition_assets WHERE asset_id = ANY(%s::text[]) ORDER BY asset_id",
                    (wanted,),
                )
                return [_row_to_dict(cur, row) for row in cur.fetchall()]

    def list_received_batches(
        self,
        *,
        batch_ids: list[str] | None = None,
        updated_since: datetime | None = None,
        source_system: str | None = None,
    ) -> list[dict[str, Any]]:
        self.ensure_schema()
        where = []
        params: list[Any] = []
        if batch_ids is not None:
            wanted = [str(batch_id) for batch_id in batch_ids if str(batch_id).strip()]
            if not wanted:
                return []
            where.append("batch_id = ANY(%s::varchar[])")
            params.append(wanted)
        if source_system:
            where.append("source_system = %s")
            params.append(source_system)
        if updated_since is not None:
            where.append("COALESCE(updated_at, loaded_at) >= %s")
            params.append(updated_since.astimezone(timezone.utc).replace(tzinfo=None))
        sql = "SELECT * FROM ard_partition_batches"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY COALESCE(updated_at, loaded_at), batch_id"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return [_row_to_dict(cur, row) for row in cur.fetchall()]

    def list_received_assets(
        self,
        *,
        batch_ids: list[str] | None = None,
        asset_ids: list[str] | None = None,
        source_system: str | None = None,
    ) -> list[dict[str, Any]]:
        self.ensure_schema()
        where = []
        params: list[Any] = []
        if batch_ids is not None:
            wanted = [str(batch_id) for batch_id in batch_ids if str(batch_id).strip()]
            if not wanted:
                return []
            where.append("b.batch_id = ANY(%s::varchar[])")
            params.append(wanted)
        if asset_ids is not None:
            wanted_assets = [str(asset_id) for asset_id in asset_ids if str(asset_id).strip()]
            if not wanted_assets:
                return []
            where.append("a.asset_id = ANY(%s::varchar[])")
            params.append(wanted_assets)
        if source_system:
            where.append("b.source_system = %s")
            params.append(source_system)
        sql = """
            SELECT a.*, b.batch_id AS batch_key, b.data_type
            FROM ard_partition_assets a
            JOIN ard_partition_batches b ON b.id = a.batch_id
        """
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY a.asset_id"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return [_row_to_dict(cur, row) for row in cur.fetchall()]

    def list_received_observations(
        self,
        *,
        batch_ids: list[str] | None = None,
        observation_ids: list[str] | None = None,
        source_system: str | None = None,
    ) -> list[dict[str, Any]]:
        self.ensure_schema()
        where = []
        params: list[Any] = []
        if batch_ids is not None:
            wanted = [str(batch_id) for batch_id in batch_ids if str(batch_id).strip()]
            if not wanted:
                return []
            where.append("b.batch_id = ANY(%s::varchar[])")
            params.append(wanted)
        if observation_ids is not None:
            wanted_observations = [str(observation_id) for observation_id in observation_ids if str(observation_id).strip()]
            if not wanted_observations:
                return []
            where.append("o.observation_id = ANY(%s::varchar[])")
            params.append(wanted_observations)
        if source_system:
            where.append("b.source_system = %s")
            params.append(source_system)
        sql = """
            SELECT o.*, b.batch_id AS batch_key, b.data_type
            FROM ard_partition_observations o
            JOIN ard_partition_batches b ON b.id = o.batch_id
        """
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY o.observation_id"
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
        source_task_id: str | None = None,
        retry_strategy: str | None = None,
        failure_reason: str | None = None,
    ) -> dict[str, Any]:
        self.ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                batch_status = "retrying" if "retry" in operation else "queued"
                cur.execute(
                    """
                        UPDATE partition_batches
                        SET attempt_count = attempt_count + 1,
                            status = %s,
                            last_task_id = %s,
                            quality_status = NULL,
                            quality_report_id = NULL,
                            quality_failure_reason = NULL,
                            ingest_status = CASE
                              WHEN data_type = ANY(%s::text[]) THEN 'not_ready'
                              ELSE 'not_supported'
                            END,
                            ingest_job_id = NULL,
                            ingest_error = NULL,
                            ingested_at = NULL,
                            updated_at = now()
                        WHERE batch_id = %s
                      AND status NOT IN ('queued', 'running', 'retrying', 'cancel_requested', 'archived')
                    RETURNING attempt_count
                    """,
                    (batch_status, task_id, sorted(INGEST_TRACKED_DATA_TYPES), batch_id),
                )
                row = cur.fetchone()
                if row is None:
                    cur.execute("SELECT status FROM partition_batches WHERE batch_id = %s", (batch_id,))
                    status_row = cur.fetchone()
                    if status_row is None:
                        raise KeyError(batch_id)
                    if str(status_row[0] or "") == "archived":
                        raise PartitionBatchArchivedError(f"Partition batch is archived: {batch_id}")
                    raise PartitionBatchAlreadyActiveError(f"Partition batch already has an active task: {batch_id}")
                self._increment_asset_attempts(cur, batch_id, asset_ids)
                cur.execute(
                    """
                    INSERT INTO partition_job_attempts (
                      task_id, batch_id, asset_ids, operation, status, attempt_no, payload, requested_by,
                      source_task_id, retry_strategy, failure_reason
                    )
                    VALUES (%s, %s, %s, %s, 'queued', %s, %s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (
                        task_id,
                        batch_id,
                        asset_ids or [],
                        operation,
                        row[0],
                        self._jsonb(payload),
                        requested_by,
                        source_task_id,
                        retry_strategy,
                        failure_reason,
                    ),
                )
                attempt = _dict_row(cur)
            conn.commit()
        return attempt

    def start_attempt(self, task_id: str) -> bool:
        self.ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE partition_job_attempts
                    SET status = 'running',
                        started_at = COALESCE(started_at, now()),
                        updated_at = now()
                    WHERE task_id = %s
                      AND status = 'queued'
                    RETURNING batch_id, asset_ids
                    """,
                    (task_id,),
                )
                row = cur.fetchone()
                if row is None:
                    conn.commit()
                    return False
                batch_id, asset_ids = row
                cur.execute(
                    "UPDATE partition_batches SET status = 'running', last_task_id = %s, updated_at = now() WHERE batch_id = %s",
                    (task_id, batch_id),
                )
                self._update_assets(cur, batch_id, asset_ids, "running")
            conn.commit()
        return True

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
                self._apply_quality_result(cur, batch_id, result)
                self._refresh_ingest_readiness(cur, batch_id, result)
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
                    "UPDATE partition_job_attempts SET status = %s, error_type = %s, error_message = %s, finished_at = now(), updated_at = now() WHERE task_id = %s RETURNING batch_id, asset_ids",
                    (status, error_type, error, task_id),
                )
                row = cur.fetchone()
                if row is None:
                    return
                batch_id, asset_ids = row
                scoped_asset_ids = self._failed_asset_ids_from_error(cur, batch_id, asset_ids or [], error, error_type=error_type)
                if scoped_asset_ids:
                    self._update_assets(cur, batch_id, scoped_asset_ids, status, last_error=error)
                    unaffected_asset_ids = [asset_id for asset_id in list(asset_ids or []) if asset_id not in set(scoped_asset_ids)]
                    if unaffected_asset_ids:
                        self._update_assets(cur, batch_id, unaffected_asset_ids, "pending", last_error=None)
                    self._refresh_batch_from_assets(cur, batch_id)
                    cur.execute(
                        "UPDATE partition_batches SET last_error = %s, manual_required_at = CASE WHEN status = 'manual_required' THEN COALESCE(manual_required_at, now()) ELSE NULL END, updated_at = now() WHERE batch_id = %s",
                        (error, batch_id),
                    )
                else:
                    cur.execute(
                        "UPDATE partition_batches SET status = %s, last_error = %s, manual_required_at = CASE WHEN %s THEN now() ELSE manual_required_at END, updated_at = now() WHERE batch_id = %s",
                        (status, error, manual_required, batch_id),
                    )
                    self._update_assets(cur, batch_id, asset_ids, status, last_error=error)
            conn.commit()

    def mark_batch_queued(self, batch_id: str, task_id: str, *, operation: str) -> None:
        self.ensure_schema()
        status = "retrying" if "retry" in operation else "queued"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        UPDATE partition_batches
                        SET status = %s,
                            last_task_id = %s,
                            quality_status = NULL,
                            quality_report_id = NULL,
                            quality_failure_reason = NULL,
                            ingest_status = CASE
                              WHEN data_type = ANY(%s::text[]) THEN 'not_ready'
                              ELSE 'not_supported'
                            END,
                            ingest_job_id = NULL,
                            ingest_error = NULL,
                            ingested_at = NULL,
                            updated_at = now()
                        WHERE batch_id = %s
                    """,
                    (status, task_id, sorted(INGEST_TRACKED_DATA_TYPES), batch_id),
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
                if status not in {"queued", "running", "retrying", "cancel_requested"}:
                    return self.get_attempt(task_id)
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
                    "UPDATE partition_job_attempts SET status = 'cancelled', finished_at = now(), updated_at = now() WHERE task_id = %s AND status IN ('queued', 'running', 'retrying', 'cancel_requested') RETURNING batch_id, asset_ids",
                    (task_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return self.get_attempt(task_id)
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

    def list_tasks(
        self,
        *,
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        self.ensure_schema()
        where = []
        params: list[Any] = []
        if status:
            where.append("a.status = %s")
            params.append(status)
        if data_type:
            where.append("b.data_type = %s")
            params.append(data_type)
        if keyword:
            where.append("(a.task_id ILIKE %s OR b.batch_id ILIKE %s OR b.batch_name ILIKE %s OR COALESCE(a.error_message, '') ILIKE %s)")
            params.extend([f"%{keyword}%"] * 4)
        sql = """
            SELECT
              a.*,
                  b.batch_name,
                  b.data_type,
                  b.status AS batch_status,
                  b.quality_status,
                  b.quality_report_id,
                  b.quality_failure_reason,
                  b.ingest_status,
                  b.ingest_job_id,
                  b.ingest_error,
                  b.ingested_at,
                  COALESCE(NULLIF(array_length(a.asset_ids, 1), 0), asset_counts.asset_count, 0) AS asset_count
            FROM partition_job_attempts a
            JOIN partition_batches b ON b.batch_id = a.batch_id
            LEFT JOIN (
              SELECT batch_id, count(*)::int AS asset_count
              FROM partition_assets
              GROUP BY batch_id
            ) asset_counts ON asset_counts.batch_id = a.batch_id
        """
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY a.created_at DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = [_row_to_dict(cur, row) for row in cur.fetchall()]
        return [_task_row_from_joined_attempt(row) for row in rows]

    def count_tasks(
        self,
        *,
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
    ) -> int:
        self.ensure_schema()
        where = []
        params: list[Any] = []
        if status:
            where.append("a.status = %s")
            params.append(status)
        if data_type:
            where.append("b.data_type = %s")
            params.append(data_type)
        if keyword:
            where.append("(a.task_id ILIKE %s OR b.batch_id ILIKE %s OR b.batch_name ILIKE %s OR COALESCE(a.error_message, '') ILIKE %s)")
            params.extend([f"%{keyword}%"] * 4)
        sql = """
            SELECT count(*)::int
            FROM partition_job_attempts a
            JOIN partition_batches b ON b.batch_id = a.batch_id
        """
        if where:
            sql += " WHERE " + " AND ".join(where)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
        return int(row[0] if row else 0)

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

    def _failed_asset_ids_from_error(
        self,
        cur,
        batch_id: str,
        asset_ids: list[str],
        error: str,
        *,
        error_type: str | None = None,
    ) -> list[str] | None:
        normalized_error_type = str(error_type or "").strip().lower()
        if normalized_error_type != "source_missing":
            return None
        target_ids = list(asset_ids or [])
        if not target_ids:
            cur.execute("SELECT asset_id FROM partition_assets WHERE batch_id = %s", (batch_id,))
            target_ids = [str(row[0]) for row in cur.fetchall()]
        if not target_ids:
            return None
        cur.execute(
            "SELECT asset_id, source_uri FROM partition_assets WHERE batch_id = %s AND asset_id = ANY(%s)",
            (batch_id, target_ids),
        )
        text = str(error or "")
        matched: list[str] = []
        for asset_id, source_uri in cur.fetchall():
            source_uri = str(source_uri or "").strip()
            if source_uri and source_uri in text:
                matched.append(str(asset_id))
                continue
            source_name = source_uri.rsplit("/", 1)[-1]
            if source_name and source_name in text:
                matched.append(str(asset_id))
        return matched or None

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

    def _repair_missing_payload_assets(self, batch_id: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM partition_batches WHERE batch_id = %s", (batch_id,))
                row = cur.fetchone()
                if row is None:
                    return
                batch = _row_to_dict(cur, row)
                assets = _assets_from_record(batch)
                if not assets:
                    return
                for asset in assets:
                    cur.execute(
                        """
                        SELECT 1
                        FROM partition_assets
                        WHERE batch_id = %s
                          AND (asset_id = %s OR source_uri = %s OR scene_id IS NOT DISTINCT FROM %s)
                        LIMIT 1
                        """,
                        (batch_id, asset["asset_id"], asset["source_uri"], asset["scene_id"]),
                    )
                    if cur.fetchone() is not None:
                        continue
                    cur.execute("SELECT batch_id FROM partition_assets WHERE asset_id = %s", (asset["asset_id"],))
                    existing = cur.fetchone()
                    existing_batch_id = None if existing is None else str(existing[0] or "")
                    asset = _dedupe_asset_id_for_batch(asset, existing_batch_id=existing_batch_id)
                    cur.execute(
                        """
                        MERGE INTO partition_assets target
                        USING (
                          SELECT
                            %(asset_id)s::text AS asset_id,
                            %(batch_id)s::text AS batch_id,
                            %(data_type)s::text AS data_type,
                            %(scene_id)s::text AS scene_id,
                            %(source_uri)s::text AS source_uri,
                            %(asset_payload)s::jsonb AS asset_payload
                        ) source
                        ON (target.asset_id = source.asset_id)
                        WHEN NOT MATCHED THEN INSERT (
                          asset_id, batch_id, data_type, scene_id, source_uri, asset_payload
                        ) VALUES (
                          source.asset_id, source.batch_id, source.data_type, source.scene_id, source.source_uri, source.asset_payload
                        )
                        """,
                        _jsonb_record(asset, "asset_payload"),
                    )
            conn.commit()

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
        cur.execute("SELECT data_type FROM partition_batches WHERE batch_id = %s", (batch_id,))
        batch_row = cur.fetchone()
        data_type = None if batch_row is None else batch_row[0]
        cur.execute("SELECT status, last_error FROM partition_assets WHERE batch_id = %s", (batch_id,))
        rows = cur.fetchall()
        if not rows:
            if data_type == "carbon":
                cur.execute(
                    "UPDATE partition_batches SET status = 'succeeded', last_error = NULL, partitioned_at = now(), manual_required_at = NULL, updated_at = now() WHERE batch_id = %s",
                    (batch_id,),
                )
                return
            cur.execute(
                "UPDATE partition_batches SET status = 'failed', last_error = 'No partition assets found for batch', manual_required_at = now(), updated_at = now() WHERE batch_id = %s",
                (batch_id,),
            )
            return
        if all(row[0] == "succeeded" for row in rows):
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

    def _apply_quality_result(self, cur, batch_id: str, result: dict[str, Any]) -> None:
        quality = _quality_result_summary(result)
        if quality is None:
            return
        if quality["quality_status"] == "FAIL":
            reason = quality.get("quality_failure_reason") or f"Quality status is {quality['quality_status']}"
            cur.execute(
                """
                UPDATE partition_batches
                SET quality_status = %s,
                    quality_report_id = %s,
                    quality_failure_reason = %s,
                    status = 'manual_required',
                    last_error = %s,
                    manual_required_at = COALESCE(manual_required_at, now()),
                    updated_at = now()
                WHERE batch_id = %s
                """,
                (
                    quality["quality_status"],
                    quality.get("quality_report_id"),
                    quality.get("quality_failure_reason"),
                    reason,
                    batch_id,
                ),
            )
            return
        cur.execute(
            """
            UPDATE partition_batches
            SET quality_status = %s,
                quality_report_id = %s,
                quality_failure_reason = %s,
                updated_at = now()
            WHERE batch_id = %s
            """,
            (
                quality["quality_status"],
                quality.get("quality_report_id"),
                quality.get("quality_failure_reason"),
                batch_id,
            ),
        )

    def _refresh_ingest_readiness(self, cur, batch_id: str, result: dict[str, Any]) -> None:
        cur.execute("SELECT data_type, status FROM partition_batches WHERE batch_id = %s", (batch_id,))
        row = cur.fetchone()
        if row is None:
            return
        data_type, batch_status = row
        ingest_status = _ingest_status_for_batch(
            data_type=data_type,
            batch_status=batch_status,
            result=result,
        )
        cur.execute(
            """
            UPDATE partition_batches
            SET ingest_status = %s,
                ingest_job_id = NULL,
                ingest_error = NULL,
                ingested_at = CASE
                  WHEN %s = 'ingested' THEN now()
                  ELSE NULL
                END,
                updated_at = now()
            WHERE batch_id = %s
            """,
            (
                ingest_status,
                ingest_status,
                batch_id,
            ),
        )

    def _connect(self):
        try:
            from cube_web.services.db_pool import _PostgresPool
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise RuntimeError("PostgreSQL partition job storage requires `psycopg`") from exc
        return _PostgresPool.for_dsn(self.dsn).connection()

    def _jsonb(self, value: dict[str, Any]):
        from psycopg.types.json import Jsonb

        return Jsonb(value, dumps=lambda item: json.dumps(item, ensure_ascii=False))

    def _sync_ard_loader_schema(self, cur, schema: dict[str, Any], record: dict[str, Any]) -> None:
        if not self._has_ard_loader_schema(cur):
            raise RuntimeError("ARD loader schema tables are required for partition schema import in PostgreSQL")
        batch_row = _ard_loader_batch_record(schema, record)
        cur.execute(
            """
            MERGE INTO ard_partition_batches target
            USING (
              SELECT
                %(schema_version)s::varchar(20) AS schema_version,
                %(batch_id)s::varchar(100) AS batch_id,
                %(batch_name)s::varchar(200) AS batch_name,
                %(data_type)s::varchar(50) AS data_type,
                %(source_system)s::varchar(50) AS source_system,
                %(loaded_at)s::timestamp AS loaded_at,
                %(updated_at)s::timestamp AS updated_at,
                %(raw_meta_uri)s::varchar(500) AS raw_meta_uri,
                %(priority)s::int AS priority,
                %(max_auto_retries)s::int AS max_auto_retries,
                %(status)s::varchar(50) AS status
            ) source
            ON (target.batch_id = source.batch_id)
            WHEN MATCHED THEN UPDATE SET
              schema_version = source.schema_version,
              batch_name = source.batch_name,
              data_type = source.data_type,
              source_system = source.source_system,
              loaded_at = COALESCE(source.loaded_at, target.loaded_at),
              updated_at = COALESCE(source.updated_at, target.updated_at),
              raw_meta_uri = source.raw_meta_uri,
              priority = source.priority,
              max_auto_retries = source.max_auto_retries
            WHEN NOT MATCHED THEN INSERT (
              schema_version, batch_id, batch_name, data_type, source_system,
              loaded_at, updated_at, raw_meta_uri, priority, max_auto_retries, status
            ) VALUES (
              source.schema_version, source.batch_id, source.batch_name, source.data_type, source.source_system,
              source.loaded_at, source.updated_at, source.raw_meta_uri, source.priority, source.max_auto_retries, source.status
            )
            """,
            batch_row,
        )
        cur.execute("SELECT id FROM ard_partition_batches WHERE batch_id = %s", (record["batch_id"],))
        ard_batch_id = _dict_row(cur)["id"]
        self._sync_ard_loader_batch_members(cur, ard_batch_id, record)

    def _sync_ard_loader_batch_members(self, cur, ard_batch_id: int, record: dict[str, Any]) -> None:
        data_type = str(record["data_type"])
        if data_type == "carbon":
            cur.execute("DELETE FROM ard_partition_assets WHERE batch_id = %s", (ard_batch_id,))
            observations = _ard_observations_from_record(record, ard_batch_id)
            for observation in observations:
                cur.execute(
                    """
                    MERGE INTO ard_partition_observations target
                    USING (
                      SELECT
                        %(batch_id)s::int AS batch_id,
                        %(observation_id)s::varchar(100) AS observation_id,
                        %(source_uri)s::varchar(500) AS source_uri,
                        %(source_index)s::int AS source_index,
                        %(acq_time)s::timestamp AS acq_time,
                        %(sensor)s::varchar(100) AS sensor,
                        %(product_family)s::varchar(100) AS product_family,
                        %(product_type)s::varchar(100) AS product_type,
                        %(resolution)s::double precision AS resolution,
                        %(lon)s::double precision AS lon,
                        %(lat)s::double precision AS lat,
                        %(xco2)s::double precision AS xco2,
                        %(quality_flag)s::varchar(20) AS quality_flag,
                        %(corners)s::json AS corners
                    ) source
                    ON (target.observation_id = source.observation_id)
                    WHEN MATCHED THEN UPDATE SET
                      batch_id = source.batch_id,
                      source_uri = source.source_uri,
                      source_index = source.source_index,
                      acq_time = source.acq_time,
                      sensor = source.sensor,
                      product_family = source.product_family,
                      product_type = source.product_type,
                      resolution = source.resolution,
                      lon = source.lon,
                      lat = source.lat,
                      xco2 = source.xco2,
                      quality_flag = source.quality_flag,
                      corners = source.corners
                    WHEN NOT MATCHED THEN INSERT (
                      batch_id, observation_id, source_uri, source_index, acq_time, sensor,
                      product_family, product_type, resolution, lon, lat, xco2, quality_flag, corners
                    ) VALUES (
                      source.batch_id, source.observation_id, source.source_uri, source.source_index, source.acq_time, source.sensor,
                      source.product_family, source.product_type, source.resolution, source.lon, source.lat, source.xco2, source.quality_flag, source.corners
                    )
                    """,
                    _json_record(observation, "corners"),
                )
            observation_ids = [row["observation_id"] for row in observations]
            if observation_ids:
                cur.execute(
                    "DELETE FROM ard_partition_observations WHERE batch_id = %s AND NOT (observation_id = ANY(%s::varchar[]))",
                    (ard_batch_id, observation_ids),
                )
            else:
                cur.execute("DELETE FROM ard_partition_observations WHERE batch_id = %s", (ard_batch_id,))
            return

        cur.execute("DELETE FROM ard_partition_observations WHERE batch_id = %s", (ard_batch_id,))
        assets = _ard_assets_from_record(record, ard_batch_id)
        for asset in assets:
            cur.execute(
                """
                MERGE INTO ard_partition_assets target
                USING (
                  SELECT
                    %(batch_id)s::int AS batch_id,
                    %(asset_id)s::varchar(100) AS asset_id,
                    %(source_uri)s::varchar(500) AS source_uri,
                    %(scene_id)s::varchar(100) AS scene_id,
                    %(acq_time)s::timestamp AS acq_time,
                    %(sensor)s::varchar(100) AS sensor,
                    %(product_family)s::varchar(100) AS product_family,
                    %(resolution)s::double precision AS resolution,
                    %(bbox)s::json AS bbox,
                    %(corners)s::json AS corners,
                    %(bands)s::json AS bands,
                    %(band)s::varchar(50) AS band,
                    %(file_format)s::varchar(50) AS file_format,
                    %(polarization)s::varchar(20) AS polarization,
                    %(sidecars)s::json AS sidecars,
                    %(orbit_direction)s::varchar(20) AS orbit_direction,
                    %(relative_orbit)s::varchar(50) AS relative_orbit,
                    %(product_name)s::varchar(200) AS product_name,
                    %(product_year)s::int AS product_year,
                    %(product_period)s::varchar(50) AS product_period,
                    %(variable)s::varchar(100) AS variable,
                    %(unit)s::varchar(50) AS unit
                ) source
                ON (target.asset_id = source.asset_id)
                WHEN MATCHED THEN UPDATE SET
                  batch_id = source.batch_id,
                  source_uri = source.source_uri,
                  scene_id = source.scene_id,
                  acq_time = source.acq_time,
                  sensor = source.sensor,
                  product_family = source.product_family,
                  resolution = source.resolution,
                  bbox = source.bbox,
                  corners = source.corners,
                  bands = source.bands,
                  band = source.band,
                  file_format = source.file_format,
                  polarization = source.polarization,
                  sidecars = source.sidecars,
                  orbit_direction = source.orbit_direction,
                  relative_orbit = source.relative_orbit,
                  product_name = source.product_name,
                  product_year = source.product_year,
                  product_period = source.product_period,
                  variable = source.variable,
                  unit = source.unit
                WHEN NOT MATCHED THEN INSERT (
                  batch_id, asset_id, source_uri, scene_id, acq_time, sensor,
                  product_family, resolution, bbox, corners, bands, band, file_format,
                  polarization, sidecars, orbit_direction, relative_orbit,
                  product_name, product_year, product_period, variable, unit
                ) VALUES (
                  source.batch_id, source.asset_id, source.source_uri, source.scene_id, source.acq_time, source.sensor,
                  source.product_family, source.resolution, source.bbox, source.corners, source.bands, source.band, source.file_format,
                  source.polarization, source.sidecars, source.orbit_direction, source.relative_orbit,
                  source.product_name, source.product_year, source.product_period, source.variable, source.unit
                )
                """,
                _json_record(asset, "bbox", "corners", "bands", "sidecars"),
            )
        asset_ids = [row["asset_id"] for row in assets]
        if asset_ids:
            cur.execute(
                "DELETE FROM ard_partition_assets WHERE batch_id = %s AND NOT (asset_id = ANY(%s::varchar[]))",
                (ard_batch_id, asset_ids),
            )
        else:
            cur.execute("DELETE FROM ard_partition_assets WHERE batch_id = %s", (ard_batch_id,))

    def _has_ard_loader_schema(self, cur) -> bool:
        if self._ard_loader_schema_available is not None:
            return self._ard_loader_schema_available
        cur.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
              AND table_name IN ('ard_partition_batches', 'ard_partition_assets', 'ard_partition_observations')
            """
        )
        self._ard_loader_schema_available = int(cur.fetchone()[0]) == 3
        return self._ard_loader_schema_available


_store: PartitionJobStore | None = None


def get_partition_job_store() -> PartitionJobStore:
    global _store
    if _store is None:
        _store = PostgresPartitionJobStore(runtime_config.postgres_dsn())
        if runtime_config.load_demo_partition_schemas():
            from cube_web.services.partition_loaded_schemas import ensure_standard_partition_schemas

            ensure_standard_partition_schemas(_store)
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
    if schema.get("loaded_at") not in {None, ""}:
        _parse_iso_datetime(schema.get("loaded_at"), "loaded_at")
    if schema.get("updated_at") not in {None, ""}:
        _parse_iso_datetime(schema.get("updated_at"), "updated_at")
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
        payload.setdefault("partition_method", schema.get("partition_method"))
        apply_resolution_grid_defaults(payload, data_type=data_type)
    elif data_type == "carbon":
        payload.setdefault("selected_observations", copy.deepcopy(schema.get("observations") or schema.get("selected_observations") or []))
    _validate_ard_payload(payload, data_type=data_type)
    return {
        "batch_id": batch_id,
        "batch_name": batch_name,
        "data_type": data_type,
        "source_system": _optional_text(schema.get("source_system")) or "loader",
        "source_schema": copy.deepcopy(schema),
        "normalized_payload": payload,
        "priority": int(schema.get("priority") or 0),
        "max_auto_retries": int(schema.get("max_auto_retries") if schema.get("max_auto_retries") is not None else 1),
        "ingest_status": _initial_ingest_status(data_type),
    }


def _should_sync_ard_loader_schema(record: dict[str, Any]) -> bool:
    source_system = str(record.get("source_system") or "").strip().lower()
    if source_system == "runtime":
        return False
    if source_system.startswith("standard_loaded_"):
        return False
    return True


def _ard_loader_batch_record(schema: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": str(schema.get("schema_version") or "1.0").strip() or "1.0",
        "batch_id": record["batch_id"],
        "batch_name": record["batch_name"],
        "data_type": record["data_type"],
        "source_system": _optional_text(schema.get("source_system")) or "loader",
        "loaded_at": _optional_datetime(schema.get("loaded_at")),
        "updated_at": _optional_datetime(schema.get("updated_at")),
        "raw_meta_uri": _optional_text(schema.get("raw_meta_uri")),
        "priority": record["priority"],
        "max_auto_retries": record["max_auto_retries"],
        "status": "pending",
    }


def _ard_assets_from_record(record: dict[str, Any], ard_batch_id: int) -> list[dict[str, Any]]:
    payload = record["normalized_payload"]
    rows: list[dict[str, Any]] = []
    for base_row, item in zip(_assets_from_record(record), payload.get("selected_assets") or []):
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "batch_id": ard_batch_id,
                "asset_id": base_row["asset_id"],
                "source_uri": base_row["source_uri"],
                "scene_id": base_row["scene_id"],
                "acq_time": _optional_datetime(item.get("acq_time")),
                "sensor": _optional_text(item.get("sensor")),
                "product_family": _optional_text(item.get("product_family")),
                "resolution": _optional_resolution(item.get("resolution")),
                "bbox": _json_value(item.get("bbox")),
                "corners": _json_value(item.get("corners")),
                "bands": _json_value(item.get("bands")),
                "band": _optional_text(item.get("band")),
                "file_format": _optional_text(item.get("file_format")) or "GeoTIFF",
                "polarization": _optional_text(item.get("polarization")),
                "sidecars": _json_value(item.get("sidecars")),
                "orbit_direction": _optional_text(item.get("orbit_direction")),
                "relative_orbit": _optional_text(item.get("relative_orbit")),
                "product_name": _optional_text(item.get("product_name")),
                "product_year": _optional_int(item.get("product_year")),
                "product_period": _optional_text(item.get("product_period")),
                "variable": _optional_text(item.get("variable")),
                "unit": _optional_text(item.get("unit")),
            }
        )
    return rows


def _ard_observations_from_record(record: dict[str, Any], ard_batch_id: int) -> list[dict[str, Any]]:
    payload = record["normalized_payload"]
    rows: list[dict[str, Any]] = []
    for item in payload.get("selected_observations") or []:
        if not isinstance(item, dict):
            continue
        observation_id = _optional_text(item.get("observation_id"))
        if not observation_id:
            continue
        source_uri = _optional_text(item.get("source_uri"))
        if not source_uri:
            continue
        rows.append(
            {
                "batch_id": ard_batch_id,
                "observation_id": observation_id,
                "source_uri": source_uri,
                "source_index": _optional_int(item.get("source_index"), default=0),
                "acq_time": _optional_datetime(item.get("acq_time")),
                "sensor": _optional_text(item.get("sensor")),
                "product_family": _optional_text(item.get("product_family")),
                "product_type": _optional_text(item.get("product_type")),
                "resolution": _optional_resolution(item.get("resolution")),
                "lon": _optional_float(item.get("lon")),
                "lat": _optional_float(item.get("lat")),
                "xco2": _optional_float(item.get("xco2")),
                "quality_flag": _optional_text(item.get("quality_flag")),
                "corners": _json_value(item.get("corners")),
            }
        )
    return rows


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


def _runtime_assets_from_payload(record: dict[str, Any]) -> list[dict[str, Any]]:
    payload = record.get("normalized_payload") or {}
    data_type = str(record.get("data_type") or "optical")
    items = payload.get("selected_observations") if data_type == "carbon" else payload.get("selected_assets")
    if not isinstance(items, list):
        return []
    batch_id = str(record["batch_id"])
    assets = []
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        source_uri = str(item.get("source_uri") or item.get("observation_id") or item.get("source_index") or f"{batch_id}:{idx}")
        scene_id = item.get("scene_id") or item.get("product_year") or item.get("observation_id") or item.get("source_index")
        asset_id = str(item.get("asset_id") or f"{batch_id}:{_stable_asset_key(source_uri, idx)}")
        assets.append(
            {
                "asset_id": asset_id,
                "batch_id": batch_id,
                "data_type": data_type,
                "scene_id": None if scene_id is None else str(scene_id),
                "source_uri": source_uri,
                "asset_payload": copy.deepcopy(item),
            }
        )
    return assets


def _dedupe_asset_id_for_batch(asset: dict[str, Any], *, existing_batch_id: str | None) -> dict[str, Any]:
    if not existing_batch_id or existing_batch_id == asset["batch_id"]:
        return asset
    item = copy.deepcopy(asset.get("asset_payload") or {})
    item.pop("asset_id", None)
    source_uri = str(asset.get("source_uri") or "")
    asset = dict(asset)
    asset["asset_id"] = f"{asset['batch_id']}:{_stable_asset_key(source_uri, 0)}"
    asset["asset_payload"] = item
    return asset


def _batch_has_matching_asset(assets: Any, batch_id: str, incoming: dict[str, Any]) -> bool:
    for asset in assets:
        if asset.get("batch_id") != batch_id:
            continue
        if asset.get("asset_id") == incoming.get("asset_id"):
            return True
        if asset.get("source_uri") == incoming.get("source_uri"):
            return True
        if incoming.get("scene_id") is not None and asset.get("scene_id") == incoming.get("scene_id"):
            return True
    return False


def _same_runtime_asset(existing: dict[str, Any], incoming: dict[str, Any]) -> bool:
    return (
        existing.get("batch_id") == incoming.get("batch_id")
        and existing.get("data_type") == incoming.get("data_type")
        and existing.get("scene_id") == incoming.get("scene_id")
        and existing.get("source_uri") == incoming.get("source_uri")
        and (existing.get("asset_payload") or {}) == (incoming.get("asset_payload") or {})
    )


def _task_row_from_attempt(
    attempt: dict[str, Any],
    batch: dict[str, Any],
    assets: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    batch_id = str(attempt.get("batch_id") or batch.get("batch_id") or "")
    asset_ids = [str(item) for item in attempt.get("asset_ids") or [] if item]
    asset_count = len(asset_ids)
    if not asset_count:
        asset_count = sum(1 for asset in assets.values() if asset.get("batch_id") == batch_id)
    raw_result = attempt.get("runner_result")
    result: dict[str, Any] = raw_result if isinstance(raw_result, dict) else {}
    return {
        "task_id": attempt.get("task_id"),
        "status": attempt.get("status"),
        "data_type": batch.get("data_type") or result.get("data_type"),
        "operation": _public_task_operation(attempt.get("operation")),
        "batch_id": batch_id,
        "batch_name": batch.get("batch_name") or batch_id,
        "batch_status": batch.get("status"),
        "quality_status": batch.get("quality_status"),
        "quality_report_id": batch.get("quality_report_id"),
        "quality_failure_reason": batch.get("quality_failure_reason"),
        "ingest_status": batch.get("ingest_status"),
        "ingest_job_id": batch.get("ingest_job_id"),
        "ingest_error": batch.get("ingest_error"),
        "ingested_at": batch.get("ingested_at"),
        "asset_ids": asset_ids,
        "asset_count": asset_count,
        "attempt_no": attempt.get("attempt_no"),
        "requested_by": attempt.get("requested_by"),
        "source_task_id": attempt.get("source_task_id"),
        "retry_strategy": attempt.get("retry_strategy"),
        "failure_reason": attempt.get("failure_reason"),
        "created_at": attempt.get("created_at"),
        "updated_at": attempt.get("updated_at"),
        "started_at": attempt.get("started_at"),
        "finished_at": attempt.get("finished_at"),
        "error_type": attempt.get("error_type"),
        "error_message": attempt.get("error_message"),
        "result_summary": _task_result_summary(result),
    }


def _task_row_from_joined_attempt(row: dict[str, Any]) -> dict[str, Any]:
    raw_result = row.get("runner_result")
    result: dict[str, Any] = raw_result if isinstance(raw_result, dict) else {}
    asset_ids = [str(item) for item in row.get("asset_ids") or [] if item]
    return {
        "task_id": row.get("task_id"),
        "status": row.get("status"),
        "data_type": row.get("data_type") or result.get("data_type"),
        "operation": _public_task_operation(row.get("operation")),
        "batch_id": row.get("batch_id"),
        "batch_name": row.get("batch_name") or row.get("batch_id"),
        "batch_status": row.get("batch_status"),
        "quality_status": row.get("quality_status"),
        "quality_report_id": row.get("quality_report_id"),
        "quality_failure_reason": row.get("quality_failure_reason"),
        "ingest_status": row.get("ingest_status"),
        "ingest_job_id": row.get("ingest_job_id"),
        "ingest_error": row.get("ingest_error"),
        "ingested_at": row.get("ingested_at"),
        "asset_ids": asset_ids,
        "asset_count": int(row.get("asset_count") or 0),
        "attempt_no": row.get("attempt_no"),
        "requested_by": row.get("requested_by"),
        "source_task_id": row.get("source_task_id"),
        "retry_strategy": row.get("retry_strategy"),
        "failure_reason": row.get("failure_reason"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "started_at": row.get("started_at"),
        "finished_at": row.get("finished_at"),
        "error_type": row.get("error_type"),
        "error_message": row.get("error_message"),
        "result_summary": _task_result_summary(result),
    }


def _public_task_operation(operation: Any) -> Any:
    if not isinstance(operation, str):
        return operation
    if operation.endswith("_run"):
        return "run"
    if operation.endswith("_retry"):
        return "retry"
    return operation


def _task_result_summary(result: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(result, dict) or not result:
        return {}
    return {
        "rows": result.get("rows") or result.get("total_index_rows") or result.get("metadata_rows"),
        "quality_status": result.get("quality_status") or (result.get("quality_report") or {}).get("status")
        if isinstance(result.get("quality_report") or {}, dict)
        else result.get("quality_status"),
        "quality_report_id": result.get("quality_report_id")
        or ((result.get("quality_report") or {}).get("report_id") if isinstance(result.get("quality_report") or {}, dict) else None),
        "run_dir": result.get("run_dir"),
        "rows_path": result.get("rows_path") or result.get("output_path"),
        "execution_engine": result.get("execution_engine") or result.get("partition_backend"),
    }


def _task_search_text(row: dict[str, Any]) -> str:
    values = [
        row.get("task_id"),
        row.get("status"),
        row.get("data_type"),
        row.get("operation"),
        row.get("batch_id"),
        row.get("batch_name"),
        row.get("error_message"),
    ]
    return " ".join(str(value) for value in values if value).lower()


def _validate_ard_payload(payload: dict[str, Any], *, data_type: str) -> None:
    if data_type == "carbon":
        _validate_carbon_observations(payload.get("selected_observations"))
        return
    _validate_raster_assets(payload.get("selected_assets"), data_type=data_type)


def _validate_raster_assets(value: Any, *, data_type: str) -> None:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{data_type} schema requires non-empty selected_assets/assets")
    for idx, asset in enumerate(value, start=1):
        if not isinstance(asset, dict):
            raise ValueError(f"{data_type} asset #{idx} must be an object")
        prefix = f"{data_type} asset #{idx}"
        for field in ("source_uri", "scene_id", "acq_time", "sensor", "product_family"):
            _require_text(asset, field, prefix)
        _parse_iso_datetime(asset["acq_time"], f"{prefix}.acq_time")
        _validate_bands(asset, prefix)
        _validate_corners(asset.get("corners"), f"{prefix}.corners")
        _validate_resolution(asset.get("resolution"), f"{prefix}.resolution")


def _validate_carbon_observations(value: Any) -> None:
    if not isinstance(value, list) or not value:
        raise ValueError("carbon schema requires non-empty selected_observations/observations")
    for idx, observation in enumerate(value, start=1):
        if not isinstance(observation, dict):
            raise ValueError(f"carbon observation #{idx} must be an object")
        prefix = f"carbon observation #{idx}"
        for field in ("source_uri", "observation_id", "acq_time", "sensor", "product_family"):
            _require_text(observation, field, prefix)
        _parse_iso_datetime(observation["acq_time"], f"{prefix}.acq_time")
        _validate_resolution(observation.get("resolution"), f"{prefix}.resolution")
        _validate_location(observation, prefix)


def _require_text(row: dict[str, Any], field: str, prefix: str) -> str:
    value = str(row.get(field) or "").strip()
    if not value:
        raise ValueError(f"{prefix}.{field} is required")
    return value


def _parse_iso_datetime(value: Any, label: str) -> None:
    try:
        datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        raise ValueError(f"{label} must be an ISO8601 datetime") from None


def _validate_bands(asset: dict[str, Any], prefix: str) -> None:
    bands = asset.get("bands")
    if isinstance(bands, list):
        if any(str(item).strip() for item in bands):
            return
    elif bands is not None and str(bands).strip():
        return
    for field in ("band", "polarization", "variable"):
        if str(asset.get(field) or "").strip():
            return
    raise ValueError(f"{prefix}.bands or polarization is required")


def _validate_corners(value: Any, label: str) -> None:
    if not isinstance(value, list) or len(value) != 4:
        raise ValueError(f"{label} must contain 4 [lon, lat] points")
    for point in value:
        if not isinstance(point, (list, tuple)) or len(point) != 2:
            raise ValueError(f"{label} must contain 4 [lon, lat] points")
        lon = _float_value(point[0], label)
        lat = _float_value(point[1], label)
        if not (-180.0 <= lon <= 180.0 and -90.0 <= lat <= 90.0):
            raise ValueError(f"{label} coordinate out of range")


def _validate_resolution(value: Any, label: str) -> None:
    resolution = _resolution_value(value, label)
    if resolution <= 0:
        raise ValueError(f"{label} must be greater than 0")


def _validate_location(observation: dict[str, Any], prefix: str) -> None:
    if "corners" in observation:
        _validate_corners(observation.get("corners"), f"{prefix}.corners")
        return
    if "footprint" in observation or "footprint_geojson" in observation:
        return
    lon = observation.get("lon", observation.get("center_lon"))
    lat = observation.get("lat", observation.get("center_lat"))
    lon_value = _float_value(lon, f"{prefix}.lon")
    lat_value = _float_value(lat, f"{prefix}.lat")
    if not (-180.0 <= lon_value <= 180.0 and -90.0 <= lat_value <= 90.0):
        raise ValueError(f"{prefix} coordinates out of range")


def _float_value(value: Any, label: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{label} must be numeric") from None


def _resolution_value(value: Any, label: str) -> float:
    if isinstance(value, str):
        text = value.strip().lower()
        if text.endswith("m"):
            text = text[:-1].strip()
        value = text
    return _float_value(value, label)


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_datetime(value: Any) -> datetime | None:
    text = _optional_text(value)
    if not text:
        return None
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _ard_datetime_or_none(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = _optional_text(value)
        if not text:
            return None
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _optional_int(value: Any, *, default: int | None = None) -> int | None:
    if value is None or value == "":
        return default
    return int(value)


def _optional_resolution(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return _resolution_value(value, "resolution")


def _json_value(value: Any) -> Any:
    return None if value is None else copy.deepcopy(value)


def _stable_asset_key(value: str, idx: int) -> str:
    import hashlib

    digest = hashlib.sha1(f"{idx}:{value}".encode("utf-8")).hexdigest()[:16]
    return digest


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _initial_ingest_status(data_type: Any) -> str:
    return "not_ready" if _supports_ingest_status(data_type) else "not_supported"


def _supports_ingest_status(data_type: Any) -> bool:
    return str(data_type or "").strip().lower() in INGEST_TRACKED_DATA_TYPES


def _ingest_status_for_batch(*, data_type: Any, batch_status: Any, result: dict[str, Any]) -> str:
    if not _supports_ingest_status(data_type):
        return "not_supported"
    if str(batch_status or "") != "succeeded":
        return "not_ready"
    explicit_status = _explicit_ingest_status(result)
    if explicit_status:
        return explicit_status
    return "ingested" if _result_implies_ingested(result) else "ready"


def _explicit_ingest_status(result: dict[str, Any]) -> str:
    ingest_status = str(result.get("ingest_status") or "").strip().lower()
    return ingest_status if ingest_status in {"not_ready", "ready", "previewed", "ingested", "failed"} else ""


def _result_implies_ingested(result: dict[str, Any]) -> bool:
    ingest_enabled = result.get("ingest_enabled")
    if isinstance(ingest_enabled, bool):
        return ingest_enabled
    if isinstance(ingest_enabled, str):
        value = ingest_enabled.strip().lower()
        if value in {"false", "0", "no"}:
            return False
        if value in {"true", "1", "yes"}:
            return True
    return False


def _max_auto_retries_value(primary: Any, fallback: Any = None) -> int:
    value = primary if primary is not None else fallback
    if value is None or value == "":
        return 1
    return max(0, int(value))


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


def _quality_result_summary(result: dict[str, Any]) -> dict[str, str | None] | None:
    if not isinstance(result, dict):
        return None
    report = result.get("quality_report")
    if not isinstance(report, dict):
        report = {}
    status = str(result.get("quality_status") or report.get("status") or "").strip().upper()
    if not status:
        return None
    report_id = str(result.get("quality_report_id") or report.get("report_id") or "").strip() or None
    reason = str(
        result.get("quality_failure_reason")
        or result.get("quality_error")
        or _quality_failure_reason(report)
        or ""
    ).strip() or None
    return {
        "quality_status": status,
        "quality_report_id": report_id,
        "quality_failure_reason": reason,
    }


def _quality_failure_reason(report: dict[str, Any]) -> str | None:
    checks = report.get("checks")
    if not isinstance(checks, list):
        return None
    failed: list[str] = []
    for check in checks:
        if not isinstance(check, dict) or str(check.get("status") or "").upper() not in {"FAIL", "WARN"}:
            continue
        name = str(check.get("name") or "quality").strip()
        message = str(check.get("message") or "").strip()
        failed.append(f"{name}: {message}" if message else name)
    if not failed:
        return None
    return "; ".join(failed[:3])[:500]


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

    return {
        key: (Jsonb(value, dumps=lambda item: json.dumps(item, ensure_ascii=False)) if key in json_keys else value)
        for key, value in record.items()
    }


def _json_record(record: dict[str, Any], *json_keys: str) -> dict[str, Any]:
    from psycopg.types.json import Json

    return {
        key: (Json(value, dumps=lambda item: json.dumps(item, ensure_ascii=False)) if key in json_keys else value)
        for key, value in record.items()
    }


def _row_to_dict(cur, row: tuple[Any, ...]) -> dict[str, Any]:
    return {desc.name if hasattr(desc, "name") else desc[0]: value for desc, value in zip(cur.description, row)}


def _dict_row(cur) -> dict[str, Any]:
    row = cur.fetchone()
    return _row_to_dict(cur, row)
