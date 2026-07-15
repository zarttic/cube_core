from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from typing import Any

_MISSING_OBJECT_CODES = {"NoSuchKey", "NoSuchObject", "ResourceNotFound"}


def version_prefix(dataset_id: str, output_version: str) -> str:
    for value in (dataset_id, output_version):
        if not value or "/" in value or "\\" in value or value in {".", ".."}:
            raise ValueError("dataset_id and output_version must be non-empty path segments")
    return f"partition/{dataset_id}/versions/{output_version}/"


class PartitionObjectStore:
    def __init__(self, minio: Any, *, bucket: str) -> None:
        if not bucket:
            raise ValueError("bucket is required")
        self._minio = minio
        self._bucket = bucket

    def put_entity_tile(
        self,
        dataset_id: str,
        output_version: str,
        tile_name: str,
        tile_bytes: bytes,
        checksum: str | None = None,
    ) -> dict[str, Any]:
        key = f"{version_prefix(dataset_id, output_version)}tiles/{self._tile_name(tile_name)}"
        actual_checksum = sha256(tile_bytes).hexdigest()
        if checksum is not None and checksum != actual_checksum:
            raise ValueError("tile checksum does not match tile bytes")

        existing = self._stat_object(key)
        if existing is None:
            self._minio.put_object(
                self._bucket,
                key,
                BytesIO(tile_bytes),
                len(tile_bytes),
                content_type="image/tiff",
                metadata={"checksum-sha256": actual_checksum},
            )
        elif self._stat_size(existing) != len(tile_bytes) or self._stat_checksum(existing) != actual_checksum:
            raise ValueError(f"immutable object collision for {key}")

        return {
            "object_key": key,
            "tile_uri": f"s3://{self._bucket}/{key}",
            "checksum": actual_checksum,
            "byte_size": len(tile_bytes),
        }

    def verify_version_manifest(
        self,
        dataset_id: str,
        output_version: str,
        manifest: Iterable[Mapping[str, Any]],
    ) -> list[str]:
        keys = self._verified_manifest_keys(dataset_id, output_version, manifest, allow_missing=False)
        return keys

    def cleanup_unreferenced_version(
        self,
        domain_store: Any,
        dataset_id: str,
        output_version: str,
        *,
        older_than: datetime,
    ) -> dict[str, Any]:
        state = domain_store.get_output_cleanup_state(dataset_id, output_version)
        publication_referenced = domain_store.output_has_publication_reference(dataset_id, output_version)
        if not state:
            raise RuntimeError("unknown_output")

        expected_prefix = version_prefix(dataset_id, output_version)
        if bool(state.get("is_current")) or state.get("current_output_version") == output_version:
            raise RuntimeError("current")
        if publication_referenced:
            raise RuntimeError("publication_referenced")
        if state.get("object_prefix") != expected_prefix:
            raise RuntimeError("wrong_prefix")
        if self._is_newer_than_cutoff(state, older_than):
            raise RuntimeError("young")

        manifest = state.get("manifest")
        if not isinstance(manifest, list):
            raise RuntimeError("wrong_prefix")
        keys = self._verified_manifest_keys(dataset_id, output_version, manifest, allow_missing=True)
        for key in keys:
            self._remove_if_present(key)

        remaining = self._keys_under_prefix(expected_prefix)
        if remaining:
            raise RuntimeError("cleanup_incomplete")
        return {
            "dataset_id": dataset_id,
            "output_version": output_version,
            "deleted_keys": keys,
            "cleanup_complete": True,
        }

    @staticmethod
    def _tile_name(tile_name: str) -> str:
        if not tile_name or "/" in tile_name or "\\" in tile_name or tile_name in {".", ".."}:
            raise ValueError("tile_name must be a non-empty path segment")
        return tile_name

    def _verified_manifest_keys(
        self,
        dataset_id: str,
        output_version: str,
        manifest: Iterable[Mapping[str, Any]],
        *,
        allow_missing: bool,
    ) -> list[str]:
        prefix = version_prefix(dataset_id, output_version)
        verified: list[str] = []
        seen: set[str] = set()
        for record in manifest:
            key = str(record.get("object_key") or "")
            checksum = str(record.get("checksum") or "")
            byte_size = record.get("byte_size")
            if not key.startswith(prefix) or not checksum or not isinstance(byte_size, int) or byte_size < 0:
                raise RuntimeError("wrong_prefix")
            expected_uri = f"s3://{self._bucket}/{key}"
            if record.get("tile_uri") != expected_uri or key in seen:
                raise RuntimeError("wrong_prefix")
            seen.add(key)
            stat = self._stat_object(key)
            if stat is None:
                if allow_missing:
                    continue
                raise RuntimeError(f"missing_object: {key}")
            if self._stat_size(stat) != byte_size or self._stat_checksum(stat) != checksum:
                raise RuntimeError(f"manifest_verification_failed: {key}")
            verified.append(key)
        return verified

    def _is_newer_than_cutoff(self, state: Mapping[str, Any], cutoff: datetime) -> bool:
        if cutoff.tzinfo is None:
            raise ValueError("older_than must be timezone-aware")
        timestamps = [self._as_datetime(state.get(name)) for name in ("completed_at", "failed_at")]
        known = [timestamp for timestamp in timestamps if timestamp is not None]
        return not known or max(known) > cutoff.astimezone(UTC)

    @staticmethod
    def _as_datetime(value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not isinstance(value, datetime):
            raise RuntimeError("young")
        if value.tzinfo is None:
            raise RuntimeError("young")
        return value.astimezone(UTC)

    def _stat_object(self, key: str) -> Any | None:
        try:
            return self._minio.stat_object(self._bucket, key)
        except (FileNotFoundError, KeyError):
            return None
        except Exception as exc:
            if getattr(exc, "code", None) in _MISSING_OBJECT_CODES:
                return None
            raise

    @staticmethod
    def _stat_size(stat: Any) -> int:
        size = getattr(stat, "size", None)
        if not isinstance(size, int):
            raise RuntimeError("manifest_verification_failed")
        return size

    @staticmethod
    def _stat_checksum(stat: Any) -> str | None:
        metadata = getattr(stat, "metadata", None)
        if not isinstance(metadata, Mapping):
            return None
        return metadata.get("checksum-sha256") or metadata.get("x-amz-meta-checksum-sha256")

    def _remove_if_present(self, key: str) -> None:
        try:
            self._minio.remove_object(self._bucket, key)
        except (FileNotFoundError, KeyError):
            return
        except Exception as exc:
            if getattr(exc, "code", None) in _MISSING_OBJECT_CODES:
                return
            raise

    def _keys_under_prefix(self, prefix: str) -> list[str]:
        return [
            str(item.object_name)
            for item in self._minio.list_objects(self._bucket, prefix=prefix, recursive=True)
            if str(item.object_name).startswith(prefix)
        ]
