from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from cube_web.services.partition_object_store import PartitionObjectStore, version_prefix


class FakeMinio:
    def __init__(self) -> None:
        self.objects: dict[str, dict[str, object]] = {}
        self.put_calls = 0

    def put_object(self, _bucket, key, data, length, **kwargs) -> None:
        self.put_calls += 1
        payload = data.read()
        assert len(payload) == length
        self.objects[key] = {"bytes": payload, "metadata": kwargs["metadata"]}

    def stat_object(self, _bucket, key):
        record = self.objects[key]
        return SimpleNamespace(size=len(record["bytes"]), metadata=record["metadata"])

    def remove_object(self, _bucket, key) -> None:
        del self.objects[key]

    def list_objects(self, _bucket, *, prefix, recursive):
        assert recursive is True
        return [SimpleNamespace(object_name=key) for key in sorted(self.objects) if key.startswith(prefix)]

    def keys(self) -> list[str]:
        return sorted(self.objects)


class FakeDomainStore:
    def __init__(self, record: dict[str, object]) -> None:
        self.state = {
            "is_current": False,
            "object_prefix": version_prefix("dataset-a", "version-a"),
            "completed_at": datetime.now(UTC) - timedelta(days=2),
            "failed_at": None,
            "manifest": [record],
        }
        self.publication_referenced = False

    def get_output_cleanup_state(self, dataset_id: str, output_version: str):
        assert (dataset_id, output_version) == ("dataset-a", "version-a")
        return self.state

    def output_has_publication_reference(self, dataset_id: str, output_version: str) -> bool:
        assert (dataset_id, output_version) == ("dataset-a", "version-a")
        return self.publication_referenced

    def seed_cleanup_guard(self, guard: str) -> None:
        if guard == "current":
            self.state["is_current"] = True
        elif guard == "publication_referenced":
            self.publication_referenced = True
        elif guard == "young":
            self.state["completed_at"] = datetime.now(UTC)
        elif guard == "wrong_prefix":
            self.state["object_prefix"] = "partition/dataset-a/versions/version-b/"
        else:  # pragma: no cover
            raise AssertionError(guard)


@pytest.fixture
def fake_minio() -> FakeMinio:
    return FakeMinio()


@pytest.fixture
def object_record(fake_minio: FakeMinio) -> dict[str, object]:
    return PartitionObjectStore(fake_minio, bucket="cube").put_entity_tile(
        "dataset-a", "version-a", "tile-1.tif", b"tile"
    )


def test_entity_tile_uses_immutable_dataset_version_prefix(fake_minio: FakeMinio) -> None:
    objects = PartitionObjectStore(fake_minio, bucket="cube")
    record = objects.put_entity_tile("dataset-a", "version-a", "tile-1.tif", b"tile")

    assert record["tile_uri"] == "s3://cube/partition/dataset-a/versions/version-a/tiles/tile-1.tif"
    assert record["checksum"] == "8b668b8994aa845107399994593d0ca831520be5257f005351a0ec13e97a39be"
    assert not any("current" in key for key in fake_minio.keys())


def test_entity_tile_reuses_only_verified_identical_object(fake_minio: FakeMinio) -> None:
    objects = PartitionObjectStore(fake_minio, bucket="cube")
    objects.put_entity_tile("dataset-a", "version-a", "tile-1.tif", b"tile")
    record = objects.put_entity_tile("dataset-a", "version-a", "tile-1.tif", b"tile")

    assert fake_minio.put_calls == 1
    assert record["byte_size"] == 4
    with pytest.raises(ValueError, match="immutable object collision"):
        objects.put_entity_tile("dataset-a", "version-a", "tile-1.tif", b"other")


def test_verify_manifest_rejects_checksum_mismatch(fake_minio: FakeMinio, object_record: dict[str, object]) -> None:
    objects = PartitionObjectStore(fake_minio, bucket="cube")
    fake_minio.objects[str(object_record["object_key"])]["metadata"] = {"checksum-sha256": "0" * 64}

    with pytest.raises(RuntimeError, match="manifest_verification_failed"):
        objects.verify_version_manifest("dataset-a", "version-a", [object_record])


@pytest.mark.parametrize("guard", ["current", "publication_referenced", "young", "wrong_prefix"])
def test_cleanup_refuses_any_unsafe_version(
    fake_minio: FakeMinio,
    object_record: dict[str, object],
    guard: str,
) -> None:
    objects = PartitionObjectStore(fake_minio, bucket="cube")
    domain_store = FakeDomainStore(object_record)
    domain_store.seed_cleanup_guard(guard)

    with pytest.raises(RuntimeError, match=guard):
        objects.cleanup_unreferenced_version(
            domain_store,
            "dataset-a",
            "version-a",
            older_than=datetime.now(UTC) - timedelta(hours=24),
        )
    assert fake_minio.keys()


def test_cleanup_is_idempotent_after_all_guards_pass(fake_minio: FakeMinio, object_record: dict[str, object]) -> None:
    objects = PartitionObjectStore(fake_minio, bucket="cube")
    domain_store = FakeDomainStore(object_record)

    first = objects.cleanup_unreferenced_version(
        domain_store, "dataset-a", "version-a", older_than=datetime.now(UTC)
    )
    second = objects.cleanup_unreferenced_version(
        domain_store, "dataset-a", "version-a", older_than=datetime.now(UTC)
    )

    assert first["deleted_keys"] == [object_record["object_key"]]
    assert second["deleted_keys"] == []
    assert fake_minio.keys() == []


@pytest.mark.parametrize("value", ["", ".", "..", "dataset/a", "dataset\\a"])
def test_version_prefix_rejects_path_traversal(value: str) -> None:
    with pytest.raises(ValueError, match="path segments"):
        version_prefix(value, "version-a")
