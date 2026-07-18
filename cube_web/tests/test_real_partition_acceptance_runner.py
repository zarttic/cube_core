from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts" / "run_real_partition_acceptance.py"
SPEC = importlib.util.spec_from_file_location("real_partition_acceptance_runner", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


def _manifest() -> dict:
    datasets = []
    for index, data_type in enumerate(runner.DATA_TYPES):
        suffix = ".nc4" if data_type == "carbon" else ".tif"
        uri_field = "source_uri" if data_type == "carbon" else "cog_uri"
        datasets.append(
            {
                "dataset_id": f"dataset-{data_type}",
                "dataset_code": f"DS-{data_type}",
                "dataset_title": data_type,
                "data_type": data_type,
                "scenes": [
                    {
                        "scene_id": f"scene-{data_type}",
                        "scene_key": f"scene-{data_type}",
                        "assets": [
                            {
                                "asset_id": f"asset-{data_type}",
                                uri_field: f"s3://cube/cube/source/{data_type}/small{suffix}",
                                "checksum": str(index + 1) * 64,
                                "bands": [{"band_code": "B01", "band_name": "Band 1"}],
                            }
                        ],
                    }
                ],
            }
        )
    return {"schema_version": "test", "datasets": datasets}


def test_manifest_validation_requires_all_four_real_data_types(tmp_path: Path) -> None:
    path = tmp_path / "prepared.json"
    path.write_text(json.dumps(_manifest()), encoding="utf-8")
    loaded = runner.load_manifest(path)
    assert runner.validate_manifest_contract(loaded) == {
        "optical": 1,
        "radar": 1,
        "product": 1,
        "carbon": 1,
    }

    loaded["datasets"].pop()
    path.write_text(json.dumps(loaded), encoding="utf-8")
    with pytest.raises(ValueError, match="carbon"):
        runner.load_manifest(path)


def test_prepared_assets_are_derived_into_required_scene_band_shapes() -> None:
    def cog(role, bands=1):
        return {
            "role": role,
            "s3_uri": f"s3://cube/prepared/{role}.tif",
            "sha256": "a" * 64,
            "bbox_wgs84": [100, 20, 101, 21],
            "crs": "EPSG:4326",
            "resolution_native": [30, 30],
            "band_count": bands,
        }

    prepared = {"assets": [cog("optical", 3), cog("radar_vv"), cog("radar_vh"), cog("product_smoke"), cog("product_standard_window")]}
    manifest = runner.derive_manifest(prepared, {"s3_uri": "s3://cube/cube/source/carbon/a.nc4", "sha256": "b" * 64})
    by_type = {item["data_type"]: item for item in manifest["datasets"]}
    assert len(by_type["optical"]["scenes"][0]["assets"][0]["bands"]) == 3
    assert len(by_type["radar"]["scenes"][0]["assets"]) == 2
    assert len(by_type["product"]["scenes"]) == 2
    assert [scene["assets"][0]["attributes"]["product_year"] for scene in by_type["product"]["scenes"]] == [2026, 2020]
    assert [scene["assets"][0]["acquisition_time"][:4] for scene in by_type["product"]["scenes"]] == ["2026", "2020"]
    assert by_type["carbon"]["scenes"][0]["assets"][0]["source_format"] == "netcdf"


def test_carbon_discovery_stats_and_hashes_unique_nc4() -> None:
    content = b"real-carbon-sample"

    class Response:
        def stream(self, _size):
            yield content

        def close(self):
            pass

        def release_conn(self):
            pass

    class Minio:
        def list_objects(self, bucket, prefix, recursive):
            assert (bucket, prefix, recursive) == ("cube", "cube/source/carbon/", True)
            return [type("Object", (), {"object_name": "cube/source/carbon/sample.nc4"})()]

        def stat_object(self, _bucket, _key):
            return type("Stat", (), {"size": len(content), "metadata": {"x-amz-meta-sha256": hashlib.sha256(content).hexdigest()}})()

        def get_object(self, _bucket, _key):
            return Response()

    asset = runner.discover_carbon_asset(Minio(), bucket="cube")
    assert asset["sha256"] == hashlib.sha256(content).hexdigest()
    assert asset["s3_uri"].endswith("sample.nc4")


def test_namespace_is_complete_deterministic_and_preserves_source_uris() -> None:
    original = _manifest()
    namespaced = runner.namespace_manifest(original, "accept-123")
    assert namespaced["load_batch_id"] == "accept-123-batch"
    assert original["datasets"][0]["dataset_id"] == "dataset-optical"
    for dataset in namespaced["datasets"]:
        assert dataset["dataset_id"].startswith("accept-123-")
        for scene in dataset["scenes"]:
            assert scene["scene_id"].startswith("accept-123-")
            assert scene["identity_key"].startswith("accept-123:")
            for asset in scene["assets"]:
                assert asset["asset_id"].startswith("accept-123-")
                assert (asset.get("source_uri") or asset.get("cog_uri")).startswith("s3://cube/")


class _Client:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict | None]] = []

    def request(self, method: str, path: str, body: dict | None = None) -> dict:
        self.calls.append((method, path, body))
        if path.startswith("/v1/partition/tasks/"):
            return {"task_id": path.rsplit("/", 1)[-1], "status": "completed"}
        grid = body["datasets"][0]["partition"]["grid_type"]
        return {"task_id": f"task-{grid}", "status": "queued"}


def test_submit_grid_runs_covers_contract_and_never_caps_cells() -> None:
    manifest = runner.namespace_manifest(_manifest(), "accept-grid")
    client = _Client()
    results = runner.submit_grid_runs(client, manifest, poll_seconds=0)

    assert [row["task_id"] for row in results] == ["task-geohash", "task-mgrs", "task-isea4h"]
    posts = [body for method, path, body in client.calls if method == "POST"]
    assert len(posts) == 3
    for body, (grid, method, level) in zip(posts, runner.GRID_CASES, strict=True):
        assert {entry["partition"]["grid_type"] for entry in body["datasets"]} == {grid}
        assert {entry["partition"]["partition_method"] for entry in body["datasets"]} == {method}
        assert {entry["partition"]["requested_grid_level"] for entry in body["datasets"]} == {level}
        assert {entry["partition"]["max_cells_per_asset"] for entry in body["datasets"]} == {0}
        assert len(body["datasets"]) == 4
        carbon = next(entry for entry in body["datasets"] if entry["dataset_id"].endswith("dataset-carbon"))
        assert carbon["partition"]["max_observations"] == 50
    assert runner.GRID_CASES[-1] == ("isea4h", "entity", 6)


def test_wait_for_task_times_out_and_accepts_failure_terminal(monkeypatch) -> None:
    class Pending:
        def request(self, *_args):
            return {"status": "running"}

    ticks = iter((0.0, 0.0, 2.0))
    monkeypatch.setattr(runner.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(runner.time, "sleep", lambda _value: None)
    with pytest.raises(TimeoutError):
        runner.wait_for_task(Pending(), "task", poll_seconds=0, timeout_seconds=1)

    class Failed:
        def request(self, *_args):
            return {"status": "failed", "error": "controlled"}

    monkeypatch.setattr(runner.time, "monotonic", lambda: 0.0)
    assert runner.wait_for_task(Failed(), "task", poll_seconds=0)["status"] == "failed"


def test_cleanup_is_parameterized_prefix_scoped_and_rejects_sql_input() -> None:
    statements = runner.cleanup_sql("real-accept-123")
    assert statements
    assert all("%s" in statement for statement in statements)
    assert all("real-accept-123" not in statement for statement in statements)
    with pytest.raises(ValueError):
        runner.cleanup_sql("x%' OR TRUE --")


def test_import_payload_contains_no_runtime_credentials() -> None:
    manifest = runner.namespace_manifest(_manifest(), "accept-safe")
    payload = runner.import_payload(manifest)
    serialized = json.dumps(payload).casefold()
    for forbidden in ("password", "secret_key", "access_key", "postgres_dsn", "minio_endpoint"):
        assert forbidden not in serialized
    assert payload["load_batch_id"] == manifest["load_batch_id"]
    assert "batch_id" not in payload


def test_quality_ingest_gate_checks_full_error_export(monkeypatch) -> None:
    manifest = runner.namespace_manifest(_manifest(), "accept-quality")

    class Client:
        def __init__(self):
            self.exports = []

        def request(self, method, path, body=None):
            if method == "POST":
                dataset_id = path.split("/")[3]
                return {"quality_run_id": f"quality-{dataset_id}"}
            if path.startswith("/v1/quality/records/"):
                return {"status": "warn", "error_count": 2}
            if path.startswith("/v1/ingest-runs"):
                return {"items": [{"ingest_run_id": "ingest-1", "status": "completed"}]}
            raise AssertionError(path)

        def request_bytes(self, method, path):
            self.exports.append((method, path))
            return b"[{\"error\": 1}]", {"X-Export-Count": "2"}

    monkeypatch.setattr(runner.time, "monotonic", lambda: 0.0)
    client = Client()
    reports = runner.run_quality_ingest_gate(client, manifest, poll_seconds=0)
    assert len(reports) == 4
    assert len(client.exports) == 4


def test_quality_probe_manifests_are_isolated_and_have_distinct_severity() -> None:
    manifest = runner.namespace_manifest(_manifest(), "accept-failure")
    warning = runner.quality_probe_manifest(manifest, "warn")
    failure = runner.quality_probe_manifest(manifest, "fail")
    warning_asset = warning["datasets"][0]["scenes"][0]["assets"][0]
    failure_asset = failure["datasets"][0]["scenes"][0]["assets"][0]
    assert warning["datasets"][0]["data_type"] == "product"
    assert warning["datasets"][0]["dataset_id"] != failure["datasets"][0]["dataset_id"]
    assert warning["load_batch_id"] != manifest["load_batch_id"]
    assert warning_asset["attributes"]["quality_metadata_defects"] == [{
        "error_code": "acceptance_declared_defect",
        "message": "controlled real acceptance quality warn",
        "field": "acceptance_probe",
        "severity": "warning",
    }]
    assert warning_asset["attributes"]["product_year"]
    assert "product_year" not in failure_asset["attributes"]
    assert (failure_asset.get("source_uri") or failure_asset.get("cog_uri")).startswith("s3://cube/")


def test_partition_failure_is_never_reported_as_acceptance_success() -> None:
    runner.assert_partition_success([{"task_id": "ok", "final": {"status": "completed", "result": {"backend": "ray"}}}])
    with pytest.raises(RuntimeError, match="partition acceptance failed"):
        runner.assert_partition_success([{"task_id": "bad", "final": {"status": "failed", "error": "boom"}}])
    with pytest.raises(RuntimeError, match="missing Ray execution evidence"):
        runner.assert_partition_success([{"task_id": "local", "final": {"status": "completed", "result": {}}}])


def test_repeated_partition_submission_must_return_original_task_id() -> None:
    manifest = runner.namespace_manifest(_manifest(), "accept-idem")
    originals = [{"task_id": f"task-{payload['datasets'][0]['partition']['grid_type']}"} for payload in runner.build_grid_run_payloads(manifest)]

    class Client:
        mismatch = False

        def request(self, _method, _path, body):
            grid = body["datasets"][0]["partition"]["grid_type"]
            return {"task_id": "wrong" if self.mismatch else f"task-{grid}"}

    client = Client()
    runner.verify_idempotent_submissions(client, manifest, originals)
    client.mismatch = True
    with pytest.raises(RuntimeError, match="not idempotent"):
        runner.verify_idempotent_submissions(client, manifest, originals)


def test_database_inspection_is_read_only_scoped_and_checks_orphans(monkeypatch) -> None:
    values = iter((6, 7, 6, 4, 0, 0, 0, 0, 6, 12, 24, 12, 24, 6, 0))

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, sql, params):
            assert sql.lstrip().startswith("SELECT")
            assert params == ("accept-db%",)

        def fetchone(self):
            return (next(values),)

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def cursor(self):
            return Cursor()

    monkeypatch.setitem(sys.modules, "psycopg", type("Psycopg", (), {"connect": staticmethod(lambda dsn: Connection())}))
    counts = runner.inspect_database("sentinel-dsn", "accept-db")
    assert counts["partition_datasets"] == 6


def test_publication_lifecycle_requires_active_then_withdrawn(monkeypatch) -> None:
    class Client:
        def __init__(self):
            self.withdrawn = False

        def request(self, method, path, body=None):
            if path.endswith("/publish"):
                return {"publication_id": "publication-1", "status": "publishing"}
            if path.endswith("/withdraw"):
                assert body["reason"]
                self.withdrawn = True
                return {"publication_id": "publication-1", "status": "withdrawing"}
            if path.endswith("/publications?page_size=100"):
                return {"items": [{"publication_id": "publication-1", "status": "withdrawn" if self.withdrawn else "active"}]}
            raise AssertionError((method, path))

    monkeypatch.setattr(runner.time, "monotonic", lambda: 0.0)
    report = runner.verify_publication_lifecycle(Client(), "dataset", poll_seconds=0)
    assert report["active"]["status"] == "active"
    assert report["withdrawn"]["status"] == "withdrawn"


def test_cancel_probe_uses_bounded_product_scene(monkeypatch) -> None:
    manifest = runner.namespace_manifest(_manifest(), "accept-cancel")

    class Client:
        def request(self, method, path, body=None):
            if path == "/v1/partition/runs":
                partition = body["datasets"][0]["partition"]
                assert partition["max_cells_per_asset"] == 50
                return {"task_id": "cancel-task", "status": "queued"}
            if path.endswith("/cancel"):
                return {"task_id": "cancel-task", "status": "cancel_requested"}
            if path.endswith("/retry"):
                return {"task_id": "retry-task", "status": "queued"}
            if path.endswith("/retry-task"):
                return {"task_id": "retry-task", "status": "completed"}
            return {"task_id": "cancel-task", "status": "cancelled"}

    monkeypatch.setattr(runner.time, "monotonic", lambda: 0.0)
    report = runner.probe_task_cancellation(Client(), manifest, poll_seconds=0)
    assert report["final"]["status"] == "cancelled"
    assert report["retried"]["task_id"] == "retry-task"
    assert report["retry_final"]["status"] == "completed"
