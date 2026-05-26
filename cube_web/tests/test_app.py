import subprocess
import time

from fastapi.testclient import TestClient
from s2sphere import CellId

import cube_web.app as web_app
from cube_web.app import ENCODER_SDK_CLASS, app


client = TestClient(app)


def test_header_navigation_does_not_expose_quality_as_top_level_item():
    nav_source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "data" / "navigation.js").read_text(
        encoding="utf-8"
    )

    assert "{ label: '自动化质检'," not in nav_source


def test_partition_view_uses_explicit_module_endpoint_mapping():
    source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "views" / "PartitionView.vue").read_text(
        encoding="utf-8"
    )

    assert "const partitionEndpointsByModule = {" in source
    assert "optical: 'optical'" in source
    assert "carbon: 'carbon'" in source
    assert "radar: 'radar'" in source
    assert "product: 'product'" in source
    assert "activeModule.value === 'carbon' ? 'carbon' : 'optical'" not in source


def test_home_page_serves_index():
    resp = client.get("/")
    assert resp.status_code == 200
    assert "分析就绪数据剖分管理系统" in resp.text


def test_encoding_page_serves_html():
    resp = client.get("/encoding")
    assert resp.status_code == 200
    assert "分析就绪数据剖分管理系统" in resp.text


def test_legacy_encoding_html_serves_vue_app():
    resp = client.get("/encoding.html")
    assert resp.status_code == 200
    assert "分析就绪数据剖分管理系统" in resp.text


def test_cube_web_imports_encoder_package():
    assert ENCODER_SDK_CLASS.__name__ == "CubeEncoderSDK"


def test_grid_locate_sdk_endpoint():
    resp = client.post("/v1/grid/locate", json={"grid_type": "geohash", "level": 7, "point": [116.391, 39.907]})
    assert resp.status_code == 200
    body = resp.json()
    assert CellId.from_token(body["cell"]["space_code"]).level() == 7


def test_code_parse_sdk_endpoint():
    locate_resp = client.post("/v1/grid/locate", json={"grid_type": "geohash", "level": 7, "point": [116.391, 39.907]})
    space_code = locate_resp.json()["cell"]["space_code"]
    resp = client.post("/v1/code/parse", json={"st_code": f"gh:7:{space_code}:202603091530:v1"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["grid_type"] == "geohash"
    assert body["level"] == 7


def test_partition_openapi_exposes_contract_models():
    schema = client.get("/openapi.json").json()

    assert "PartitionDemoRequest" in schema["components"]["schemas"]
    assert "PartitionRetryRequest" in schema["components"]["schemas"]
    assert "PartitionTaskResponse" in schema["components"]["schemas"]


def test_carbon_partition_demo_endpoint(monkeypatch):
    def fake_run_carbon_partition_demo():
        return {
            "status": "completed",
            "data_type": "carbon_satellite",
            "rows": 12,
            "distinct_space_codes": 5,
            "elapsed_sec": 0.12,
            "rows_per_sec": 100.0,
            "grid_type": "geohash",
            "grid_level": 7,
            "workers": 2,
            "partition_backend": "process",
            "output_path": "/tmp/demo/carbon_observation_rows.jsonl",
        }

    monkeypatch.setattr("cube_web.app._run_carbon_partition_demo", fake_run_carbon_partition_demo)

    body = web_app.partition_carbon_demo()

    assert body["status"] == "completed"
    assert body["rows"] == 12
    assert body["distinct_space_codes"] == 5


def test_optical_partition_demo_endpoint(monkeypatch):
    def fake_run_optical_partition_demo(payload=None):
        assert payload is None
        return {
            "status": "completed",
            "data_type": "optical",
            "asset_count": 2,
            "grid_task_count": 16,
            "rows": 16,
            "cog_elapsed_sec": 0.1,
            "partition_elapsed_sec": 0.2,
            "total_elapsed_sec": 0.4,
            "grid_type": "geohash",
            "grid_level": 7,
            "workers": 2,
            "output_path": "/tmp/demo/index_rows.jsonl",
            "quality_status": "WARN",
            "quality_report_path": "/tmp/demo/quality_report.json",
            "quality_report": {"status": "WARN", "checks": [{"name": "logical_duplicates", "status": "WARN"}]},
        }

    monkeypatch.setattr("cube_web.app._run_optical_partition_demo", fake_run_optical_partition_demo)

    body = web_app.partition_optical_demo()

    assert body["status"] == "completed"
    assert body["asset_count"] == 2
    assert body["rows"] == 16
    assert body["quality_status"] == "WARN"


def test_optical_partition_demo_endpoint_accepts_frontend_payload(monkeypatch):
    expected_payload = {
        "grid_type": "geohash",
        "grid_level": 5,
        "batch_id": "OPTICAL_BATCH_20260522_135546",
        "batch_name": "Shandong_mosaic_optocal",
        "selected_assets": [
            {
                "source_uri": "Shandong_mosaic_2020Q3_sr_band4_cut/Shandong_mosaic_2020Q3_sr_band4_cut.tif",
                "scene_id": "Shandong_mosaic_2020Q3",
                "acq_time": "2020-07-01T00:00:00Z",
                "bands": ["sr_band4"],
                "corners": [
                    [114.757377, 38.503521],
                    [122.774914, 38.503521],
                    [122.774914, 33.857041],
                    [114.757377, 33.857041],
                ],
            }
        ],
    }

    def fake_run_optical_partition_demo(payload=None):
        assert payload == expected_payload
        return {
            "status": "completed",
            "mode": "partition_demo",
            "data_type": "optical",
            "asset_count": 1,
            "grid_task_count": 12,
            "rows": 12,
            "grid_type": payload["grid_type"],
            "grid_level": payload["grid_level"],
            "execution_engine": "ray",
            "output_path": "/tmp/demo/index_rows.jsonl",
        }

    monkeypatch.setattr("cube_web.app._run_optical_partition_demo", fake_run_optical_partition_demo)

    resp = client.post("/v1/partition/optical/demo", json=expected_payload)

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "completed"
    assert body["mode"] == "partition_demo"
    assert body["execution_engine"] == "ray"
    assert body["grid_level"] == 5


def test_partition_demo_rejects_invalid_grid_level():
    resp = client.post("/v1/partition/optical/demo", json={"grid_type": "geohash", "grid_level": 0})

    assert resp.status_code == 422


def test_partition_demo_can_run_as_async_task(monkeypatch):
    def fake_run_product_partition_demo(payload=None):
        assert payload == {"grid_type": "geohash", "grid_level": 5}
        return {
            "status": "completed",
            "mode": "partition_demo",
            "data_type": "product",
            "rows": 20,
        }

    monkeypatch.setattr("cube_web.app._run_product_partition_demo", fake_run_product_partition_demo)

    submit_resp = client.post("/v1/partition/product/tasks/demo", json={"grid_type": "geohash", "grid_level": 5})

    assert submit_resp.status_code == 202
    submitted = submit_resp.json()
    assert submitted["status"] in {"queued", "running", "completed"}
    assert submitted["data_type"] == "product"
    assert submitted["operation"] == "demo"

    task_body = None
    for _ in range(20):
        task_resp = client.get(f"/v1/partition/tasks/{submitted['task_id']}")
        assert task_resp.status_code == 200
        task_body = task_resp.json()
        if task_body["status"] == "completed":
            break
        time.sleep(0.01)

    assert task_body is not None
    assert task_body["status"] == "completed"
    assert task_body["result"]["rows"] == 20


def test_optical_partition_test_endpoint(monkeypatch):
    def fake_run_optical_partition_test(payload=None):
        assert payload["input_dir"] == "/home/lyjdev/projects/cube_project/cube_split/data/optocal"
        return {
            "status": "completed",
            "mode": "partition_test_no_ingest",
            "data_type": "optical",
            "input_dir": payload["input_dir"],
            "run_dir": "/tmp/run",
            "rows_path": "/tmp/run/index_rows.jsonl",
            "rows": 147,
            "grid_type": "geohash",
            "grid_level": 5,
            "ingest_enabled": False,
            "quality_status": "PASS",
            "quality_report_path": "/tmp/run/quality_report.json",
            "quality_report": {"status": "PASS", "summary": {"index_rows": 147}},
        }

    monkeypatch.setattr("cube_web.app._run_optical_partition_test", fake_run_optical_partition_test)

    resp = client.post(
        "/v1/partition/optical/test",
        json={"input_dir": "/home/lyjdev/projects/cube_project/cube_split/data/optocal"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "completed"
    assert body["mode"] == "partition_test_no_ingest"
    assert body["ingest_enabled"] is False
    assert body["quality_status"] == "PASS"


def test_optical_partition_retry_endpoint_reruns_warning_assets(monkeypatch):
    original_payload = {
        "grid_type": "geohash",
        "grid_level": 5,
        "selected_assets": [
            {
                "source_uri": "scene_a/asset_a.tif",
                "scene_id": "scene_a",
                "acq_time": "2020-07-01T00:00:00Z",
                "bands": ["sr_band2"],
                "corners": [[117.0, 36.0], [117.2, 36.0], [117.2, 35.8], [117.0, 35.8]],
            },
            {
                "source_uri": "scene_b/asset_b.tif",
                "scene_id": "scene_b",
                "acq_time": "2020-07-01T00:00:00Z",
                "bands": ["sr_band3"],
                "corners": [[117.0, 36.0], [117.2, 36.0], [117.2, 35.8], [117.0, 35.8]],
            },
        ],
    }
    retry_request = {
        "request": {"endpoint": "optical", "payload": original_payload},
        "last_result": {
            "quality_report": {
                "status": "WARN",
                "checks": [
                    {
                        "name": "pixel_sample",
                        "status": "WARN",
                        "metrics": {"zero_assets": [{"path": "/tmp/demo/cog/asset_b_cog.tif"}]},
                    }
                ],
            }
        },
    }

    def fake_run_optical_partition_from_payload(payload=None, mode="partition_demo"):
        assert mode == "partition_retry"
        assert [asset["source_uri"] for asset in payload["selected_assets"]] == ["scene_b/asset_b.tif"]
        return {"status": "completed", "mode": mode, "data_type": "optical", "rows": 3}

    monkeypatch.setattr("cube_web.app._run_optical_partition_from_payload", fake_run_optical_partition_from_payload)

    resp = client.post("/v1/partition/optical/retry", json=retry_request)

    assert resp.status_code == 200
    body = resp.json()
    assert body["mode"] == "partition_retry"
    assert body["retry"]["strategy"] == "warning_assets"
    assert body["retry"]["warning_check_names"] == ["pixel_sample"]
    assert body["retry"]["retried_asset_count"] == 1


def test_optical_partition_retry_endpoint_falls_back_to_full_request(monkeypatch):
    original_payload = {
        "grid_type": "geohash",
        "grid_level": 5,
        "selected_assets": [{"source_uri": "scene_a/asset_a.tif"}, {"source_uri": "scene_b/asset_b.tif"}],
    }
    retry_request = {
        "request": {"endpoint": "optical", "payload": original_payload},
        "last_result": {"quality_report": {"status": "WARN", "checks": [{"name": "logical_duplicates", "status": "WARN"}]}},
    }

    def fake_run_optical_partition_from_payload(payload=None, mode="partition_demo"):
        assert mode == "partition_retry"
        assert payload == original_payload
        return {"status": "completed", "mode": mode, "data_type": "optical", "rows": 5}

    monkeypatch.setattr("cube_web.app._run_optical_partition_from_payload", fake_run_optical_partition_from_payload)

    resp = client.post("/v1/partition/optical/retry", json=retry_request)

    assert resp.status_code == 200
    body = resp.json()
    assert body["retry"]["strategy"] == "full_request"
    assert body["retry"]["warning_check_names"] == ["logical_duplicates"]
    assert body["retry"]["retried_asset_count"] == 0


def test_carbon_partition_retry_endpoint(monkeypatch):
    def fake_run_carbon_partition_retry(payload=None):
        assert payload == {"request": {"endpoint": "carbon", "payload": {}}, "last_result": {"status": "completed"}}
        return {
            "status": "completed",
            "mode": "partition_retry",
            "data_type": "carbon_satellite",
            "rows": 10,
            "retry": {"strategy": "full_request"},
        }

    monkeypatch.setattr("cube_web.app._run_carbon_partition_retry", fake_run_carbon_partition_retry)

    resp = client.post(
        "/v1/partition/carbon/retry",
        json={"request": {"endpoint": "carbon", "payload": {}}, "last_result": {"status": "completed"}},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["mode"] == "partition_retry"
    assert body["retry"]["strategy"] == "full_request"


def test_product_partition_demo_endpoint(monkeypatch):
    def fake_run_product_partition_demo(payload=None):
        assert payload == {"grid_type": "geohash", "grid_level": 5}
        return {
            "status": "completed",
            "mode": "partition_demo",
            "data_type": "product",
            "rows": 20,
            "grid_type": "geohash",
            "grid_level": 5,
            "output_path": "/tmp/product/index_rows.jsonl",
        }

    monkeypatch.setattr("cube_web.app._run_product_partition_demo", fake_run_product_partition_demo)

    resp = client.post("/v1/partition/product/demo", json={"grid_type": "geohash", "grid_level": 5})

    assert resp.status_code == 200
    body = resp.json()
    assert body["data_type"] == "product"
    assert body["mode"] == "partition_demo"
    assert body["rows"] == 20


def test_product_partition_retry_endpoint(monkeypatch):
    def fake_run_product_partition_retry(payload=None):
        assert payload == {"request": {"endpoint": "product", "payload": {}}, "last_result": {"status": "completed"}}
        return {
            "status": "completed",
            "mode": "partition_retry",
            "data_type": "product",
            "rows": 20,
            "retry": {"strategy": "full_request"},
        }

    monkeypatch.setattr("cube_web.app._run_product_partition_retry", fake_run_product_partition_retry)

    resp = client.post(
        "/v1/partition/product/retry",
        json={"request": {"endpoint": "product", "payload": {}}, "last_result": {"status": "completed"}},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["mode"] == "partition_retry"
    assert body["retry"]["strategy"] == "full_request"


def test_radar_partition_endpoints_return_not_implemented():
    demo_resp = client.post("/v1/partition/radar/demo", json={})
    retry_resp = client.post("/v1/partition/radar/retry", json={})

    assert demo_resp.status_code == 501
    assert retry_resp.status_code == 501
    assert "not implemented" in demo_resp.json()["detail"]
    assert "not implemented" in retry_resp.json()["detail"]


def test_optical_quality_endpoint(monkeypatch):
    run_dir = "/tmp/cube_web_partition_demo/quality/run"

    def fake_run_quality_check(args):
        assert args.run_dir == run_dir
        assert args.target_crs == "EPSG:4326"
        return {
            "status": "PASS",
            "summary": {"index_rows": 3, "failed_checks": 0, "warning_checks": 0},
            "checks": [{"name": "index_rows", "status": "PASS", "message": "ok"}],
        }

    monkeypatch.setattr("cube_web.app.run_optical_quality_check", fake_run_quality_check)

    resp = client.post("/v1/quality/optical/run", json={"run_dir": run_dir, "target_crs": "EPSG:4326"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "PASS"
    assert body["summary"]["index_rows"] == 3


def test_quality_report_rejects_run_dir_outside_allowed_roots():
    resp = client.post("/v1/quality/optical/report", json={"run_dir": "/tmp/not-a-cube-web-run"})

    assert resp.status_code == 403
    assert "run_dir must be under" in resp.json()["detail"]


def test_optical_quality_latest_endpoint(monkeypatch):
    def fake_latest_run_dir():
        return "/tmp/latest-run"

    def fake_run_quality_check(args):
        assert args.run_dir == "/tmp/latest-run"
        assert args.target_crs == "EPSG:4326"
        return {
            "status": "WARN",
            "summary": {"index_rows": 9, "failed_checks": 0, "warning_checks": 1},
            "checks": [{"name": "logical_duplicates", "status": "WARN", "message": "duplicate"}],
        }

    monkeypatch.setattr("cube_web.app._latest_optical_quality_run_dir", fake_latest_run_dir)
    monkeypatch.setattr("cube_web.app.run_optical_quality_check", fake_run_quality_check)

    resp = client.post("/v1/quality/optical/latest", json={})

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "WARN"
    assert body["run_dir"] == "/tmp/latest-run"
    assert body["summary"]["warning_checks"] == 1


def test_optical_quality_latest_reads_existing_report_without_rerun(monkeypatch, tmp_path):
    run_dir = tmp_path / "dataset_a" / "run_20260515_010203"
    run_dir.mkdir(parents=True)
    (run_dir / "index_rows.jsonl").write_text("{}\n", encoding="utf-8")
    (run_dir / "quality_report.json").write_text(
        """
        {
          "status": "PASS",
          "run_dir": "/old/path",
          "target_crs": "EPSG:4326",
          "generated_at": "2026-05-15T01:02:03Z",
          "summary": {"index_rows": 11, "failed_checks": 0, "warning_checks": 0},
          "checks": [{"name": "index_rows", "status": "PASS", "message": "cached"}],
          "assets": []
        }
        """,
        encoding="utf-8",
    )

    def fail_if_called(args):
        raise AssertionError("latest should read quality_report.json instead of re-running quality checks")

    monkeypatch.setattr("cube_web.app._latest_optical_quality_run_dir", lambda: str(run_dir))
    monkeypatch.setattr("cube_web.app.run_optical_quality_check", fail_if_called)

    body = web_app.quality_optical_latest({})

    assert body["status"] == "PASS"
    assert body["run_dir"] == str(run_dir)
    assert body["summary"]["index_rows"] == 11
    assert body["checks"][0]["message"] == "cached"


def test_optical_quality_report_endpoint_reads_existing_report_without_rerun(monkeypatch, tmp_path):
    run_dir = tmp_path / "dataset_a" / "run_20260515_010203"
    run_dir.mkdir(parents=True)
    (run_dir / "index_rows.jsonl").write_text("{}\n", encoding="utf-8")
    (run_dir / "quality_report.json").write_text(
        """
        {
          "status": "WARN",
          "target_crs": "EPSG:4326",
          "generated_at": "2026-05-15T01:02:03Z",
          "summary": {"index_rows": 7, "failed_checks": 0, "warning_checks": 1},
          "checks": [{"name": "logical_duplicates", "status": "WARN", "message": "cached warn"}],
          "assets": []
        }
        """,
        encoding="utf-8",
    )

    def fail_if_called(args):
        raise AssertionError("report viewing should not re-run quality checks")

    monkeypatch.setattr("cube_web.app.run_optical_quality_check", fail_if_called)
    monkeypatch.setattr("cube_web.app._allowed_quality_roots", lambda: [tmp_path.resolve()])

    body = web_app.quality_optical_report({"run_dir": str(run_dir)})

    assert body["status"] == "WARN"
    assert body["run_dir"] == str(run_dir)
    assert body["summary"]["index_rows"] == 7
    assert body["checks"][0]["message"] == "cached warn"


def test_optical_quality_report_pdf_endpoint_reads_existing_report_without_rerun(monkeypatch, tmp_path):
    run_dir = tmp_path / "dataset_a" / "run_20260515_010203"
    run_dir.mkdir(parents=True)
    (run_dir / "index_rows.jsonl").write_text("{}\n", encoding="utf-8")
    (run_dir / "quality_report.json").write_text(
        """
        {
          "status": "PASS",
          "target_crs": "EPSG:4326",
          "generated_at": "2026-05-15T01:02:03Z",
          "summary": {"index_rows": 7, "asset_count": 2, "failed_checks": 0, "warning_checks": 0},
          "checks": [{"name": "index_rows", "status": "PASS", "message": "cached"}],
          "assets": [{"path": "/data/a.tif", "crs": "EPSG:4326"}]
        }
        """,
        encoding="utf-8",
    )

    def fail_if_called(args):
        raise AssertionError("PDF export should not re-run quality checks")

    monkeypatch.setattr("cube_web.app.run_optical_quality_check", fail_if_called)
    monkeypatch.setattr("cube_web.app._allowed_quality_roots", lambda: [tmp_path.resolve()])

    response = web_app.quality_optical_report_pdf({"run_dir": str(run_dir)})

    assert response.media_type == "application/pdf"
    assert response.body.startswith(b"%PDF-")
    assert b"Quality Inspection Report" not in response.body
    pdf_path = tmp_path / "quality_report.pdf"
    txt_path = tmp_path / "quality_report.txt"
    pdf_path.write_bytes(response.body)
    subprocess.run(["pdftotext", str(pdf_path), str(txt_path)], check=True)
    text = txt_path.read_text(encoding="utf-8")
    assert "质检报告" in text
    assert "质检概要" in text


def test_optical_quality_history_endpoint(monkeypatch, tmp_path):
    run_dir = tmp_path / "dataset_a" / "run_20260515_010203"
    run_dir.mkdir(parents=True)
    (run_dir / "index_rows.jsonl").write_text("{}\n", encoding="utf-8")
    (run_dir / "quality_report.json").write_text(
        """
        {
          "status": "PASS",
          "target_crs": "EPSG:4326",
          "generated_at": "2026-05-15T01:02:03Z",
          "summary": {
            "index_rows": 1,
            "asset_count": 1,
            "passed_checks": 7,
            "warning_checks": 0,
            "failed_checks": 0
          }
        }
        """,
        encoding="utf-8",
    )

    def fake_run_dirs():
        return [run_dir]

    monkeypatch.setattr("cube_web.app._optical_quality_run_dirs", fake_run_dirs)

    body = web_app.quality_optical_history({"target_crs": "EPSG:4326"})

    assert body["count"] == 1
    record = body["records"][0]
    assert record["dataset"] == "dataset_a"
    assert record["run_name"] == "run_20260515_010203"
    assert record["status"] == "PASS"
    assert record["summary"]["index_rows"] == 1


def test_product_quality_history_endpoint(monkeypatch, tmp_path):
    run_dir = tmp_path / "product_epsg4326" / "run_20260515_010203"
    run_dir.mkdir(parents=True)
    (run_dir / "index_rows.jsonl").write_text("{}\n", encoding="utf-8")
    (run_dir / "quality_report.json").write_text(
        """
        {
          "status": "PASS",
          "target_crs": "EPSG:4326",
          "generated_at": "2026-05-15T01:02:03Z",
          "summary": {
            "index_rows": 5,
            "asset_count": 5,
            "product_years": [1980, 1990, 2000, 2010, 2020],
            "passed_checks": 8,
            "warning_checks": 0,
            "failed_checks": 0
          }
        }
        """,
        encoding="utf-8",
    )

    monkeypatch.setattr("cube_web.app._quality_run_dirs", lambda data_type: [run_dir])

    body = web_app.quality_product_history({"target_crs": "EPSG:4326"})

    assert body["count"] == 1
    record = body["records"][0]
    assert record["data_type"] == "product"
    assert record["dataset"] == "product_epsg4326"
    assert record["summary"]["index_rows"] == 5
