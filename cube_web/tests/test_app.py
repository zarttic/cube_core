import subprocess

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
    def fake_run_optical_partition_demo():
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
        }

    monkeypatch.setattr("cube_web.app._run_optical_partition_demo", fake_run_optical_partition_demo)

    body = web_app.partition_optical_demo()

    assert body["status"] == "completed"
    assert body["asset_count"] == 2
    assert body["rows"] == 16


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


def test_optical_quality_endpoint(monkeypatch):
    def fake_run_quality_check(args):
        assert args.run_dir == "/tmp/run"
        assert args.target_crs == "EPSG:4326"
        return {
            "status": "PASS",
            "summary": {"index_rows": 3, "failed_checks": 0, "warning_checks": 0},
            "checks": [{"name": "index_rows", "status": "PASS", "message": "ok"}],
        }

    monkeypatch.setattr("cube_web.app.run_optical_quality_check", fake_run_quality_check)

    resp = client.post("/v1/quality/optical/run", json={"run_dir": "/tmp/run", "target_crs": "EPSG:4326"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "PASS"
    assert body["summary"]["index_rows"] == 3


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
