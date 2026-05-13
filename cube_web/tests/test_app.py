from fastapi.testclient import TestClient
from s2sphere import CellId

import cube_web.app as web_app
from cube_web.app import ENCODER_SDK_CLASS, app


client = TestClient(app)


def test_home_page_serves_index():
    resp = client.get("/")
    assert resp.status_code == 200
    assert "分析就绪数据剖分管理系统" in resp.text


def test_encoding_page_serves_html():
    resp = client.get("/encoding")
    assert resp.status_code == 200
    assert "cube_web SDK Backend (/v1/*)" in resp.text


def test_styles_asset_serves_css():
    resp = client.get("/styles.css")
    assert resp.status_code == 200
    assert ".site-header" in resp.text


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
