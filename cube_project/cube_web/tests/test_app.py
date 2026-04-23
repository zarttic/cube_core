from fastapi.testclient import TestClient
from s2sphere import CellId

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
