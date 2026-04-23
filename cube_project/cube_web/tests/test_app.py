from fastapi.testclient import TestClient

from cube_web.app import ENCODER_SDK_CLASS, app


client = TestClient(app)


def test_home_page_serves_index():
    resp = client.get("/")
    assert resp.status_code == 200
    assert "分析就绪数据剖分管理系统" in resp.text


def test_encoding_page_serves_html():
    resp = client.get("/encoding")
    assert resp.status_code == 200
    assert "cube_encoder API (/v1/*)" in resp.text


def test_styles_asset_serves_css():
    resp = client.get("/styles.css")
    assert resp.status_code == 200
    assert ".site-header" in resp.text


def test_cube_web_imports_encoder_package():
    assert ENCODER_SDK_CLASS.__name__ == "CubeEncoderSDK"
