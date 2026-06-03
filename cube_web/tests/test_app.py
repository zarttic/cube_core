from __future__ import annotations

import base64
import hashlib
import hmac
import json
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from fastapi import FastAPI
from s2sphere import CellId

import cube_web.app as web_app
import cube_web.routes.partition_adapters as partition_adapters
import cube_web.routes.quality_adapters as quality_adapters
from cube_web.app import ENCODER_SDK_CLASS, app
from cube_web.services import config_store as config_store_module
from cube_web.services import health_service
from cube_web.services import partition_job_store as partition_job_store_module
from cube_web.services import partition_runners
from cube_web.services import quality_report_store as quality_report_store_module
from cube_web.services.config_store import set_config_store
from cube_web.services.partition_defaults import default_grid_level_for_resolution
from cube_web.services.partition_job_store import InMemoryPartitionJobStore, set_partition_job_store
from cube_web.services.partition_loaded_schemas import ensure_standard_partition_schemas, standard_partition_schemas
from cube_web.services.partition_service import PartitionBackend, PartitionService
from cube_web.services.quality_report_store import set_quality_report_store

client = TestClient(app)


def make_jwt(payload, secret="your-secret-key-here-change-in-production"):
    header = {"alg": "HS256", "typ": "JWT"}

    def encode(data):
        raw = json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

    signing_input = f"{encode(header)}.{encode(payload)}"
    signature = hmac.new(secret.encode("utf-8"), signing_input.encode("ascii"), hashlib.sha256).digest()
    encoded_signature = base64.urlsafe_b64encode(signature).rstrip(b"=").decode("ascii")
    return f"{signing_input}.{encoded_signature}"


class FakeQualityReportStore:
    def __init__(self):
        self.reports = {}
        self.history = {"optical": [], "product": []}

    def ensure_schema(self):
        return None

    def upsert_report(self, data_type, run_dir, report):
        report = dict(report)
        report_id = str(report.get("report_id") or f"{data_type}-report-{len(self.reports) + 1}")
        report.update(
            {
                "report_id": report_id,
                "data_type": data_type,
                "run_dir": str(run_dir),
                "generated_at": report.get("generated_at") or "2026-05-15T01:02:03Z",
            }
        )
        self.reports[(data_type, report_id)] = report
        self.history.setdefault(data_type, [])
        self.history[data_type] = [row for row in self.history[data_type] if row["report_id"] != report_id]
        self.history[data_type].insert(
            0,
            {
                "report_id": report_id,
                "data_type": data_type,
                "run_dir": str(run_dir),
                "run_name": str(run_dir).rstrip("/").split("/")[-1],
                "dataset": str(run_dir).rstrip("/").split("/")[-2],
                "status": report.get("status", "UNKNOWN"),
                "target_crs": report.get("target_crs"),
                "generated_at": report["generated_at"],
                "summary": report.get("summary", {}),
            },
        )
        return report

    def get_report(self, data_type, report_id):
        return self.reports.get((data_type, report_id))

    def latest_report(self, data_type):
        rows = self.history.get(data_type, [])
        if not rows:
            return None
        return self.get_report(data_type, rows[0]["report_id"])

    def list_reports(self, data_type, limit=20):
        return list(self.history.get(data_type, []))[:limit]


class FakeConfigStore:
    def __init__(self):
        self.config = config_store_module.default_config()
        self.updated_at = None

    def ensure_schema(self):
        return None

    def get_config_record(self):
        return {"config": config_store_module.normalized_config(self.config), "updated_at": self.updated_at}

    def update_config(self, config):
        self.config = config_store_module.normalized_config(config)
        self.updated_at = "2026-05-26T08:00:00+00:00"
        return self.get_config_record()

    def reset_config(self):
        self.config = config_store_module.default_config()
        self.updated_at = "2026-05-26T08:00:00+00:00"
        return self.get_config_record()


def ard_raster_asset(
    source_uri: str,
    scene_id: str,
    *,
    data_type: str = "optical",
    asset_id: str | None = None,
    band: str | None = None,
    resolution: Any = 10,
) -> dict:
    band = band or ("product_value" if data_type == "product" else "vv" if data_type == "radar" else "b1")
    asset = {
        "source_uri": source_uri,
        "scene_id": scene_id,
        "acq_time": "2026-05-30T00:00:00Z",
        "bands": [band],
        "band": band,
        "corners": [[100.0, 27.0], [105.0, 27.0], [105.0, 23.0], [100.0, 23.0]],
        "resolution": resolution,
        "sensor": {
            "optical": "optical_mosaic",
            "product": "data_product",
            "radar": "sentinel1_sar",
        }[data_type],
        "product_family": {
            "optical": "other",
            "product": "product",
            "radar": "sentinel1",
        }[data_type],
    }
    if asset_id:
        asset["asset_id"] = asset_id
    if data_type == "product":
        asset["product_name"] = "test_product"
        asset["product_year"] = 2026
    if data_type == "radar":
        asset["polarization"] = band
    return asset


def ard_carbon_observation(observation_id: str = "obs-a") -> dict:
    return {
        "source_uri": "s3://cube/cube/source/carbon/oco2.jsonl",
        "observation_id": observation_id,
        "acq_time": "2026-05-30T00:00:00Z",
        "resolution": 10,
        "sensor": "oco2",
        "product_family": "xco2",
        "lon": 100.0,
        "lat": 25.0,
    }


@pytest.fixture(autouse=True)
def quality_store(monkeypatch):
    monkeypatch.setenv("CUBE_WEB_POSTGRES_DSN", "postgresql://postgres:postgres@127.0.0.1:55432/cube")
    monkeypatch.setenv("CUBE_WEB_RAY_ADDRESS", "ray://10.136.1.13:10001")
    monkeypatch.setenv("CUBE_WEB_MINIO_ENDPOINT", "10.136.1.14:9000")
    monkeypatch.setenv("CUBE_WEB_MINIO_BUCKET", "cube")
    monkeypatch.setenv("CUBE_WEB_AUTH_JWT_SECRET_KEY", "your-secret-key-here-change-in-production")
    monkeypatch.delenv("CUBE_WEB_LOAD_DEMO_PARTITION_SCHEMAS", raising=False)
    store = FakeQualityReportStore()
    set_quality_report_store(store)
    config_store = FakeConfigStore()
    set_config_store(config_store)
    set_partition_job_store(InMemoryPartitionJobStore())
    web_app.partition_workflow_service._store = None
    yield store
    set_quality_report_store(None)
    set_config_store(None)
    set_partition_job_store(None)
    web_app.partition_workflow_service._store = None


def test_header_navigation_does_not_expose_quality_as_top_level_item():
    nav_source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "data" / "navigation.js").read_text(
        encoding="utf-8"
    )
    app_source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "App.vue").read_text(encoding="utf-8")

    assert "{ label: '自动化质检'," not in nav_source
    assert "{ label: '分析就绪数据剖分', kind: 'internal', path: '/partition' }" in nav_source
    assert "{ label: '全球离散格网模型与编码', kind: 'internal', path: '/encoding' }" in nav_source
    assert "runtimeNavigation()" in nav_source
    assert ':href="item.path"' in app_source
    assert "currentNavItems" in app_source
    assert "targetFromAuthState(state)" in app_source
    assert "normalizePath(window.location.pathname)" in app_source


def test_auth_redirect_uses_clicked_page_as_redirect_uri():
    store_source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "stores" / "subUser.js").read_text(
        encoding="utf-8"
    )

    assert "function authRedirectUri(targetPath)" in store_source
    assert "base.pathname = target.pathname;" in store_source
    assert "base.search = target.search;" in store_source
    assert "sessionStorage.setItem('oauth_target', target);" in store_source
    assert "const redirectUri = authRedirectUri(target);" in store_source


def test_frontend_auth_bootstrap_uses_runtime_config_flag():
    app_source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "App.vue").read_text(encoding="utf-8")
    config_source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "config.js").read_text(encoding="utf-8")
    store_source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "stores" / "subUser.js").read_text(
        encoding="utf-8"
    )

    assert "await loadAuthRuntimeConfig();" in app_source
    assert "if (authRequired()) {" in app_source
    assert "fetch('/api/config'" in config_source
    assert "auth_required" in config_source
    assert "navigation" in config_source
    assert "http://10.136." not in config_source
    assert "if (authRequired()) {" in store_source


def test_partition_view_uses_explicit_module_endpoint_mapping():
    source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "views" / "PartitionView.vue").read_text(
        encoding="utf-8"
    )

    assert "const partitionEndpointsByModule = {" in source
    assert "optical: 'optical'" in source
    assert "carbon: 'carbon'" in source
    assert "radar: 'radar'" in source
    assert "product: 'product'" in source
    assert "const testModules = new Set(['optical', 'carbon', 'radar', 'product']);" in source
    assert "const operation = testModules.has(activeModule.value) ? 'test' : 'run';" in source
    assert "activeModule === 'entity'" not in source
    assert "activeModule.value === 'entity'" not in source
    assert ">实体剖分</button>" not in source
    assert '<el-option label="四边形格网" value="geohash" />' in source
    assert '<el-option label="平面格网" value="tile_matrix" />' in source
    assert '<el-option label="六边形格网" value="isea4h" />' in source
    assert '<el-option label="MGRS (逻辑剖分)" value="mgrs" />' not in source
    assert 'v-model="radarGridType"' in source
    assert 'v-model="productGridType"' in source
    assert "grid_level_mode: isGridLevelManual('radar') || useEntityPartition ? 'manual' : 'auto'" in source
    assert "grid_level_mode: isGridLevelManual('product') || useEntityPartition ? 'manual' : 'auto'" in source
    assert "if (gridType === 'isea4h') return resolution < 10 ? 5 : defaultEntityGridLevel;" in source
    assert "if (resolution < 10) return 8;" in source
    assert "if (resolution <= 30) return 7;" in source
    assert "const partitionStageDetailVisible = ref(false);" in source
    assert "function openPartitionStageDetail(stage)" in source
    assert '@click="openPartitionStageDetail(stage)"' in source
    assert 'title="剖分进程详情"' in source
    assert "const partitionContextDetailVisible = ref(false);" in source
    assert "function openPartitionContextDetail(item)" in source
    assert '@click="openPartitionContextDetail(item)"' in source
    assert 'title="剖分信息详情"' in source
    assert "partition-stage-detail-message" in source
    assert "const selectedCarbonObservations = computed(() => {" in source
    assert "selected_observations: selectedObservations" in source
    assert "selectedRadarAssets" in source
    assert "const managedOpticalBatches = ref([]);" in source
    assert "const partitionBatchDetailVisible = ref(false);" in source
    assert "const visibleOpticalBatches = computed(() => managedOpticalBatches.value);" in source
    assert "const visibleCarbonBatches = computed(() => managedCarbonBatches.value);" in source
    assert "const visibleRadarBatches = computed(() => managedRadarBatches.value);" in source
    assert "const visibleProductBatches = computed(() => managedProductBatches.value);" in source
    assert "async function loadPartitionBatches()" in source
    assert "requestGet(`${partitionPrefix}/batches?include_succeeded=false&limit=200`)" in source
    assert "requestGet(`${partitionPrefix}/batches/${batchId}/attempts`)" in source
    assert "取消会立即请求执行层中断当前任务" in source
    assert "重试失败资产" in source
    assert "partitionBatchDetailTab === 'attempts'" in source
    assert "visibleOpticalBatches" in source
    assert "const selectedProductAssets = computed(() => {" in source
    assert "const productMapGeometries = computed(() => selectedProductAssets.value" in source
    assert "activeModule.value === 'radar'" in source
    assert "? selectedRadarAssets.value" in source
    assert "? radarGridType.value" in source
    assert "const defaultEntityGridLevel = 4;" in source
    assert "const entityGridLevel = ref(defaultEntityGridLevel);" in source
    assert "activeModule.value === 'radar' ? radarGridLevel.value" in source
    assert "activeModule === 'product' ? '产品范围地图预览'" in source
    assert "selected_assets: selectedAssets" in source
    assert "function buildPartitionFailureResult(error, request = {})" in source
    assert "const partitionFailureMessage = computed" in source
    assert "partitionFailureMessage" in source
    assert "剖分失败，详情已写入执行结果" not in source
    assert "requestPartitionOperation(partitionPrefix, endpoint, operation, payload)" in source
    assert "/tasks/${operation}" in source
    assert '<el-option label="碳卫星" value="carbon" />' in source
    assert '<el-option label="雷达遥感" value="radar" />' in source
    assert "carbon_rows: '观测行文件读取'" in source
    assert "function qualitySourceText()" in source
    assert "schema-grid" in source
    assert "buildLocalPartitionBatchDetail" not in source
    assert "dataRowsByModule" not in source
    assert "filteredDataRows" not in source
    assert "is_local_demo" not in source
    assert "runDemoForBatch" not in source
    assert "const carbonObservationSchema = [" not in source
    assert "const productAssetSchema = [" not in source
    assert "const radarAssetSchema = [" not in source
    assert "RADAR_BATCH_YANGZHOU_S1_2018_2020" not in source
    assert "PRODUCT_BATCH_DIANZHONG_1980_2020" not in source
    assert "const dianzhongProductBbox = [100.644783, 23.28638, 104.829333, 27.061367];" not in source
    assert '<el-option label="本地数据源" value="local" />' not in source
    assert "activeModule.value === 'carbon' ? 'carbon' : 'optical'" not in source
    assert "观测足迹匹配" not in source
    assert "面积加权" not in source
    assert "最近邻" not in source


@pytest.mark.parametrize(
    ("resolution", "grid_type", "expected_level"),
    [
        (5, "geohash", 8),
        ("9.9m", "tile_matrix", 8),
        ("10m", "geohash", 7),
        (30, "tile_matrix", 7),
        (31, "geohash", 6),
        (5, "isea4h", 5),
        ("10m", "isea4h", 4),
        (30, "isea4h", 4),
    ],
)
def test_partition_resolution_grid_level_defaults(resolution, grid_type, expected_level):
    assert default_grid_level_for_resolution(resolution, grid_type=grid_type) == expected_level


def test_config_view_does_not_expose_mgrs_partition_grid_type():
    source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "views" / "ConfigView.vue").read_text(
        encoding="utf-8"
    )

    assert '<el-option label="四边形格网" value="geohash" />' in source
    assert '<el-option label="平面格网" value="tile_matrix" />' in source
    assert '<el-option label="六边形格网" value="isea4h" />' in source
    assert '<el-option label="MGRS" value="mgrs" />' not in source


def test_encoding_view_displays_generic_grid_type_names():
    source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "views" / "EncodingView.vue").read_text(
        encoding="utf-8"
    )

    assert "tile_matrix: '平面格网'" in source
    assert '<input v-model="division.gridType" type="radio" value="tile_matrix">' in source
    assert '<option value="tile_matrix">平面格网</option>' in source
    assert '<input v-model="topology.gridType" type="radio" value="tile_matrix">' in source
    assert "Geohash" not in source
    assert "MGRS" not in source
    assert "Tile Matrix" not in source
    assert "ISEA4H" not in source
    assert "tm:8:8/420/71:202603091530:v1" in source
    assert "层级与空间编码" in source


def test_quality_report_store_requires_explicit_postgres_dsn(monkeypatch):
    captured = {}

    class DummyPostgresQualityReportStore:
        def __init__(self, dsn):
            captured["dsn"] = dsn

    monkeypatch.delenv("CUBE_WEB_POSTGRES_DSN", raising=False)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("CUBE_WEB_ENV_FILE", "/tmp/cube-web-missing-env-file")
    monkeypatch.setattr(
        quality_report_store_module,
        "PostgresQualityReportStore",
        DummyPostgresQualityReportStore,
    )
    quality_report_store_module.set_quality_report_store(None)

    try:
        with pytest.raises(RuntimeError, match="PostgreSQL DSN is required"):
            quality_report_store_module.get_quality_report_store()
        assert "dsn" not in captured
    finally:
        quality_report_store_module.set_quality_report_store(None)


def test_home_page_serves_index():
    resp = client.get("/")
    assert resp.status_code == 200
    assert "分析就绪数据剖分管理系统" in resp.text


def test_health_reports_runtime_config_sources():
    resp = client.get("/health")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    values = body["checks"]["config"]["values"]
    assert values["postgres_dsn"]["source"] == "environment"
    assert values["postgres_dsn"]["value"] == "postgresql://***:***@127.0.0.1:55432/cube"
    assert values["ray_address"]["source"] == "environment"
    assert values["minio_endpoint"]["value"] == "10.136.1.14:9000"
    assert values["minio_bucket"]["value"] == "cube"
    assert "value" not in values["minio_secret_key"]


def test_health_selectively_runs_dependency_checks(monkeypatch):
    monkeypatch.setattr(health_service, "_check_postgres", lambda: {"status": "ok", "latency_ms": 1})
    monkeypatch.setattr(health_service, "_check_ray", lambda: {"status": "fail", "message": "ray down"})
    monkeypatch.setattr(health_service, "_check_minio", lambda: {"status": "ok", "latency_ms": 2})
    monkeypatch.setattr(health_service, "_check_minio_bucket", lambda: {"status": "ok", "bucket": "cube"})

    resp = client.get("/health?checks=config,postgres,ray&check=minio&check=bucket")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "degraded"
    assert body["failed_checks"] == ["ray"]
    assert set(body["checks"]) == {"service", "config", "postgres", "ray", "minio", "bucket"}


def test_encoding_page_serves_html():
    resp = client.get("/encoding")
    assert resp.status_code == 200
    assert "分析就绪数据剖分管理系统" in resp.text


def test_legacy_encoding_html_serves_vue_app():
    resp = client.get("/encoding.html")
    assert resp.status_code == 200
    assert "分析就绪数据剖分管理系统" in resp.text


def test_config_page_serves_vue_app():
    resp = client.get("/config")
    assert resp.status_code == 200
    assert "分析就绪数据剖分管理系统" in resp.text


def test_auth_config_exposes_subsystem_client(monkeypatch):
    monkeypatch.setenv("CUBE_WEB_AUTH_MAIN_SYSTEM_URL", "http://10.136.1.14:5177")
    monkeypatch.setenv("CUBE_WEB_AUTH_CLIENT_ID", "system_ard")
    monkeypatch.setenv("CUBE_WEB_AUTH_REDIRECT_URI", "http://10.136.1.14:50040/callback")
    monkeypatch.setenv("CUBE_WEB_PORTAL_PARTITION_SERVICE_URL", "http://10.136.1.14:5176/#/partition")
    monkeypatch.setenv("CUBE_WEB_PORTAL_DISPATCH_URL", "http://10.136.1.14:5176/#/dispatch")
    monkeypatch.setenv("CUBE_WEB_AUTH_REQUIRED", "0")

    resp = client.get("/api/config")

    assert resp.status_code == 200
    assert resp.json() == {
        "client_id": "system_ard",
        "redirect_uri": "http://10.136.1.14:50040/callback",
        "main_system_url": "http://10.136.1.14:5177",
        "auth_required": False,
        "navigation": [
            {"label": "首页", "kind": "external", "url": "http://10.136.1.14:5176/#/home"},
            {"label": "剖分数据服务", "kind": "external", "url": "http://10.136.1.14:5176/#/partition"},
            {"label": "资源调度", "kind": "external", "url": "http://10.136.1.14:5176/#/dispatch"},
            {"label": "ARD数据载入", "kind": "external", "url": "http://10.136.1.14:5177/ard"},
            {"label": "后台管理", "kind": "external", "url": "http://10.136.1.14:5177/admin"},
        ],
    }


def test_auth_config_uses_runtime_defaults_when_portal_env_is_empty(monkeypatch):
    monkeypatch.delenv("CUBE_WEB_AUTH_MAIN_SYSTEM_URL", raising=False)
    monkeypatch.delenv("CUBE_WEB_PORTAL_MAIN_URL", raising=False)
    monkeypatch.delenv("CUBE_WEB_PORTAL_DATA_INGEST_URL", raising=False)
    monkeypatch.delenv("CUBE_WEB_PORTAL_PARTITION_SERVICE_URL", raising=False)
    monkeypatch.delenv("CUBE_WEB_PORTAL_DISPATCH_URL", raising=False)
    monkeypatch.delenv("CUBE_WEB_PORTAL_ADMIN_URL", raising=False)
    store = FakeConfigStore()
    store.update_config(
        {
            "runtime": {
                "portal": {
                    "navigation": [
                        {"label": "ARD数据载入", "kind": "external", "url": "http://10.136.1.14:9001"},
                        {"label": "剖分数据服务", "kind": "internal", "path": "/partition"},
                    ]
                }
            }
        }
    )
    set_config_store(store)

    resp = client.get("/api/config")

    assert resp.status_code == 200
    assert resp.json()["navigation"] == [
        {"label": "首页", "kind": "external", "url": "http://10.136.1.14:5176/#/home"},
        {"label": "剖分数据服务", "kind": "external", "url": "http://10.136.1.14:5176/#/partition"},
        {"label": "资源调度", "kind": "external", "url": "http://10.136.1.14:5176/#/dispatch"},
        {"label": "ARD数据载入", "kind": "external", "url": "http://10.136.1.14:5177/ard"},
        {"label": "后台管理", "kind": "external", "url": "http://10.136.1.14:5177/admin"},
    ]


def test_auth_config_exposes_runtime_auth_switch(monkeypatch):
    monkeypatch.setenv("CUBE_WEB_AUTH_REQUIRED", "1")

    resp = client.get("/api/config")

    assert resp.status_code == 200
    assert resp.json()["auth_required"] is True


def test_auth_verify_and_me_accept_hs256_jwt():
    token = make_jwt(
        {
            "sub": "u-1",
            "username": "alice",
            "role": "管理员",
            "avatar_url": "http://example.test/avatar.png",
            "exp": time.time() + 3600,
        }
    )
    headers = {"Authorization": f"Bearer {token}"}

    verify_resp = client.get("/api/verify", headers=headers)
    me_resp = client.get("/api/me", headers=headers)

    assert verify_resp.status_code == 200
    assert verify_resp.json()["valid"] is True
    assert verify_resp.json()["sub"] == "u-1"
    assert me_resp.status_code == 200
    assert me_resp.json()["username"] == "alice"
    assert me_resp.json()["role"] == "管理员"
    assert me_resp.json()["avatarUrl"] == "http://example.test/avatar.png"


def test_auth_required_rejects_v1_without_bearer(monkeypatch):
    monkeypatch.setenv("CUBE_WEB_AUTH_REQUIRED", "1")

    resp = client.post("/v1/config/get", json={})

    assert resp.status_code == 401
    assert resp.json()["detail"] == "Missing Authorization header"


def test_auth_required_allows_v1_with_valid_bearer(monkeypatch):
    monkeypatch.setenv("CUBE_WEB_AUTH_REQUIRED", "1")
    token = make_jwt({"sub": "u-1", "username": "alice", "exp": time.time() + 3600})

    resp = client.post("/v1/config/get", json={}, headers={"Authorization": f"Bearer {token}"})

    assert resp.status_code == 200
    assert resp.json()["config"]["partition"]["optical"]["grid_type"] == "geohash"


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
    assert "ConfigResponse" in schema["components"]["schemas"]


def test_partition_services_split_production_and_legacy_registries():
    assert web_app.partition_service.registry["optical"].run is not None
    assert web_app.partition_service.registry["optical"].demo is None
    assert web_app.partition_service.registry["optical"].test is None
    assert web_app.partition_service.registry["optical"].retry is None
    assert web_app.legacy_partition_service.registry["optical"].run is None
    assert web_app.legacy_partition_service.registry["optical"].demo is not None
    assert web_app.legacy_partition_service.registry["optical"].test is not None
    assert web_app.legacy_partition_service.registry["optical"].retry is not None
    assert web_app.legacy_partition_service.task_store is web_app.partition_service.task_store


def test_partition_router_dispatches_run_and_legacy_operations_to_separate_services():
    production = PartitionService(
        {
            "optical": PartitionBackend(
                data_type="optical",
                run=lambda payload=None: {"status": "completed", "source": "production", "payload": payload or {}},
            )
        }
    )
    legacy = PartitionService(
        {
            "optical": PartitionBackend(
                data_type="optical",
                demo=lambda payload=None: {"status": "completed", "source": "legacy-demo", "payload": payload or {}},
                test=lambda payload=None: {"status": "completed", "source": "legacy-test", "payload": payload or {}},
            )
        },
        task_store=production.task_store,
    )
    route_app = FastAPI()
    route_app.include_router(web_app.partition_route.create_partition_router(service=production, legacy_service=legacy))
    route_client = TestClient(route_app)

    run_resp = route_client.post("/partition/optical/run", json={"grid_type": "geohash", "grid_level": 4})
    demo_resp = route_client.post("/partition/optical/demo", json={"grid_type": "geohash", "grid_level": 4})
    test_resp = route_client.post("/partition/optical/test", json={"grid_type": "geohash", "grid_level": 4})

    assert run_resp.status_code == 200
    assert run_resp.json()["source"] == "production"
    assert demo_resp.status_code == 200
    assert demo_resp.json()["source"] == "legacy-demo"
    assert test_resp.status_code == 200
    assert test_resp.json()["source"] == "legacy-test"


def test_config_get_returns_defaults():
    resp = client.post("/v1/config/get", json={})

    assert resp.status_code == 200
    body = resp.json()
    assert body["config"]["partition"]["optical"]["grid_type"] == "geohash"
    assert body["config"]["partition"]["optical"]["grid_level"] == 5
    assert body["config"]["ingest"]["optical"]["metadata_backend"] == "postgres"
    assert body["config"]["ingest"]["optical"]["asset_storage_backend"] == "minio"
    assert body["config"]["ingest"]["optical"]["minio_endpoint"] == "10.136.1.14:9000"
    assert body["config"]["ingest"]["optical"]["minio_bucket"] == "cube"
    assert body["runtime"]["postgres_dsn"] == "postgresql://***:***@127.0.0.1:55432/cube"
    assert body["runtime"]["ray_address"] == "ray://10.136.1.13:10001"
    assert body["runtime"]["minio"] == {
        "endpoint": "10.136.1.14:9000",
        "bucket": "cube",
        "secure": False,
    }


def test_config_update_persists_normalized_values():
    resp = client.post(
        "/v1/config/update",
        json={
            "config": {
                "partition": {
                    "optical": {
                        "grid_type": "tile_matrix",
                        "grid_level": 8,
                        "ray_parallelism": 0,
                        "cover_mode": "contain",
                    }
                },
                "ingest": {"optical": {"dataset": "customer_demo", "sensor": "landsat", "quality_rule": "latest_wins"}},
                "quality": {"optical": {"history_limit": 50}},
                "runtime": {"portal": {"navigation": [{"label": "unused", "kind": "external", "url": "http://example.test"}]}},
                "unused": {"value": True},
            }
        },
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["config"]["partition"]["optical"]["grid_type"] == "tile_matrix"
    assert body["config"]["partition"]["optical"]["grid_level"] == 8
    assert body["config"]["partition"]["optical"]["ray_parallelism"] == 0
    assert body["config"]["partition"]["optical"]["cover_mode"] == "contain"
    assert body["config"]["ingest"]["optical"]["dataset"] == "customer_demo"
    assert body["config"]["quality"]["optical"]["history_limit"] == 50
    assert "runtime" not in body["config"]
    assert "unused" not in body["config"]

    get_resp = client.post("/v1/config/get", json={})
    assert get_resp.json()["config"]["partition"]["optical"]["grid_type"] == "tile_matrix"
    assert "runtime" not in get_resp.json()["config"]


def test_config_update_accepts_tile_matrix_grid_type():
    resp = client.post(
        "/v1/config/update",
        json={"config": {"partition": {"optical": {"grid_type": "tile_matrix", "grid_level": 5}}}},
    )

    assert resp.status_code == 200
    assert resp.json()["config"]["partition"]["optical"]["grid_type"] == "tile_matrix"


def test_config_update_rejects_mgrs_partition_grid_type():
    resp = client.post(
        "/v1/config/update",
        json={"config": {"partition": {"optical": {"grid_type": "mgrs", "grid_level": 5}}}},
    )

    assert resp.status_code == 422
    assert "grid_type" in resp.json()["detail"]


def test_stored_config_migrates_legacy_mgrs_grid_type():
    config = config_store_module.normalized_stored_config(
        {"partition": {"optical": {"grid_type": "mgrs", "grid_level": 5}}}
    )

    assert config["partition"]["optical"]["grid_type"] == "tile_matrix"


def test_config_update_rejects_invalid_values():
    resp = client.post("/v1/config/update", json={"config": {"partition": {"optical": {"grid_level": 0}}}})

    assert resp.status_code == 422
    assert "grid_level" in resp.json()["detail"]


def test_config_update_rejects_legacy_contains_cover_mode():
    resp = client.post("/v1/config/update", json={"config": {"partition": {"optical": {"cover_mode": "contains"}}}})

    assert resp.status_code == 422
    assert "cover_mode" in resp.json()["detail"]


def test_partition_demo_rejects_legacy_contains_cover_mode():
    resp = client.post(
        "/v1/partition/optical/demo",
        json={
            "grid_type": "geohash",
            "grid_level": 5,
            "cover_mode": "contains",
        },
    )

    assert resp.status_code == 422


def test_carbon_partition_demo_endpoint(monkeypatch):
    def fake_run_carbon_partition_demo():
        return {
            "status": "completed",
            "mode": "partition_demo",
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

    monkeypatch.setattr(partition_adapters, "run_carbon_partition_demo", fake_run_carbon_partition_demo)

    body = partition_adapters.partition_carbon_demo()

    assert body["status"] == "completed"
    assert body["rows"] == 12
    assert body["distinct_space_codes"] == 5


def test_carbon_partition_test_endpoint(monkeypatch):
    expected_payload = {"grid_type": "isea4h", "grid_level": 5}

    def fake_run_carbon_partition_test(payload=None):
        assert payload == expected_payload
        return {
            "status": "completed",
            "mode": "partition_test_no_ingest",
            "data_type": "carbon_satellite",
            "rows": 12,
            "distinct_space_codes": 5,
            "elapsed_sec": 0.12,
            "rows_per_sec": 100.0,
            "grid_type": "isea4h",
            "grid_level": 5,
            "workers": 2,
            "partition_backend": "ray",
            "execution_engine": "ray",
            "ingest_enabled": False,
            "output_path": "/tmp/demo/carbon_observation_rows.jsonl",
        }

    monkeypatch.setattr(partition_adapters, "run_carbon_partition_test", fake_run_carbon_partition_test)

    resp = client.post("/v1/partition/carbon/test", json=expected_payload)

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "completed"
    assert body["mode"] == "partition_test_no_ingest"
    assert body["data_type"] == "carbon_satellite"
    assert body["ingest_enabled"] is False
    assert body["rows"] == 12


def test_carbon_partition_test_runner_marks_safe_mode(monkeypatch):
    captured = {}

    def fake_run_carbon_partition_demo(mode="partition_demo", payload=None):
        captured["mode"] = mode
        captured["payload"] = payload
        return {
            "status": "completed",
            "mode": mode,
            "data_type": "carbon_satellite",
            "ingest_enabled": mode != "partition_test_no_ingest",
        }

    monkeypatch.setattr(partition_runners, "_run_carbon_partition_demo", fake_run_carbon_partition_demo)

    result = partition_runners._run_carbon_partition_test({"selected_observations": [{"source_index": 3}]})

    assert captured["mode"] == "partition_test_no_ingest"
    assert captured["payload"]["selected_observations"][0]["source_index"] == 3
    assert result["mode"] == "partition_test_no_ingest"
    assert result["ingest_enabled"] is False


def test_carbon_partition_retry_runner_reuses_request_payload(monkeypatch):
    captured = {}

    def fake_run_carbon_partition_demo(mode="partition_demo", payload=None):
        captured["mode"] = mode
        captured["payload"] = payload
        return {
            "status": "completed",
            "mode": mode,
            "data_type": "carbon_satellite",
        }

    monkeypatch.setattr(partition_runners, "_run_carbon_partition_demo", fake_run_carbon_partition_demo)

    result = partition_runners._run_carbon_partition_retry(
        {
            "request": {
                "payload": {
                    "selected_observations": [{"source_index": 3}],
                }
            }
        }
    )

    assert captured["mode"] == "partition_demo"
    assert captured["payload"]["selected_observations"][0]["source_index"] == 3
    assert result["mode"] == "partition_retry"


def test_carbon_selected_source_indexes_are_normalized():
    payload = {
        "selected_observations": [
            {"source_index": "3"},
            {"source_index": 1},
            {"source_index": 3},
            {"source_index": None},
            {"source_index": "bad"},
        ]
    }

    assert partition_runners._carbon_selected_source_indexes(payload) == (1, 3)


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
            "quality_report_id": "optical-demo-report",
            "quality_report": {
                "report_id": "optical-demo-report",
                "status": "WARN",
                "checks": [{"name": "logical_duplicates", "status": "WARN"}],
            },
        }

    monkeypatch.setattr(partition_adapters, "run_optical_partition_demo", fake_run_optical_partition_demo)

    body = partition_adapters.partition_optical_demo()

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

    monkeypatch.setattr(partition_adapters, "run_optical_partition_demo", fake_run_optical_partition_demo)

    resp = client.post("/v1/partition/optical/demo", json=expected_payload)

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "completed"
    assert body["mode"] == "partition_demo"
    assert body["execution_engine"] == "ray"
    assert body["grid_level"] == 5


def test_optical_partition_runner_uses_config_defaults_without_overriding_payload(monkeypatch, tmp_path):
    config_store = FakeConfigStore()
    config_store.update_config(
        {
            "partition": {
                "optical": {
                    "grid_type": "tile_matrix",
                    "grid_level": 9,
                    "target_crs": "EPSG:3857",
                    "partition_backend": "thread",
                    "ray_parallelism": 0,
                }
            }
        }
    )
    set_config_store(config_store)
    captured = {}

    def fake_run_logical_partition(args):
        captured.update(vars(args))
        run_dir = tmp_path / "run"
        run_dir.mkdir()
        return {
            "run_dir": str(run_dir),
            "execution_engine": args.partition_backend,
            "total_index_rows": 0,
            "ray_parallelism": args.ray_parallelism,
        }

    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job.run_logical_partition", fake_run_logical_partition)
    monkeypatch.setattr("cube_web.services.quality_checks.run_optical_quality_check", None)

    result = partition_runners._run_optical_partition_from_payload(
        {"input_dir": str(tmp_path), "grid_level": 4, "cube_version": "cube-smoke"},
        mode="partition_test_no_ingest",
    )

    assert result["status"] == "completed"
    assert captured["grid_type"] == "tile_matrix"
    assert captured["grid_level"] == 4
    assert captured["target_crs"] == "EPSG:3857"
    assert captured["partition_backend"] == "thread"
    assert captured["ray_parallelism"] == 0
    assert captured["cube_version"] == "cube-smoke"
    assert captured["quality_rule"] == "best_quality_wins"
    assert captured["db_path"] == ""
    assert captured["cog_materialize_mode"] == "copy"
    assert captured["cog_output_root"].endswith("optical_cog_store")


def test_optical_partition_runner_infers_grid_level_from_selected_asset_resolution(monkeypatch, tmp_path):
    captured = {}

    def fake_run_logical_partition(args):
        captured.update(vars(args))
        run_dir = tmp_path / "run"
        run_dir.mkdir()
        return {
            "run_dir": str(run_dir),
            "execution_engine": args.partition_backend,
            "total_index_rows": 0,
            "ray_parallelism": args.ray_parallelism,
        }

    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job.run_logical_partition", fake_run_logical_partition)
    monkeypatch.setattr("cube_web.services.quality_checks.run_optical_quality_check", None)

    partition_runners._run_optical_partition_from_payload(
        {
            "input_dir": str(tmp_path),
            "selected_assets": [ard_raster_asset("s3://cube/cube/source/optocal/scene_10m.tif", "scene-10m", resolution="10m")],
        },
        mode="partition_test_no_ingest",
    )

    assert captured["grid_type"] == "geohash"
    assert captured["grid_level"] == 7


def test_optical_partition_runner_dispatches_isea4h_to_entity_partition(monkeypatch, tmp_path):
    captured = {}

    def fake_run_entity_partition(args):
        captured.update(vars(args))
        run_dir = tmp_path / "entity-run"
        run_dir.mkdir()
        rows_path = run_dir / "entity_index_rows.jsonl"
        rows_path.write_text("", encoding="utf-8")
        return {
            "run_dir": str(run_dir),
            "rows_path": str(rows_path),
            "execution_engine": "local",
            "partition_type": "entity",
            "grid_type": "isea4h",
            "grid_level": 5,
            "inferred_grid_level": 5,
            "total_index_rows": 0,
            "ray_parallelism": 0,
        }

    def fail_logical_partition(_args):
        raise AssertionError("isea4h optical partition should use entity partition")

    monkeypatch.setattr("cube_split.jobs.entity_partition_job.run_entity_partition", fake_run_entity_partition)
    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job.run_logical_partition", fail_logical_partition)
    monkeypatch.setattr("cube_web.services.quality_checks.run_optical_quality_check", None)

    result = partition_runners._run_optical_partition_from_payload(
        {
            "input_dir": str(tmp_path),
            "grid_type": "isea4h",
            "grid_level": 9,
            "target_pixels_per_hex_edge": 512,
        },
        mode="partition_test_no_ingest",
    )

    assert result["status"] == "completed"
    assert result["partition_type"] == "entity"
    assert result["output_path"].endswith("entity_index_rows.jsonl")
    assert captured["grid_type"] == "isea4h"
    assert captured["grid_level"] == 9
    assert captured["target_pixels_per_hex_edge"] == 512


def test_optical_partition_runner_dispatches_tile_matrix_to_logical_partition(monkeypatch, tmp_path):
    captured = {}

    def fake_run_logical_partition(args):
        captured.update(vars(args))
        run_dir = tmp_path / "logical-run"
        run_dir.mkdir()
        rows_path = run_dir / "index_rows.jsonl"
        rows_path.write_text("", encoding="utf-8")
        return {
            "run_dir": str(run_dir),
            "rows_path": str(rows_path),
            "execution_engine": "thread",
            "grid_type": args.grid_type,
            "grid_level": args.grid_level,
            "total_index_rows": 0,
            "ray_parallelism": 0,
        }

    def fail_entity_partition(_args):
        raise AssertionError("tile_matrix optical partition should use logical partition")

    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job.run_logical_partition", fake_run_logical_partition)
    monkeypatch.setattr("cube_split.jobs.entity_partition_job.run_entity_partition", fail_entity_partition)
    monkeypatch.setattr("cube_web.services.quality_checks.run_optical_quality_check", None)

    result = partition_runners._run_optical_partition_from_payload(
        {
            "input_dir": str(tmp_path),
            "grid_type": "tile_matrix",
            "grid_level": 5,
            "partition_backend": "thread",
        },
        mode="partition_test_no_ingest",
    )

    assert result["status"] == "completed"
    assert captured["grid_type"] == "tile_matrix"
    assert captured["grid_level"] == 5
    assert result["output_path"].endswith("index_rows.jsonl")


def test_optical_partition_runner_allows_manual_isea4h_level(monkeypatch, tmp_path):
    captured = {}

    def fake_run_entity_partition(args):
        captured.update(vars(args))
        run_dir = tmp_path / "entity-run"
        run_dir.mkdir()
        rows_path = run_dir / "entity_index_rows.jsonl"
        rows_path.write_text("", encoding="utf-8")
        return {
            "run_dir": str(run_dir),
            "rows_path": str(rows_path),
            "execution_engine": "local",
            "partition_type": "entity",
            "grid_type": "isea4h",
            "grid_level": args.grid_level,
            "total_index_rows": 0,
            "ray_parallelism": 0,
        }

    monkeypatch.setattr("cube_split.jobs.entity_partition_job.run_entity_partition", fake_run_entity_partition)
    monkeypatch.setattr("cube_web.services.quality_checks.run_optical_quality_check", None)

    partition_runners._run_optical_partition_from_payload(
        {
            "input_dir": str(tmp_path),
            "grid_type": "isea4h",
            "grid_level": 7,
            "grid_level_mode": "manual",
        },
        mode="partition_test_no_ingest",
    )

    assert captured["grid_level"] == 7


def test_optical_partition_test_runner_defaults_isea4h_to_level4(monkeypatch, tmp_path):
    captured = {}

    def fake_run_entity_partition(args):
        captured.update(vars(args))
        run_dir = tmp_path / "entity-run"
        run_dir.mkdir()
        rows_path = run_dir / "entity_index_rows.jsonl"
        rows_path.write_text("", encoding="utf-8")
        return {
            "run_dir": str(run_dir),
            "rows_path": str(rows_path),
            "execution_engine": "ray",
            "partition_type": "entity",
            "grid_type": "isea4h",
            "grid_level": args.grid_level,
            "total_index_rows": 0,
            "ray_parallelism": 0,
        }

    monkeypatch.setattr("cube_split.jobs.entity_partition_job.run_entity_partition", fake_run_entity_partition)
    monkeypatch.setattr("cube_web.services.quality_checks.run_optical_quality_check", None)

    partition_runners._run_optical_partition_from_payload(
        {
            "input_dir": str(tmp_path),
            "grid_type": "isea4h",
        },
        mode="partition_test_no_ingest",
    )

    assert captured["grid_level"] == 4


def test_entity_partition_runner_uses_entity_job_and_disables_ingest_for_test(monkeypatch, tmp_path):
    captured = {}

    def fake_run_entity_partition(args):
        captured.update(vars(args))
        run_dir = tmp_path / "entity-run"
        run_dir.mkdir()
        rows_path = run_dir / "entity_index_rows.jsonl"
        rows_path.write_text("", encoding="utf-8")
        return {
            "run_dir": str(run_dir),
            "rows_path": str(rows_path),
            "execution_engine": "ray",
            "partition_type": "entity",
            "grid_type": "isea4h",
            "grid_level": args.grid_level,
            "total_index_rows": 0,
            "ray_parallelism": 4,
            "ingest_enabled": bool(args.ingest_enabled),
        }

    monkeypatch.setattr("cube_split.jobs.entity_partition_job.run_entity_partition", fake_run_entity_partition)
    monkeypatch.setattr("cube_web.services.quality_checks.run_optical_quality_check", None)

    result = partition_runners._run_entity_partition_test(
        {
            "input_dir": str(tmp_path),
            "selected_assets": [],
        }
    )

    assert result["status"] == "completed"
    assert result["data_type"] == "entity"
    assert result["mode"] == "partition_test_no_ingest"
    assert result["partition_type"] == "entity"
    assert result["output_path"].endswith("entity_index_rows.jsonl")
    assert result["ingest_enabled"] is False
    assert captured["grid_level"] == 4
    assert captured["partition_backend"] == "ray"
    assert captured["ray_address"] == "ray://10.136.1.13:10001"
    assert captured["metadata_backend"] == "postgres"
    assert captured["asset_storage_backend"] == "minio"
    assert captured["ingest_enabled"] is False


def test_partition_demo_rejects_invalid_grid_level():
    resp = client.post("/v1/partition/optical/demo", json={"grid_type": "geohash", "grid_level": 0})

    assert resp.status_code == 422


def test_partition_demo_rejects_mgrs_grid_type():
    resp = client.post("/v1/partition/optical/demo", json={"grid_type": "mgrs", "grid_level": 5})

    assert resp.status_code == 422


def test_partition_run_can_run_as_async_task(monkeypatch):
    def fake_run_product_partition_demo(payload=None):
        assert payload == {"grid_type": "geohash", "grid_level": 5}
        return {
            "status": "completed",
            "mode": "partition_demo",
            "data_type": "product",
            "rows": 20,
        }

    monkeypatch.setattr(partition_adapters, "run_product_partition_demo", fake_run_product_partition_demo)

    submit_resp = client.post("/v1/partition/product/tasks/run", json={"grid_type": "geohash", "grid_level": 5})

    assert submit_resp.status_code == 202
    submitted = submit_resp.json()
    assert submitted["status"] in {"queued", "running", "completed"}
    assert submitted["data_type"] == "product"
    assert submitted["operation"] == "run"

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


def test_partition_demo_task_endpoint_remains_compatible(monkeypatch):
    def fake_run_product_partition_demo(payload=None):
        assert payload == {"grid_type": "geohash", "grid_level": 5}
        return {"status": "completed", "mode": "partition_demo", "data_type": "product", "rows": 20}

    monkeypatch.setattr(partition_adapters, "run_product_partition_demo", fake_run_product_partition_demo)

    submit_resp = client.post("/v1/partition/product/tasks/demo", json={"grid_type": "geohash", "grid_level": 5})

    assert submit_resp.status_code == 202
    submitted = submit_resp.json()
    assert submitted["data_type"] == "product"
    assert submitted["operation"] == "demo"


def test_partition_test_can_run_as_async_task(monkeypatch):
    expected_payload = {"grid_type": "isea4h", "grid_level": 4, "grid_level_mode": "manual"}

    def fake_run_optical_partition_test(payload=None):
        assert payload == expected_payload
        return {
            "status": "completed",
            "mode": "partition_test_no_ingest",
            "data_type": "optical",
            "rows": 64,
            "ingest_enabled": False,
        }

    monkeypatch.setattr(partition_adapters, "run_optical_partition_test", fake_run_optical_partition_test)

    submit_resp = client.post("/v1/partition/optical/tasks/test", json=expected_payload)

    assert submit_resp.status_code == 202
    submitted = submit_resp.json()
    assert submitted["status"] in {"queued", "running", "completed"}
    assert submitted["data_type"] == "optical"
    assert submitted["operation"] == "test"

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
    assert task_body["result"]["mode"] == "partition_test_no_ingest"
    assert task_body["result"]["ingest_enabled"] is False
    assert task_body["result"]["rows"] == 64


def test_standard_loaded_schemas_seed_task_store():
    store = InMemoryPartitionJobStore()

    inserted = ensure_standard_partition_schemas(store)

    expected_ids = [schema["batch_id"] for schema in standard_partition_schemas()]
    assert inserted == expected_ids
    batches = store.list_batches(include_succeeded=True, limit=20)
    assert {batch["batch_id"] for batch in batches} == set(expected_ids)

    optical = store.get_batch("OPTICAL_BATCH_20260522_135546")
    carbon = store.get_batch("CARBON_BATCH_20201231_A")
    radar = store.get_batch("RADAR_BATCH_YANGZHOU_S1_2018_2020")
    product = store.get_batch("PRODUCT_BATCH_DIANZHONG_1980_2020")

    assert optical["data_type"] == "optical"
    assert optical["status"] == "pending"
    assert optical["normalized_payload"]["selected_assets"][0]["sensor"] == "optical_mosaic"
    assert optical["normalized_payload"]["selected_assets"][0]["product_family"] == "other"
    assert carbon["data_type"] == "carbon"
    assert carbon["normalized_payload"]["selected_observations"][0]["sensor"] == "oco2"
    assert carbon["normalized_payload"]["selected_observations"][0]["product_family"] == "xco2"
    assert radar["data_type"] == "radar"
    assert radar["normalized_payload"]["selected_assets"][0]["source_uri"].endswith("20180603_VH.dat")
    assert product["data_type"] == "product"
    assert product["normalized_payload"]["selected_assets"][0]["scene_id"] == "dianzhong_ecological_security_1980"

    assert len(store.list_assets("OPTICAL_BATCH_20260522_135546")) == 4
    assert len(store.list_assets("CARBON_BATCH_20201231_A")) == 4
    assert len(store.list_assets("RADAR_BATCH_YANGZHOU_S1_2018_2020")) == 48
    assert len(store.list_assets("PRODUCT_BATCH_DIANZHONG_1980_2020")) == 5


def test_standard_loaded_schemas_do_not_overwrite_existing_batch_state():
    store = InMemoryPartitionJobStore()
    schema = standard_partition_schemas()[0]
    store.upsert_schema(schema)
    task_id = "existing-task"
    store.create_attempt(task_id=task_id, batch_id=schema["batch_id"], operation="auto_run", payload={})
    store.fail_attempt(task_id, "existing failure", manual_required=True, error_type="validation")

    inserted = ensure_standard_partition_schemas(store)

    assert schema["batch_id"] not in inserted
    batch = store.get_batch(schema["batch_id"])
    assert batch["status"] == "manual_required"
    assert batch["attempt_count"] == 1
    assert batch["last_error"] == "existing failure"


def test_partition_job_store_does_not_seed_demo_schemas_by_default(monkeypatch):
    class DummyPostgresPartitionJobStore(InMemoryPartitionJobStore):
        def __init__(self, dsn):
            super().__init__()
            self.dsn = dsn

    monkeypatch.setattr(partition_job_store_module, "PostgresPartitionJobStore", DummyPostgresPartitionJobStore)
    set_partition_job_store(None)

    try:
        store = partition_job_store_module.get_partition_job_store()

        assert store.list_batches(include_succeeded=True) == []
        assert store.dsn == "postgresql://postgres:postgres@127.0.0.1:55432/cube"
    finally:
        set_partition_job_store(None)


def test_partition_job_store_seeds_demo_schemas_when_enabled(monkeypatch):
    class DummyPostgresPartitionJobStore(InMemoryPartitionJobStore):
        def __init__(self, dsn):
            super().__init__()
            self.dsn = dsn

    monkeypatch.setenv("CUBE_WEB_LOAD_DEMO_PARTITION_SCHEMAS", "1")
    monkeypatch.setattr(partition_job_store_module, "PostgresPartitionJobStore", DummyPostgresPartitionJobStore)
    set_partition_job_store(None)

    try:
        store = partition_job_store_module.get_partition_job_store()

        expected_ids = {schema["batch_id"] for schema in standard_partition_schemas()}
        batches = store.list_batches(include_succeeded=True, limit=20)
        assert {batch["batch_id"] for batch in batches} == expected_ids
    finally:
        set_partition_job_store(None)


def test_partition_schema_import_lists_batches_and_assets():
    payload = {
        "batch_id": "BATCH_IMPORT_1",
        "batch_name": "Imported optical batch",
        "data_type": "optical",
        "assets": [ard_raster_asset("s3://cube/cube/source/optocal/a.tif", "scene-a")],
    }

    import_resp = client.post("/v1/partition/schemas/import", json=payload)
    list_resp = client.get("/v1/partition/batches", params={"data_type": "optical"})
    assets_resp = client.get("/v1/partition/batches/BATCH_IMPORT_1/assets")

    assert import_resp.status_code == 200
    assert import_resp.json()["status"] == "pending"
    assert list_resp.status_code == 200
    assert [batch["batch_id"] for batch in list_resp.json()["batches"]] == ["BATCH_IMPORT_1"]
    assert assets_resp.status_code == 200
    assets = assets_resp.json()["assets"]
    assert len(assets) == 1
    assert assets[0]["scene_id"] == "scene-a"


def test_radar_partition_schema_import_lists_batches_and_assets():
    payload = {
        "batch_id": "RADAR_IMPORT_1",
        "batch_name": "Imported radar batch",
        "data_type": "radar",
        "assets": [ard_raster_asset("/data/radar/20180615_VV.dat", "S1_20180615", data_type="radar")],
    }

    import_resp = client.post("/v1/partition/schemas/import", json=payload)
    list_resp = client.get("/v1/partition/batches", params={"data_type": "radar"})
    assets_resp = client.get("/v1/partition/batches/RADAR_IMPORT_1/assets")

    assert import_resp.status_code == 200
    assert import_resp.json()["status"] == "pending"
    assert list_resp.status_code == 200
    assert [batch["batch_id"] for batch in list_resp.json()["batches"]] == ["RADAR_IMPORT_1"]
    assert assets_resp.status_code == 200
    assets = assets_resp.json()["assets"]
    assert len(assets) == 1
    assert assets[0]["scene_id"] == "S1_20180615"


def test_partition_job_store_reassigns_duplicate_asset_id_to_new_batch():
    store = InMemoryPartitionJobStore()
    first_batch = {
        "batch_id": "BATCH_IMPORT_DUPLICATE_1",
        "batch_name": "First imported optical batch",
        "data_type": "optical",
        "assets": [
            ard_raster_asset(
                "s3://cube/cube/source/optocal/duplicate-a.tif",
                "scene-a",
                asset_id="asset-duplicate",
            )
        ],
    }
    second_batch = {
        "batch_id": "BATCH_IMPORT_DUPLICATE_2",
        "batch_name": "Second imported optical batch",
        "data_type": "optical",
        "assets": [
            ard_raster_asset(
                "s3://cube/cube/source/optocal/duplicate-b.tif",
                "scene-b",
                asset_id="asset-duplicate",
            )
        ],
    }

    store.upsert_schema(first_batch)
    store.create_attempt(
        task_id="partition-duplicate-1",
        batch_id="BATCH_IMPORT_DUPLICATE_1",
        operation="auto_run",
        payload={"batch_id": "BATCH_IMPORT_DUPLICATE_1"},
    )
    store.start_attempt("partition-duplicate-1")
    store.fail_attempt("partition-duplicate-1", "quality warning requires manual review", manual_required=True)

    first_assets = store.list_assets("BATCH_IMPORT_DUPLICATE_1")
    assert len(first_assets) == 1
    assert first_assets[0]["asset_id"] == "asset-duplicate"
    assert first_assets[0]["status"] == "manual_required"
    assert first_assets[0]["attempt_count"] == 1

    store.upsert_schema(second_batch)

    assert store.list_assets("BATCH_IMPORT_DUPLICATE_1") == []
    second_assets = store.list_assets("BATCH_IMPORT_DUPLICATE_2")
    assert len(second_assets) == 1
    assert second_assets[0]["asset_id"] == "asset-duplicate"
    assert second_assets[0]["scene_id"] == "scene-b"
    assert second_assets[0]["source_uri"] == "s3://cube/cube/source/optocal/duplicate-b.tif"
    assert second_assets[0]["status"] == "pending"
    assert second_assets[0]["attempt_count"] == 0
    assert second_assets[0]["last_error"] is None
    assert second_assets[0]["partitioned_at"] is None


def test_partition_schema_import_rejects_incomplete_ard_asset():
    resp = client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_BAD_SCHEMA",
            "data_type": "optical",
            "assets": [
                {
                    "source_uri": "s3://cube/cube/source/optocal/a.tif",
                    "scene_id": "scene-a",
                    "acq_time": "2026-05-30T00:00:00Z",
                    "bands": ["b1"],
                }
            ],
        },
    )

    assert resp.status_code == 422
    assert "sensor is required" in resp.json()["detail"]


def test_radar_schema_first_batch_run_accepts_non_sentinel_filename(monkeypatch):
    source_uri = "s3://cube/cube/source/radar/schema_named_asset.tif"
    import_resp = client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "RADAR_SCHEMA_FIRST",
            "batch_name": "Schema first radar",
            "data_type": "radar",
            "assets": [
                ard_raster_asset(
                    source_uri,
                    "ARD_RADAR_SCENE_001",
                    data_type="radar",
                    band="vh",
                )
            ],
        },
    )
    captured = {}

    def fake_run_radar_partition_demo(payload=None):
        captured["payload"] = payload
        return {"status": "completed", "mode": "partition_demo", "data_type": "radar", "rows": 1}

    monkeypatch.setattr(partition_adapters, "run_radar_partition_demo", fake_run_radar_partition_demo)

    assert import_resp.status_code == 200
    submit_resp = client.post("/v1/partition/batches/RADAR_SCHEMA_FIRST/run", json={})
    assert submit_resp.status_code == 202
    task_id = submit_resp.json()["task_id"]
    for _ in range(20):
        task_resp = client.get(f"/v1/partition/tasks/{task_id}")
        if task_resp.json()["status"] == "completed":
            break
        time.sleep(0.01)

    selected = captured["payload"]["selected_assets"][0]
    assert selected["source_uri"] == source_uri
    assert selected["scene_id"] == "ARD_RADAR_SCENE_001"
    assert selected["polarization"] == "vh"


def test_partition_schema_import_infers_non_carbon_grid_level_from_resolution():
    optical_resp = client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_RESOLUTION_OPTICAL",
            "data_type": "optical",
            "assets": [ard_raster_asset("s3://cube/cube/source/optocal/10m.tif", "scene-10m", resolution="10m")],
        },
    )
    radar_resp = client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_RESOLUTION_RADAR",
            "data_type": "radar",
            "assets": [ard_raster_asset("/data/radar/20180615_VV.dat", "S1_20180615", data_type="radar", resolution=10)],
        },
    )
    product_resp = client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_RESOLUTION_PRODUCT",
            "data_type": "product",
            "assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/product/30m.tif",
                    "product-2020",
                    data_type="product",
                    resolution=30,
                )
            ],
        },
    )
    carbon_resp = client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_RESOLUTION_CARBON",
            "data_type": "carbon",
            "observations": [ard_carbon_observation("obs-a")],
        },
    )

    assert optical_resp.status_code == 200
    assert radar_resp.status_code == 200
    assert product_resp.status_code == 200
    assert carbon_resp.status_code == 200
    assert optical_resp.json()["normalized_payload"]["grid_level"] == 7
    assert radar_resp.json()["normalized_payload"]["grid_level"] == 7
    assert product_resp.json()["normalized_payload"]["grid_level"] == 7
    assert optical_resp.json()["normalized_payload"]["grid_level_mode"] == "auto"
    assert "grid_level" not in carbon_resp.json()["normalized_payload"]


def test_partition_schema_import_defaults_isea4h_grid_level_to_4():
    resp = client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_ISEA4H_DEFAULT_LEVEL",
            "data_type": "optical",
            "normalized_payload": {"grid_type": "isea4h"},
            "assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/optocal/10m.tif",
                    "scene-10m",
                    resolution="10m",
                )
            ],
        },
    )

    assert resp.status_code == 200
    assert resp.json()["normalized_payload"]["grid_level"] == 4
    assert resp.json()["normalized_payload"]["grid_level_mode"] == "auto"


def test_partition_batch_attempts_listed_for_batch_detail(monkeypatch):
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_ATTEMPTS",
            "batch_name": "Attempt detail",
            "data_type": "optical",
            "assets": [ard_raster_asset("s3://cube/cube/source/optocal/a.tif", "scene-a", asset_id="asset-a")],
        },
    )

    def fake_run_optical_partition_demo(payload=None):
        return {"status": "completed", "mode": "partition_demo", "data_type": "optical", "rows": 1}

    monkeypatch.setattr(partition_adapters, "run_optical_partition_demo", fake_run_optical_partition_demo)

    submit_resp = client.post("/v1/partition/batches/BATCH_ATTEMPTS/run", json={})
    assert submit_resp.status_code == 202
    task_id = submit_resp.json()["task_id"]
    for _ in range(20):
        task_resp = client.get(f"/v1/partition/tasks/{task_id}")
        if task_resp.json()["status"] == "completed":
            break
        time.sleep(0.01)

    attempts_resp = client.get("/v1/partition/batches/BATCH_ATTEMPTS/attempts")
    assert attempts_resp.status_code == 200
    attempts = attempts_resp.json()["attempts"]
    assert len(attempts) == 1
    assert attempts[0]["task_id"] == task_id
    assert attempts[0]["operation"] == "auto_run"


def test_partition_batch_run_marks_success_and_hides_from_pending_list(monkeypatch):
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_RUN_SUCCESS",
            "batch_name": "Run success",
            "data_type": "product",
            "assets": [ard_raster_asset("s3://cube/cube/source/product/a.tif", "product-a", data_type="product")],
            "normalized_payload": {"grid_type": "geohash", "grid_level": 5},
        },
    )

    def fake_run_product_partition_demo(payload=None):
        assert payload["batch_id"] == "BATCH_RUN_SUCCESS"
        return {"status": "completed", "mode": "partition_demo", "data_type": "product", "rows": 2}

    monkeypatch.setattr(partition_adapters, "run_product_partition_demo", fake_run_product_partition_demo)

    submit_resp = client.post("/v1/partition/batches/BATCH_RUN_SUCCESS/run", json={})
    assert submit_resp.status_code == 202
    task_id = submit_resp.json()["task_id"]

    for _ in range(20):
        task_resp = client.get(f"/v1/partition/tasks/{task_id}")
        if task_resp.json()["status"] == "completed":
            break
        time.sleep(0.01)

    batch_resp = client.get("/v1/partition/batches/BATCH_RUN_SUCCESS")
    pending_resp = client.get("/v1/partition/batches")
    history_resp = client.get("/v1/partition/batches", params={"include_succeeded": True})

    assert batch_resp.json()["status"] == "succeeded"
    assert batch_resp.json()["partitioned_at"]
    assert "BATCH_RUN_SUCCESS" not in [batch["batch_id"] for batch in pending_resp.json()["batches"]]
    assert "BATCH_RUN_SUCCESS" in [batch["batch_id"] for batch in history_resp.json()["batches"]]


def test_partition_batch_run_reuses_active_task_instead_of_duplicate_attempt(monkeypatch):
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_ACTIVE_IDEMPOTENT",
            "batch_name": "Active idempotent",
            "data_type": "optical",
            "assets": [ard_raster_asset("s3://cube/cube/source/optocal/active.tif", "active")],
        },
    )
    release_runner = threading.Event()
    calls = []

    def slow_optical_partition_demo(payload=None):
        calls.append(payload)
        release_runner.wait(timeout=3)
        return {"status": "completed", "mode": "partition_demo", "data_type": "optical", "rows": 1}

    monkeypatch.setattr(partition_adapters, "run_optical_partition_demo", slow_optical_partition_demo)

    try:
        first_resp = client.post("/v1/partition/batches/BATCH_ACTIVE_IDEMPOTENT/run", json={})
        second_resp = client.post("/v1/partition/batches/BATCH_ACTIVE_IDEMPOTENT/run", json={})

        assert first_resp.status_code == 202
        assert second_resp.status_code == 202
        assert second_resp.json()["task_id"] == first_resp.json()["task_id"]

        batch = client.get("/v1/partition/batches/BATCH_ACTIVE_IDEMPOTENT").json()
        attempts = client.get("/v1/partition/batches/BATCH_ACTIVE_IDEMPOTENT/attempts").json()["attempts"]
        assert batch["last_task_id"] == first_resp.json()["task_id"]
        assert batch["attempt_count"] == 1
        assert len(attempts) == 1
    finally:
        release_runner.set()

    task_id = first_resp.json()["task_id"]
    for _ in range(40):
        task = client.get(f"/v1/partition/tasks/{task_id}").json()
        if task["status"] == "completed":
            break
        time.sleep(0.02)

    batch = client.get("/v1/partition/batches/BATCH_ACTIVE_IDEMPOTENT").json()
    assert batch["status"] == "succeeded"
    assert len(calls) == 1


def test_partition_batch_run_persists_quality_pass(monkeypatch):
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_QUALITY_PASS",
            "batch_name": "Quality pass",
            "data_type": "optical",
            "assets": [ard_raster_asset("s3://cube/cube/source/optocal/q-pass.tif", "q-pass")],
        },
    )

    def fake_run_optical_partition_demo(payload=None):
        return {
            "status": "completed",
            "mode": "partition_demo",
            "data_type": "optical",
            "rows": 3,
            "quality_status": "PASS",
            "quality_report_id": "quality-pass-report",
            "quality_report": {
                "report_id": "quality-pass-report",
                "status": "PASS",
                "checks": [{"name": "index_rows", "status": "PASS"}],
            },
        }

    monkeypatch.setattr(partition_adapters, "run_optical_partition_demo", fake_run_optical_partition_demo)

    submit_resp = client.post("/v1/partition/batches/BATCH_QUALITY_PASS/run", json={})
    task_id = submit_resp.json()["task_id"]
    for _ in range(20):
        task_resp = client.get(f"/v1/partition/tasks/{task_id}")
        if task_resp.json()["status"] == "completed":
            break
        time.sleep(0.01)

    batch = client.get("/v1/partition/batches/BATCH_QUALITY_PASS").json()
    attempts = client.get("/v1/partition/batches/BATCH_QUALITY_PASS/attempts").json()["attempts"]
    assert batch["status"] == "succeeded"
    assert batch["quality_status"] == "PASS"
    assert batch["quality_report_id"] == "quality-pass-report"
    assert attempts[0]["runner_result"]["quality_status"] == "PASS"


def test_partition_batch_quality_fail_marks_manual_required(monkeypatch):
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_QUALITY_FAIL",
            "batch_name": "Quality fail",
            "data_type": "optical",
            "assets": [ard_raster_asset("s3://cube/cube/source/optocal/q-fail.tif", "q-fail")],
        },
    )

    def fake_run_optical_partition_demo(payload=None):
        return {
            "status": "completed",
            "mode": "partition_demo",
            "data_type": "optical",
            "rows": 3,
            "quality_status": "FAIL",
            "quality_report_id": "quality-fail-report",
            "quality_report": {
                "report_id": "quality-fail-report",
                "status": "FAIL",
                "checks": [{"name": "asset_readability", "status": "FAIL", "message": "missing COG asset"}],
            },
        }

    monkeypatch.setattr(partition_adapters, "run_optical_partition_demo", fake_run_optical_partition_demo)

    submit_resp = client.post("/v1/partition/batches/BATCH_QUALITY_FAIL/run", json={})
    task_id = submit_resp.json()["task_id"]
    for _ in range(20):
        task_resp = client.get(f"/v1/partition/tasks/{task_id}")
        if task_resp.json()["status"] == "completed":
            break
        time.sleep(0.01)

    batch = client.get("/v1/partition/batches/BATCH_QUALITY_FAIL").json()
    assets = client.get("/v1/partition/batches/BATCH_QUALITY_FAIL/assets").json()["assets"]
    assert batch["status"] == "manual_required"
    assert batch["quality_status"] == "FAIL"
    assert batch["quality_report_id"] == "quality-fail-report"
    assert "asset_readability" in batch["last_error"]
    assert assets[0]["status"] == "succeeded"


def test_partition_batch_quality_warn_enters_manual_queue_and_retries_warning_asset(monkeypatch):
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_QUALITY_WARN_RETRY",
            "batch_name": "Quality warn retry",
            "data_type": "optical",
            "assets": [
                ard_raster_asset("s3://cube/cube/source/optocal/a.tif", "warn-a", asset_id="asset-a"),
                ard_raster_asset("s3://cube/cube/source/optocal/b.tif", "warn-b", asset_id="asset-b"),
            ],
        },
    )
    calls = []

    def fake_run_optical_partition_demo(payload=None):
        calls.append([asset["asset_id"] for asset in payload["selected_assets"]])
        if len(calls) == 1:
            return {
                "status": "completed",
                "mode": "partition_demo",
                "data_type": "optical",
                "rows": 3,
                "quality_status": "WARN",
                "quality_report_id": "quality-warn-report",
                "quality_report": {
                    "report_id": "quality-warn-report",
                    "status": "WARN",
                    "checks": [
                        {
                            "name": "pixel_sample",
                            "status": "WARN",
                            "metrics": {"zero_assets": [{"path": "/tmp/demo/cog/b_cog.tif"}]},
                        }
                    ],
                },
            }
        assert calls[-1] == ["asset-b"]
        return {
            "status": "completed",
            "mode": "partition_demo",
            "data_type": "optical",
            "rows": 1,
            "quality_status": "PASS",
            "quality_report_id": "quality-warn-pass-report",
        }

    monkeypatch.setattr(partition_adapters, "run_optical_partition_demo", fake_run_optical_partition_demo)

    submit_resp = client.post("/v1/partition/batches/BATCH_QUALITY_WARN_RETRY/run", json={})
    task_id = submit_resp.json()["task_id"]
    for _ in range(30):
        if client.get(f"/v1/partition/tasks/{task_id}").json()["status"] == "completed":
            break
        time.sleep(0.01)

    warned_batch = client.get("/v1/partition/batches/BATCH_QUALITY_WARN_RETRY").json()
    assert warned_batch["status"] == "manual_required"
    assert warned_batch["quality_status"] == "WARN"
    assert warned_batch["last_error"] == "pixel_sample"

    retry_resp = client.post("/v1/partition/batches/BATCH_QUALITY_WARN_RETRY/retry", json={})
    retry_task_id = retry_resp.json()["task_id"]
    for _ in range(30):
        if client.get(f"/v1/partition/tasks/{retry_task_id}").json()["status"] == "completed":
            break
        time.sleep(0.01)

    passed_batch = client.get("/v1/partition/batches/BATCH_QUALITY_WARN_RETRY").json()
    attempts = client.get("/v1/partition/batches/BATCH_QUALITY_WARN_RETRY/attempts").json()["attempts"]
    assert calls == [["asset-a", "asset-b"], ["asset-b"]]
    assert passed_batch["status"] == "succeeded"
    assert passed_batch["quality_status"] == "PASS"
    assert attempts[0]["operation"] == "manual_retry"
    assert attempts[0]["asset_ids"] == ["asset-b"]
    assert attempts[0]["retry_strategy"] == "quality_warning_assets"


def test_partition_batch_quality_warn_retry_matches_hashed_cog_warning_assets(monkeypatch):
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_QUALITY_WARN_RETRY_HASHED",
            "batch_name": "Quality warn retry hashed",
            "data_type": "optical",
            "assets": [
                ard_raster_asset("s3://cube/cube/source/optocal/hashed-a.tif", "warn-hashed-a", asset_id="asset-a"),
                ard_raster_asset("s3://cube/cube/source/optocal/hashed-b.tif", "warn-hashed-b", asset_id="asset-b"),
            ],
        },
    )
    calls = []

    def fake_run_optical_partition_demo(payload=None):
        calls.append([asset["asset_id"] for asset in payload["selected_assets"]])
        if len(calls) == 1:
            return {
                "status": "completed",
                "mode": "partition_demo",
                "data_type": "optical",
                "rows": 3,
                "quality_status": "WARN",
                "quality_report_id": "quality-warn-hashed-report",
                "quality_report": {
                    "report_id": "quality-warn-hashed-report",
                    "status": "WARN",
                    "checks": [
                        {
                            "name": "pixel_sample",
                            "status": "WARN",
                            "metrics": {
                                "zero_assets": [
                                    {"path": "s3://cube/demo/hashed-b_a1b2c3d4e5_cog.tif"},
                                ]
                            },
                        }
                    ],
                },
            }
        assert calls[-1] == ["asset-b"]
        return {
            "status": "completed",
            "mode": "partition_demo",
            "data_type": "optical",
            "rows": 1,
            "quality_status": "PASS",
            "quality_report_id": "quality-warn-hashed-pass-report",
        }

    monkeypatch.setattr(partition_adapters, "run_optical_partition_demo", fake_run_optical_partition_demo)

    submit_resp = client.post("/v1/partition/batches/BATCH_QUALITY_WARN_RETRY_HASHED/run", json={})
    task_id = submit_resp.json()["task_id"]
    for _ in range(30):
        if client.get(f"/v1/partition/tasks/{task_id}").json()["status"] == "completed":
            break
        time.sleep(0.01)

    warned_batch = client.get("/v1/partition/batches/BATCH_QUALITY_WARN_RETRY_HASHED").json()
    assert warned_batch["status"] == "manual_required"
    assert warned_batch["quality_status"] == "WARN"

    retry_resp = client.post("/v1/partition/batches/BATCH_QUALITY_WARN_RETRY_HASHED/retry", json={})
    retry_task_id = retry_resp.json()["task_id"]
    for _ in range(30):
        if client.get(f"/v1/partition/tasks/{retry_task_id}").json()["status"] == "completed":
            break
        time.sleep(0.01)

    passed_batch = client.get("/v1/partition/batches/BATCH_QUALITY_WARN_RETRY_HASHED").json()
    attempts = client.get("/v1/partition/batches/BATCH_QUALITY_WARN_RETRY_HASHED/attempts").json()["attempts"]
    assert calls == [["asset-a", "asset-b"], ["asset-b"]]
    assert passed_batch["status"] == "succeeded"
    assert passed_batch["quality_status"] == "PASS"
    assert attempts[0]["operation"] == "manual_retry"
    assert attempts[0]["asset_ids"] == ["asset-b"]
    assert attempts[0]["retry_strategy"] == "quality_warning_assets"


def test_partition_batch_retry_clears_stale_quality_result_then_persists_pass(monkeypatch):
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_QUALITY_RETRY_PASS",
            "batch_name": "Quality retry pass",
            "data_type": "optical",
            "assets": [ard_raster_asset("s3://cube/cube/source/optocal/q-retry.tif", "q-retry")],
        },
    )
    release_retry = threading.Event()
    calls = []

    def fake_run_optical_partition_demo(payload=None):
        calls.append(payload)
        if len(calls) == 1:
            return {
                "status": "completed",
                "mode": "partition_demo",
                "data_type": "optical",
                "rows": 3,
                "quality_status": "FAIL",
                "quality_report_id": "quality-retry-fail-report",
                "quality_failure_reason": "cloud mask failed",
            }
        release_retry.wait(timeout=3)
        return {
            "status": "completed",
            "mode": "partition_demo",
            "data_type": "optical",
            "rows": 3,
            "quality_status": "PASS",
            "quality_report_id": "quality-retry-pass-report",
            "quality_report": {"report_id": "quality-retry-pass-report", "status": "PASS"},
        }

    monkeypatch.setattr(partition_adapters, "run_optical_partition_demo", fake_run_optical_partition_demo)

    submit_resp = client.post("/v1/partition/batches/BATCH_QUALITY_RETRY_PASS/run", json={})
    task_id = submit_resp.json()["task_id"]
    for _ in range(20):
        if client.get(f"/v1/partition/tasks/{task_id}").json()["status"] == "completed":
            break
        time.sleep(0.01)

    failed_batch = client.get("/v1/partition/batches/BATCH_QUALITY_RETRY_PASS").json()
    assert failed_batch["status"] == "manual_required"
    assert failed_batch["quality_status"] == "FAIL"
    assert failed_batch["quality_report_id"] == "quality-retry-fail-report"

    retry_resp = client.post("/v1/partition/batches/BATCH_QUALITY_RETRY_PASS/retry", json={})
    retry_task_id = retry_resp.json()["task_id"]
    for _ in range(50):
        retry_batch = client.get("/v1/partition/batches/BATCH_QUALITY_RETRY_PASS").json()
        if len(calls) == 2 and retry_batch["status"] == "running":
            break
        time.sleep(0.02)

    retry_batch = client.get("/v1/partition/batches/BATCH_QUALITY_RETRY_PASS").json()
    assert retry_batch["status"] == "running"
    assert retry_batch["quality_status"] is None
    assert retry_batch["quality_report_id"] is None
    assert retry_batch["quality_failure_reason"] is None

    release_retry.set()
    for _ in range(40):
        task = client.get(f"/v1/partition/tasks/{retry_task_id}").json()
        if task["status"] == "completed":
            break
        time.sleep(0.02)

    passed_batch = client.get("/v1/partition/batches/BATCH_QUALITY_RETRY_PASS").json()
    attempts = client.get("/v1/partition/batches/BATCH_QUALITY_RETRY_PASS/attempts").json()["attempts"]
    assert passed_batch["status"] == "succeeded"
    assert passed_batch["last_error"] is None
    assert passed_batch["quality_status"] == "PASS"
    assert passed_batch["quality_report_id"] == "quality-retry-pass-report"
    assert [attempt["operation"] for attempt in attempts] == ["manual_retry", "auto_run"]
    assert attempts[0]["source_task_id"] == task_id
    assert attempts[0]["retry_strategy"] == "full_batch"
    assert attempts[0]["failure_reason"] == "cloud mask failed"


def test_partition_batch_auto_retries_once_then_requires_manual(monkeypatch):
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_FAIL_RETRY",
            "batch_name": "Fail retry",
            "data_type": "optical",
            "max_auto_retries": 1,
            "assets": [ard_raster_asset("s3://cube/cube/source/optocal/fail.tif", "fail")],
        },
    )

    def fail_optical(_payload=None):
        raise RuntimeError("temporary network timeout")

    monkeypatch.setattr(partition_adapters, "run_optical_partition_demo", fail_optical)

    submit_resp = client.post("/v1/partition/batches/BATCH_FAIL_RETRY/run", json={})
    assert submit_resp.status_code == 202
    first_task_id = submit_resp.json()["task_id"]

    for _ in range(50):
        batch = client.get("/v1/partition/batches/BATCH_FAIL_RETRY").json()
        if batch["status"] == "manual_required":
            break
        time.sleep(0.02)

    batch = client.get("/v1/partition/batches/BATCH_FAIL_RETRY").json()
    assert batch["status"] == "manual_required"
    assert batch["attempt_count"] == 2
    assert batch["last_task_id"] != first_task_id
    assert "temporary network timeout" in batch["last_error"]
    attempts = client.get("/v1/partition/batches/BATCH_FAIL_RETRY/attempts").json()["attempts"]
    assert attempts[0]["error_type"] == "transient"
    assert attempts[0]["operation"] == "auto_retry"
    assert attempts[0]["source_task_id"] == first_task_id
    assert attempts[0]["retry_strategy"] == "full_batch"
    assert "temporary network timeout" in attempts[0]["failure_reason"]


def test_partition_manual_retry_gets_fresh_auto_retry_budget(monkeypatch):
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_MANUAL_RETRY_BUDGET",
            "batch_name": "Manual retry budget",
            "data_type": "optical",
            "max_auto_retries": 1,
            "assets": [ard_raster_asset("s3://cube/cube/source/optocal/manual-budget.tif", "manual-budget")],
        },
    )
    calls = []

    def fail_optical(_payload=None):
        calls.append(_payload)
        raise RuntimeError("temporary network timeout")

    monkeypatch.setattr(partition_adapters, "run_optical_partition_demo", fail_optical)

    first_resp = client.post("/v1/partition/batches/BATCH_MANUAL_RETRY_BUDGET/run", json={})
    assert first_resp.status_code == 202
    for _ in range(50):
        batch = client.get("/v1/partition/batches/BATCH_MANUAL_RETRY_BUDGET").json()
        if batch["status"] == "manual_required" and batch["attempt_count"] == 2:
            break
        time.sleep(0.02)

    retry_resp = client.post("/v1/partition/batches/BATCH_MANUAL_RETRY_BUDGET/retry", json={})
    assert retry_resp.status_code == 202
    for _ in range(50):
        batch = client.get("/v1/partition/batches/BATCH_MANUAL_RETRY_BUDGET").json()
        if batch["status"] == "manual_required" and batch["attempt_count"] == 4:
            break
        time.sleep(0.02)

    batch = client.get("/v1/partition/batches/BATCH_MANUAL_RETRY_BUDGET").json()
    attempts = client.get("/v1/partition/batches/BATCH_MANUAL_RETRY_BUDGET/attempts").json()["attempts"]
    assert batch["status"] == "manual_required"
    assert batch["attempt_count"] == 4
    assert len(calls) == 4
    assert [attempt["operation"] for attempt in attempts] == ["auto_retry", "manual_retry", "auto_retry", "auto_run"]
    assert attempts[0]["source_task_id"] == attempts[1]["task_id"]
    assert attempts[1]["source_task_id"] == attempts[2]["task_id"]


def test_partition_batch_source_missing_requires_manual_without_auto_retry(monkeypatch):
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_SOURCE_MISSING",
            "batch_name": "Source missing",
            "data_type": "optical",
            "max_auto_retries": 1,
            "assets": [ard_raster_asset("s3://cube/cube/source/optocal/missing.tif", "missing")],
        },
    )

    def fail_optical(_payload=None):
        raise RuntimeError("source missing")

    monkeypatch.setattr(partition_adapters, "run_optical_partition_demo", fail_optical)

    submit_resp = client.post("/v1/partition/batches/BATCH_SOURCE_MISSING/run", json={})
    assert submit_resp.status_code == 202
    task_id = submit_resp.json()["task_id"]

    for _ in range(50):
        batch = client.get("/v1/partition/batches/BATCH_SOURCE_MISSING").json()
        if batch["status"] == "manual_required":
            break
        time.sleep(0.02)

    batch = client.get("/v1/partition/batches/BATCH_SOURCE_MISSING").json()
    attempts = client.get("/v1/partition/batches/BATCH_SOURCE_MISSING/attempts").json()["attempts"]
    assert batch["status"] == "manual_required"
    assert batch["attempt_count"] == 1
    assert batch["last_task_id"] == task_id
    assert attempts[0]["error_type"] == "source_missing"


def test_partition_asset_retry_submits_only_selected_asset(monkeypatch):
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_ASSET_RETRY",
            "batch_name": "Asset retry",
            "data_type": "optical",
            "max_auto_retries": 0,
            "assets": [
                ard_raster_asset("s3://cube/cube/source/optocal/a.tif", "a", asset_id="asset-a"),
                ard_raster_asset("s3://cube/cube/source/optocal/b.tif", "b", asset_id="asset-b"),
            ],
        },
    )
    calls = []

    def fake_run_optical_partition_demo(payload=None):
        calls.append([asset["asset_id"] for asset in payload["selected_assets"]])
        if len(calls) == 1:
            return {
                "status": "completed",
                "mode": "partition_demo",
                "data_type": "optical",
                "rows": 1,
                "asset_results": [
                    {"asset_id": "asset-a", "status": "succeeded"},
                    {"asset_id": "asset-b", "status": "failed", "error_type": "transient", "last_error": "temporary network timeout"},
                ],
            }
        return {"status": "completed", "mode": "partition_demo", "data_type": "optical", "rows": 1}

    monkeypatch.setattr(partition_adapters, "run_optical_partition_demo", fake_run_optical_partition_demo)

    run_resp = client.post("/v1/partition/batches/BATCH_ASSET_RETRY/run", json={})
    run_task_id = run_resp.json()["task_id"]
    for _ in range(20):
        task_done = client.get(f"/v1/partition/tasks/{run_task_id}").json()["status"] == "completed"
        batch = client.get("/v1/partition/batches/BATCH_ASSET_RETRY").json()
        if task_done and batch["status"] == "manual_required":
            break
        time.sleep(0.01)

    submit_resp = client.post("/v1/partition/assets/retry", json={"asset_ids": ["asset-b"]})
    assert submit_resp.status_code == 202
    task_id = submit_resp.json()["task_id"]
    for _ in range(20):
        if client.get(f"/v1/partition/tasks/{task_id}").json()["status"] == "completed":
            break
        time.sleep(0.01)

    assert calls == [["asset-a", "asset-b"], ["asset-b"]]
    batch = client.get("/v1/partition/batches/BATCH_ASSET_RETRY").json()
    assets = client.get("/v1/partition/batches/BATCH_ASSET_RETRY/assets").json()["assets"]
    statuses = {asset["asset_id"]: asset["status"] for asset in assets}
    attempt_counts = {asset["asset_id"]: asset["attempt_count"] for asset in assets}
    assert batch["status"] == "succeeded"
    assert statuses == {"asset-a": "succeeded", "asset-b": "succeeded"}
    assert attempt_counts == {"asset-a": 1, "asset-b": 2}


def test_partition_asset_retry_rejects_non_retryable_assets():
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_ASSET_RETRY_REJECT",
            "batch_name": "Asset retry reject",
            "data_type": "optical",
            "assets": [ard_raster_asset("s3://cube/cube/source/optocal/pending.tif", "pending", asset_id="asset-pending")],
        },
    )

    resp = client.post("/v1/partition/assets/retry", json={"asset_ids": ["asset-pending"]})

    assert resp.status_code == 422
    assert "not retryable" in resp.json()["detail"]


def test_partition_asset_results_auto_retry_then_manual_asset_retry(monkeypatch):
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_ASSET_LEVEL_RETRY",
            "batch_name": "Asset level retry",
            "data_type": "optical",
            "max_auto_retries": 1,
            "assets": [
                ard_raster_asset("s3://cube/cube/source/optocal/a.tif", "a", asset_id="asset-a"),
                ard_raster_asset("s3://cube/cube/source/optocal/b.tif", "b", asset_id="asset-b"),
            ],
        },
    )
    calls = []

    def fake_run_optical_partition_demo(payload=None):
        selected_asset_ids = [asset["asset_id"] for asset in payload["selected_assets"]]
        calls.append(selected_asset_ids)
        if len(calls) == 1:
            return {
                "status": "completed",
                "mode": "partition_demo",
                "data_type": "optical",
                "rows": 1,
                "run_dir": "/tmp/asset-level-run-1",
                "asset_results": [
                    {"asset_id": "asset-a", "status": "succeeded"},
                    {
                        "asset_id": "asset-b",
                        "status": "failed",
                        "error_type": "transient",
                        "last_error": "temporary network timeout",
                    },
                ],
            }
        if len(calls) == 2:
            assert selected_asset_ids == ["asset-b"]
            return {
                "status": "completed",
                "mode": "partition_demo",
                "data_type": "optical",
                "rows": 0,
                "run_dir": "/tmp/asset-level-run-2",
                "asset_results": [
                    {
                        "asset_id": "asset-b",
                        "status": "failed",
                        "error_type": "transient",
                        "last_error": "temporary network timeout",
                    }
                ],
            }
        assert selected_asset_ids == ["asset-b"]
        return {
            "status": "completed",
            "mode": "partition_demo",
            "data_type": "optical",
            "rows": 1,
            "run_dir": "/tmp/asset-level-run-3",
            "asset_results": [
                {"asset_id": "asset-b", "status": "succeeded"},
                {
                    "asset_id": "asset-a",
                    "status": "failed",
                    "error_type": "transient",
                    "last_error": "ignored extra asset result",
                },
            ],
        }

    monkeypatch.setattr(partition_adapters, "run_optical_partition_demo", fake_run_optical_partition_demo)

    submit_resp = client.post("/v1/partition/batches/BATCH_ASSET_LEVEL_RETRY/run", json={})
    assert submit_resp.status_code == 202
    for _ in range(80):
        batch = client.get("/v1/partition/batches/BATCH_ASSET_LEVEL_RETRY").json()
        if batch["status"] == "manual_required":
            break
        time.sleep(0.02)

    batch = client.get("/v1/partition/batches/BATCH_ASSET_LEVEL_RETRY").json()
    assets = client.get("/v1/partition/batches/BATCH_ASSET_LEVEL_RETRY/assets").json()["assets"]
    statuses = {asset["asset_id"]: asset["status"] for asset in assets}
    attempt_counts = {asset["asset_id"]: asset["attempt_count"] for asset in assets}

    assert calls[:2] == [["asset-a", "asset-b"], ["asset-b"]]
    assert batch["status"] == "manual_required"
    assert batch["attempt_count"] == 2
    assert statuses == {"asset-a": "succeeded", "asset-b": "manual_required"}
    assert attempt_counts == {"asset-a": 1, "asset-b": 2}
    assert "temporary network timeout" in batch["last_error"]

    retry_resp = client.post("/v1/partition/assets/retry", json={"asset_ids": ["asset-b"]})
    assert retry_resp.status_code == 202
    retry_task_id = retry_resp.json()["task_id"]
    for _ in range(40):
        task = client.get(f"/v1/partition/tasks/{retry_task_id}").json()
        if task["status"] == "completed":
            break
        time.sleep(0.02)

    batch = client.get("/v1/partition/batches/BATCH_ASSET_LEVEL_RETRY").json()
    assets = client.get("/v1/partition/batches/BATCH_ASSET_LEVEL_RETRY/assets").json()["assets"]
    statuses = {asset["asset_id"]: asset["status"] for asset in assets}
    attempt_counts = {asset["asset_id"]: asset["attempt_count"] for asset in assets}
    attempts = client.get("/v1/partition/batches/BATCH_ASSET_LEVEL_RETRY/attempts").json()["attempts"]

    assert calls == [["asset-a", "asset-b"], ["asset-b"], ["asset-b"]]
    assert batch["status"] == "succeeded"
    assert batch["attempt_count"] == 3
    assert statuses == {"asset-a": "succeeded", "asset-b": "succeeded"}
    assert attempt_counts == {"asset-a": 1, "asset-b": 3}
    assert [attempt["operation"] for attempt in attempts] == ["manual_asset_retry", "auto_retry", "auto_run"]
    assert attempts[1]["source_task_id"] == attempts[2]["task_id"]
    assert attempts[1]["retry_strategy"] == "retryable_assets"
    assert "asset-b: temporary network timeout" in attempts[1]["failure_reason"]
    assert attempts[0]["source_task_id"] == attempts[1]["task_id"]
    assert attempts[0]["retry_strategy"] == "selected_assets"
    assert "asset-b: temporary network timeout" in attempts[0]["failure_reason"]


def test_partition_task_cancel_marks_attempt_cancel_requested(monkeypatch):
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_CANCEL",
            "batch_name": "Cancel",
            "data_type": "product",
            "assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/product/a.tif",
                    "cancel-a-scene",
                    data_type="product",
                    asset_id="cancel-a",
                )
            ],
        },
    )

    def slow_product(_payload=None):
        time.sleep(0.2)
        return {"status": "completed", "mode": "partition_demo", "data_type": "product", "rows": 1}

    monkeypatch.setattr(partition_adapters, "run_product_partition_demo", slow_product)

    submit_resp = client.post("/v1/partition/batches/BATCH_CANCEL/run", json={})
    task_id = submit_resp.json()["task_id"]
    cancel_resp = client.post(f"/v1/partition/tasks/{task_id}/cancel")

    assert cancel_resp.status_code == 200
    assert cancel_resp.json()["status"] in {"cancel_requested", "cancelled"}


def test_partition_task_cancel_keeps_running_task_from_completing(monkeypatch):
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_CANCEL_RUNNING",
            "batch_name": "Cancel running",
            "data_type": "product",
            "assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/product/b.tif",
                    "cancel-b-scene",
                    data_type="product",
                    asset_id="cancel-b",
                )
            ],
        },
    )

    def slow_product(payload=None):
        time.sleep(0.1)
        return {"status": "completed", "mode": "partition_demo", "data_type": "product", "rows": 1}

    monkeypatch.setattr(partition_adapters, "run_product_partition_demo", slow_product)

    submit_resp = client.post("/v1/partition/batches/BATCH_CANCEL_RUNNING/run", json={})
    task_id = submit_resp.json()["task_id"]
    client.post(f"/v1/partition/tasks/{task_id}/cancel")

    for _ in range(50):
        task = client.get(f"/v1/partition/tasks/{task_id}").json()
        if task["status"] in {"cancelled", "cancel_requested"}:
            break
        time.sleep(0.01)

    task = client.get(f"/v1/partition/tasks/{task_id}").json()
    assert task["status"] in {"cancelled", "cancel_requested"}


def test_partition_cancelled_runner_marks_batch_cancelled(monkeypatch):
    from cube_split.jobs.cancellation import PartitionCancelledError

    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_CANCEL_EXCEPTION",
            "batch_name": "Cancel exception",
            "data_type": "optical",
            "assets": [ard_raster_asset("s3://cube/cube/source/optocal/c.tif", "cancel-ex-scene", asset_id="cancel-ex")],
        },
    )

    def cancelled_optical(payload=None):
        raise PartitionCancelledError("Partition task cancelled")

    monkeypatch.setattr(partition_adapters, "run_optical_partition_demo", cancelled_optical)

    submit_resp = client.post("/v1/partition/batches/BATCH_CANCEL_EXCEPTION/run", json={})
    task_id = submit_resp.json()["task_id"]
    for _ in range(20):
        task = client.get(f"/v1/partition/tasks/{task_id}").json()
        if task["status"] == "cancelled":
            break
        time.sleep(0.01)

    task = client.get(f"/v1/partition/tasks/{task_id}").json()
    batch = client.get("/v1/partition/batches/BATCH_CANCEL_EXCEPTION").json()
    assert task["status"] == "cancelled"
    assert batch["status"] == "cancelled"


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
            "quality_report_id": "optical-test-report",
            "quality_report": {"report_id": "optical-test-report", "status": "PASS", "summary": {"index_rows": 147}},
        }

    monkeypatch.setattr(partition_adapters, "run_optical_partition_test", fake_run_optical_partition_test)

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


def test_optical_ingest_preview_does_not_write(monkeypatch, quality_store):
    run_dir = Path("/tmp/cube_web_partition_demo/test_app_ingest_preview")
    run_dir.mkdir(parents=True, exist_ok=True)
    asset_path = run_dir / "asset_cog.tif"
    row = {
        "scene_id": "scene-1",
        "band": "sr_band2",
        "asset_path": str(asset_path),
        "acq_time": "2020-07-01T00:00:00Z",
        "grid_type": "geohash",
        "grid_level": 5,
        "space_code": "wx4g0",
        "st_code": "gh:5:wx4g0:20200701:v1",
        "time_bucket": "20200701",
        "cell_min_lon": 116.0,
        "cell_min_lat": 39.9,
        "cell_max_lon": 116.1,
        "cell_max_lat": 40.0,
        "window_col_off": 1,
        "window_row_off": 2,
        "window_width": 4,
        "window_height": 5,
    }
    (run_dir / "index_rows.jsonl").write_text(__import__("json").dumps(row) + "\n", encoding="utf-8")
    quality_store.upsert_report(
        "optical",
        str(run_dir),
        {
            "report_id": "optical-ingest-preview",
            "status": "PASS",
            "target_crs": "EPSG:4326",
            "summary": {"index_rows": 1, "failed_checks": 0, "warning_checks": 0},
            "checks": [],
            "assets": [],
        },
    )

    def fail_if_called(args):
        raise AssertionError("preview must not call run_ingest")

    monkeypatch.setattr("cube_web.services.ingest_service.ray_ingest_job.run_ingest", fail_if_called)
    monkeypatch.setattr(
        "cube_web.services.ingest_service._existing_conflicts",
        lambda raw_records, cube_records: {"raw_asset_rows": 0, "cube_fact_rows": 0},
    )

    resp = client.post("/v1/ingest/optical/preview", json={"report_id": "optical-ingest-preview"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["mode"] == "pre_ingest_preview"
    assert body["would_write"] is False
    assert body["input_rows"] == 1
    assert body["raw_asset_rows"] == 1
    assert body["cube_fact_rows"] == 1
    assert body["cube_version"].startswith("demo-")


def test_optical_ingest_confirm_uses_demo_versions_and_minio_storage(monkeypatch, quality_store):
    run_dir = "/tmp/cube_web_partition_demo/test_app_ingest_confirm"
    Path(run_dir).mkdir(parents=True, exist_ok=True)
    (Path(run_dir) / "index_rows.jsonl").write_text("", encoding="utf-8")
    quality_store.upsert_report(
        "optical",
        run_dir,
        {
            "report_id": "optical-ingest-confirm",
            "status": "PASS",
            "target_crs": "EPSG:4326",
            "summary": {"index_rows": 1, "failed_checks": 0, "warning_checks": 0},
            "checks": [],
            "assets": [],
        },
    )
    captured = {}

    def fake_run_ingest(args):
        captured.update(vars(args))
        return {
            "run_dir": args.run_dir,
            "input_rows": 1,
            "materialized_cog_assets": 1,
            "raw_asset_rows": 1,
            "cube_fact_rows": 1,
            "metadata_backend": args.metadata_backend,
            "asset_storage_backend": args.asset_storage_backend,
        }

    monkeypatch.setattr("cube_web.services.ingest_service.ray_ingest_job.run_ingest", fake_run_ingest)

    resp = client.post("/v1/ingest/optical/confirm", json={"report_id": "optical-ingest-confirm"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["mode"] == "confirmed_ingest"
    assert body["status"] == "succeeded"
    assert body["cube_version"].startswith("demo-")
    assert captured["metadata_backend"] == "postgres"
    assert captured["asset_storage_backend"] == "minio"
    assert captured["minio_endpoint"] == "10.136.1.14:9000"
    assert captured["minio_bucket"] == "cube"
    assert captured["cog_materialize_mode"] == "symlink"
    assert captured["asset_version"].startswith("demo-")


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

    monkeypatch.setattr(partition_adapters, "run_optical_partition_from_payload", fake_run_optical_partition_from_payload)

    resp = client.post("/v1/partition/optical/retry", json=retry_request)

    assert resp.status_code == 200
    body = resp.json()
    assert body["mode"] == "partition_retry"
    assert body["retry"]["strategy"] == "warning_assets"
    assert body["retry"]["warning_check_names"] == ["pixel_sample"]
    assert body["retry"]["retried_asset_count"] == 1


def test_optical_partition_retry_endpoint_matches_hashed_cog_warning_assets(monkeypatch):
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
                        "metrics": {"zero_assets": [{"path": "s3://cube/demo/asset_b_a1b2c3d4e5_cog.tif"}]},
                    }
                ],
            }
        },
    }

    def fake_run_optical_partition_from_payload(payload=None, mode="partition_demo"):
        assert mode == "partition_retry"
        assert [asset["source_uri"] for asset in payload["selected_assets"]] == ["scene_b/asset_b.tif"]
        return {"status": "completed", "mode": mode, "data_type": "optical", "rows": 3}

    monkeypatch.setattr(partition_adapters, "run_optical_partition_from_payload", fake_run_optical_partition_from_payload)

    resp = client.post("/v1/partition/optical/retry", json=retry_request)

    assert resp.status_code == 200
    body = resp.json()
    assert body["retry"]["strategy"] == "warning_assets"
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

    monkeypatch.setattr(partition_adapters, "run_optical_partition_from_payload", fake_run_optical_partition_from_payload)

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

    monkeypatch.setattr(partition_adapters, "run_carbon_partition_retry", fake_run_carbon_partition_retry)

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

    monkeypatch.setattr(partition_adapters, "run_product_partition_demo", fake_run_product_partition_demo)

    resp = client.post("/v1/partition/product/demo", json={"grid_type": "geohash", "grid_level": 5})

    assert resp.status_code == 200
    body = resp.json()
    assert body["data_type"] == "product"
    assert body["mode"] == "partition_demo"
    assert body["rows"] == 20


def test_product_partition_test_endpoint(monkeypatch):
    expected_payload = {
        "grid_type": "geohash",
        "grid_level": 5,
        "selected_assets": [{"source_uri": "product_1980.tif", "product_year": 1980}],
    }

    def fake_run_product_partition_test(payload=None):
        assert payload == expected_payload
        return {
            "status": "completed",
            "mode": "partition_test_no_ingest",
            "data_type": "product",
            "rows": 8,
            "ingest_enabled": False,
        }

    monkeypatch.setattr(partition_adapters, "run_product_partition_test", fake_run_product_partition_test)

    resp = client.post("/v1/partition/product/test", json=expected_payload)

    assert resp.status_code == 200
    body = resp.json()
    assert body["data_type"] == "product"
    assert body["mode"] == "partition_test_no_ingest"
    assert body["ingest_enabled"] is False
    assert body["rows"] == 8


def test_product_partition_test_runner_uses_selected_assets(monkeypatch, tmp_path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    selected = source_dir / "product_1980.tif"
    ignored = source_dir / "product_1990.tif"
    selected.write_text("selected", encoding="utf-8")
    ignored.write_text("ignored", encoding="utf-8")
    captured = {}

    def fake_demo_run_dir(name):
        run_dir = tmp_path / "run-root"
        run_dir.mkdir()
        return run_dir

    def fake_run_product_partition(args):
        captured["input_dir"] = Path(args.input_dir)
        captured["manifest_path"] = Path(args.manifest_path)
        captured["files"] = sorted(path.name for path in Path(args.input_dir).iterdir())
        captured["partition_backend"] = args.partition_backend
        captured["ray_address"] = args.ray_address
        captured["metadata_backend"] = args.metadata_backend
        captured["asset_storage_backend"] = args.asset_storage_backend
        captured["minio_endpoint"] = args.minio_endpoint
        captured["minio_bucket"] = args.minio_bucket
        captured["ingest_enabled"] = args.ingest_enabled
        run_dir = tmp_path / "run-root" / "output" / "run"
        run_dir.mkdir(parents=True)
        rows_path = run_dir / "index_rows.jsonl"
        rows_path.write_text("", encoding="utf-8")
        return {
            "status": "completed",
            "data_type": "product",
            "run_dir": str(run_dir),
            "rows_path": str(rows_path),
            "rows": 0,
            "grid_type": args.grid_type,
            "grid_level": args.grid_level,
            "partition_backend_used": "thread",
        }

    monkeypatch.setattr(partition_runners, "_demo_run_dir", fake_demo_run_dir)
    monkeypatch.setattr("cube_split.jobs.product_partition_job.run_product_partition", fake_run_product_partition)
    monkeypatch.setattr("cube_web.services.quality_checks.run_product_quality_check", None)

    result = partition_runners._run_product_partition_test(
        {
            "input_dir": str(source_dir),
            "selected_assets": [
                {
                    **ard_raster_asset("product_1980.tif", "product-1980", data_type="product", resolution=30),
                    "source_uri": "product_1980.tif",
                    "product_name": "测试产品",
                    "product_year": 1980,
                    "acq_time": "1980-01-01T00:00:00Z",
                    "bbox": [100.0, 23.0, 105.0, 27.0],
                    "corners": [[100.0, 27.0], [105.0, 27.0], [105.0, 23.0], [100.0, 23.0]],
                }
            ],
        }
    )

    assert result["mode"] == "partition_test_no_ingest"
    assert result["ingest_enabled"] is False
    assert captured["partition_backend"] == "ray"
    assert captured["ray_address"] == "ray://10.136.1.13:10001"
    assert captured["metadata_backend"] == "postgres"
    assert captured["asset_storage_backend"] == "minio"
    assert captured["minio_endpoint"] == "10.136.1.14:9000"
    assert captured["minio_bucket"] == "cube"
    assert captured["ingest_enabled"] is False
    assert result["selected_asset_count"] == 1
    assert captured["files"] == ["product_1980.tif"]
    assert captured["input_dir"].name == "input"
    manifest = json.loads(captured["manifest_path"].read_text(encoding="utf-8"))
    assert manifest["data_type"] == "product"
    assert manifest["assets"][0]["source_uri"] == str(captured["input_dir"] / "product_1980.tif")
    assert manifest["assets"][0]["product_year"] == 1980
    assert manifest["assets"][0]["bbox"] == [100.0, 23.0, 105.0, 27.0]
    assert manifest["assets"][0]["corners"] == [[100.0, 27.0], [105.0, 27.0], [105.0, 23.0], [100.0, 23.0]]


def test_product_partition_test_runner_dispatches_isea4h_to_entity_partition(monkeypatch, tmp_path):
    captured = {}

    def fake_run_entity_partition(args):
        captured.update(vars(args))
        run_dir = tmp_path / "entity-run"
        run_dir.mkdir()
        rows_path = run_dir / "entity_index_rows.jsonl"
        rows_path.write_text("", encoding="utf-8")
        return {
            "status": "completed",
            "data_type": "product",
            "partition_type": "entity",
            "run_dir": str(run_dir),
            "rows_path": str(rows_path),
            "total_index_rows": 0,
            "grid_type": "isea4h",
            "grid_level": args.grid_level,
            "partition_backend_used": args.partition_backend,
            "execution_engine": args.partition_backend,
            "ray_parallelism": args.ray_parallelism,
            "ingest_enabled": False,
        }

    def fail_product_partition(_args):
        raise AssertionError("isea4h product partition should use entity partition")

    monkeypatch.setattr("cube_split.jobs.entity_partition_job.run_entity_partition", fake_run_entity_partition)
    monkeypatch.setattr("cube_split.jobs.product_partition_job.run_product_partition", fail_product_partition)
    monkeypatch.setattr("cube_web.services.quality_checks.run_product_quality_check", None)

    result = partition_runners._run_product_partition_test(
        {
            "input_dir": str(tmp_path),
            "grid_type": "isea4h",
            "grid_level": 6,
            "grid_level_mode": "manual",
            "target_pixels_per_hex_edge": 512,
        }
    )

    assert result["mode"] == "partition_test_no_ingest"
    assert result["data_type"] == "product"
    assert result["partition_type"] == "entity"
    assert result["output_path"].endswith("entity_index_rows.jsonl")
    assert captured["data_type"] == "product"
    assert captured["product_family"] == "product"
    assert captured["grid_type"] == "isea4h"
    assert captured["grid_level"] == 6
    assert captured["target_pixels_per_hex_edge"] == 512
    assert captured["time_granularity"] == "year"
    assert captured["ingest_enabled"] is False


def test_product_partition_test_runner_dispatches_tile_matrix_to_logical_partition(monkeypatch, tmp_path):
    captured = {}

    def fake_run_product_partition(args):
        captured.update(vars(args))
        run_dir = tmp_path / "logical-run"
        run_dir.mkdir()
        rows_path = run_dir / "index_rows.jsonl"
        rows_path.write_text("", encoding="utf-8")
        return {
            "status": "completed",
            "data_type": "product",
            "run_dir": str(run_dir),
            "rows_path": str(rows_path),
            "total_index_rows": 0,
            "grid_type": args.grid_type,
            "grid_level": args.grid_level,
            "partition_backend_used": args.partition_backend,
            "execution_engine": args.partition_backend,
            "ray_parallelism": args.ray_parallelism,
            "ingest_enabled": False,
        }

    def fail_entity_partition(_args):
        raise AssertionError("tile_matrix product partition should use logical partition")

    monkeypatch.setattr("cube_split.jobs.product_partition_job.run_product_partition", fake_run_product_partition)
    monkeypatch.setattr("cube_split.jobs.entity_partition_job.run_entity_partition", fail_entity_partition)
    monkeypatch.setattr("cube_web.services.quality_checks.run_product_quality_check", None)

    result = partition_runners._run_product_partition_test(
        {
            "input_dir": str(tmp_path),
            "grid_type": "tile_matrix",
            "grid_level": 5,
            "partition_backend": "thread",
        }
    )

    assert result["mode"] == "partition_test_no_ingest"
    assert result["data_type"] == "product"
    assert result["output_path"].endswith("index_rows.jsonl")
    assert captured["data_type"] == "product"
    assert captured["product_family"] == "product"
    assert captured["grid_type"] == "tile_matrix"
    assert captured["grid_level"] == 5
    assert captured["partition_backend"] == "thread"
    assert captured["ingest_enabled"] is False


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

    monkeypatch.setattr(partition_adapters, "run_product_partition_retry", fake_run_product_partition_retry)

    resp = client.post(
        "/v1/partition/product/retry",
        json={"request": {"endpoint": "product", "payload": {}}, "last_result": {"status": "completed"}},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["mode"] == "partition_retry"
    assert body["retry"]["strategy"] == "full_request"


def test_radar_partition_demo_endpoint(monkeypatch):
    def fake_run_radar_partition_demo(payload=None):
        assert payload == {"grid_type": "geohash", "grid_level": 5}
        return {
            "status": "completed",
            "mode": "partition_demo",
            "data_type": "radar",
            "rows": 12,
        }

    monkeypatch.setattr(partition_adapters, "run_radar_partition_demo", fake_run_radar_partition_demo)

    resp = client.post("/v1/partition/radar/demo", json={"grid_type": "geohash", "grid_level": 5})

    assert resp.status_code == 200
    body = resp.json()
    assert body["data_type"] == "radar"
    assert body["mode"] == "partition_demo"


def test_radar_partition_test_runner_uses_selected_assets(monkeypatch, tmp_path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    selected = source_dir / "schema_named_radar_asset.dat"
    selected_hdr = source_dir / "schema_named_radar_asset.hdr"
    ignored = source_dir / "20180615_VH.dat"
    selected.write_text("selected", encoding="utf-8")
    selected_hdr.write_text("hdr", encoding="utf-8")
    ignored.write_text("ignored", encoding="utf-8")
    captured = {}

    def fake_demo_run_dir(name):
        run_dir = tmp_path / "run-root"
        run_dir.mkdir()
        return run_dir

    def fake_run_logical_partition(args):
        captured["input_dir"] = Path(args.input_dir)
        captured["manifest_path"] = Path(args.manifest_path)
        captured["files"] = sorted(path.name for path in Path(args.input_dir).iterdir())
        captured["data_type"] = args.data_type
        captured["product_family"] = args.product_family
        captured["grid_level"] = args.grid_level
        captured["partition_backend"] = args.partition_backend
        captured["metadata_backend"] = args.metadata_backend
        captured["asset_storage_backend"] = args.asset_storage_backend
        captured["ingest_enabled"] = args.ingest_enabled
        captured["cube_version"] = args.cube_version
        captured["quality_rule"] = args.quality_rule
        captured["db_path"] = args.db_path
        captured["cog_materialize_mode"] = args.cog_materialize_mode
        captured["cog_output_root"] = args.cog_output_root
        run_dir = tmp_path / "run-root" / "output" / "run"
        run_dir.mkdir(parents=True)
        rows_path = run_dir / "index_rows.jsonl"
        rows_path.write_text("", encoding="utf-8")
        return {
            "status": "completed",
            "data_type": "radar",
            "run_dir": str(run_dir),
            "rows_path": str(rows_path),
            "total_index_rows": 0,
            "grid_type": args.grid_type,
            "grid_level": args.grid_level,
            "partition_backend_used": "thread",
            "execution_engine": "thread",
        }

    monkeypatch.setattr(partition_runners, "_demo_run_dir", fake_demo_run_dir)
    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job.run_logical_partition", fake_run_logical_partition)

    result = partition_runners._run_radar_partition_test(
        {
            "input_dir": str(source_dir),
            "selected_assets": [
                {
                    **ard_raster_asset(
                        "schema_named_radar_asset.dat",
                        "SCHEMA_RADAR_SCENE",
                        data_type="radar",
                        band="vv",
                        resolution=10,
                    ),
                    "acq_time": "2018-06-15T00:00:00Z",
                    "bbox": [119.2, 32.2, 119.5, 32.7],
                    "corners": [[119.2, 32.7], [119.5, 32.7], [119.5, 32.2], [119.2, 32.2]],
                }
            ],
        }
    )

    assert result["mode"] == "partition_test_no_ingest"
    assert result["ingest_enabled"] is False
    assert result["selected_asset_count"] == 1
    assert captured["data_type"] == "radar"
    assert captured["product_family"] == "sentinel1"
    assert captured["grid_level"] == 7
    assert captured["partition_backend"] == "thread"
    assert captured["metadata_backend"] == "none"
    assert captured["asset_storage_backend"] == "local"
    assert captured["ingest_enabled"] is False
    assert captured["cube_version"] == "radar_v1"
    assert captured["quality_rule"] == "best_quality_wins"
    assert captured["db_path"] == ""
    assert captured["cog_materialize_mode"] == "copy"
    assert captured["cog_output_root"].endswith("radar_cog_store")
    assert captured["files"] == ["schema_named_radar_asset.dat", "schema_named_radar_asset.hdr"]
    manifest = json.loads(captured["manifest_path"].read_text(encoding="utf-8"))
    assert manifest["data_type"] == "radar"
    assert manifest["assets"][0]["source_uri"] == str(captured["input_dir"] / "schema_named_radar_asset.dat")
    assert manifest["assets"][0]["scene_id"] == "SCHEMA_RADAR_SCENE"
    assert manifest["assets"][0]["band"] == "vv"


def test_radar_partition_test_runner_dispatches_isea4h_to_entity_partition(monkeypatch, tmp_path):
    captured = {}

    def fake_run_entity_partition(args):
        captured.update(vars(args))
        run_dir = tmp_path / "entity-run"
        run_dir.mkdir()
        rows_path = run_dir / "entity_index_rows.jsonl"
        rows_path.write_text("", encoding="utf-8")
        return {
            "status": "completed",
            "data_type": "radar",
            "partition_type": "entity",
            "run_dir": str(run_dir),
            "rows_path": str(rows_path),
            "total_index_rows": 0,
            "grid_type": "isea4h",
            "grid_level": args.grid_level,
            "partition_backend_used": args.partition_backend,
            "execution_engine": args.partition_backend,
            "ray_parallelism": args.ray_parallelism,
            "ingest_enabled": False,
        }

    def fail_logical_partition(_args):
        raise AssertionError("isea4h radar partition should use entity partition")

    monkeypatch.setattr("cube_split.jobs.entity_partition_job.run_entity_partition", fake_run_entity_partition)
    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job.run_logical_partition", fail_logical_partition)

    result = partition_runners._run_radar_partition_test(
        {
            "input_dir": str(tmp_path),
            "grid_type": "isea4h",
            "grid_level": 6,
            "grid_level_mode": "manual",
            "partition_backend": "thread",
            "target_pixels_per_hex_edge": 512,
        }
    )

    assert result["mode"] == "partition_test_no_ingest"
    assert result["data_type"] == "radar"
    assert result["partition_type"] == "entity"
    assert result["output_path"].endswith("entity_index_rows.jsonl")
    assert captured["data_type"] == "radar"
    assert captured["product_family"] == "sentinel1"
    assert captured["grid_type"] == "isea4h"
    assert captured["grid_level"] == 6
    assert captured["target_pixels_per_hex_edge"] == 512
    assert captured["partition_backend"] == "thread"
    assert captured["metadata_backend"] == "none"
    assert captured["asset_storage_backend"] == "local"
    assert captured["ingest_enabled"] is False


def test_radar_partition_test_runner_dispatches_tile_matrix_to_logical_partition(monkeypatch, tmp_path):
    captured = {}

    def fake_run_logical_partition(args):
        captured.update(vars(args))
        run_dir = tmp_path / "logical-run"
        run_dir.mkdir()
        rows_path = run_dir / "index_rows.jsonl"
        rows_path.write_text("", encoding="utf-8")
        return {
            "status": "completed",
            "data_type": "radar",
            "run_dir": str(run_dir),
            "rows_path": str(rows_path),
            "total_index_rows": 0,
            "grid_type": args.grid_type,
            "grid_level": args.grid_level,
            "partition_backend_used": args.partition_backend,
            "execution_engine": args.partition_backend,
            "ray_parallelism": args.ray_parallelism,
            "ingest_enabled": False,
        }

    def fail_entity_partition(_args):
        raise AssertionError("tile_matrix radar partition should use logical partition")

    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job.run_logical_partition", fake_run_logical_partition)
    monkeypatch.setattr("cube_split.jobs.entity_partition_job.run_entity_partition", fail_entity_partition)

    result = partition_runners._run_radar_partition_test(
        {
            "input_dir": str(tmp_path),
            "grid_type": "tile_matrix",
            "grid_level": 5,
            "partition_backend": "thread",
        }
    )

    assert result["mode"] == "partition_test_no_ingest"
    assert result["data_type"] == "radar"
    assert result["output_path"].endswith("index_rows.jsonl")
    assert captured["data_type"] == "radar"
    assert captured["product_family"] == "sentinel1"
    assert captured["grid_type"] == "tile_matrix"
    assert captured["grid_level"] == 5
    assert captured["partition_backend"] == "thread"
    assert captured["metadata_backend"] == "none"
    assert captured["asset_storage_backend"] == "local"
    assert captured["ingest_enabled"] is False


def test_radar_partition_retry_endpoint(monkeypatch):
    def fake_run_radar_partition_retry(payload=None):
        assert payload == {"request": {"endpoint": "radar", "payload": {}}, "last_result": {"status": "completed"}}
        return {
            "status": "completed",
            "mode": "partition_retry",
            "data_type": "radar",
            "rows": 20,
            "retry": {"strategy": "full_request"},
        }

    monkeypatch.setattr(partition_adapters, "run_radar_partition_retry", fake_run_radar_partition_retry)

    resp = client.post(
        "/v1/partition/radar/retry",
        json={"request": {"endpoint": "radar", "payload": {}}, "last_result": {"status": "completed"}},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["mode"] == "partition_retry"
    assert body["retry"]["strategy"] == "full_request"


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

    monkeypatch.setattr("cube_web.services.quality_checks.run_optical_quality_check", fake_run_quality_check)

    resp = client.post("/v1/quality/optical/run", json={"run_dir": run_dir, "target_crs": "EPSG:4326"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "PASS"
    assert body["report_id"]
    assert body["run_dir"] == run_dir
    assert body["summary"]["index_rows"] == 3


def test_carbon_quality_endpoint(monkeypatch):
    run_dir = "/tmp/cube_web_partition_demo/carbon/run"

    def fake_run_quality_check(args):
        assert args.run_dir == run_dir
        assert args.target_crs == "EPSG:4326"
        return {
            "status": "PASS",
            "summary": {"index_rows": 2, "observation_rows": 2, "quality_counts": {"1": 2}, "failed_checks": 0, "warning_checks": 0},
            "checks": [{"name": "carbon_rows", "status": "PASS", "message": "ok"}],
            "assets": [],
        }

    monkeypatch.setattr("cube_web.services.quality_checks.run_carbon_quality_check", fake_run_quality_check)

    resp = client.post("/v1/quality/carbon/run", json={"run_dir": run_dir, "target_crs": "EPSG:4326"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "PASS"
    assert body["data_type"] == "carbon"
    assert body["report_id"]
    assert body["run_dir"] == run_dir
    assert body["summary"]["observation_rows"] == 2


def test_quality_report_requires_report_id():
    resp = client.post("/v1/quality/optical/report", json={})

    assert resp.status_code == 422


def test_optical_quality_latest_endpoint(quality_store):
    quality_store.upsert_report(
        "optical",
        "/tmp/latest-run",
        {
            "report_id": "optical-latest",
            "status": "WARN",
            "target_crs": "EPSG:4326",
            "summary": {"index_rows": 9, "failed_checks": 0, "warning_checks": 1},
            "checks": [{"name": "logical_duplicates", "status": "WARN", "message": "duplicate"}],
            "assets": [],
        },
    )

    resp = client.post("/v1/quality/optical/latest", json={})

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "WARN"
    assert body["run_dir"] == "/tmp/latest-run"
    assert body["summary"]["warning_checks"] == 1


def test_optical_quality_latest_reads_database_without_rerun(monkeypatch, quality_store):
    quality_store.upsert_report(
        "optical",
        "/tmp/dataset_a/run_20260515_010203",
        {
          "report_id": "optical-existing",
          "status": "PASS",
          "target_crs": "EPSG:4326",
          "generated_at": "2026-05-15T01:02:03Z",
          "summary": {"index_rows": 11, "failed_checks": 0, "warning_checks": 0},
          "checks": [{"name": "index_rows", "status": "PASS", "message": "cached"}],
          "assets": []
        },
    )

    def fail_if_called(args):
        raise AssertionError("latest should read the database instead of re-running quality checks")

    monkeypatch.setattr("cube_web.services.quality_checks.run_optical_quality_check", fail_if_called)

    body = quality_adapters.quality_optical_latest({})

    assert body["status"] == "PASS"
    assert body["run_dir"] == "/tmp/dataset_a/run_20260515_010203"
    assert body["summary"]["index_rows"] == 11
    assert body["checks"][0]["message"] == "cached"


def test_optical_quality_report_endpoint_reads_database_without_rerun(monkeypatch, quality_store):
    quality_store.upsert_report(
        "optical",
        "/tmp/dataset_a/run_20260515_010203",
        {
          "report_id": "optical-report",
          "status": "WARN",
          "target_crs": "EPSG:4326",
          "generated_at": "2026-05-15T01:02:03Z",
          "summary": {"index_rows": 7, "failed_checks": 0, "warning_checks": 1},
          "checks": [{"name": "logical_duplicates", "status": "WARN", "message": "cached warn"}],
          "assets": []
        },
    )

    def fail_if_called(args):
        raise AssertionError("report viewing should not re-run quality checks")

    monkeypatch.setattr("cube_web.services.quality_checks.run_optical_quality_check", fail_if_called)

    body = quality_adapters.quality_optical_report({"report_id": "optical-report"})

    assert body["status"] == "WARN"
    assert body["run_dir"] == "/tmp/dataset_a/run_20260515_010203"
    assert body["summary"]["index_rows"] == 7
    assert body["checks"][0]["message"] == "cached warn"


def test_optical_quality_report_pdf_endpoint_reads_database_without_rerun(monkeypatch, tmp_path, quality_store):
    quality_store.upsert_report(
        "optical",
        "/tmp/dataset_a/run_20260515_010203",
        {
          "report_id": "optical-pdf",
          "status": "PASS",
          "target_crs": "EPSG:4326",
          "generated_at": "2026-05-15T01:02:03Z",
          "summary": {"index_rows": 7, "asset_count": 2, "failed_checks": 0, "warning_checks": 0},
          "checks": [{"name": "index_rows", "status": "PASS", "message": "cached"}],
          "assets": [{"path": "/data/a.tif", "crs": "EPSG:4326"}]
        },
    )

    def fail_if_called(args):
        raise AssertionError("PDF export should not re-run quality checks")

    monkeypatch.setattr("cube_web.services.quality_checks.run_optical_quality_check", fail_if_called)

    response = quality_adapters.quality_optical_report_pdf({"report_id": "optical-pdf"})

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


def test_optical_quality_report_txt_endpoint_reads_database_without_rerun(monkeypatch, quality_store):
    quality_store.upsert_report(
        "optical",
        "/tmp/dataset_a/run_20260515_010203",
        {
          "report_id": "optical-txt",
          "status": "WARN",
          "target_crs": "EPSG:4326",
          "generated_at": "2026-05-15T01:02:03Z",
          "summary": {"index_rows": 7, "asset_count": 2, "failed_checks": 0, "warning_checks": 1},
          "checks": [{"name": "logical_duplicates", "status": "WARN", "message": "cached warn"}],
          "assets": [{"path": "/data/a.tif", "crs": "EPSG:4326"}]
        },
    )

    def fail_if_called(args):
        raise AssertionError("TXT export should not re-run quality checks")

    monkeypatch.setattr("cube_web.services.quality_checks.run_optical_quality_check", fail_if_called)

    response = quality_adapters.quality_optical_report_txt({"report_id": "optical-txt"})

    assert response.media_type == "text/plain"
    text = response.body.decode("utf-8")
    assert "质检报告" in text
    assert "质检状态：WARN" in text
    assert "检查项" in text


def test_quality_report_txt_routes_are_registered():
    route_paths = {route.path for route in app.routes}

    assert "/v1/quality/optical/report/txt" in route_paths
    assert "/v1/quality/product/report/txt" in route_paths
    assert "/v1/quality/carbon/report/txt" in route_paths


def test_optical_quality_history_endpoint(quality_store):
    quality_store.upsert_report(
        "optical",
        "/tmp/dataset_a/run_20260515_010203",
        {
          "report_id": "optical-history",
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
        },
    )

    body = quality_adapters.quality_optical_history({"target_crs": "EPSG:4326"})

    assert body["count"] == 1
    record = body["records"][0]
    assert record["dataset"] == "dataset_a"
    assert record["run_name"] == "run_20260515_010203"
    assert record["status"] == "PASS"
    assert record["summary"]["index_rows"] == 1


def test_product_quality_history_endpoint(quality_store):
    quality_store.upsert_report(
        "product",
        "/tmp/product_epsg4326/run_20260515_010203",
        {
          "report_id": "product-history",
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
        },
    )

    body = quality_adapters.quality_product_history({"target_crs": "EPSG:4326"})

    assert body["count"] == 1
    record = body["records"][0]
    assert record["data_type"] == "product"
    assert record["dataset"] == "product_epsg4326"
    assert record["summary"]["index_rows"] == 5


def test_carbon_quality_history_endpoint(quality_store):
    quality_store.upsert_report(
        "carbon",
        "/tmp/carbon/run_20260515_010203",
        {
          "report_id": "carbon-history",
          "status": "PASS",
          "target_crs": "EPSG:4326",
          "generated_at": "2026-05-15T01:02:03Z",
          "summary": {
            "index_rows": 2,
            "observation_rows": 2,
            "quality_counts": {"1": 2},
            "passed_checks": 7,
            "warning_checks": 0,
            "failed_checks": 0
          }
        },
    )

    body = quality_adapters.quality_carbon_history({"target_crs": "EPSG:4326"})

    assert body["count"] == 1
    record = body["records"][0]
    assert record["data_type"] == "carbon"
    assert record["dataset"] == "carbon"
    assert record["summary"]["observation_rows"] == 2
