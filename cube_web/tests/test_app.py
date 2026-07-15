from __future__ import annotations

import base64
import hashlib
import hmac
import json
import threading
import time
from pathlib import Path
from typing import Any
from unittest.mock import Mock
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import cube_web.app as web_app
import cube_web.routes.partition_adapters as partition_adapters
from cube_web.app import ENCODER_SDK_CLASS, app
from cube_web.services import auth_service as auth_service_module
from cube_web.services import config_store as config_store_module
from cube_web.services import health_service, partition_runners
from cube_web.services import partition_job_store as partition_job_store_module
from cube_web.services.config_store import set_config_store
from cube_web.services.db_pool import _PoolContext
from cube_web.services.partition_contracts import StrictPartitionRequest
from cube_web.services.partition_defaults import default_grid_level_for_resolution
from cube_web.services.partition_job_store import InMemoryPartitionJobStore, set_partition_job_store
from cube_web.services.partition_loaded_schemas import ensure_standard_partition_schemas, standard_partition_schemas
from cube_web.services.partition_remote_job import run_task as run_remote_partition_task
from cube_web.services.partition_service import PartitionBackend, PartitionService, PartitionTask
from cube_web.services.partition_workflow import PartitionWorkflowService, _partition_slots_for_batch, classify_partition_error

client = TestClient(app)


def normalized_task_run_request() -> dict:
    return {
        "batch_id": "batch-01",
        "grid_type": "geohash",
        "requested_grid_level": 7,
        "partition_method": "logical",
        "cover_mode": "minimal",
        "time_granularity": "day",
        "max_cells_per_asset": 0,
        "datasets": [
            {
                "dataset_id": "dataset-a",
                "dataset_code": "DS-A",
                "dataset_title": "Dataset A",
                "data_type": "optical",
                "product_type": "L2A",
                "assets": [
                    {
                        "source_asset_id": "asset-a",
                        "cog_uri": "s3://cube/loader/dataset-a/asset-a.tif",
                        "checksum": "a" * 64,
                        "bbox": [100.0, 20.0, 101.0, 21.0],
                        "crs": "EPSG:4326",
                        "time_start": "2026-07-01T00:00:00Z",
                        "time_end": "2026-07-01T00:05:00Z",
                        "attributes": {"scene_id": "scene-a"},
                    }
                ],
                "bands": [
                    {
                        "source_asset_id": "asset-a",
                        "band_code": "B04",
                        "band_name": "Red",
                        "band_type": "spectral",
                        "unit": None,
                        "display_order": 4,
                        "attributes": {"wavelength_nm": 665},
                    }
                ],
                "attributes": {},
            }
        ],
    }


def test_app_startup_reconcile_failure_does_not_block_requests(monkeypatch, caplog):
    def broken_reconcile():
        raise ValueError("PostgreSQL DSN is required")

    monkeypatch.setattr(web_app.partition_workflow_service, "reconcile_orphaned_tasks", broken_reconcile)
    caplog.set_level("WARNING", logger="cube_web.app")

    with TestClient(web_app.create_app()) as startup_client:
        response = startup_client.get("/")

    assert response.status_code == 200
    assert response.json() == {"service": "cube-web", "status": "ok"}
    assert "Skipping partition task reconcile during startup: PostgreSQL DSN is required" in caplog.text


def test_postgres_pool_context_commits_before_releasing_connection():
    conn = _FakePoolConn()
    pool = _FakePool(conn)

    with _PoolContext(pool) as borrowed:
        assert borrowed is conn

    assert conn.commit_calls == 1
    assert conn.rollback_calls == 0
    assert pool.released == [conn]


def test_postgres_pool_context_rolls_back_on_exception_and_reuses_connection():
    conn = _FakePoolConn()
    pool = _FakePool(conn)

    with pytest.raises(RuntimeError, match="boom"):
        with _PoolContext(pool):
            raise RuntimeError("boom")

    assert conn.commit_calls == 0
    assert conn.rollback_calls == 1
    assert pool.released == [conn]


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
        self.history = {"optical": [], "radar": [], "product": []}

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

    def list_reports(self, data_type, limit=20, *, offset=0, status=None, keyword=None):
        return self._filtered_history(data_type, status=status, keyword=keyword)[offset : offset + limit]

    def count_reports(self, data_type, *, status=None, keyword=None):
        return len(self._filtered_history(data_type, status=status, keyword=keyword))

    def _filtered_history(self, data_type, *, status=None, keyword=None):
        rows = list(self.history.get(data_type, []))
        if status:
            rows = [row for row in rows if row["status"] == status]
        if keyword:
            needle = str(keyword).lower()
            rows = [
                row
                for row in rows
                if any(needle in str(row.get(key) or "").lower() for key in ("dataset", "run_name", "run_dir", "report_id"))
            ]
        return rows


class FakeConfigStore:
    def __init__(self):
        self.config = config_store_module.default_config()
        self.updated_at = None

    def get_config_record(self):
        return {"config": config_store_module.normalized_config(self.config), "updated_at": self.updated_at}

    def update_config(self, config):
        self.config = config_store_module.normalized_config(config)
        self.updated_at = "2026-05-26T08:00:00+00:00"
        return self.get_config_record()


class _FakePoolConn:
    def __init__(self, *, commit_error: Exception | None = None):
        self.commit_calls = 0
        self.rollback_calls = 0
        self.close_calls = 0
        self.commit_error = commit_error
        self.closed = False

    def commit(self):
        self.commit_calls += 1
        if self.commit_error is not None:
            raise self.commit_error

    def rollback(self):
        self.rollback_calls += 1

    def close(self):
        self.close_calls += 1
        self.closed = True


class _FakePool:
    def __init__(self, conn):
        self.conn = conn
        self.released = []
        self._created = 1
        self._lock = threading.Lock()

    def _acquire(self):
        return self.conn

    def _release(self, conn):
        self.released.append(conn)


def test_postgres_pool_context_discards_connection_when_commit_fails():
    conn = _FakePoolConn(commit_error=RuntimeError("commit failed"))
    pool = _FakePool(conn)

    with _PoolContext(pool):
        pass

    assert conn.commit_calls == 1
    assert conn.rollback_calls == 1
    assert conn.close_calls == 1
    assert pool.released == []


def test_postgres_pool_context_does_not_release_closed_connection():
    conn = _FakePoolConn()
    pool = _FakePool(conn)

    with pytest.raises(RuntimeError, match="boom"):
        with _PoolContext(pool):
            conn.close()
            raise RuntimeError("boom")

    assert conn.rollback_calls == 1
    assert pool.released == []

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
def runtime_environment(monkeypatch):
    monkeypatch.setenv("CUBE_WEB_POSTGRES_DSN", "postgresql://test_user:test_password@10.3.100.180:15400/postgres")
    monkeypatch.setenv("CUBE_WEB_RAY_ADDRESS", "10.3.100.182:6379")
    monkeypatch.setenv("CUBE_WEB_MINIO_ENDPOINT", "10.3.100.179:9000")
    monkeypatch.setenv("CUBE_WEB_MINIO_BUCKET", "cube")
    monkeypatch.setenv("CUBE_WEB_AUTH_JWT_SECRET_KEY", "your-secret-key-here-change-in-production")
    monkeypatch.setenv("CUBE_WEB_AUTH_REQUIRED", "0")
    monkeypatch.delenv("CUBE_WEB_LOAD_DEMO_PARTITION_SCHEMAS", raising=False)
    config_store = FakeConfigStore()
    set_config_store(config_store)
    set_partition_job_store(InMemoryPartitionJobStore())
    web_app.partition_workflow_service._store = None
    yield
    set_config_store(None)
    set_partition_job_store(None)
    web_app.partition_workflow_service._store = None


def test_header_navigation_does_not_expose_quality_as_top_level_item():
    nav_source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "data" / "navigation.js").read_text(encoding="utf-8")
    app_source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "App.vue").read_text(encoding="utf-8")

    assert "{ label: '自动化质检'," not in nav_source
    assert "{ label: '首页', kind: 'external', url: portalHomeUrl }" in nav_source
    assert "{ label: 'ARD数据载入', kind: 'external', url: '/ard' }" in nav_source
    assert "{ label: '分析就绪数据剖分', kind: 'internal', path: '/partition' }" in nav_source
    assert "{ label: '剖分数据服务', kind: 'external', url: '/partition' }" in nav_source
    assert "{ label: '资源调度', kind: 'external', url: '/dispatch' }" in nav_source
    assert "{ label: '后台管理', kind: 'external', url: '/admin' }" in nav_source
    assert "{ label: '全球离散格网模型与编码', kind: 'internal', path: '/encoding' }" in nav_source
    order_source = nav_source.split("const headerLabelOrder = [", 1)[1].split("];", 1)[0]
    assert order_source.index("'首页'") < order_source.index("'ARD数据载入'")
    assert order_source.index("'ARD数据载入'") < order_source.index("'分析就绪数据剖分'")
    assert order_source.index("'分析就绪数据剖分'") < order_source.index("'剖分数据服务'")
    assert order_source.index("'剖分数据服务'") < order_source.index("'资源调度'")
    assert order_source.index("'资源调度'") < order_source.index("'后台管理'")
    assert order_source.index("'后台管理'") < order_source.index("'全球离散格网模型与编码'")
    assert "runtimeNavigation()" in nav_source
    assert "normalizeNavItem(item)" in nav_source
    assert "HomeView" not in app_source
    assert "'/':" not in app_source
    assert ':href="item.path"' in app_source
    assert "currentNavItems" in app_source
    assert "const isAdmin = computed(() => userStore.role.value === '管理员');" in app_source
    assert "const currentNavItems = computed(() => navItems(isAdmin.value));" in app_source
    assert "function redirectNonAdminFromPartition()" in app_source
    assert "window.location.replace(portalHomeUrl);" in app_source
    assert "const publicNavLabels = new Set(['全球离散格网模型与编码']);" in nav_source
    assert ".filter((item) => isAdmin || publicNavLabels.has(item.label))" in nav_source
    assert "targetFromAuthState(state)" in app_source
    assert "normalizePath(window.location.pathname)" in app_source


def test_auth_redirect_routes_through_backend_login_with_fixed_callback():
    store_source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "stores" / "subUser.js").read_text(encoding="utf-8")

    assert "function authRedirectUri()" in store_source
    assert "redirect_uri: authRedirectUri()," in store_source
    assert "window.location.href = `/api/auth/login?${query.toString()}`;" in store_source
    assert "base.pathname = target.pathname;" not in store_source
    assert "sessionStorage.setItem('oauth_target', target);" not in store_source


def test_frontend_auth_bootstrap_uses_runtime_config_flag():
    app_source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "App.vue").read_text(encoding="utf-8")
    config_source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "config.js").read_text(encoding="utf-8")
    store_source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "stores" / "subUser.js").read_text(encoding="utf-8")

    assert "await loadAuthRuntimeConfig();" in app_source
    assert "if (authRequired()) {" in app_source
    assert "fetch('/api/config'" in config_source
    assert "auth_required" in config_source
    assert "navigation" in config_source
    assert "http://10.136." not in config_source
    assert "if (authRequired()) {" in store_source
    assert "const target = targetFromAuthState(state) || safeLocalTarget(params.get('target')) || '/';" in app_source
    assert "function safeLocalTarget(value)" in app_source
    initialize_source = app_source.split("async function initializeAuth()", 1)[1].split("async function handleLogout()", 1)[0]
    mounted_source = app_source.split("onMounted(async () => {", 1)[1].split("});", 1)[0]
    assert initialize_source.index("if (code) {") < initialize_source.index("syncPathFromLocation();")
    assert "authReady.value = false;" in initialize_source
    assert "authReady.value = true;" in initialize_source
    assert "syncPathFromLocation();" not in mounted_source
    assert '<component v-if="authReady" :is="currentView" />' in app_source


def test_partition_view_uses_explicit_module_endpoint_mapping():
    source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "views" / "PartitionView.vue").read_text(encoding="utf-8")

    assert "const partitionEndpointsByModule = {" in source
    assert "optical: 'optical'" in source
    assert "carbon: 'carbon'" in source
    assert "radar: 'radar'" in source
    assert "product: 'product'" in source
    assert "const partitionModules = new Set(['optical', 'carbon', 'radar', 'product']);" in source
    assert "const operation = 'run';" in source
    assert "testModules" not in source
    assert "activeModule === 'entity'" not in source
    assert "activeModule.value === 'entity'" not in source
    assert ">实体剖分</button>" not in source
    assert '<el-option label="GeoHash格网" value="geohash" />' in source
    assert '<el-option label="MGRS格网" value="mgrs" />' in source
    assert '<el-option label="六边形格网" value="isea4h" />' in source
    assert 'value="s2"' not in source
    assert 'value="tile_matrix"' not in source
    assert 'value="plane_grid"' not in source
    assert 'v-model="radarGridType"' in source
    assert 'v-model="productGridType"' in source
    carbon_block = source.split("<template v-else-if=\"activeModule === 'carbon'\">", 1)[1].split(
        "<template v-else-if=\"activeModule === 'radar'\">",
        1,
    )[0]
    radar_block = source.split("<template v-else-if=\"activeModule === 'radar'\">", 1)[1].split(
        "<template v-else-if=\"activeModule === 'product'\">",
        1,
    )[0]
    product_block = source.split("<template v-else-if=\"activeModule === 'product'\">", 1)[1].split("<template v-else>", 1)[0]
    assert 'value="isea4h"' not in carbon_block
    assert 'value="isea4h"' in radar_block
    assert 'value="isea4h"' in product_block
    assert "const opticalPartitionMethod = ref('logical');" in source
    assert "const radarPartitionMethod = ref('logical');" in source
    assert "const productPartitionMethod = ref('logical');" in source
    assert 'v-model="opticalPartitionMethod"' in source
    assert 'v-model="radarPartitionMethod"' in source
    assert 'v-model="productPartitionMethod"' in source
    assert "function partitionMethodForModule(moduleName = activeModule.value)" in source
    assert "function gridLevelModeForModule(moduleName = activeModule.value)" in source
    assert "partition_method: partitionMethodForModule('radar')" in source
    assert "partition_method: partitionMethodForModule('product')" in source
    assert "if (partitionMethod === 'entity') return defaultEntityGridLevel;" in source
    assert "if (resolution < 10) return 8;" in source
    assert "if (resolution <= 30) return 7;" in source
    assert "const partitionStageDetailVisible = ref(false);" in source
    assert "function openPartitionStageDetail(stage)" in source
    assert '@click="openPartitionStageDetail(stage)"' in source
    assert 'title="剖分进程详情"' in source
    assert "function pruneBatchSelection(selectedIds, batches)" in source
    assert "preferredBatchId" not in source
    assert "selectedOpticalBatchIds.value = pruneBatchSelection(selectedOpticalBatchIds.value, managedOpticalBatches.value);" in source
    assert "selectedCarbonBatchIds.value = pruneBatchSelection(selectedCarbonBatchIds.value, managedCarbonBatches.value);" in source
    assert "selectedRadarBatchIds.value = pruneBatchSelection(selectedRadarBatchIds.value, managedRadarBatches.value);" in source
    assert "selectedProductBatchIds.value = pruneBatchSelection(selectedProductBatchIds.value, managedProductBatches.value);" in source
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
    assert "requestGet(`${partitionPrefix}/batches?limit=500`)" in source
    assert "function partitionBatchNeedsIngestAttention(batch)" in source
    assert "['ready', 'previewed', 'failed'].includes(batch?.ingest_status)" in source
    assert "function shouldDisplayManagedBatch(batch)" in source
    assert "if (batch?.status === 'archived') return false;" in source
    assert "return !partitionBatchAllSlotsCompleted(batch);" in source
    assert "function partitionSlots(batch)" in source
    assert "function partitionSlotGroups(batch)" in source
    assert "function partitionSlotStatusText(status)" in source
    assert "function partitionSlotStatusType(status)" in source
    assert 'class="partition-slot-grid"' in source
    assert 'class="partition-slot-chip"' in source
    assert "requestGet(`${partitionPrefix}/batches/${batchId}/attempts`)" in source
    assert (
        "partitionBatchDetail.value = resolved ? { ...resolved, id: batchId, batch_id: batchId } : { id: batchId, batch_id: batchId };"
        in source
    )
    assert "return (batch.assets || []).map((asset) => {" in source
    assert "取消会立即请求执行层中断当前任务" in source
    assert "重试失败资产" in source
    assert "async function archivePartitionBatch(batch)" in source
    assert "function partitionTaskDisplayStatus(task)" in source
    assert "return task?.status;" in source
    assert "excludeArchivedBatch" in source
    assert "requestGet(`${partitionPrefix}/tasks/${taskId}`)" in source
    assert "const query = partitionTaskQuery({ keyword: taskId, limit: 20 });" not in source
    assert "const completedResult = row.result || row.result_summary || {};" in source
    assert "const cleanRow = Object.fromEntries(Object.entries(row).filter(([, value]) => value !== undefined));" in source
    assert "{ label: '入库状态', value: partitionIngestStatusText(partitionIngestStatus(result)) }" in source
    assert "{ label: '正式入库'" not in source
    assert "function partitionSupportsIngestStatus(dataType)" in source
    assert "return ['optical', 'entity', 'radar', 'product'].includes(dataType);" in source
    assert "function initialPartitionIngestStatus(dataType)" in source
    assert "return result.ingest_enabled === false ? 'ready' : 'ingested';" in source
    assert "ready: '待补入库'" in source
    assert "async function previewOpticalIngest()" not in source
    assert "async function confirmOpticalIngest()" not in source
    assert "const opticalIngestConfirmReady = computed" not in source
    assert "payload.batch_id = result.batch_id;" not in source
    assert "ingest_status: initialPartitionIngestStatus(dataType)" in source
    assert "setPartitionStage('persist', 'done', partitionPersistDoneText(lastPartitionResult.value, '执行结果已返回。'));" in source
    assert (
        "const ingestSummary = partitionBatchNeedsIngestAttention(batch) ? ` · ${partitionIngestStatusText(batch.ingest_status)}` : '';"
        in source
    )
    assert "{ label: '批次状态', value: partitionStatusText(result.batch_status) }" in source
    assert "const activePartitionTasks = ref([]);" in source
    assert "const partitionTaskTotal = ref(0);" in source
    assert "const activePartitionTaskTotal = ref(0);" in source
    assert "const activePartitionTaskQueueStats = computed(() => partitionTaskStats(activePartitionTasks.value));" in source
    assert (
        "const activePartitionTaskDrawerTitle = computed(() => `${dataLabelsByModule[activeModule.value] || '当前模块'}剖分任务队列`);"
        in source
    )
    assert "function partitionTaskQuery(params)" in source
    assert "async function loadActivePartitionTasks(page = activePartitionTaskPage.value)" in source
    assert "data_type: activeModule.value" in source
    assert "async function openActivePartitionTaskDrawer()" in source
    assert '<el-drawer v-model="partitionTaskDrawerVisible" :title="activePartitionTaskDrawerTitle"' in source
    assert ':data="activePartitionTasks"' in source
    assert 'v-model:current-page="activePartitionTaskPage"' in source
    assert 'v-model:page-size="activePartitionTaskPageSize"' in source
    assert 'empty-text="当前类别暂无剖分任务"' in source
    assert "partitionTaskCanArchiveBatch(row)" in source
    assert "function partitionTaskCanRequeueBatch(task)" in source
    assert "requestJson(`${partitionPrefix}/batches/${batchId}/requeue`, {})" in source
    assert "打回队列" in source
    assert "const partitionResultArchiveBatch = computed" in source
    assert "async function archiveLastPartitionResultBatch()" in source
    assert "function applyArchivedPartitionBatch(batchId, archivedBatch = null)" in source
    assert "loadActivePartitionTasks(activePartitionTaskPage.value)" in source
    assert "async function syncSubmittedPartitionTask(taskId)" in source
    assert "startPartitionTaskSync(submitted.task_id)" in source
    assert "const partitionActiveStatuses = ['queued', 'running', 'retrying', 'cancel_requested'];" in source
    assert "function partitionBatchCanRun(batch)" in source
    assert 'v-else-if="partitionBatchCanRun(partitionBatchDetail)"' in source
    assert "requestJson(`${partitionPrefix}/batches/${batchId}/archive`, {})" in source
    assert "不再处理" in source
    assert "archived: '已归档'" in source
    assert "partitionBatchDetailTab === 'attempts'" in source
    assert "visibleOpticalBatches" in source
    assert "const selectedProductAssets = computed(() => {" in source
    assert "const productMapGeometries = computed(() => mapGeometryItemsFromFootprints(productMapFootprints.value" in source
    assert "activeModule.value === 'radar'" in source
    assert "? selectedRadarAssets.value" in source
    assert "? radarGridType.value" in source
    assert "const defaultEntityGridLevel = 6;" in source
    assert "const entityGridLevel = ref(defaultEntityGridLevel);" in source
    assert "const radarEntityGridLevel = ref(defaultEntityGridLevel);" in source
    assert "const productEntityGridLevel = ref(defaultEntityGridLevel);" in source
    assert "return partitionMethodForModule('radar') === 'entity' ? radarEntityGridLevel.value : radarGridLevel.value;" in source
    assert "return partitionMethodForModule('product') === 'entity' ? productEntityGridLevel.value : productGridLevel.value;" in source
    assert "activeModule === 'product' ? '产品范围地图预览'" not in source
    assert "selected_assets: selectedAssets" in source
    assert "function buildPartitionFailureResult(error, request = {})" in source
    assert "partition_method: payload.partition_method || partitionMethodForModule(dataType)" in source
    assert "const partitionFailureMessage = computed" in source
    assert "partitionFailureMessage" in source
    assert "剖分失败，详情已写入执行结果" not in source
    assert "submitPartitionOperation(partitionPrefix, endpoint, operation, payload)" in source
    assert "function buildPartitionSubmittedResult(submitted, request, selectedCount)" in source
    assert "剖分任务已提交，后台将连接 Ray 集群异步执行。" in source
    assert "开始剖分" not in source
    assert "提交剖分任务" in source
    assert "/tasks/${operation}" in source
    quality_source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "views" / "QualityRecordsView.vue").read_text(encoding="utf-8")
    assert "/records?" in quality_source
    assert "/records/${id}/results" in quality_source
    assert "/records/${id}/errors" in quality_source
    assert "/records/${id}/errors/export?format=${format}" in quality_source
    assert "report_id" not in quality_source
    assert "PDF" not in quality_source and "TXT" not in quality_source
    assert "schema-grid" in source
    assert "defaultOpticalSchemaFields" in source
    assert "defaultCarbonSchemaFields" in source
    assert "defaultRadarSchemaFields" in source
    assert "defaultProductSchemaFields" in source
    assert "function schemaForBatch(batch)" in source
    assert "function schemaCollapseTitle(batch)" in source
    assert "function schemaFromManagedBatch(batch)" in source
    assert "schema: schemaFromManagedBatch(batch)" in source
    assert 'class="batch-schema-collapse"' in source
    assert ':title="schemaCollapseTitle(batch)"' in source
    assert "schemaForBatch(batch)" in source
    assert "雷达栅格源文件路径或 MinIO 对象 URL" in source
    assert "Sentinel-1 场景标识" in source
    assert "band / polarization" in source
    assert "覆盖范围 bbox（WGS84）" in source
    assert "光学栅格源文件路径或 MinIO 对象 URL" in source
    assert "碳卫星源文件路径或 MinIO 对象 URL" in source
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


def test_api_client_uses_request_timeout():
    source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "api" / "client.js").read_text(encoding="utf-8")

    assert "const REQUEST_TIMEOUT_MS = 30000;" in source
    assert "AbortController" in source
    assert "signal," in source
    assert "timeoutSignal()" in source


def test_partition_view_deduplicates_map_preview_and_grid_cover_requests():
    source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "views" / "PartitionView.vue").read_text(encoding="utf-8")

    assert "function normalizedCornersKey(corners)" in source
    assert "function normalizedBboxKey(corners)" in source
    assert "function uniqueFootprintAssets(assets, labelForAsset)" in source
    assert "const opticalMapFootprints = computed(() => uniqueFootprintAssets(" in source
    assert "const productMapFootprints = computed(() => uniqueFootprintAssets(" in source
    assert "const radarMapFootprints = computed(() => uniqueFootprintAssets(" in source
    assert "function uniqueGridCoverFootprints(footprints)" in source
    assert "function uniqueGridGeometryItems(chunks, gridType, level)" in source
    assert "const footprints = uniqueGridCoverFootprints(mapFootprints).slice(0, 30);" in source
    assert "mapGridGeometries.value = uniqueGridGeometryItems(chunks, gridType, gridLevel);" in source
    assert "selectedAssets.slice(0, 30).map" not in source
    assert "mapGridGeometries.value = chunks.flat();" not in source


def test_globe_map_allows_close_zoom_and_does_not_refocus_unchanged_layers():
    source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "components" / "GlobeMap.vue").read_text(encoding="utf-8")

    assert "const MIN_3D_ZOOM_DISTANCE = 5000;" in source
    assert "minimumZoomDistance = MIN_3D_ZOOM_DISTANCE" in source
    assert "maximumZoomDistance = MAX_ZOOM_DISTANCE" in source
    assert "let lastFocusSignature = '';" in source
    assert "function geometrySignature()" in source
    assert "const shouldRefocus = refocus && signature !== lastFocusSignature;" in source
    assert "if (!shouldRefocus) return;" in source
    assert "return Math.max(300000, Math.min(18000000, height));" in source
    assert "lastFocusSignature = '';" in source
    assert "minimumZoomDistance = 500000" not in source


@pytest.mark.parametrize(
    ("resolution", "grid_type", "expected_level"),
    [
        (5, "s2", 8),
        ("9.9m", "tile_matrix", 8),
        ("10m", "s2", 7),
        (30, "tile_matrix", 7),
        (31, "s2", 6),
        (5, "isea4h", 6),
        ("10m", "isea4h", 6),
        (30, "isea4h", 6),
    ],
)
def test_partition_resolution_grid_level_defaults(resolution, grid_type, expected_level):
    assert default_grid_level_for_resolution(resolution, grid_type=grid_type) == expected_level


def test_partition_schema_rejects_isea4h_logical_payload():
    resp = client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_ISEA4H_LOGICAL_LEVEL",
            "data_type": "optical",
            "normalized_payload": {
                "grid_type": "isea4h",
                "partition_method": "logical",
            },
            "assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/optocal/5m.tif",
                    "scene-5m",
                    resolution=5,
                )
            ],
        },
    )

    assert resp.status_code == 422
    assert "isea4h requires partition_method=entity" in resp.json()["detail"]


def test_config_view_exposes_frozen_partition_grid_types_only():
    source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "views" / "ConfigView.vue").read_text(encoding="utf-8")

    assert '<el-option label="GeoHash格网" value="geohash" />' in source
    assert '<el-option label="MGRS格网" value="mgrs" />' in source
    assert '<el-option label="六边形格网" value="isea4h" />' in source
    assert 'value="s2"' not in source
    assert 'value="tile_matrix"' not in source
    assert 'value="plane_grid"' not in source


def test_encoding_view_exposes_frozen_grid_type_names():
    source = (web_app._repo_root() / "cube_web" / "frontend" / "src" / "views" / "EncodingView.vue").read_text(encoding="utf-8")

    assert "geohash: 'GeoHash格网'" in source
    assert "mgrs: 'MGRS格网'" in source
    assert '<input v-model="division.gridType" type="radio" value="geohash">' in source
    assert '<option value="geohash">GeoHash格网</option>' in source
    assert '<input v-model="topology.gridType" type="radio" value="mgrs">' in source
    assert 'value="s2"' not in source
    assert 'value="tile_matrix"' not in source
    assert "gh:6:wx4g0e:202603091530" in source


def test_root_smoke_endpoint():
    resp = client.get("/")

    assert resp.status_code == 200
    assert resp.json() == {"service": "cube-web", "status": "ok"}


@pytest.mark.parametrize("path", ["/encoding", "/encoding.html", "/config", "/callback"])
def test_backend_does_not_serve_frontend_routes(path):
    resp = client.get(path)
    assert resp.status_code == 404


def test_health_reports_runtime_config_sources():
    resp = client.get("/health")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    values = body["checks"]["config"]["values"]
    assert values["postgres_dsn"]["source"] == "environment"
    assert values["postgres_dsn"]["value"] == "postgresql://***:***@10.3.100.180:15400/postgres"
    assert values["ray_address"]["source"] == "environment"
    assert values["minio_endpoint"]["value"] == "10.3.100.179:9000"
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


def test_auth_config_exposes_subsystem_client(monkeypatch):
    monkeypatch.setenv("CUBE_WEB_ENV_FILE", "/tmp/cube-web-missing-env-file")
    monkeypatch.setenv("CUBE_WEB_AUTH_MAIN_SYSTEM_URL", "http://portal.example.test")
    monkeypatch.setenv("CUBE_WEB_AUTH_CLIENT_ID", "system_ard")
    monkeypatch.setenv("CUBE_WEB_AUTH_REDIRECT_URI", "http://web.example.test/callback")
    monkeypatch.setenv("CUBE_WEB_PORTAL_HOME_URL", "http://portal.example.test/#/home")
    monkeypatch.setenv("CUBE_WEB_PORTAL_PARTITION_SERVICE_URL", "http://portal.example.test/#/partition")
    monkeypatch.setenv("CUBE_WEB_PORTAL_DISPATCH_URL", "http://portal.example.test/#/dispatch")
    monkeypatch.setenv("CUBE_WEB_AUTH_REQUIRED", "0")

    resp = client.get("/api/config")

    assert resp.status_code == 200
    assert resp.json() == {
        "client_id": "system_ard",
        "redirect_uri": "http://web.example.test/callback",
        "main_system_url": "http://portal.example.test",
        "auth_required": False,
        "navigation": [
            {"label": "首页", "kind": "external", "url": "http://portal.example.test/#/home"},
            {"label": "ARD数据载入", "kind": "external", "url": "http://portal.example.test/ard"},
            {"label": "剖分数据服务", "kind": "external", "url": "http://portal.example.test/#/partition"},
            {"label": "资源调度", "kind": "external", "url": "http://portal.example.test/#/dispatch"},
            {"label": "后台管理", "kind": "external", "url": "http://portal.example.test/admin"},
        ],
    }


def test_auth_config_uses_runtime_defaults_when_portal_env_is_empty(monkeypatch):
    monkeypatch.setenv("CUBE_WEB_ENV_FILE", "/tmp/cube-web-missing-env-file")
    monkeypatch.delenv("CUBE_WEB_AUTH_MAIN_SYSTEM_URL", raising=False)
    monkeypatch.delenv("CUBE_WEB_PORTAL_MAIN_URL", raising=False)
    monkeypatch.delenv("CUBE_WEB_PORTAL_HOME_URL", raising=False)
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
                        {"label": "ARD数据载入", "kind": "external", "url": "http://ignored.example.test/ard"},
                        {"label": "剖分数据服务", "kind": "internal", "path": "/partition"},
                    ]
                }
            }
        }
    )
    set_config_store(store)

    resp = client.get("/api/config")

    assert resp.status_code == 200
    assert resp.json()["navigation"] == []


def test_auth_config_exposes_runtime_auth_switch(monkeypatch):
    monkeypatch.setenv("CUBE_WEB_AUTH_REQUIRED", "1")

    resp = client.get("/api/config")

    assert resp.status_code == 200
    assert resp.json()["auth_required"] is True


def test_auth_login_redirects_to_oauth_provider_with_encoded_target(monkeypatch):
    monkeypatch.setenv("CUBE_WEB_AUTH_MAIN_SYSTEM_URL", "http://portal.example.test")
    monkeypatch.setenv("CUBE_WEB_AUTH_CLIENT_ID", "system_ard")
    redirect_uri = "http://web.example.test/callback"

    resp = client.get(
        "/api/auth/login",
        params={"target": "/partition?tab=quality", "redirect_uri": redirect_uri},
        follow_redirects=False,
    )

    assert resp.status_code == 307
    location = resp.headers["location"]
    parsed = urlsplit(location)
    query = parse_qs(parsed.query)
    state = query["state"][0]

    assert f"{parsed.scheme}://{parsed.netloc}{parsed.path}" == "http://portal.example.test/api/authorize"
    assert query["client_id"] == ["system_ard"]
    assert query["redirect_uri"] == [redirect_uri]
    assert auth_service_module.decode_state(state)["target"] == "/partition?tab=quality"
    assert auth_service_module.decode_state(state)["redirect_uri"] == redirect_uri


def test_auth_callback_uses_redirect_uri_from_state(monkeypatch):
    monkeypatch.setenv("CUBE_WEB_AUTH_MAIN_SYSTEM_URL", "http://portal.example.test")
    monkeypatch.setenv("CUBE_WEB_AUTH_CLIENT_ID", "system_ard")
    monkeypatch.setenv("CUBE_WEB_AUTH_CLIENT_SECRET", "ard_secret_abc123")
    captured: dict[str, Any] = {}

    def fake_post_form(url: str, payload: dict[str, Any], token: str | None = None) -> dict[str, Any]:
        captured["url"] = url
        captured["payload"] = payload
        captured["token"] = token
        return {"access_token": "access-token-123", "token_type": "bearer", "expires_in": 1800}

    monkeypatch.setattr(auth_service_module, "_post_form", fake_post_form)
    redirect_uri = "http://web.example.test/callback"
    state = auth_service_module.encode_state("/partition", redirect_uri=redirect_uri)

    resp = client.get("/api/callback", params={"code": "code-123", "state": state})

    assert resp.status_code == 200
    assert resp.json()["access_token"] == "access-token-123"
    assert captured["url"] == "http://portal.example.test/api/exchange_code"
    assert captured["payload"]["redirect_uri"] == redirect_uri
    assert captured["payload"]["code"] == "code-123"


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


def test_auth_required_allows_partition_schema_import_without_bearer(monkeypatch):
    monkeypatch.setenv("CUBE_WEB_AUTH_REQUIRED", "1")

    resp = client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "AUTH_PUBLIC_IMPORT",
            "data_type": "optical",
            "assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/optocal/auth-public-import.tif",
                    "auth-public-import",
                )
            ],
        },
    )

    assert resp.status_code == 200
    assert resp.json()["batch_id"] == "AUTH_PUBLIC_IMPORT"


def test_auth_required_allows_v1_with_valid_bearer(monkeypatch):
    monkeypatch.setenv("CUBE_WEB_AUTH_REQUIRED", "1")
    token = make_jwt({"sub": "u-1", "username": "alice", "exp": time.time() + 3600})

    resp = client.post("/v1/config/get", json={}, headers={"Authorization": f"Bearer {token}"})

    assert resp.status_code == 200
    assert resp.json()["config"]["partition"]["optical"]["grid_type"] == "geohash"


def test_cube_web_imports_encoder_package():
    assert ENCODER_SDK_CLASS.__name__ == "CubeEncoderSDK"


def test_grid_locate_sdk_endpoint():
    resp = client.post("/v1/grid/locate", json={"grid_type": "geohash", "requested_grid_level": 7, "point": [116.391, 39.907]})
    assert resp.status_code == 200
    body = resp.json()
    assert body["cell"]["grid_type"] == "geohash"
    assert body["cell"]["grid_level"] == 7


def test_code_parse_sdk_endpoint():
    locate_resp = client.post("/v1/grid/locate", json={"grid_type": "geohash", "requested_grid_level": 7, "point": [116.391, 39.907]})
    cell = locate_resp.json()["cell"]
    address = {
        "grid_type": cell["grid_type"],
        "grid_level": cell["grid_level"],
        "space_code": cell["space_code"],
        "topology_code": cell["topology_code"],
    }
    gen_resp = client.post("/v1/code/st", json={"address": address, "timestamp": "2026-03-09T15:30:00Z", "time_granularity": "minute"})
    assert gen_resp.status_code == 200
    st_code = gen_resp.json()["st_code"]

    resp = client.post("/v1/code/parse", json={"st_code": st_code})
    assert resp.status_code == 200
    body = resp.json()
    assert body["grid_type"] == "geohash"
    assert body["grid_level"] == 7


def test_spatiotemporal_query_sdk_endpoint_with_bbox(monkeypatch):
    captured = {}

    def fake_query_carbon_observations(**kwargs):
        captured.update(kwargs)
        return [{"observation_id": "obs-1", "space_code": "85230a2ffffffff", "time_bucket": "20201231"}]

    monkeypatch.setattr("cube_web.routes.sdk.query_carbon_observations", fake_query_carbon_observations)

    resp = client.post(
        "/v1/query/st",
        json={
            "data_type": "carbon",
            "bbox": [116.38, 39.90, 116.40, 39.91],
            "time_start": "20201231",
            "time_end": "20210101",
            "grid_type": "isea4h",
            "grid_level": 5,
        },
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["query_mode"] == "bbox"
    assert body["statistics"]["count"] == 1
    assert body["items"][0]["observation_id"] == "obs-1"
    assert captured["bbox"] == [116.38, 39.9, 116.4, 39.91]
    assert captured["time_start"] == "20201231"
    assert captured["time_end"] == "20210101"


def test_spatiotemporal_query_sdk_endpoint_with_point(monkeypatch):
    captured = {}

    def fake_query_carbon_observations(**kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr("cube_web.routes.sdk.query_carbon_observations", fake_query_carbon_observations)

    resp = client.post(
        "/v1/query/st",
        json={
            "data_type": "carbon",
            "point": [116.391, 39.907],
            "time_start": "20201231",
            "time_end": "20201231",
        },
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["query_mode"] == "point"
    assert body["statistics"]["count"] == 0
    bbox = captured["bbox"]
    assert bbox[0] < 116.391 < bbox[2]
    assert bbox[1] < 39.907 < bbox[3]


def test_partition_openapi_exposes_contract_models():
    schema = client.get("/openapi.json").json()

    assert "StrictPartitionRequest" in schema["components"]["schemas"]
    assert "PartitionRetryRequest" in schema["components"]["schemas"]
    assert "PartitionTaskResponse" in schema["components"]["schemas"]
    assert "ConfigResponse" in schema["components"]["schemas"]
    assert "SpatiotemporalQueryRequest" in schema["components"]["schemas"]


def _strict_task_route_client(scheduler: Mock) -> TestClient:
    class FakeWorkflow:
        def submit_strict(self, data_type, request, *, requested_by="operator"):
            return scheduler(data_type, request, requested_by=requested_by)

    route_app = FastAPI()
    production = PartitionService({"optical": PartitionBackend(data_type="optical", run=lambda payload=None: {})})
    route_app.include_router(web_app.partition_route.create_partition_router(service=production, workflow=FakeWorkflow()))
    return TestClient(route_app)


def _scheduled_task(*_args, **_kwargs) -> PartitionTask:
    return PartitionTask(
        task_id="partition-strict",
        status="queued",
        data_type="optical",
        operation="run",
        created_at=0.0,
        updated_at=0.0,
    )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p.pop("batch_id"),
        lambda p: p.pop("partition_method"),
        lambda p: p["datasets"][0].pop("assets"),
        lambda p: p["datasets"][0].pop("bands"),
        lambda p: p.update(grid_level=7),
        lambda p: p.update(grid_level_mode="manual"),
        lambda p: p.update(dataset_ids=["dataset-a"]),
        lambda p: p.update(partition_method="entity"),
        lambda p: p["datasets"][0].update(observations=[]),
    ],
)
def test_partition_tasks_run_rejects_non_contract_bodies(mutate) -> None:
    scheduler = Mock(side_effect=_scheduled_task)
    route_client = _strict_task_route_client(scheduler)
    payload = normalized_task_run_request()
    mutate(payload)

    response = route_client.post("/partition/optical/tasks/run", json=payload)

    assert response.status_code == 422
    scheduler.assert_not_called()


@pytest.mark.parametrize(
    ("grid_type", "level"),
    [
        ("geohash", 0),
        ("geohash", 13),
        ("mgrs", -1),
        ("mgrs", 6),
        ("isea4h", -1),
        ("isea4h", 16),
    ],
)
def test_partition_tasks_run_rejects_requested_grid_level_outside_m1_ranges(grid_type, level) -> None:
    scheduler = Mock(side_effect=_scheduled_task)
    route_client = _strict_task_route_client(scheduler)
    payload = normalized_task_run_request()
    payload.update(grid_type=grid_type, requested_grid_level=level)
    payload["partition_method"] = "entity" if grid_type == "isea4h" else "logical"

    response = route_client.post("/partition/optical/tasks/run", json=payload)

    assert response.status_code == 422
    scheduler.assert_not_called()


@pytest.mark.parametrize(
    ("path", "mutate"),
    [
        ("/partition/unknown/tasks/run", lambda p: None),
        ("/partition/radar/tasks/run", lambda p: None),
        (
            "/partition/optical/tasks/run",
            lambda p: p["datasets"].append({**p["datasets"][0], "dataset_id": "dataset-b", "data_type": "radar"}),
        ),
    ],
)
def test_partition_tasks_run_rejects_path_body_type_mismatches(path, mutate) -> None:
    scheduler = Mock(side_effect=_scheduled_task)
    route_client = _strict_task_route_client(scheduler)
    payload = normalized_task_run_request()
    mutate(payload)

    response = route_client.post(path, json=payload)

    assert response.status_code == 422
    scheduler.assert_not_called()


def test_partition_tasks_run_schedules_exact_normalized_body() -> None:
    scheduler = Mock(side_effect=_scheduled_task)
    route_client = _strict_task_route_client(scheduler)
    payload = normalized_task_run_request()

    response = route_client.post("/partition/optical/tasks/run", json=payload)

    assert response.status_code == 202
    scheduler.assert_called_once_with("optical", StrictPartitionRequest.model_validate(payload), requested_by="operator")


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
    assert body["config"]["ingest"]["optical"]["postgres_batch_size"] == 1000
    assert body["config"]["ingest"]["optical"]["minio_endpoint"] == "10.3.100.179:9000"
    assert body["config"]["ingest"]["optical"]["minio_bucket"] == "cube"
    assert body["runtime"]["postgres_dsn"] == "postgresql://***:***@10.3.100.180:15400/postgres"
    assert body["runtime"]["ray_address"] == "10.3.100.182:6379"
    assert body["runtime"]["minio"] == {
        "endpoint": "10.3.100.179:9000",
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
                        "grid_type": "geohash",
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
    assert body["config"]["partition"]["optical"]["grid_type"] == "geohash"
    assert body["config"]["partition"]["optical"]["grid_level"] == 8
    assert body["config"]["partition"]["optical"]["ray_parallelism"] == 0
    assert body["config"]["partition"]["optical"]["cover_mode"] == "contain"
    assert body["config"]["ingest"]["optical"]["dataset"] == "customer_demo"
    assert body["config"]["quality"]["optical"]["history_limit"] == 50
    assert "runtime" not in body["config"]
    assert "unused" not in body["config"]

    get_resp = client.post("/v1/config/get", json={})
    assert get_resp.json()["config"]["partition"]["optical"]["grid_type"] == "geohash"
    assert "runtime" not in get_resp.json()["config"]


def test_config_update_accepts_mgrs_grid_type():
    resp = client.post(
        "/v1/config/update",
        json={"config": {"partition": {"optical": {"grid_type": "mgrs", "grid_level": 0}}}},
    )

    assert resp.status_code == 200
    assert resp.json()["config"]["partition"]["optical"]["grid_type"] == "mgrs"
    assert resp.json()["config"]["partition"]["optical"]["grid_level"] == 0


def test_config_update_rejects_legacy_plane_grid_type():
    resp = client.post(
        "/v1/config/update",
        json={"config": {"partition": {"optical": {"grid_type": "plane_grid", "grid_level": 5}}}},
    )

    assert resp.status_code == 422


def test_config_update_accepts_unlimited_max_cells_per_asset():
    resp = client.post(
        "/v1/config/update",
        json={"config": {"partition": {"optical": {"max_cells_per_asset": 0}}}},
    )

    assert resp.status_code == 200
    assert resp.json()["config"]["partition"]["optical"]["max_cells_per_asset"] == 0


def test_config_update_rejects_legacy_tile_matrix_grid_type():
    resp = client.post(
        "/v1/config/update",
        json={"config": {"partition": {"optical": {"grid_type": "tile_matrix", "grid_level": 5}}}},
    )

    assert resp.status_code == 422
    assert "grid_type" in resp.json()["detail"]


def test_stored_config_rejects_legacy_grid_type():
    with pytest.raises(ValueError, match="grid_type"):
        config_store_module.normalized_stored_config({"partition": {"optical": {"grid_type": "s2", "grid_level": 5}}})


def test_default_partition_config_has_no_source_conversion_controls():
    config = config_store_module.default_config()

    assert "cog_compress" not in config["partition"]["optical"]
    assert "cog_predictor" not in config["partition"]["optical"]


def test_config_update_rejects_invalid_values():
    resp = client.post("/v1/config/update", json={"config": {"partition": {"optical": {"grid_level": 0}}}})

    assert resp.status_code == 422
    assert "grid_level" in resp.json()["detail"]


def test_config_update_rejects_legacy_contains_cover_mode():
    resp = client.post("/v1/config/update", json={"config": {"partition": {"optical": {"cover_mode": "contains"}}}})

    assert resp.status_code == 422
    assert "cover_mode" in resp.json()["detail"]


def test_partition_demo_compatibility_adapter_keeps_legacy_payload_untyped():
    legacy = PartitionService(
        {
            "optical": PartitionBackend(
                data_type="optical",
                demo=lambda payload=None: {"status": "completed", "payload": payload or {}},
            )
        }
    )
    route_app = FastAPI()
    route_app.include_router(web_app.partition_route.create_partition_router(legacy_service=legacy))

    response = TestClient(route_app).post(
        "/partition/optical/demo",
        json={"grid_type": "s2", "grid_level": 0, "cover_mode": "contains"},
    )

    assert response.status_code == 200
    assert response.json()["payload"]["cover_mode"] == "contains"


def test_carbon_partition_demo_endpoint(monkeypatch):
    def fake_run_carbon_partition_demo():
        return {
            "status": "completed",
            "mode": "partition_demo",
            "data_type": "carbon",
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
    assert body["data_type"] == "carbon"
    assert body["rows"] == 12
    assert body["distinct_space_codes"] == 5


def test_carbon_partition_test_endpoint(monkeypatch):
    expected_payload = {"grid_type": "isea4h", "grid_level": 5}

    def fake_run_carbon_partition_test(payload=None):
        assert payload == expected_payload
        return {
            "status": "completed",
            "mode": "partition_test_no_ingest",
            "data_type": "carbon",
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
    assert body["data_type"] == "carbon"
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
            "data_type": "carbon",
            "ingest_enabled": mode != "partition_test_no_ingest",
        }

    monkeypatch.setattr(partition_runners, "_run_carbon_partition_demo", fake_run_carbon_partition_demo)

    result = partition_runners._run_carbon_partition_test({"selected_observations": [{"source_index": 3}]})

    assert captured["mode"] == "partition_test_no_ingest"
    assert captured["payload"]["selected_observations"][0]["source_index"] == 3
    assert result["data_type"] == "carbon"
    assert result["mode"] == "partition_test_no_ingest"
    assert result["ingest_enabled"] is False


def test_carbon_partition_demo_runner_does_not_claim_ingest_enabled(monkeypatch, tmp_path):
    source_dir = tmp_path / "carbon-source"
    source_dir.mkdir()
    rows_path = tmp_path / "run-root" / "output" / "rows.jsonl"
    rows_path.parent.mkdir(parents=True)
    rows_path.write_text('{"space_code":"cell-1","quality_flag":"PASS"}\n', encoding="utf-8")

    def fake_partition_run_dir(data_type, mode):
        run_dir = tmp_path / "run-root"
        run_dir.mkdir(exist_ok=True)
        return run_dir

    def fake_run_carbon_partition(args):
        assert Path(args.input_dir) == source_dir
        return {
            "run_dir": str(rows_path.parent),
            "rows_path": str(rows_path),
            "rows": 1,
            "grid_type": args.grid_type,
            "grid_level": args.grid_level,
            "partition_backend_used": "ray",
            "execution_engine": "ray",
            "ray_address": args.ray_address,
        }

    monkeypatch.setattr(partition_runners, "_partition_run_dir", fake_partition_run_dir)
    monkeypatch.setattr("cube_split.jobs.carbon_partition_job.run_carbon_partition", fake_run_carbon_partition)
    monkeypatch.setattr(
        "cube_split.ingest.carbon_ingest_job.run_carbon_ingest",
        lambda args: (_ for _ in ()).throw(AssertionError("demo should not ingest")),
    )

    result = partition_runners._run_carbon_partition_demo(payload={"input_dir": str(source_dir)})

    assert result["data_type"] == "carbon"
    assert result["ingest_enabled"] is False
    assert result["rows"] == 1


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"grid_type": "s2", "grid_level": 5}, "grid_type must be one of"),
        ({"grid_type": "mgrs", "grid_level": 6}, "requested_grid_level"),
        ({"grid_type": "isea4h", "grid_level": 5, "partition_method": "logical"}, "requires partition_method=entity"),
    ],
)
def test_carbon_partition_runner_rejects_unfrozen_grid_contract(payload, message):
    with pytest.raises(ValueError, match=message):
        partition_runners._run_carbon_partition_demo(payload=payload)


def test_carbon_partition_run_ingests_rows(monkeypatch, tmp_path):
    source_dir = tmp_path / "carbon-source"
    source_dir.mkdir()
    rows_path = tmp_path / "run-root" / "output" / "carbon_observation_rows.jsonl"
    rows_path.parent.mkdir(parents=True)
    rows_path.write_text('{"space_code":"cell-1","quality_flag":"PASS"}\n', encoding="utf-8")
    captured = {}

    def fake_partition_run_dir(data_type, mode):
        run_dir = tmp_path / "run-root"
        run_dir.mkdir(exist_ok=True)
        return run_dir

    def fake_run_carbon_partition(args):
        return {
            "run_dir": str(rows_path.parent),
            "rows_path": str(rows_path),
            "rows": 1,
            "grid_type": args.grid_type,
            "grid_level": args.grid_level,
            "partition_backend_used": "ray",
            "execution_engine": "ray",
            "ray_address": args.ray_address,
        }

    def fake_run_carbon_ingest(args):
        captured.update(vars(args))
        return {"input_rows": 1, "carbon_fact_rows": 1, "metadata_backend": args.metadata_backend}

    monkeypatch.setattr(partition_runners, "_partition_run_dir", fake_partition_run_dir)
    monkeypatch.setattr("cube_split.jobs.carbon_partition_job.run_carbon_partition", fake_run_carbon_partition)
    monkeypatch.setattr("cube_split.ingest.carbon_ingest_job.run_carbon_ingest", fake_run_carbon_ingest)

    result = partition_runners._run_carbon_partition_demo(
        mode="partition_run",
        payload={
            "input_dir": str(source_dir),
            "batch_id": "CARBON_RUN_INGEST",
            "cube_version": "carbon-v1",
            "metadata_backend": "postgres",
        },
    )

    assert result["ingest_enabled"] is True
    assert result["ingest_stats"]["carbon_fact_rows"] == 1
    assert captured["job_id"] == "CARBON_RUN_INGEST"
    assert captured["rows_path"] == str(rows_path)
    assert captured["cube_version"] == "carbon-v1"
    assert captured["metadata_backend"] == "postgres"


def test_carbon_partition_retry_runner_reuses_request_payload(monkeypatch):
    captured = {}

    def fake_run_carbon_partition_demo(mode="partition_demo", payload=None):
        captured["mode"] = mode
        captured["payload"] = payload
        return {
            "status": "completed",
            "mode": mode,
            "data_type": "carbon",
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

    assert captured["mode"] == "partition_retry"
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


def test_carbon_selected_source_indexes_are_ignored_for_source_uri_payload():
    payload = {
        "selected_observations": [
            {"source_uri": "s3://cube/cube/source/carbon/obs-a.nc4", "source_index": 0},
            {"source_uri": "s3://cube/cube/source/carbon/obs-b.nc4", "source_index": 0},
        ]
    }

    assert partition_runners._carbon_selected_source_indexes(payload) is None


def test_carbon_source_uris_are_normalized():
    payload = {
        "source_uri": "s3://cube/cube/source/carbon/top.nc4",
        "selected_observations": [
            {"source_uri": "s3://cube/cube/source/carbon/top.nc4"},
            {"source_uri": "s3://cube/cube/source/carbon/extra.nc4"},
            {"source_uri": ""},
            {},
        ],
    }

    assert partition_runners._carbon_source_uris(payload) == (
        "s3://cube/cube/source/carbon/top.nc4",
        "s3://cube/cube/source/carbon/extra.nc4",
    )


def test_carbon_partition_demo_runner_passes_source_uris_from_payload(monkeypatch, tmp_path):
    rows_path = tmp_path / "run-root" / "output" / "rows.jsonl"
    rows_path.parent.mkdir(parents=True)
    rows_path.write_text('{"space_code":"cell-1","quality_flag":"PASS"}\n', encoding="utf-8")
    captured = {}

    def fake_partition_run_dir(data_type, mode):
        run_dir = tmp_path / "run-root"
        run_dir.mkdir(exist_ok=True)
        return run_dir

    def fake_run_carbon_partition(args):
        captured["input_dir"] = Path(args.input_dir)
        captured["source_uris"] = args.source_uris
        return {
            "run_dir": str(rows_path.parent),
            "rows_path": str(rows_path),
            "rows": 1,
            "grid_type": args.grid_type,
            "grid_level": args.grid_level,
            "partition_backend_used": "ray",
            "execution_engine": "ray",
            "ray_address": args.ray_address,
        }

    monkeypatch.setattr(partition_runners, "_partition_run_dir", fake_partition_run_dir)
    monkeypatch.setattr("cube_split.jobs.carbon_partition_job.run_carbon_partition", fake_run_carbon_partition)
    monkeypatch.setattr(
        "cube_split.ingest.carbon_ingest_job.run_carbon_ingest",
        lambda args: {"input_rows": 1, "carbon_fact_rows": 1, "metadata_backend": "postgres"},
    )

    result = partition_runners._run_carbon_partition_demo(payload={"source_uri": "s3://cube/cube/source/carbon/oco2.nc4"})

    assert result["data_type"] == "carbon"
    assert captured["input_dir"] == tmp_path / "run-root" / "input"
    assert captured["source_uris"] == ("s3://cube/cube/source/carbon/oco2.nc4",)


def test_carbon_partition_run_accepts_selected_observation_source_uris(monkeypatch, tmp_path):
    rows_path = tmp_path / "run-root" / "output" / "rows.jsonl"
    rows_path.parent.mkdir(parents=True)
    rows_path.write_text('{"space_code":"cell-1","quality_flag":"PASS"}\n', encoding="utf-8")
    captured = {}

    def fake_partition_run_dir(data_type, mode):
        run_dir = tmp_path / "run-root"
        run_dir.mkdir(exist_ok=True)
        return run_dir

    def fake_run_carbon_partition(args):
        captured["source_uris"] = args.source_uris
        return {
            "run_dir": str(rows_path.parent),
            "rows_path": str(rows_path),
            "rows": 1,
            "grid_type": args.grid_type,
            "grid_level": args.grid_level,
            "partition_backend_used": "ray",
            "execution_engine": "ray",
            "ray_address": args.ray_address,
        }

    monkeypatch.setattr(partition_runners, "_partition_run_dir", fake_partition_run_dir)
    monkeypatch.setattr("cube_split.jobs.carbon_partition_job.run_carbon_partition", fake_run_carbon_partition)
    monkeypatch.setattr(
        "cube_split.ingest.carbon_ingest_job.run_carbon_ingest",
        lambda args: {"input_rows": 1, "carbon_fact_rows": 1, "metadata_backend": "postgres"},
    )

    result = partition_runners._run_carbon_partition_demo(
        mode="partition_run",
        payload={
            "selected_observations": [
                {"source_uri": "s3://cube/cube/source/carbon/obs-a.nc4", "source_index": 0},
                {"source_uri": "s3://cube/cube/source/carbon/obs-b.nc4", "source_index": 0},
            ]
        },
    )

    assert result["data_type"] == "carbon"
    assert captured["source_uris"] == (
        "s3://cube/cube/source/carbon/obs-a.nc4",
        "s3://cube/cube/source/carbon/obs-b.nc4",
    )


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
                    "grid_type": "geohash",
                    "grid_level": 9,
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

    result = partition_runners._run_optical_partition_from_payload(
        {"input_dir": str(tmp_path), "grid_level": 4, "cube_version": "cube-smoke"},
        mode="partition_test_no_ingest",
    )

    assert result["mode"] == "partition_test_no_ingest"
    assert captured["grid_type"] == "geohash"
    assert captured["grid_level"] == 4
    assert "target_crs" not in captured
    assert captured["partition_backend"] == "thread"
    assert captured["ray_parallelism"] == 0
    assert captured["cube_version"] == "cube-smoke"
    assert captured["quality_rule"] == "best_quality_wins"
    assert captured["db_path"] == ""
    assert captured["postgres_batch_size"] == 1000
    assert captured["cog_materialize_mode"] == "copy"
    assert captured["cog_output_root"].endswith("optical_cog_store")


def test_optical_partition_runner_preserves_e2e_performance_params(monkeypatch, tmp_path):
    captured = {}

    def fake_run_logical_partition(args):
        captured.update(vars(args))
        run_dir = tmp_path / "run"
        run_dir.mkdir()
        rows_path = run_dir / "index_rows.jsonl"
        rows_path.write_text("", encoding="utf-8")
        return {
            "run_dir": str(run_dir),
            "rows_path": str(rows_path),
            "execution_engine": args.partition_backend,
            "total_index_rows": 0,
            "ray_parallelism": args.ray_parallelism,
            "ingest_enabled": False,
        }

    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job.run_logical_partition", fake_run_logical_partition)

    partition_runners._run_optical_partition_from_payload(
        {
            "input_dir": str(tmp_path),
            "grid_type": "geohash",
            "grid_level": 5,
            "partition_backend": "thread",
            "ray_parallelism": 6,
            "chunk_size": 3,
            "max_cells_per_asset": 50,
            "partition_prefix_len": 4,
            "minio_upload_workers": 11,
            "postgres_batch_size": 250,
        },
        mode="partition_run",
    )

    assert captured["partition_backend"] == "thread"
    assert captured["ray_parallelism"] == 6
    assert captured["chunk_size"] == 3
    assert captured["max_cells_per_asset"] == 50
    assert captured["partition_prefix_len"] == 4
    assert captured["minio_upload_workers"] == 11
    assert captured["postgres_batch_size"] == 250


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

    partition_runners._run_optical_partition_from_payload(
        {
            "input_dir": str(tmp_path),
            "selected_assets": [ard_raster_asset("s3://cube/cube/source/optocal/scene_10m.tif", "scene-10m", resolution="10m")],
        },
        mode="partition_test_no_ingest",
    )

    assert captured["grid_type"] == "geohash"
    assert captured["grid_level"] == 7


def test_optical_partition_runner_allows_remote_selected_assets_without_existing_input_dir(monkeypatch, tmp_path):
    captured = {}

    def fake_run_logical_partition(args):
        captured.update(vars(args))
        run_dir = tmp_path / "run"
        run_dir.mkdir()
        return {
            "run_dir": str(run_dir),
            "execution_engine": args.partition_backend,
            "total_index_rows": 1,
            "ray_parallelism": args.ray_parallelism,
        }

    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job.run_logical_partition", fake_run_logical_partition)

    result = partition_runners._run_optical_partition_from_payload(
        {
            "input_dir": str(tmp_path / "missing"),
            "selected_assets": [ard_raster_asset("s3://cube/cube/source/optocal/remote-optical.tif", "remote-optical")],
            "partition_backend": "ray",
            "grid_type": "geohash",
            "grid_level": 5,
        },
        mode="partition_run",
    )

    assert result["mode"] == "partition_run"
    assert Path(captured["input_dir"]).name == "input"
    assert Path(captured["input_dir"]).exists()
    manifest = json.loads(Path(captured["manifest_path"]).read_text(encoding="utf-8"))
    assert manifest["assets"][0]["source_uri"] == "s3://cube/cube/source/optocal/remote-optical.tif"


def test_product_partition_runner_allows_remote_selected_assets_without_existing_input_dir(monkeypatch, tmp_path):
    captured = {}

    def fake_run_product_partition(args):
        captured.update(vars(args))
        run_dir = tmp_path / "run"
        run_dir.mkdir()
        rows_path = run_dir / "index_rows.jsonl"
        rows_path.write_text("", encoding="utf-8")
        return {
            "run_dir": str(run_dir),
            "rows_path": str(rows_path),
            "execution_engine": args.partition_backend,
            "total_index_rows": 1,
            "ray_parallelism": args.ray_parallelism,
            "ingest_enabled": True,
        }

    monkeypatch.setattr("cube_split.jobs.product_partition_job.run_product_partition", fake_run_product_partition)

    result = partition_runners._run_product_partition_demo(
        {
            "input_dir": str(tmp_path / "missing"),
            "selected_assets": [
                ard_raster_asset("s3://cube/cube/source/product/remote-product.tif", "remote-product", data_type="product")
            ],
            "partition_backend": "ray",
            "grid_type": "geohash",
            "grid_level": 5,
        },
        mode="partition_run",
    )

    assert result["mode"] == "partition_run"
    assert Path(captured["input_dir"]).name == "input"
    assert Path(captured["input_dir"]).exists()
    manifest = json.loads(Path(captured["manifest_path"]).read_text(encoding="utf-8"))
    assert manifest["assets"][0]["source_uri"] == "s3://cube/cube/source/product/remote-product.tif"


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

    result = partition_runners._run_optical_partition_from_payload(
        {
            "input_dir": str(tmp_path),
            "grid_type": "isea4h",
            "grid_level": 9,
        },
        mode="partition_test_no_ingest",
    )

    assert result["status"] == "completed"
    assert result["partition_type"] == "entity"
    assert result["output_path"].endswith("entity_index_rows.jsonl")
    assert captured["grid_type"] == "isea4h"
    assert captured["grid_level"] == 9
    assert "target_pixels_per_hex_edge" not in captured
    assert captured["ray_parallelism"] == 0


def test_optical_partition_runner_rejects_inconsistent_isea4h_method(tmp_path):
    with pytest.raises(ValueError, match="isea4h requires partition_method=entity"):
        partition_runners._run_optical_partition_from_payload(
            {
                "input_dir": str(tmp_path),
                "grid_type": "isea4h",
                "grid_level": 5,
                "partition_method": "logical",
            },
            mode="partition_test_no_ingest",
        )


def test_optical_partition_runner_rejects_inconsistent_geohash_method(tmp_path):
    with pytest.raises(ValueError, match="geohash requires partition_method=logical"):
        partition_runners._run_optical_partition_from_payload(
            {
                "input_dir": str(tmp_path),
                "grid_type": "geohash",
                "grid_level": 5,
                "partition_method": "entity",
            },
            mode="partition_test_no_ingest",
        )


def test_optical_partition_runner_rejects_legacy_grid_type(tmp_path):
    with pytest.raises(ValueError, match="grid_type must be one of"):
        partition_runners._run_optical_partition_from_payload(
            {"input_dir": str(tmp_path), "grid_type": "s2", "grid_level": 6},
            mode="partition_test_no_ingest",
        )


def test_optical_partition_runner_dispatches_geohash_to_logical_partition(monkeypatch, tmp_path):
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
        raise AssertionError("geohash optical partition should use logical partition")

    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job.run_logical_partition", fake_run_logical_partition)
    monkeypatch.setattr("cube_split.jobs.entity_partition_job.run_entity_partition", fail_entity_partition)

    result = partition_runners._run_optical_partition_from_payload(
        {
            "input_dir": str(tmp_path),
            "grid_type": "geohash",
            "grid_level": 5,
            "partition_backend": "thread",
        },
        mode="partition_test_no_ingest",
    )

    assert result["status"] == "completed"
    assert captured["grid_type"] == "geohash"
    assert captured["grid_level"] == 5
    assert result["output_path"].endswith("index_rows.jsonl")


def test_optical_partition_runner_rejects_legacy_plane_grid(tmp_path):
    with pytest.raises(ValueError, match="grid_type must be one of"):
        partition_runners._run_optical_partition_from_payload(
            {"input_dir": str(tmp_path), "grid_type": "plane_grid", "grid_level": 11},
            mode="partition_test_no_ingest",
        )


@pytest.mark.parametrize("grid_level", [0, 7])
def test_optical_partition_runner_allows_manual_isea4h_level(monkeypatch, tmp_path, grid_level):
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

    partition_runners._run_optical_partition_from_payload(
        {
            "input_dir": str(tmp_path),
            "grid_type": "isea4h",
            "grid_level": grid_level,
        },
        mode="partition_test_no_ingest",
    )

    assert captured["grid_level"] == grid_level


def test_optical_partition_test_runner_passes_none_for_auto_isea4h_level(monkeypatch, tmp_path):
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

    partition_runners._run_optical_partition_from_payload(
        {
            "input_dir": str(tmp_path),
            "grid_type": "isea4h",
        },
        mode="partition_test_no_ingest",
    )

    assert captured["grid_level"] == 6


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
    assert captured["grid_level"] == 6
    assert captured["partition_backend"] == "ray"
    assert captured["ray_address"] == "10.3.100.182:6379"
    assert captured["metadata_backend"] == "postgres"
    assert captured["asset_storage_backend"] == "minio"
    assert captured["ingest_enabled"] is False


def test_partition_run_accepts_mgrs_grid_type_in_request_model():
    route_app = FastAPI()
    production = PartitionService(
        {
            "optical": PartitionBackend(
                data_type="optical",
                run=lambda payload=None: {"status": "completed", "source": "production", "payload": payload or {}},
            )
        }
    )
    route_app.include_router(web_app.partition_route.create_partition_router(service=production))
    route_client = TestClient(route_app)

    resp = route_client.post("/partition/optical/run", json={"grid_type": "mgrs", "grid_level": 5})

    assert resp.status_code == 200
    assert resp.json()["payload"]["grid_type"] == "mgrs"


def test_partition_run_uses_workflow_persistence(monkeypatch):
    captured = {}

    class FakeWorkflow:
        def run_payload_sync(self, data_type, payload=None, *, requested_by="operator", timeout_seconds=None):
            captured["data_type"] = data_type
            captured["payload"] = payload
            captured["requested_by"] = requested_by
            captured["timeout_seconds"] = timeout_seconds
            return {"status": "completed", "mode": "partition_run", "data_type": data_type, "rows": 1}

    route_app = FastAPI()
    production = PartitionService(
        {
            "optical": PartitionBackend(
                data_type="optical",
                run=lambda payload=None: {"status": "completed", "source": "production", "payload": payload or {}},
            )
        }
    )
    route_app.include_router(web_app.partition_route.create_partition_router(service=production, workflow=FakeWorkflow()))
    route_client = TestClient(route_app)

    resp = route_client.post("/partition/optical/run", json={"grid_type": "mgrs", "grid_level": 5})

    assert resp.status_code == 200
    assert resp.json()["mode"] == "partition_run"
    assert captured["data_type"] == "optical"
    assert captured["requested_by"] == "operator"
    assert captured["payload"]["grid_type"] == "mgrs"


def test_partition_run_response_is_result_payload_not_internal_task_fields():
    route_app = FastAPI()
    production = PartitionService(
        {
            "optical": PartitionBackend(
                data_type="optical",
                run=lambda payload=None: {"status": "completed", "source": "production", "payload": payload or {}},
            )
        }
    )
    route_app.include_router(web_app.partition_route.create_partition_router(service=production))
    route_client = TestClient(route_app)

    resp = route_client.post("/partition/optical/run", json={"grid_type": "mgrs", "grid_level": 5})

    assert resp.status_code == 200
    body = resp.json()
    assert "task_id" not in body
    assert body["status"] == "completed"
    assert body["source"] == "production"


def test_partition_task_detail_reads_persisted_attempt_without_memory_task():
    store = InMemoryPartitionJobStore()
    store.upsert_schema(
        {
            "batch_id": "PERSISTED_TASK_DETAIL",
            "batch_name": "Persisted task detail",
            "data_type": "product",
            "assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/product/persisted-detail.tif",
                    "persisted-detail-scene",
                    data_type="product",
                    asset_id="persisted-detail",
                )
            ],
        }
    )
    task_id = "partition-persisted-detail"
    store.create_attempt(task_id=task_id, batch_id="PERSISTED_TASK_DETAIL", operation="auto_run", payload={})
    store.succeed_attempt(task_id, {"status": "completed", "data_type": "product", "rows": 3})
    service = PartitionService({"product": PartitionBackend(data_type="product", run=lambda payload=None: {})})
    workflow = PartitionWorkflowService(service, store=store)
    route_app = FastAPI()
    route_app.include_router(web_app.partition_route.create_partition_router(service=service, workflow=workflow))
    route_client = TestClient(route_app)

    task_resp = route_client.get(f"/partition/tasks/{task_id}")

    assert task_resp.status_code == 200
    task = task_resp.json()
    assert task["task_id"] == task_id
    assert task["status"] == "completed"
    assert task["data_type"] == "product"
    assert task["operation"] == "run"
    assert task["result"]["rows"] == 3
    assert isinstance(task["created_at"], float)
    assert isinstance(task["updated_at"], float)


def test_partition_task_cancel_uses_persisted_attempt_when_memory_task_missing():
    store = InMemoryPartitionJobStore()
    store.upsert_schema(
        {
            "batch_id": "PERSISTED_TASK_CANCEL",
            "batch_name": "Persisted task cancel",
            "data_type": "product",
            "assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/product/persisted-cancel.tif",
                    "persisted-cancel-scene",
                    data_type="product",
                    asset_id="persisted-cancel",
                )
            ],
        }
    )
    task_id = "partition-persisted-cancel"
    store.create_attempt(
        task_id=task_id,
        batch_id="PERSISTED_TASK_CANCEL",
        operation="auto_run",
        payload={"partition_backend": "thread"},
    )
    store.start_attempt(task_id)
    service = PartitionService({"product": PartitionBackend(data_type="product", run=lambda payload=None: {})})
    workflow = PartitionWorkflowService(service, store=store)
    route_app = FastAPI()
    route_app.include_router(web_app.partition_route.create_partition_router(service=service, workflow=workflow))
    route_client = TestClient(route_app)

    cancel_resp = route_client.post(f"/partition/tasks/{task_id}/cancel")

    assert cancel_resp.status_code == 200
    assert cancel_resp.json()["status"] == "cancelled"
    assert store.get_attempt(task_id)["status"] == "cancelled"


def test_partition_task_cancel_stops_remote_ray_job_when_memory_task_missing(monkeypatch):
    store = InMemoryPartitionJobStore()
    store.supports_remote_jobs = True
    store.upsert_schema(
        {
            "batch_id": "PERSISTED_TASK_CANCEL_RAY",
            "batch_name": "Persisted task cancel ray",
            "data_type": "product",
            "assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/product/persisted-cancel-ray.tif",
                    "persisted-cancel-ray-scene",
                    data_type="product",
                    asset_id="persisted-cancel-ray",
                )
            ],
        }
    )
    task_id = "partition-persisted-cancel-ray"
    store.create_attempt(
        task_id=task_id,
        batch_id="PERSISTED_TASK_CANCEL_RAY",
        operation="auto_run",
        payload={"partition_backend": "ray", "ray_address": "10.3.100.182:6379"},
    )
    store.start_attempt(task_id)
    service = PartitionService({"product": PartitionBackend(data_type="product", run=lambda payload=None: {})})
    workflow = PartitionWorkflowService(service, store=store)

    stopped = {}

    class FakeRayClient:
        def stop_job(self, job_id):
            stopped["job_id"] = job_id
            return True

        def get_job_status(self, job_id):
            stopped["status_job_id"] = job_id
            return "STOPPED"

    monkeypatch.setattr("cube_web.services.partition_workflow._build_ray_job_client", lambda address: FakeRayClient())

    result = workflow.cancel_task(task_id)

    assert stopped["job_id"] == task_id
    assert result["status"] == "cancelled"
    assert store.get_attempt(task_id)["status"] == "cancelled"


def test_partition_workflow_throttles_cancellation_checks(monkeypatch):
    class CountingStore(InMemoryPartitionJobStore):
        def __init__(self):
            super().__init__()
            self.cancel_check_count = 0

        def is_cancel_requested(self, task_id: str) -> bool:
            self.cancel_check_count += 1
            return False

    store = CountingStore()
    service = PartitionService(
        {
            "product": PartitionBackend(
                data_type="product",
                run=lambda payload=None: {
                    "status": "completed",
                    "data_type": "product",
                    "rows": sum(1 for _ in range(5) if payload["cancellation_check"]() is False),
                },
            )
        }
    )
    workflow = PartitionWorkflowService(service, store=store)
    monkeypatch.setattr("cube_web.services.partition_workflow.time.monotonic", lambda: 100.0)

    task = workflow.run_payload(
        "product",
        {
            "batch_id": "THROTTLED_CANCEL_CHECKS",
            "batch_name": "Throttled cancel checks",
            "grid_type": "geohash",
            "grid_level": 5,
            "selected_assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/product/throttled-cancel.tif",
                    "throttled-cancel",
                    data_type="product",
                    asset_id="throttled-cancel",
                )
            ],
        },
    )

    task_state = None
    for _ in range(20):
        task_state = workflow.get_task(task.task_id)
        if task_state.status == "completed":
            break
        time.sleep(0.01)

    assert task_state is not None
    assert task_state.status == "completed"
    assert task_state.result["rows"] == 5
    assert store.cancel_check_count == 1


def test_partition_workflow_direct_run_uses_stored_carbon_source_uris():
    store = InMemoryPartitionJobStore()
    store.upsert_schema(
        {
            "batch_id": "CARBON_DIRECT_SOURCE_URI",
            "batch_name": "Carbon direct source uri",
            "data_type": "carbon",
            "observations": [
                {
                    **ard_carbon_observation("carbon-direct-obs"),
                    "source_uri": "s3://cube/cube/source/carbon/stored.nc4",
                }
            ],
        }
    )
    captured = {}
    service = PartitionService(
        {
            "carbon": PartitionBackend(
                data_type="carbon",
                run=lambda payload=None: captured.setdefault("payload", payload or {}) or {"status": "completed"},
            )
        }
    )
    workflow = PartitionWorkflowService(service, store=store)

    task = workflow.run_payload(
        "carbon",
        {
            "batch_id": "CARBON_DIRECT_SOURCE_URI",
            "batch_name": "Carbon direct source uri",
            "grid_type": "isea4h",
            "grid_level": 1,
            "selected_observations": [
                {
                    "observation_id": "carbon-direct-obs",
                    "source_uri": "",
                }
            ],
        },
    )

    for _ in range(20):
        if workflow.get_task(task.task_id).status == "completed":
            break
        time.sleep(0.01)

    assert captured["payload"]["selected_observations"][0]["source_uri"] == "s3://cube/cube/source/carbon/stored.nc4"
    assert store.get_attempt(task.task_id)["payload"]["selected_observations"][0]["source_uri"] == (
        "s3://cube/cube/source/carbon/stored.nc4"
    )


def test_partition_workflow_matches_assets_by_source_uri_before_scene_id():
    store = InMemoryPartitionJobStore()
    store.upsert_schema(
        {
            "batch_id": "OPTICAL_SAME_SCENE_DIFFERENT_BANDS",
            "batch_name": "Optical same scene different bands",
            "data_type": "optical",
            "assets": [
                ard_raster_asset("s3://cube/cube/source/optocal/scene_band3.tif", "same-scene", band="sr_band3"),
                ard_raster_asset("s3://cube/cube/source/optocal/scene_band4.tif", "same-scene", band="sr_band4"),
            ],
        }
    )
    workflow = PartitionWorkflowService(PartitionService({}), store=store)
    assets_by_uri = {asset["source_uri"]: asset["asset_id"] for asset in store.list_assets("OPTICAL_SAME_SCENE_DIFFERENT_BANDS")}

    asset_ids = workflow._selected_asset_ids_for_payload(
        "OPTICAL_SAME_SCENE_DIFFERENT_BANDS",
        "optical",
        {
            "selected_assets": [
                {"source_uri": "s3://cube/cube/source/optocal/scene_band4.tif", "scene_id": "same-scene"},
                {"source_uri": "s3://cube/cube/source/optocal/scene_band3.tif", "scene_id": "same-scene"},
            ]
        },
    )

    assert asset_ids == [
        assets_by_uri["s3://cube/cube/source/optocal/scene_band4.tif"],
        assets_by_uri["s3://cube/cube/source/optocal/scene_band3.tif"],
    ]


def test_partition_workflow_classifies_source_missing_before_transient():
    error = "temporary fetch wrapper: S3 operation failed; code: NoSuchKey, message: Object does not exist"
    assert classify_partition_error(error) == "source_missing"


def test_partition_task_queue_paginates_and_validates_page_size():
    resp = client.get("/v1/partition/tasks", params={"page": 1, "page_size": 2})
    assert resp.status_code == 200
    body = resp.json()
    assert set(body) >= {"tasks", "total", "page", "page_size"}
    assert body["page"] == 1
    assert body["page_size"] == 2
    assert isinstance(body["total"], int)

    assert client.get("/v1/partition/tasks", params={"limit": 0}).status_code == 422
    assert client.get("/v1/partition/tasks", params={"limit": 501}).status_code == 200
    assert client.get("/v1/partition/tasks", params={"page": 0}).status_code == 422
    assert client.get("/v1/partition/tasks", params={"page_size": 0}).status_code == 422
    assert client.get("/v1/partition/tasks", params={"page_size": 501}).status_code == 422


def test_postgres_runtime_batch_refresh_resets_changed_assets_and_deletes_stale(monkeypatch):
    executed = []

    class FakeCursor:
        description = [
            ("batch_id",),
            ("batch_name",),
            ("data_type",),
            ("source_system",),
            ("source_schema",),
            ("normalized_payload",),
        ]

        def execute(self, sql, params=None):
            executed.append((sql, params))

        def fetchone(self):
            return (
                "PG_RUNTIME_REFRESH",
                "Postgres runtime refresh",
                "product",
                "runtime",
                {},
                {},
            )

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    store = partition_job_store_module.PostgresPartitionJobStore("postgresql://example")
    monkeypatch.setattr(store, "ensure_schema", lambda: None)
    monkeypatch.setattr(store, "_connect", lambda: FakeConnection())
    monkeypatch.setattr(store, "_jsonb", lambda value: value)

    store.ensure_runtime_batch(
        batch_id="PG_RUNTIME_REFRESH",
        batch_name="Postgres runtime refresh",
        data_type="product",
        payload={
            "selected_assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/product/pg-refresh.tif",
                    "pg-refresh",
                    data_type="product",
                    asset_id="pg-refresh",
                )
            ]
        },
    )

    sql_text = "\n".join(sql for sql, _params in executed)
    delete_params = [params for sql, params in executed if "DELETE FROM partition_assets" in sql][0]

    assert "target.asset_payload = source.asset_payload" in sql_text
    assert "THEN target.status" in sql_text
    assert "ELSE 'pending'" in sql_text
    assert "ELSE 0" in sql_text
    assert "ELSE NULL" in sql_text
    assert "NOT (asset_id = ANY(%s::text[]))" in sql_text
    assert delete_params == ("PG_RUNTIME_REFRESH", ["pg-refresh"])


def test_postgres_runtime_batch_preserves_existing_normalized_payload_for_non_runtime_batches(monkeypatch):
    executed = []

    class FakeCursor:
        description = [
            ("batch_id",),
            ("batch_name",),
            ("data_type",),
            ("source_system",),
            ("source_schema",),
            ("normalized_payload",),
        ]

        def execute(self, sql, params=None):
            executed.append((sql, params))

        def fetchone(self):
            return (
                "PG_NON_RUNTIME",
                "Non runtime batch",
                "product",
                "loader",
                {},
                {"keep": "existing"},
            )

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    store = partition_job_store_module.PostgresPartitionJobStore("postgresql://example")
    monkeypatch.setattr(store, "ensure_schema", lambda: None)
    monkeypatch.setattr(store, "_connect", lambda: FakeConnection())
    monkeypatch.setattr(store, "_jsonb", lambda value: value)

    store.ensure_runtime_batch(
        batch_id="PG_NON_RUNTIME",
        batch_name="Non runtime batch",
        data_type="product",
        payload={
            "selected_assets": [ard_raster_asset("s3://cube/cube/source/product/non-runtime.tif", "non-runtime", data_type="product")]
        },
    )

    sql_text = "\n".join(sql for sql, _params in executed)
    assert "WHEN target.source_system = 'runtime' THEN source.normalized_payload" in sql_text
    assert "ELSE target.normalized_payload" in sql_text


def test_postgres_result_implies_ingested_defaults_to_false():
    assert partition_job_store_module._result_implies_ingested({}) is False


def test_postgres_fail_attempt_preserves_manual_required_status(monkeypatch):
    executed = []

    class FakeCursor:
        def __init__(self):
            self._stage = 0

        def execute(self, sql, params=None):
            executed.append((sql, params))
            if "RETURNING batch_id, asset_ids" in sql:
                self._stage += 1

        def fetchone(self):
            if self._stage == 1:
                return ("BATCH_FAIL_STATUS", ["asset-1"])
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    store = partition_job_store_module.PostgresPartitionJobStore("postgresql://example")
    monkeypatch.setattr(store, "ensure_schema", lambda: None)
    monkeypatch.setattr(store, "_connect", lambda: FakeConnection())

    store.fail_attempt("task-1", "manual review required", manual_required=True, error_type="validation")

    sql_text = "\n".join(sql for sql, _params in executed)
    attempt_params = next(params for sql, params in executed if "UPDATE partition_job_attempts" in sql)
    batch_params = next(params for sql, params in executed if "UPDATE partition_batches" in sql)

    assert "SET status = %s" in sql_text
    assert attempt_params[0] == "manual_required"
    assert batch_params[0] == "manual_required"


def test_postgres_schema_import_syncs_ard_loader_asset_tables(monkeypatch):
    executed = []

    class FakeCursor:
        def __init__(self):
            self.description = []
            self._fetchone_result = None

        def execute(self, sql, params=None):
            executed.append((sql, params))
            if "SELECT COUNT(*)" in sql and "information_schema.tables" in sql:
                self.description = [("count",)]
                self._fetchone_result = (3,)
            elif "SELECT * FROM partition_batches" in sql:
                self.description = [
                    ("batch_id",),
                    ("batch_name",),
                    ("data_type",),
                    ("source_system",),
                    ("source_schema",),
                    ("normalized_payload",),
                    ("status",),
                    ("priority",),
                    ("attempt_count",),
                    ("max_auto_retries",),
                    ("created_at",),
                    ("updated_at",),
                ]
                self._fetchone_result = (
                    "ARD_SYNC_PRODUCT",
                    "ARD sync product",
                    "product",
                    "loader",
                    {},
                    {},
                    "pending",
                    2,
                    0,
                    3,
                    "2026-06-18T10:00:00Z",
                    "2026-06-18T10:00:00Z",
                )
            elif "SELECT id FROM ard_partition_batches" in sql:
                self.description = [("id",)]
                self._fetchone_result = (42,)
            else:
                self.description = []
                self._fetchone_result = None

        def fetchone(self):
            return self._fetchone_result

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    store = partition_job_store_module.PostgresPartitionJobStore("postgresql://example")
    monkeypatch.setattr(store, "ensure_schema", lambda: None)
    monkeypatch.setattr(store, "_connect", lambda: FakeConnection())

    batch = store.upsert_schema(
        {
            "schema_version": "1.0",
            "batch_id": "ARD_SYNC_PRODUCT",
            "batch_name": "ARD sync product",
            "data_type": "product",
            "source_system": "loader",
            "loaded_at": "2026-06-18T10:00:00Z",
            "updated_at": "2026-06-18T10:05:00Z",
            "raw_meta_uri": "s3://cube/meta/product.json",
            "priority": 2,
            "max_auto_retries": 3,
            "assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/product/ard-sync.tif",
                    "product-scene",
                    data_type="product",
                    asset_id="product-asset-1",
                )
            ],
        }
    )

    sql_text = "\n".join(sql for sql, _params in executed)
    ard_batch_params = next(params for sql, params in executed if "MERGE INTO ard_partition_batches" in sql)
    ard_asset_params = next(params for sql, params in executed if "MERGE INTO ard_partition_assets" in sql)

    assert batch["batch_id"] == "ARD_SYNC_PRODUCT"
    assert "MERGE INTO ard_partition_batches target" in sql_text
    assert "MERGE INTO ard_partition_assets target" in sql_text
    assert "DELETE FROM ard_partition_observations WHERE batch_id = %s" in sql_text
    assert ard_batch_params["schema_version"] == "1.0"
    assert ard_batch_params["source_system"] == "loader"
    assert ard_batch_params["raw_meta_uri"] == "s3://cube/meta/product.json"
    assert ard_asset_params["batch_id"] == 42
    assert ard_asset_params["asset_id"] == "product-asset-1"
    assert ard_asset_params["product_name"] == "test_product"


def test_postgres_store_ensure_schema_runs_only_once(monkeypatch):
    executed = []
    connect_calls = 0

    class FakeCursor:
        def execute(self, sql, params=None):
            executed.append((sql, params))

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_connect():
        nonlocal connect_calls
        connect_calls += 1
        return FakeConnection()

    store = partition_job_store_module.PostgresPartitionJobStore("postgresql://example")
    monkeypatch.setattr(store, "_connect", fake_connect)

    store.ensure_schema()
    store.ensure_schema()

    assert connect_calls == 1
    assert any("CREATE TABLE IF NOT EXISTS partition_batches" in sql for sql, _params in executed)


def test_postgres_store_ensure_schema_backfills_supported_and_unsupported_ingest_statuses(monkeypatch):
    executed = []

    class FakeCursor:
        def execute(self, sql, params=None):
            executed.append((sql, params))

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    store = partition_job_store_module.PostgresPartitionJobStore("postgresql://example")
    monkeypatch.setattr(store, "_connect", lambda: FakeConnection())

    store.ensure_schema()

    sql, params = next((sql, params) for sql, params in executed if "UPDATE partition_batches" in sql and "SET ingest_status = CASE" in sql)

    tracked = sorted(partition_job_store_module.INGEST_TRACKED_DATA_TYPES)
    assert "WHERE data_type = ANY(%s::text[])" in sql
    assert "NOT (data_type = ANY(%s::text[]))" in sql
    assert params == (tracked, tracked, tracked, tracked)


def test_postgres_mark_batch_queued_resets_ingest_status_for_tracked_types(monkeypatch):
    executed = []

    class FakeCursor:
        def execute(self, sql, params=None):
            executed.append((sql, params))

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    store = partition_job_store_module.PostgresPartitionJobStore("postgresql://example")
    monkeypatch.setattr(store, "ensure_schema", lambda: None)
    monkeypatch.setattr(store, "_connect", lambda: FakeConnection())

    store.mark_batch_queued("PG_BATCH", "pg-task-1", operation="run")

    sql, params = executed[0]
    assert "data_type = ANY(%s::text[])" in sql
    assert params == ("queued", "pg-task-1", sorted(partition_job_store_module.INGEST_TRACKED_DATA_TYPES), "PG_BATCH")


def test_postgres_refresh_ingest_readiness_uses_batch_data_type(monkeypatch):
    executed = []

    class FakeCursor:
        def __init__(self):
            self._fetchone_result = None

        def execute(self, sql, params=None):
            executed.append((sql, params))
            if "SELECT data_type, status FROM partition_batches" in sql:
                self._fetchone_result = ("entity", "succeeded")
            else:
                self._fetchone_result = None

        def fetchone(self):
            return self._fetchone_result

    store = partition_job_store_module.PostgresPartitionJobStore("postgresql://example")

    store._refresh_ingest_readiness(
        FakeCursor(),
        "PG_ENTITY_BATCH",
        {
            "data_type": "carbon",
            "ingest_enabled": True,
        },
    )

    sql, params = executed[1]
    assert "SET ingest_status = %s" in sql
    assert params == ("ingested", "ingested", "PG_ENTITY_BATCH")


def test_postgres_refresh_ingest_readiness_preserves_explicit_ingest_status(monkeypatch):
    executed = []

    class FakeCursor:
        def __init__(self):
            self._fetchone_result = None

        def execute(self, sql, params=None):
            executed.append((sql, params))
            if "SELECT data_type, status FROM partition_batches" in sql:
                self._fetchone_result = ("optical", "succeeded")
            else:
                self._fetchone_result = None

        def fetchone(self):
            return self._fetchone_result

    store = partition_job_store_module.PostgresPartitionJobStore("postgresql://example")

    store._refresh_ingest_readiness(
        FakeCursor(),
        "PG_OPTICAL_BATCH",
        {
            "ingest_status": "failed",
            "ingest_enabled": True,
        },
    )

    sql, params = executed[1]
    assert "SET ingest_status = %s" in sql
    assert params == ("failed", "failed", "PG_OPTICAL_BATCH")


def test_postgres_schema_import_syncs_ard_loader_observation_tables(monkeypatch):
    executed = []

    class FakeCursor:
        def __init__(self):
            self.description = []
            self._fetchone_result = None

        def execute(self, sql, params=None):
            executed.append((sql, params))
            if "SELECT COUNT(*)" in sql and "information_schema.tables" in sql:
                self.description = [("count",)]
                self._fetchone_result = (3,)
            elif "SELECT * FROM partition_batches" in sql:
                self.description = [
                    ("batch_id",),
                    ("batch_name",),
                    ("data_type",),
                    ("source_system",),
                    ("source_schema",),
                    ("normalized_payload",),
                ]
                self._fetchone_result = (
                    "ARD_SYNC_CARBON",
                    "ARD sync carbon",
                    "carbon",
                    "loader",
                    {},
                    {},
                )
            elif "SELECT id FROM ard_partition_batches" in sql:
                self.description = [("id",)]
                self._fetchone_result = (84,)
            else:
                self.description = []
                self._fetchone_result = None

        def fetchone(self):
            return self._fetchone_result

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    store = partition_job_store_module.PostgresPartitionJobStore("postgresql://example")
    monkeypatch.setattr(store, "ensure_schema", lambda: None)
    monkeypatch.setattr(store, "_connect", lambda: FakeConnection())

    store.upsert_schema(
        {
            "schema_version": "1.0",
            "batch_id": "ARD_SYNC_CARBON",
            "batch_name": "ARD sync carbon",
            "data_type": "carbon",
            "source_system": "loader",
            "observations": [ard_carbon_observation("obs-carbon-1")],
        }
    )

    sql_text = "\n".join(sql for sql, _params in executed)
    ard_observation_params = next(params for sql, params in executed if "MERGE INTO ard_partition_observations" in sql)

    assert "MERGE INTO ard_partition_batches target" in sql_text
    assert "MERGE INTO ard_partition_observations target" in sql_text
    assert "DELETE FROM ard_partition_assets WHERE batch_id = %s" in sql_text
    assert ard_observation_params["batch_id"] == 84
    assert ard_observation_params["observation_id"] == "obs-carbon-1"
    assert ard_observation_params["sensor"] == "oco2"


def test_postgres_schema_import_requires_ard_loader_tables(monkeypatch):
    class FakeCursor:
        def __init__(self):
            self.description = []
            self._fetchone_result = None

        def execute(self, sql, params=None):
            if "SELECT COUNT(*)" in sql and "information_schema.tables" in sql:
                self.description = [("count",)]
                self._fetchone_result = (0,)
            elif "SELECT * FROM partition_batches" in sql:
                self.description = [
                    ("batch_id",),
                    ("batch_name",),
                    ("data_type",),
                    ("source_system",),
                    ("source_schema",),
                    ("normalized_payload",),
                ]
                self._fetchone_result = (
                    "ARD_SYNC_FAIL",
                    "ARD sync fail",
                    "product",
                    "loader",
                    {},
                    {},
                )
            else:
                self.description = []
                self._fetchone_result = None

        def fetchone(self):
            return self._fetchone_result

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    store = partition_job_store_module.PostgresPartitionJobStore("postgresql://example")
    monkeypatch.setattr(store, "ensure_schema", lambda: None)
    monkeypatch.setattr(store, "_connect", lambda: FakeConnection())

    with pytest.raises(RuntimeError, match="ARD loader schema tables are required"):
        store.upsert_schema(
            {
                "batch_id": "ARD_SYNC_FAIL",
                "data_type": "product",
                "assets": [
                    ard_raster_asset(
                        "s3://cube/cube/source/product/fail.tif",
                        "fail-scene",
                        data_type="product",
                    )
                ],
            }
        )


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
    assert radar["normalized_payload"]["selected_assets"][0]["source_uri"].startswith("s3://cube/cube/source/radar/")
    assert "/yangzhou_s1_2018_2020/Data/" in radar["normalized_payload"]["selected_assets"][0]["source_uri"]
    assert radar["normalized_payload"]["selected_assets"][0]["source_uri"].endswith("20180603_VH.dat")
    assert product["data_type"] == "product"
    assert product["normalized_payload"]["selected_assets"][0]["scene_id"] == "dianzhong_ecological_security_1980"

    assert len(store.list_assets("OPTICAL_BATCH_20260522_135546")) == 4
    assert len(store.list_assets("CARBON_BATCH_20201231_A")) == 4
    assert len(store.list_assets("RADAR_BATCH_YANGZHOU_S1_2018_2020")) == 48
    assert len(store.list_assets("PRODUCT_BATCH_DIANZHONG_1980_2020")) == 5
    all_sources = [
        str(asset.get("source_uri") or "")
        for schema in standard_partition_schemas()
        for asset in (schema.get("assets") or schema.get("observations") or [])
    ]
    assert all(source.startswith("s3://") for source in all_sources)
    assert not any("/home/" in source for source in all_sources)


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


def test_schema_upsert_removes_assets_no_longer_in_payload():
    store = InMemoryPartitionJobStore()
    store.upsert_schema(
        {
            "batch_id": "UPSERT_ASSET_TRIM",
            "data_type": "optical",
            "assets": [
                ard_raster_asset("s3://cube/cube/source/optocal/a.tif", "scene-a"),
                ard_raster_asset("s3://cube/cube/source/optocal/b.tif", "scene-b"),
            ],
        }
    )

    store.upsert_schema(
        {
            "batch_id": "UPSERT_ASSET_TRIM",
            "data_type": "optical",
            "assets": [ard_raster_asset("s3://cube/cube/source/optocal/b.tif", "scene-b")],
        }
    )

    assets = store.list_assets("UPSERT_ASSET_TRIM")
    assert [asset["source_uri"] for asset in assets] == ["s3://cube/cube/source/optocal/b.tif"]


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
        assert store.dsn == "postgresql://test_user:test_password@10.3.100.180:15400/postgres"
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
    assert import_resp.json()["source_system"] == "loader"
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


def test_partition_schema_reconcile_by_batch_ids():
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "RECONCILE_BATCH_A",
            "batch_name": "Reconcile batch A",
            "data_type": "optical",
            "assets": [ard_raster_asset("s3://cube/cube/source/optocal/reconcile-a.tif", "reconcile-a", asset_id="reconcile-a")],
        },
    )

    resp = client.post(
        "/v1/partition/schemas/reconcile",
        json={
            "source_system": "loader",
            "batch_ids": ["RECONCILE_BATCH_A", "RECONCILE_BATCH_MISSING"],
            "include_assets": True,
            "include_attempts": False,
        },
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["source_system"] == "loader"
    assert body["summary"]["requested_batches"] == 2
    assert body["summary"]["known_batches"] == 1
    assert body["summary"]["missing_batches"] == 1
    assert body["missing_batch_ids"] == ["RECONCILE_BATCH_MISSING"]
    known = next(item for item in body["batches"] if item["batch_id"] == "RECONCILE_BATCH_A")
    missing = next(item for item in body["batches"] if item["batch_id"] == "RECONCILE_BATCH_MISSING")
    assert known["known"] is True
    assert known["status"] == "pending"
    assert known["asset_counts"]["total"] == 1
    assert known["assets"][0]["asset_id"] == "reconcile-a"
    assert known["attempts"] == []
    assert missing == {"batch_id": "RECONCILE_BATCH_MISSING", "known": False, "status": "missing"}


def test_partition_schema_reconcile_by_asset_ids_and_attempts(monkeypatch):
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "RECONCILE_BATCH_B",
            "batch_name": "Reconcile batch B",
            "data_type": "product",
            "assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/product/reconcile-b.tif",
                    "reconcile-b",
                    data_type="product",
                    asset_id="reconcile-b",
                )
            ],
            "normalized_payload": {"grid_type": "geohash", "grid_level": 5},
        },
    )

    def fake_run_product_partition_run(payload=None):
        return {"status": "completed", "mode": "partition_run", "data_type": "product", "rows": 1}

    monkeypatch.setattr(partition_adapters, "run_product_partition_run", fake_run_product_partition_run)

    submit_resp = client.post("/v1/partition/batches/RECONCILE_BATCH_B/run", json={})
    assert submit_resp.status_code == 202
    task_id = submit_resp.json()["task_id"]
    for _ in range(20):
        if client.get(f"/v1/partition/tasks/{task_id}").json()["status"] == "completed":
            break
        time.sleep(0.01)

    resp = client.post(
        "/v1/partition/schemas/reconcile",
        json={
            "asset_ids": ["reconcile-b", "missing-asset"],
            "include_assets": True,
            "include_attempts": True,
        },
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["summary"]["requested_assets"] == 2
    assert body["summary"]["known_assets"] == 1
    assert body["summary"]["missing_assets"] == 1
    assert body["missing_asset_ids"] == ["missing-asset"]
    batch = next(item for item in body["batches"] if item["batch_id"] == "RECONCILE_BATCH_B")
    assert batch["known"] is True
    assert batch["status"] == "pending"
    assert batch["source_system"] == "loader"
    assert batch["asset_counts"]["total"] == 1
    assert batch["assets"][0]["asset_id"] == "reconcile-b"
    assert batch["attempts"] == []


def test_partition_schema_reconcile_by_updated_since():
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "RECONCILE_BATCH_UPDATED",
            "batch_name": "Reconcile updated",
            "data_type": "carbon",
            "updated_at": "2026-06-18T09:30:00Z",
            "observations": [ard_carbon_observation("reconcile-obs-a")],
        },
    )

    resp = client.post(
        "/v1/partition/schemas/reconcile",
        json={
            "updated_since": "2026-06-18T09:00:00Z",
            "include_assets": False,
            "include_attempts": False,
        },
    )

    assert resp.status_code == 200
    body = resp.json()
    batch = next(item for item in body["batches"] if item["batch_id"] == "RECONCILE_BATCH_UPDATED")
    assert batch["known"] is True
    assert batch["status"] == "pending"
    assert batch["asset_counts"]["total"] == 1
    assert batch["assets"] == []
    assert batch["attempts"] == []


def test_partition_schema_reconcile_by_observation_ids():
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "RECONCILE_BATCH_CARBON",
            "batch_name": "Reconcile carbon batch",
            "data_type": "carbon",
            "observations": [ard_carbon_observation("reconcile-obs-b")],
        },
    )

    resp = client.post(
        "/v1/partition/schemas/reconcile",
        json={
            "observation_ids": ["reconcile-obs-b", "missing-obs"],
            "include_assets": True,
        },
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["summary"]["requested_observations"] == 2
    assert body["summary"]["known_observations"] == 1
    assert body["summary"]["missing_observations"] == 1
    assert body["missing_observation_ids"] == ["missing-obs"]
    batch = next(item for item in body["batches"] if item["batch_id"] == "RECONCILE_BATCH_CARBON")
    assert batch["known"] is True
    assert batch["status"] == "pending"
    assert batch["asset_counts"]["total"] == 1
    assert batch["assets"][0]["kind"] == "observation"
    assert batch["assets"][0]["observation_id"] == "reconcile-obs-b"


def test_partition_schema_reconcile_requires_selector():
    resp = client.post("/v1/partition/schemas/reconcile", json={"include_assets": True})

    assert resp.status_code == 422
    assert "one of batch_ids, asset_ids, observation_ids, or updated_since is required" in resp.json()["detail"]


def test_partition_job_store_keeps_duplicate_asset_id_batches_separate():
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

    first_assets = store.list_assets("BATCH_IMPORT_DUPLICATE_1")
    assert len(first_assets) == 1
    assert first_assets[0]["asset_id"] == "asset-duplicate"
    assert first_assets[0]["status"] == "manual_required"
    second_assets = store.list_assets("BATCH_IMPORT_DUPLICATE_2")
    assert len(second_assets) == 1
    assert second_assets[0]["asset_id"].startswith("BATCH_IMPORT_DUPLICATE_2:")
    assert second_assets[0]["scene_id"] == "scene-b"
    assert second_assets[0]["source_uri"] == "s3://cube/cube/source/optocal/duplicate-b.tif"
    assert second_assets[0]["status"] == "pending"
    assert second_assets[0]["attempt_count"] == 0
    assert second_assets[0]["last_error"] is None
    assert second_assets[0]["partitioned_at"] is None


def test_partition_job_store_repairs_missing_assets_from_payload():
    store = InMemoryPartitionJobStore()
    first_batch = {
        "batch_id": "BATCH_REPAIR_SOURCE",
        "batch_name": "Repair source",
        "data_type": "optical",
        "assets": [
            ard_raster_asset(
                "s3://cube/cube/source/optocal/repair-source.tif",
                "repair-source",
                asset_id="shared-repair-asset",
            )
        ],
    }
    missing_batch = {
        "batch_id": "BATCH_REPAIR_MISSING",
        "batch_name": "Repair missing",
        "data_type": "optical",
        "assets": [
            ard_raster_asset(
                "s3://cube/cube/source/optocal/repair-missing.tif",
                "repair-missing",
                asset_id="shared-repair-asset",
            )
        ],
    }
    store.upsert_schema(missing_batch)
    del store.assets["shared-repair-asset"]
    store.upsert_schema(first_batch)

    repaired = store.list_assets("BATCH_REPAIR_MISSING")

    assert len(repaired) == 1
    assert repaired[0]["asset_id"].startswith("BATCH_REPAIR_MISSING:")
    assert repaired[0]["source_uri"] == "s3://cube/cube/source/optocal/repair-missing.tif"
    assert repaired[0]["status"] == "pending"


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


def test_partition_schema_import_rejects_invalid_top_level_loaded_at():
    resp = client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_BAD_LOADED_AT",
            "data_type": "optical",
            "loaded_at": "not-a-datetime",
            "assets": [ard_raster_asset("s3://cube/cube/source/optocal/a.tif", "scene-a")],
        },
    )

    assert resp.status_code == 422
    assert "loaded_at must be an ISO8601 datetime" in resp.json()["detail"]


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

    def fake_run_radar_partition_run(payload=None):
        captured["payload"] = payload
        return {"status": "completed", "mode": "partition_run", "data_type": "radar", "rows": 1}

    monkeypatch.setattr(partition_adapters, "run_radar_partition_run", fake_run_radar_partition_run)

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
    assert "grid_level_mode" not in optical_resp.json()["normalized_payload"]
    assert "grid_level" not in carbon_resp.json()["normalized_payload"]


def test_partition_schema_import_defaults_isea4h_grid_level_to_6():
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
    assert resp.json()["normalized_payload"]["grid_level"] == 6
    assert "grid_level_mode" not in resp.json()["normalized_payload"]


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

    def fake_run_optical_partition_run(payload=None):
        return {"status": "completed", "mode": "partition_run", "data_type": "optical", "rows": 1}

    monkeypatch.setattr(partition_adapters, "run_optical_partition_run", fake_run_optical_partition_run)

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

    def fake_run_product_partition_run(payload=None):
        assert payload["batch_id"] == "BATCH_RUN_SUCCESS"
        return {"status": "completed", "mode": "partition_run", "data_type": "product", "rows": 2}

    monkeypatch.setattr(partition_adapters, "run_product_partition_run", fake_run_product_partition_run)

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


def test_partition_batch_detail_includes_partition_slots_and_rejects_duplicate_completed_slot():
    store = InMemoryPartitionJobStore()
    store.upsert_schema(
        {
            "batch_id": "BATCH_PARTITION_SLOTS",
            "batch_name": "Batch partition slots",
            "data_type": "optical",
            "normalized_payload": {
                "grid_type": "geohash",
                "selected_assets": [
                    ard_raster_asset(
                        "s3://cube/cube/source/optocal/slot-a.tif",
                        "slot-a",
                        asset_id="slot-a",
                    )
                ],
            },
            "assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/optocal/slot-a.tif",
                    "slot-a",
                    asset_id="slot-a",
                )
            ],
        }
    )
    store.create_attempt(
        task_id="partition-slot-logical",
        batch_id="BATCH_PARTITION_SLOTS",
        operation="auto_run",
        payload={"grid_type": "geohash", "partition_method": "logical"},
    )
    store.succeed_attempt(
        "partition-slot-logical",
        {
            "status": "completed",
            "data_type": "optical",
            "grid_type": "geohash",
            "partition_method": "logical",
            "partition_type": "logical",
            "rows": 3,
        },
    )

    class DummyTask:
        def __init__(self, task_id: str, payload: dict[str, Any]) -> None:
            self.task_id = task_id
            self.status = "queued"
            self.data_type = "optical"
            self.operation = "run"
            self.payload = payload

        def to_dict(self) -> dict[str, Any]:
            return {
                "task_id": self.task_id,
                "status": self.status,
                "data_type": self.data_type,
                "operation": self.operation,
            }

    class DummyTaskStore:
        def submit(
            self, data_type, operation, runner, task_id=None, on_started=None, on_succeeded=None, on_failed=None, cancellation_check=None
        ):
            del runner, on_started, on_succeeded, on_failed, cancellation_check
            return DummyTask(task_id or "dummy-task", {"data_type": data_type, "operation": operation})

    service = PartitionService(
        {
            "optical": PartitionBackend(
                data_type="optical",
                run=lambda payload=None: {"status": "completed", "payload": payload or {}},
            )
        },
        task_store=DummyTaskStore(),
    )
    workflow = PartitionWorkflowService(service, store=store)
    route_app = FastAPI()
    route_app.include_router(web_app.partition_route.create_partition_router(service=service, workflow=workflow))
    route_client = TestClient(route_app)

    detail_resp = route_client.get("/partition/batches/BATCH_PARTITION_SLOTS")
    rerun_logical_resp = route_client.post(
        "/partition/batches/BATCH_PARTITION_SLOTS/run",
        json={"config_override": {"grid_type": "geohash", "partition_method": "logical"}},
    )

    assert detail_resp.status_code == 200
    slots = detail_resp.json()["partition_slots"]
    logical_slot = next(slot for slot in slots if slot["grid_type"] == "geohash" and slot["partition_method"] == "logical")
    assert {(slot["grid_type"], slot["partition_method"]) for slot in slots} == {
        ("geohash", "logical"),
        ("mgrs", "logical"),
        ("isea4h", "entity"),
    }
    assert logical_slot["status"] == "completed"
    assert logical_slot["disabled"] is True
    assert rerun_logical_resp.status_code == 409


def test_partition_batch_slots_sort_mixed_iso_datetimes():
    batch = {"data_type": "optical", "batch_id": "BATCH_SLOT_SORT"}
    attempts = [
        {
            "task_id": "task-1",
            "operation": "auto_run",
            "status": "succeeded",
            "payload": {"grid_type": "geohash", "partition_method": "logical"},
            "finished_at": "2026-06-29T12:00:00Z",
            "updated_at": "2026-06-29T12:00:00Z",
            "created_at": "2026-06-29T12:00:00Z",
        },
    ]

    slots = _partition_slots_for_batch(batch, attempts)
    logical_slot = next(slot for slot in slots if slot["grid_type"] == "geohash" and slot["partition_method"] == "logical")
    assert logical_slot["latest_task_id"] == "task-1"


def test_partition_batch_archive_marks_handled_and_hides_from_pending_list():
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_ARCHIVE_HANDLED",
            "batch_name": "Archive handled",
            "data_type": "optical",
            "assets": [
                ard_raster_asset("s3://cube/cube/source/optocal/archive-a.tif", "archive-a", asset_id="archive-a"),
                ard_raster_asset("s3://cube/cube/source/optocal/archive-b.tif", "archive-b", asset_id="archive-b"),
            ],
        },
    )
    store = web_app.partition_workflow_service.store
    store.create_attempt(
        task_id="partition-archive-handled",
        batch_id="BATCH_ARCHIVE_HANDLED",
        operation="auto_run",
        payload={},
    )
    store.fail_attempt(
        "partition-archive-handled",
        "operator handled this failed batch outside retry",
        manual_required=True,
        error_type="validation",
    )

    archive_resp = client.post("/v1/partition/batches/BATCH_ARCHIVE_HANDLED/archive")
    detail_resp = client.get("/v1/partition/batches/BATCH_ARCHIVE_HANDLED")
    assets_resp = client.get("/v1/partition/batches/BATCH_ARCHIVE_HANDLED/assets")
    tasks_resp = client.get("/v1/partition/tasks", params={"limit": 20})
    pending_resp = client.get("/v1/partition/batches")
    archived_resp = client.get("/v1/partition/batches", params={"status": "archived"})
    history_resp = client.get("/v1/partition/batches", params={"include_succeeded": True})
    run_resp = client.post("/v1/partition/batches/BATCH_ARCHIVE_HANDLED/run", json={})
    retry_resp = client.post("/v1/partition/batches/BATCH_ARCHIVE_HANDLED/retry", json={})

    assert archive_resp.status_code == 200
    assert archive_resp.json()["status"] == "archived"
    assert detail_resp.json()["status"] == "archived"
    assert detail_resp.json()["last_error"] == "operator handled this failed batch outside retry"
    assert {asset["status"] for asset in assets_resp.json()["assets"]} == {"manual_required"}
    task_row = next(row for row in tasks_resp.json()["tasks"] if row["task_id"] == "partition-archive-handled")
    assert task_row["status"] == "failed"
    assert task_row["batch_status"] == "archived"
    assert "BATCH_ARCHIVE_HANDLED" not in [batch["batch_id"] for batch in pending_resp.json()["batches"]]
    assert "BATCH_ARCHIVE_HANDLED" in [batch["batch_id"] for batch in archived_resp.json()["batches"]]
    assert "BATCH_ARCHIVE_HANDLED" in [batch["batch_id"] for batch in history_resp.json()["batches"]]
    assert run_resp.status_code == 409
    assert retry_resp.status_code == 409


def test_partition_failed_batch_requeue_returns_to_pending_list():
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_REQUEUE_FAILED",
            "batch_name": "Requeue failed",
            "data_type": "optical",
            "assets": [ard_raster_asset("s3://cube/cube/source/optocal/requeue-a.tif", "requeue-a", asset_id="requeue-a")],
        },
    )
    store = web_app.partition_workflow_service.store
    store.create_attempt(
        task_id="partition-requeue-failed",
        batch_id="BATCH_REQUEUE_FAILED",
        operation="auto_run",
        payload={},
    )
    store.fail_attempt("partition-requeue-failed", "temporary failure", manual_required=True, error_type="validation")

    requeue_resp = client.post("/v1/partition/batches/BATCH_REQUEUE_FAILED/requeue")
    detail_resp = client.get("/v1/partition/batches/BATCH_REQUEUE_FAILED")
    assets_resp = client.get("/v1/partition/batches/BATCH_REQUEUE_FAILED/assets")
    pending_resp = client.get("/v1/partition/batches")
    tasks_resp = client.get("/v1/partition/tasks", params={"keyword": "partition-requeue-failed"})

    assert requeue_resp.status_code == 200
    assert detail_resp.json()["status"] == "pending"
    assert detail_resp.json()["last_error"] is None
    assert {asset["status"] for asset in assets_resp.json()["assets"]} == {"pending"}
    assert "BATCH_REQUEUE_FAILED" in [batch["batch_id"] for batch in pending_resp.json()["batches"]]
    task_row = tasks_resp.json()["tasks"][0]
    assert task_row["status"] == "failed"
    assert task_row["batch_status"] == "pending"


def test_partition_requeue_rejects_succeeded_batch():
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_REQUEUE_DONE",
            "batch_name": "Requeue done",
            "data_type": "product",
            "assets": [ard_raster_asset("s3://cube/cube/source/product/requeue-done.tif", "requeue-done", data_type="product")],
        },
    )
    store = web_app.partition_workflow_service.store
    store.create_attempt(
        task_id="partition-requeue-done",
        batch_id="BATCH_REQUEUE_DONE",
        operation="auto_run",
        payload={},
    )
    store.succeed_attempt("partition-requeue-done", {"status": "completed", "data_type": "product", "rows": 1})

    requeue_resp = client.post("/v1/partition/batches/BATCH_REQUEUE_DONE/requeue")
    detail_resp = client.get("/v1/partition/batches/BATCH_REQUEUE_DONE")

    assert requeue_resp.status_code == 409
    assert "not requeueable" in requeue_resp.json()["detail"]
    assert detail_resp.json()["status"] == "succeeded"


@pytest.mark.parametrize("start_attempt, expected_status", [(False, "queued"), (True, "running")])
def test_partition_batch_archive_rejects_active_batch(start_attempt, expected_status):
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": f"BATCH_ARCHIVE_ACTIVE_{expected_status.upper()}",
            "batch_name": "Archive active",
            "data_type": "optical",
            "assets": [ard_raster_asset("s3://cube/cube/source/optocal/archive-active.tif", "archive-active")],
        },
    )
    store = web_app.partition_workflow_service.store
    store.create_attempt(
        task_id=f"partition-archive-active-{expected_status}",
        batch_id=f"BATCH_ARCHIVE_ACTIVE_{expected_status.upper()}",
        operation="auto_run",
        payload={},
    )
    if start_attempt:
        store.start_attempt(f"partition-archive-active-{expected_status}")

    archive_resp = client.post(f"/v1/partition/batches/BATCH_ARCHIVE_ACTIVE_{expected_status.upper()}/archive")
    batch_resp = client.get(f"/v1/partition/batches/BATCH_ARCHIVE_ACTIVE_{expected_status.upper()}")

    assert archive_resp.status_code == 409
    assert batch_resp.json()["status"] == expected_status


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

    def slow_optical_partition_run(payload=None):
        calls.append(payload)
        release_runner.wait(timeout=3)
        return {"status": "completed", "mode": "partition_run", "data_type": "optical", "rows": 1}

    monkeypatch.setattr(partition_adapters, "run_optical_partition_run", slow_optical_partition_run)

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

    monkeypatch.setattr(partition_adapters, "run_optical_partition_run", fail_optical)

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

    monkeypatch.setattr(partition_adapters, "run_optical_partition_run", fail_optical)

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

    monkeypatch.setattr(partition_adapters, "run_optical_partition_run", fail_optical)

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


def test_partition_batch_source_missing_marks_only_missing_asset(monkeypatch):
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_SOURCE_MISSING_ONE_ASSET",
            "batch_name": "Source missing one asset",
            "data_type": "optical",
            "assets": [
                ard_raster_asset("s3://cube/cube/source/optocal/good.tif", "good", asset_id="asset-good"),
                ard_raster_asset("s3://cube/cube/source/optocal/missing.tif", "missing", asset_id="asset-missing"),
            ],
        },
    )

    def fail_optical(_payload=None):
        raise RuntimeError("S3 operation failed; code: NoSuchKey, object_name: cube/source/optocal/missing.tif")

    monkeypatch.setattr(partition_adapters, "run_optical_partition_run", fail_optical)

    submit_resp = client.post("/v1/partition/batches/BATCH_SOURCE_MISSING_ONE_ASSET/run", json={})
    assert submit_resp.status_code == 202

    for _ in range(50):
        batch = client.get("/v1/partition/batches/BATCH_SOURCE_MISSING_ONE_ASSET").json()
        if batch["status"] == "manual_required":
            break
        time.sleep(0.02)

    assets = client.get("/v1/partition/batches/BATCH_SOURCE_MISSING_ONE_ASSET/assets").json()["assets"]
    statuses = {asset["asset_id"]: asset["status"] for asset in assets}

    assert statuses == {"asset-good": "pending", "asset-missing": "manual_required"}


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

    def fake_run_optical_partition_run(payload=None):
        calls.append([asset["asset_id"] for asset in payload["selected_assets"]])
        if len(calls) == 1:
            return {
                "status": "completed",
                "mode": "partition_run",
                "data_type": "optical",
                "rows": 1,
                "asset_results": [
                    {"asset_id": "asset-a", "status": "succeeded"},
                    {"asset_id": "asset-b", "status": "failed", "error_type": "transient", "last_error": "temporary network timeout"},
                ],
            }
        return {"status": "completed", "mode": "partition_run", "data_type": "optical", "rows": 1}

    monkeypatch.setattr(partition_adapters, "run_optical_partition_run", fake_run_optical_partition_run)

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


def test_partition_asset_retry_rejects_unaffected_asset_after_source_missing(monkeypatch):
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_ASSET_RETRY_SOURCE_MISSING",
            "batch_name": "Asset retry source missing",
            "data_type": "optical",
            "assets": [
                ard_raster_asset("s3://cube/cube/source/optocal/good.tif", "good", asset_id="asset-good"),
                ard_raster_asset("s3://cube/cube/source/optocal/missing.tif", "missing", asset_id="asset-missing"),
            ],
        },
    )

    def fail_optical(_payload=None):
        raise RuntimeError("S3 operation failed; code: NoSuchKey, object_name: cube/source/optocal/missing.tif")

    monkeypatch.setattr(partition_adapters, "run_optical_partition_run", fail_optical)

    submit_resp = client.post("/v1/partition/batches/BATCH_ASSET_RETRY_SOURCE_MISSING/run", json={})
    assert submit_resp.status_code == 202

    for _ in range(50):
        batch = client.get("/v1/partition/batches/BATCH_ASSET_RETRY_SOURCE_MISSING").json()
        if batch["status"] == "manual_required":
            break
        time.sleep(0.02)

    resp = client.post("/v1/partition/assets/retry", json={"asset_ids": ["asset-good"]})

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

    def fake_run_optical_partition_run(payload=None):
        selected_asset_ids = [asset["asset_id"] for asset in payload["selected_assets"]]
        calls.append(selected_asset_ids)
        if len(calls) == 1:
            return {
                "status": "completed",
                "mode": "partition_run",
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
                "mode": "partition_run",
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
            "mode": "partition_run",
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

    monkeypatch.setattr(partition_adapters, "run_optical_partition_run", fake_run_optical_partition_run)

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


def test_partition_batch_retry_config_override_selected_assets_updates_attempt_scope(monkeypatch):
    client.post(
        "/v1/partition/schemas/import",
        json={
            "batch_id": "BATCH_RETRY_OVERRIDE_SCOPE",
            "batch_name": "Retry override scope",
            "data_type": "optical",
            "assets": [
                ard_raster_asset("s3://cube/cube/source/optocal/a.tif", "a", asset_id="asset-a"),
                ard_raster_asset("s3://cube/cube/source/optocal/b.tif", "b", asset_id="asset-b"),
            ],
        },
    )
    store = web_app.partition_workflow_service.store
    store.create_attempt(
        task_id="partition-retry-override-scope",
        batch_id="BATCH_RETRY_OVERRIDE_SCOPE",
        operation="auto_run",
        payload={},
        asset_ids=["asset-a", "asset-b"],
    )
    store.fail_attempt(
        "partition-retry-override-scope",
        "source missing s3://cube/cube/source/optocal/b.tif",
        manual_required=True,
        error_type="source_missing",
    )

    def fake_run_optical_partition_run(payload=None):
        return {
            "status": "completed",
            "mode": "partition_run",
            "data_type": "optical",
            "rows": 1,
            "asset_results": [{"asset_id": "asset-a", "status": "succeeded"}],
        }

    monkeypatch.setattr(partition_adapters, "run_optical_partition_run", fake_run_optical_partition_run)

    retry_resp = client.post(
        "/v1/partition/batches/BATCH_RETRY_OVERRIDE_SCOPE/retry",
        json={"config_override": {"selected_assets": [ard_raster_asset("s3://cube/cube/source/optocal/a.tif", "a", asset_id="asset-a")]}},
    )
    assert retry_resp.status_code == 202

    retry_task_id = retry_resp.json()["task_id"]
    for _ in range(40):
        task = client.get(f"/v1/partition/tasks/{retry_task_id}").json()
        if task["status"] == "completed":
            break
        time.sleep(0.02)

    attempts = client.get("/v1/partition/batches/BATCH_RETRY_OVERRIDE_SCOPE/attempts").json()["attempts"]
    assets = client.get("/v1/partition/batches/BATCH_RETRY_OVERRIDE_SCOPE/assets").json()["assets"]
    statuses = {asset["asset_id"]: asset["status"] for asset in assets}

    assert attempts[0]["asset_ids"] == ["asset-a"]
    assert statuses["asset-a"] == "succeeded"
    assert statuses["asset-b"] == "manual_required"


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
        return {"status": "completed", "mode": "partition_run", "data_type": "product", "rows": 1}

    monkeypatch.setattr(partition_adapters, "run_product_partition_run", slow_product)

    submit_resp = client.post("/v1/partition/batches/BATCH_CANCEL/run", json={})
    task_id = submit_resp.json()["task_id"]
    cancel_resp = client.post(f"/v1/partition/tasks/{task_id}/cancel")

    assert cancel_resp.status_code == 200
    assert cancel_resp.json()["status"] in {"cancel_requested", "cancelled"}


def test_partition_job_store_start_attempt_does_not_revive_cancelled_attempt():
    store = InMemoryPartitionJobStore()
    store.upsert_schema(
        {
            "batch_id": "BATCH_CANCEL_NO_REVIVE",
            "batch_name": "Cancel no revive",
            "data_type": "product",
            "assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/product/no-revive.tif",
                    "no-revive-scene",
                    data_type="product",
                    asset_id="no-revive-asset",
                )
            ],
        }
    )
    store.create_attempt(
        task_id="partition-no-revive",
        batch_id="BATCH_CANCEL_NO_REVIVE",
        operation="auto_run",
        payload={"partition_backend": "ray", "ray_address": "10.3.100.182:6379"},
        asset_ids=["no-revive-asset"],
        requested_by="operator",
    )
    store.request_cancel("partition-no-revive")

    assert store.start_attempt("partition-no-revive") is False
    assert store.get_attempt("partition-no-revive")["status"] == "cancelled"
    assert store.get_batch("BATCH_CANCEL_NO_REVIVE")["status"] == "cancelled"


def test_partition_job_store_request_cancel_does_not_revive_terminal_attempt():
    store = InMemoryPartitionJobStore()
    store.upsert_schema(
        {
            "batch_id": "BATCH_CANCEL_TERMINAL",
            "batch_name": "Cancel terminal",
            "data_type": "product",
            "assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/product/terminal.tif",
                    "terminal-scene",
                    data_type="product",
                    asset_id="terminal-asset",
                )
            ],
        }
    )
    store.create_attempt(
        task_id="partition-terminal",
        batch_id="BATCH_CANCEL_TERMINAL",
        operation="auto_run",
        payload={"partition_backend": "ray", "ray_address": "10.3.100.182:6379"},
        asset_ids=["terminal-asset"],
        requested_by="operator",
    )
    store.fail_attempt("partition-terminal", "existing failure", manual_required=True, error_type="validation")

    before = store.get_attempt("partition-terminal")
    after = store.request_cancel("partition-terminal")

    assert before["status"] == "failed"
    assert after["status"] == "failed"
    assert store.get_batch("BATCH_CANCEL_TERMINAL")["status"] == "manual_required"


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
        return {"status": "completed", "mode": "partition_run", "data_type": "product", "rows": 1}

    monkeypatch.setattr(partition_adapters, "run_product_partition_run", slow_product)

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

    monkeypatch.setattr(partition_adapters, "run_optical_partition_run", cancelled_optical)

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


def test_partition_cancel_orphaned_running_ray_task_marks_attempt_cancelled(monkeypatch):
    store = InMemoryPartitionJobStore()
    store.supports_remote_jobs = True
    store.upsert_schema(
        {
            "batch_id": "BATCH_ORPHAN_CANCEL",
            "batch_name": "Orphan cancel",
            "data_type": "product",
            "assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/product/orphan.tif",
                    "orphan-scene",
                    data_type="product",
                    asset_id="orphan-asset",
                )
            ],
        }
    )
    store.create_attempt(
        task_id="partition-orphan-cancel",
        batch_id="BATCH_ORPHAN_CANCEL",
        operation="auto_run",
        payload={"partition_backend": "ray", "ray_address": "10.3.100.182:6379"},
        asset_ids=["orphan-asset"],
        requested_by="operator",
    )
    store.start_attempt("partition-orphan-cancel")
    store.request_cancel("partition-orphan-cancel")

    workflow = PartitionWorkflowService(PartitionService({}), store=store)
    stopped = {}

    class FakeRayClient:
        def stop_job(self, job_id):
            stopped["job_id"] = job_id
            return True

        def get_job_status(self, job_id):
            return "STOPPED"

    monkeypatch.setattr("cube_web.services.partition_workflow._build_ray_job_client", lambda address: FakeRayClient())

    result = workflow.cancel_task("partition-orphan-cancel")
    batch = workflow.get_batch("BATCH_ORPHAN_CANCEL")

    assert stopped["job_id"] == "partition-orphan-cancel"
    assert result["status"] == "cancelled"
    assert store.get_attempt("partition-orphan-cancel")["status"] == "cancelled"
    assert batch["status"] == "cancelled"


def test_partition_active_task_lookup_keeps_remote_running_attempt_active(monkeypatch):
    store = InMemoryPartitionJobStore()
    store.supports_remote_jobs = True
    store.upsert_schema(
        {
            "batch_id": "BATCH_ORPHAN_ACTIVE",
            "batch_name": "Orphan active",
            "data_type": "product",
            "assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/product/orphan-active.tif",
                    "orphan-active-scene",
                    data_type="product",
                    asset_id="orphan-active-asset",
                )
            ],
        }
    )
    store.create_attempt(
        task_id="partition-orphan-active",
        batch_id="BATCH_ORPHAN_ACTIVE",
        operation="auto_run",
        payload={"partition_backend": "ray", "ray_address": "10.3.100.182:6379"},
        asset_ids=["orphan-active-asset"],
        requested_by="operator",
    )
    batch = store.get_batch("BATCH_ORPHAN_ACTIVE")
    assert batch is not None

    workflow = PartitionWorkflowService(PartitionService({}), store=store)
    monkeypatch.setattr(
        "cube_web.services.partition_workflow._build_ray_job_client",
        lambda address: type(
            "FakeRayClient",
            (),
            {"get_job_status": lambda self, job_id: "RUNNING"},
        )(),
    )

    active = workflow._active_task_for_batch(batch)

    assert active is not None
    assert active.task_id == "partition-orphan-active"
    assert store.get_attempt("partition-orphan-active")["status"] == "running"
    assert workflow.get_batch("BATCH_ORPHAN_ACTIVE")["status"] == "running"


def test_partition_reconcile_orphaned_tasks_marks_stopped_remote_attempts_cancelled(monkeypatch):
    store = InMemoryPartitionJobStore()
    store.supports_remote_jobs = True
    store.upsert_schema(
        {
            "batch_id": "BATCH_ORPHAN_RECONCILE",
            "batch_name": "Orphan reconcile",
            "data_type": "product",
            "assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/product/orphan-reconcile.tif",
                    "orphan-reconcile-scene",
                    data_type="product",
                    asset_id="orphan-reconcile-asset",
                )
            ],
        }
    )
    store.create_attempt(
        task_id="partition-orphan-reconcile",
        batch_id="BATCH_ORPHAN_RECONCILE",
        operation="auto_run",
        payload={"partition_backend": "ray", "ray_address": "10.3.100.182:6379"},
        asset_ids=["orphan-reconcile-asset"],
        requested_by="operator",
    )
    store.start_attempt("partition-orphan-reconcile")

    workflow = PartitionWorkflowService(PartitionService({}), store=store)
    monkeypatch.setattr(
        "cube_web.services.partition_workflow._build_ray_job_client",
        lambda address: type(
            "FakeRayClient",
            (),
            {"get_job_status": lambda self, job_id: "STOPPED"},
        )(),
    )

    cancelled = workflow.reconcile_orphaned_tasks()

    assert cancelled == 1
    assert store.get_attempt("partition-orphan-reconcile")["status"] == "cancelled"
    assert workflow.get_batch("BATCH_ORPHAN_RECONCILE")["status"] == "cancelled"


def test_partition_reconcile_orphaned_tasks_marks_failed_remote_attempts_failed(monkeypatch):
    store = InMemoryPartitionJobStore()
    store.supports_remote_jobs = True
    store.upsert_schema(
        {
            "batch_id": "BATCH_ORPHAN_FAILED",
            "batch_name": "Orphan failed",
            "data_type": "product",
            "assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/product/orphan-failed.tif",
                    "orphan-failed-scene",
                    data_type="product",
                    asset_id="orphan-failed-asset",
                )
            ],
        }
    )
    store.create_attempt(
        task_id="partition-orphan-failed",
        batch_id="BATCH_ORPHAN_FAILED",
        operation="auto_run",
        payload={"partition_backend": "ray", "ray_address": "10.3.100.182:6379"},
        asset_ids=["orphan-failed-asset"],
        requested_by="operator",
    )
    store.start_attempt("partition-orphan-failed")

    class FakeRayClient:
        def get_job_status(self, job_id):
            return "FAILED"

        def get_job_info(self, job_id):
            return type("Info", (), {"message": "ray worker crashed", "error_type": "RuntimeError"})()

    workflow = PartitionWorkflowService(PartitionService({}), store=store)
    monkeypatch.setattr("cube_web.services.partition_workflow._build_ray_job_client", lambda address: FakeRayClient())

    resolved = workflow.reconcile_orphaned_tasks()

    assert resolved == 1
    attempt = store.get_attempt("partition-orphan-failed")
    assert attempt["status"] == "failed"
    assert attempt["error_message"] == "ray worker crashed"
    assert workflow.get_batch("BATCH_ORPHAN_FAILED")["status"] == "manual_required"


def test_partition_remote_job_runs_persisted_attempt_and_stores_result(monkeypatch):
    store = InMemoryPartitionJobStore()
    store.upsert_schema(
        {
            "batch_id": "REMOTE_TASK_BATCH",
            "batch_name": "Remote task batch",
            "data_type": "product",
            "assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/product/remote-task.tif",
                    "remote-task-scene",
                    data_type="product",
                    asset_id="remote-task-asset",
                )
            ],
        }
    )
    store.create_attempt(
        task_id="partition-remote-task",
        batch_id="REMOTE_TASK_BATCH",
        operation="auto_run",
        payload={
            "partition_backend": "ray",
            "ray_address": "10.3.100.182:6379",
            "selected_assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/product/remote-task.tif",
                    "remote-task-scene",
                    data_type="product",
                    asset_id="remote-task-asset",
                )
            ],
        },
        asset_ids=["remote-task-asset"],
        requested_by="operator",
    )
    set_partition_job_store(store)

    captured = {}

    def fake_runner(payload=None):
        captured["job_id"] = payload["job_id"]
        captured["batch_id"] = payload["batch_id"]
        captured["asset_ids"] = [asset["asset_id"] for asset in payload["selected_assets"]]
        return {
            "status": "completed",
            "mode": "partition_run",
            "data_type": "product",
            "rows": 7,
        }

    monkeypatch.setattr(
        "cube_web.services.partition_remote_job._runner_for_data_type", lambda data_type: fake_runner if data_type == "product" else None
    )

    try:
        exit_code = run_remote_partition_task("partition-remote-task")
    finally:
        set_partition_job_store(None)

    assert exit_code == 0
    assert captured == {
        "job_id": "partition-remote-task",
        "batch_id": "REMOTE_TASK_BATCH",
        "asset_ids": ["remote-task-asset"],
    }
    attempt = store.get_attempt("partition-remote-task")
    assert attempt["status"] == "succeeded"
    assert attempt["runner_result"]["rows"] == 7


def test_partition_remote_job_skips_cancelled_attempt_before_runner_start(monkeypatch):
    store = InMemoryPartitionJobStore()
    store.upsert_schema(
        {
            "batch_id": "REMOTE_TASK_CANCELLED_BATCH",
            "batch_name": "Remote task cancelled batch",
            "data_type": "product",
            "assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/product/remote-cancelled.tif",
                    "remote-cancelled-scene",
                    data_type="product",
                    asset_id="remote-cancelled-asset",
                )
            ],
        }
    )
    store.create_attempt(
        task_id="partition-remote-cancelled",
        batch_id="REMOTE_TASK_CANCELLED_BATCH",
        operation="auto_run",
        payload={
            "partition_backend": "ray",
            "ray_address": "10.3.100.182:6379",
            "selected_assets": [
                ard_raster_asset(
                    "s3://cube/cube/source/product/remote-cancelled.tif",
                    "remote-cancelled-scene",
                    data_type="product",
                    asset_id="remote-cancelled-asset",
                )
            ],
        },
        asset_ids=["remote-cancelled-asset"],
        requested_by="operator",
    )
    store.request_cancel("partition-remote-cancelled")
    set_partition_job_store(store)

    runner_called = {"value": False}

    def fake_runner(payload=None):
        runner_called["value"] = True
        return {"status": "completed", "mode": "partition_run", "data_type": "product", "rows": 1}

    monkeypatch.setattr(
        "cube_web.services.partition_remote_job._runner_for_data_type", lambda data_type: fake_runner if data_type == "product" else None
    )

    try:
        exit_code = run_remote_partition_task("partition-remote-cancelled")
    finally:
        set_partition_job_store(None)

    assert exit_code == 0
    assert runner_called["value"] is False
    assert store.get_attempt("partition-remote-cancelled")["status"] == "cancelled"
    assert store.get_batch("REMOTE_TASK_CANCELLED_BATCH")["status"] == "cancelled"


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


def test_optical_partition_test_runner_returns_generated_assets(monkeypatch, tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "generated_a.tif").write_text("a", encoding="utf-8")
    (source / "ignored.txt").write_text("x", encoding="utf-8")

    def fake_run_logical_partition(args):
        run_dir = tmp_path / "run"
        run_dir.mkdir()
        rows_path = run_dir / "index_rows.jsonl"
        rows_path.write_text("", encoding="utf-8")
        return {
            "status": "completed",
            "data_type": "optical",
            "run_dir": str(run_dir),
            "rows_path": str(rows_path),
            "total_index_rows": 0,
            "grid_type": args.grid_type,
            "grid_level": args.grid_level,
            "partition_backend_used": "thread",
            "execution_engine": "thread",
            "ray_parallelism": 0,
        }

    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job.run_logical_partition", fake_run_logical_partition)

    result = partition_runners._run_optical_partition_test(
        {"input_dir": str(source), "partition_backend": "thread", "grid_type": "geohash", "grid_level": 5}
    )

    assert result["mode"] == "partition_test_no_ingest"
    assert result["assets"] == [
        {
            "asset_id": "optical:generated_a",
            "source_uri": str(source / "generated_a.tif"),
            "scene_id": "generated_a",
            "band": "b1",
            "bands": ["b1"],
            "sensor": "optical_mosaic",
            "product_family": "other",
        }
    ]


@pytest.mark.parametrize(
    ("data_type", "runner_name", "job_path"),
    [
        ("optical", "_run_optical_partition_test", "cube_split.jobs.ray_logical_partition_job.run_logical_partition"),
        ("product", "_run_product_partition_test", "cube_split.jobs.product_partition_job.run_product_partition"),
        ("radar", "_run_radar_partition_test", "cube_split.jobs.ray_logical_partition_job.run_logical_partition"),
    ],
)
def test_partition_test_runners_generate_real_assets_when_demo_data_is_missing(monkeypatch, tmp_path, data_type, runner_name, job_path):
    missing = tmp_path / "missing-input"
    captured = {}

    def fake_partition(args):
        captured["input_dir"] = Path(args.input_dir)
        run_dir = tmp_path / f"{data_type}-run"
        run_dir.mkdir()
        rows_path = run_dir / "index_rows.jsonl"
        rows_path.write_text("", encoding="utf-8")
        return {
            "status": "completed",
            "data_type": data_type,
            "run_dir": str(run_dir),
            "rows_path": str(rows_path),
            "total_index_rows": 0,
            "grid_type": args.grid_type,
            "grid_level": args.grid_level,
            "partition_backend_used": "thread",
            "execution_engine": "thread",
            "ray_parallelism": 0,
            "ingest_enabled": False,
        }

    monkeypatch.setattr(job_path, fake_partition)

    result = getattr(partition_runners, runner_name)(
        {"input_dir": str(missing), "partition_backend": "thread", "grid_type": "geohash", "grid_level": 5}
    )

    assert result["mode"] == "partition_test_no_ingest"
    assert result["ingest_enabled"] is False
    assert result["selected_asset_count"] == 1
    assert len(result["assets"]) == 1
    source_path = Path(result["assets"][0]["source_uri"])
    assert source_path.exists()
    assert any(path.is_file() and path.suffix.lower() in {".tif", ".tiff"} for path in captured["input_dir"].iterdir())
    import rasterio

    with rasterio.open(source_path) as dataset:
        assert dataset.crs.to_string() == "EPSG:4326"
        assert dataset.width == 32
        assert dataset.height == 32


def test_carbon_partition_retry_endpoint(monkeypatch):
    def fake_run_carbon_partition_retry(payload=None):
        assert payload == {"request": {"endpoint": "carbon", "payload": {}}, "last_result": {"status": "completed"}}
        return {
            "status": "completed",
            "mode": "partition_retry",
            "data_type": "carbon",
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
    assert body["data_type"] == "carbon"
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
    assert captured["ray_address"] == "10.3.100.182:6379"
    assert captured["metadata_backend"] == "postgres"
    assert captured["asset_storage_backend"] == "minio"
    assert captured["minio_endpoint"] == "10.3.100.179:9000"
    assert captured["minio_bucket"] == "cube"
    assert captured["ingest_enabled"] is False
    assert result["selected_asset_count"] == 1
    assert result["assets"][0]["source_uri"] == "product_1980.tif"
    assert result["assets"][0]["product_year"] == 1980
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

    result = partition_runners._run_product_partition_test(
        {
            "input_dir": str(tmp_path),
            "grid_type": "isea4h",
            "grid_level": 6,
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
    assert "target_pixels_per_hex_edge" not in captured
    assert captured["time_granularity"] == "year"
    assert captured["ingest_enabled"] is False
    assert captured["ray_parallelism"] == 0


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

    result = partition_runners._run_product_partition_test(
        {
            "input_dir": str(tmp_path),
            "grid_type": "geohash",
            "grid_level": 5,
            "partition_backend": "thread",
        }
    )

    assert result["mode"] == "partition_test_no_ingest"
    assert result["data_type"] == "product"
    assert result["output_path"].endswith("index_rows.jsonl")
    assert captured["data_type"] == "product"
    assert captured["product_family"] == "product"
    assert captured["grid_type"] == "geohash"
    assert captured["grid_level"] == 5
    assert captured["partition_backend"] == "thread"
    assert captured["ingest_enabled"] is False


def test_product_partition_runner_parses_minio_secure_string(monkeypatch, tmp_path):
    captured = {}

    def fake_run_product_partition(args):
        captured["minio_secure"] = args.minio_secure
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

    monkeypatch.setattr("cube_split.jobs.product_partition_job.run_product_partition", fake_run_product_partition)

    partition_runners._run_product_partition_test(
        {
            "input_dir": str(tmp_path),
            "grid_type": "geohash",
            "grid_level": 5,
            "partition_backend": "thread",
            "minio_secure": "false",
        }
    )

    assert captured["minio_secure"] is False


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
    assert result["assets"][0]["source_uri"] == "schema_named_radar_asset.dat"
    assert result["assets"][0]["scene_id"] == "SCHEMA_RADAR_SCENE"
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
            "partition_backend": "thread",
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
    assert "target_pixels_per_hex_edge" not in captured
    assert captured["partition_backend"] == "thread"
    assert captured["metadata_backend"] == "none"
    assert captured["asset_storage_backend"] == "local"
    assert captured["ingest_enabled"] is False
    assert captured["ray_parallelism"] == 0


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
            "grid_type": "geohash",
            "grid_level": 5,
            "partition_backend": "thread",
        }
    )

    assert result["mode"] == "partition_test_no_ingest"
    assert result["data_type"] == "radar"
    assert result["output_path"].endswith("index_rows.jsonl")
    assert captured["data_type"] == "radar"
    assert captured["product_family"] == "sentinel1"
    assert captured["grid_type"] == "geohash"
    assert captured["grid_level"] == 5
    assert captured["partition_backend"] == "thread"
    assert captured["metadata_backend"] == "none"
    assert captured["asset_storage_backend"] == "local"
    assert captured["ingest_enabled"] is False


def test_radar_partition_runner_rejects_legacy_plane_grid(tmp_path):
    with pytest.raises(ValueError, match="grid_type must be one of"):
        partition_runners._run_radar_partition_test({"input_dir": str(tmp_path), "grid_type": "plane_grid", "grid_level": 11})


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
