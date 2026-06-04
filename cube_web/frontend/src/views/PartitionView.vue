<script setup>
import { computed, onMounted, onUnmounted, ref, watch } from 'vue';
import { ElMessage, ElMessageBox } from 'element-plus';
import { CircleCloseFilled, Document, EditPen, Refresh, Search, VideoPlay } from '@element-plus/icons-vue';

import GlobeMap from '@/components/GlobeMap.vue';
import ConfigView from '@/views/ConfigView.vue';
import { apiPrefixes, requestGet, requestJson } from '@/api/client';

function initialModule() {
  if (window.location.pathname === '/quality') return 'quality';
  if (window.location.pathname === '/config' || window.location.pathname === '/config.html') return 'config';
  return 'optical';
}

const activeModule = ref(initialModule());
const dataDrawerVisible = ref(false);
const qualityHistoryDrawerVisible = ref(false);
const dataSearch = ref('');
const qualityHistorySearch = ref('');
const qualityHistoryStatus = ref('');
const selectedOpticalBatchIds = ref([]);
const expandedOpticalBatchId = ref('');
const deselectedOpticalAssetKeys = ref({});
const selectedCarbonBatchIds = ref([]);
const expandedCarbonBatchId = ref('');
const deselectedCarbonObservationKeys = ref({});
const selectedRadarBatchIds = ref([]);
const expandedRadarBatchId = ref('');
const deselectedRadarAssetKeys = ref({});
const selectedProductBatchIds = ref([]);
const expandedProductBatchId = ref('');
const deselectedProductAssetKeys = ref({});
const defaultLogicalGridLevel = 5;
const defaultEntityGridLevel = 4;
const gridTypeLabels = {
  geohash: '四边形格网',
  tile_matrix: '平面格网',
  isea4h: '六边形格网',
};
const partitionTaskPollIntervalMs = 1500;
const partitionTaskMaxPolls = 1200;
const opticalGridType = ref('geohash');
const opticalGridLevel = ref(defaultLogicalGridLevel);
const entityGridLevel = ref(defaultEntityGridLevel);
const radarGridType = ref('geohash');
const radarGridLevel = ref(5);
const productGridType = ref('geohash');
const productGridLevel = ref(5);
const gridLevelManualOverrides = ref({
  optical: false,
  entity: false,
  radar: false,
  product: false,
});
const mapGridLoading = ref(false);
const mapGridGeometries = ref([]);
const resultLoading = ref(false);
const resultRows = ref([]);
const lastPartitionResult = ref(null);
const lastPartitionRequest = ref(null);
const ingestLoading = ref(false);
const ingestConfirmLoading = ref(false);
const ingestPreview = ref(null);
const ingestResult = ref(null);
const partitionStartedAt = ref(null);
const partitionFinishedAt = ref(null);
const partitionElapsedSec = ref(0);
let partitionTimer = null;
const partitionStages = ref([
  { key: 'prepare', label: '准备任务', detail: '等待选择数据批次与剖分参数。', status: 'pending' },
  { key: 'queue', label: '读取数据队列', detail: '解析已载入资产、批次、波段与时间信息。', status: 'pending' },
  { key: 'partition', label: '执行剖分', detail: '生成 COG、按格网覆盖切分窗口并输出索引行。', status: 'pending' },
  { key: 'persist', label: '质检入库', detail: '执行自动质检并保存质检报告，正式入库需人工确认。', status: 'pending' },
]);
const partitionStageDetailVisible = ref(false);
const selectedPartitionStageKey = ref('');
const selectedPartitionStage = computed(() => (
  partitionStages.value.find((stage) => stage.key === selectedPartitionStageKey.value) || null
));
const partitionContextDetailVisible = ref(false);
const selectedPartitionContextLabel = ref('');
const qualityLoading = ref(false);
const qualityHistoryLoading = ref(false);
const qualityExportLoading = ref(false);
const qualityReport = ref(null);
const qualityHistory = ref([]);
const qualityError = ref('');
const qualityTargetCrs = ref('EPSG:4326');
const qualityHistoryLimit = ref(30);
const selectedQualityReportId = ref('');
const qualityDataType = ref('optical');
const qualityReportDataTypes = new Set(['optical', 'product', 'carbon']);
const ingestDefaults = ref({
  dataset: 'optical',
  sensor: 'optical_mosaic',
  quality_rule: 'best_quality_wins',
  allow_failed_quality: false,
});

function parseResolution(value) {
  if (typeof value === 'number' && Number.isFinite(value) && value > 0) return value;
  if (typeof value === 'string') {
    const match = value.match(/(\d+(?:\.\d+)?)/);
    if (!match) return null;
    const parsed = Number(match[1]);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
  }
  if (Array.isArray(value)) {
    const values = value.map(parseResolution).filter((item) => item !== null);
    return values.length ? Math.min(...values) : null;
  }
  if (value && typeof value === 'object') {
    const keys = ['resolution', 'resolution_m', 'spatial_resolution', 'spatial_resolution_m', 'x', 'y', 'width', 'height'];
    const values = keys.map((key) => parseResolution(value[key])).filter((item) => item !== null);
    return values.length ? Math.min(...values) : null;
  }
  return null;
}

function assetResolution(asset) {
  const keys = [
    'resolution',
    'resolution_m',
    'spatial_resolution',
    'spatial_resolution_m',
    'ground_resolution',
    'pixel_size',
    'pixel_size_m',
    'gsd',
    'gsd_m',
    'resolution_x',
    'resolution_y',
    'pixel_size_x',
    'pixel_size_y',
  ];
  const values = keys.map((key) => parseResolution(asset?.[key])).filter((item) => item !== null);
  return values.length ? Math.min(...values) : null;
}

function defaultGridLevelForGridType(gridType) {
  return gridType === 'isea4h' ? defaultEntityGridLevel : defaultLogicalGridLevel;
}

function formatGridType(gridType) {
  return gridTypeLabels[gridType] || gridType || '-';
}

function defaultGridLevelForResolution(resolution, gridType, fallback = defaultGridLevelForGridType(gridType)) {
  if (!Number.isFinite(resolution) || resolution <= 0) return fallback;
  if (gridType === 'isea4h') return resolution < 10 ? 5 : defaultEntityGridLevel;
  if (resolution < 10) return 8;
  if (resolution <= 30) return 7;
  return 6;
}

function defaultGridLevelFromAssets(assets, gridType, fallback = defaultGridLevelForGridType(gridType)) {
  const resolutions = (Array.isArray(assets) ? assets : [])
    .map(assetResolution)
    .filter((resolution) => resolution !== null);
  if (!resolutions.length) return fallback;
  return defaultGridLevelForResolution(Math.min(...resolutions), gridType, fallback);
}

const managedOpticalBatches = ref([]);
const managedCarbonBatches = ref([]);
const managedRadarBatches = ref([]);
const managedProductBatches = ref([]);
const partitionBatchLoading = ref(false);
const partitionBatchDetailVisible = ref(false);
const partitionBatchDetailLoading = ref(false);
const partitionBatchDetailAction = ref('');
const partitionBatchDetail = ref(null);
const partitionBatchDetailTab = ref('overview');
const partitionBatchDetailSearch = ref('');
const partitionBatchDetailAssetStatus = ref('all');
const partitionBatchDetailSelectedAssetIds = ref([]);

const visibleOpticalBatches = computed(() => managedOpticalBatches.value);
const visibleCarbonBatches = computed(() => managedCarbonBatches.value);
const visibleRadarBatches = computed(() => managedRadarBatches.value);
const visibleProductBatches = computed(() => managedProductBatches.value);

function setBatchSelection(batchId, dataType) {
  if (dataType === 'carbon') {
    selectedCarbonBatchIds.value = [batchId];
    expandedCarbonBatchId.value = batchId;
  } else if (dataType === 'radar') {
    selectedRadarBatchIds.value = [batchId];
    expandedRadarBatchId.value = batchId;
  } else if (dataType === 'product') {
    selectedProductBatchIds.value = [batchId];
    expandedProductBatchId.value = batchId;
  } else {
    selectedOpticalBatchIds.value = [batchId];
    expandedOpticalBatchId.value = batchId;
  }
}

function preferredBatchId(batches) {
  if (!Array.isArray(batches) || !batches.length) return '';
  return batches.find((batch) => batch.status !== 'succeeded')?.id || batches[0]?.id || '';
}

function partitionStatusText(status) {
  const map = {
    pending: '待处理',
    queued: '已排队',
    running: '执行中',
    retrying: '重试中',
    cancel_requested: '取消中',
    failed: '失败',
    manual_required: '人工确认',
    cancelled: '已取消',
    succeeded: '已完成',
  };
  return map[status] || status || '未知';
}

function partitionStatusType(status) {
  const map = {
    pending: 'info',
    queued: 'info',
    running: 'warning',
    retrying: 'warning',
    cancel_requested: 'warning',
    failed: 'danger',
    manual_required: 'warning',
    cancelled: 'info',
    succeeded: 'success',
  };
  return map[status] || 'info';
}

function partitionAttemptStatusText(status) {
  return partitionStatusText(status);
}

function partitionAttemptStatusType(status) {
  return partitionStatusType(status);
}

function partitionAssetStatusText(status) {
  return partitionStatusText(status);
}

function partitionAssetStatusType(status) {
  return partitionStatusType(status);
}

function partitionOperationText(operation) {
  const map = {
    auto_run: '自动执行',
    auto_retry: '自动重试',
    manual_retry: '人工重试',
    manual_asset_retry: '失败资产重试',
    demo: '批次执行',
    retry: '重试',
    test: '测试',
    run: '执行',
  };
  return map[operation] || operation || '-';
}

function formatPartitionTimestamp(value) {
  return formatQualityTime(value);
}

function partitionBatchDetailTitle(batch) {
  if (!batch) return '批次详情';
  return batch.batch_name || batch.name || batch.batch_id || batch.id || '批次详情';
}

function partitionBatchDetailSubtitle(batch) {
  if (!batch) return '';
  const parts = [batch.batch_id || batch.id || '-', dataLabelsByModule[batch.data_type] || batch.data_type || '-'];
  return parts.join(' · ');
}

function partitionBatchCanCancel(batch) {
  return ['queued', 'running', 'retrying', 'cancel_requested'].includes(batch?.status);
}

function partitionBatchCanRetrySelectedAssets(batch) {
  return Boolean(batch) && ['failed', 'manual_required'].includes(batch.status) && partitionBatchSelectedRetryableAssets.value.length > 0;
}

function partitionBatchExecutionOperation(batch) {
  return ['failed', 'manual_required'].includes(batch?.status) ? 'retry' : 'run';
}

function partitionBatchConfigOverride(batch) {
  if (batch?.data_type === 'optical') {
    return {
      grid_type: opticalGridType.value,
      grid_level: Number(selectedMapGridLevel.value),
      grid_level_mode: isGridLevelManual('optical') || opticalGridType.value === 'isea4h' ? 'manual' : 'auto',
    };
  }
  if (batch?.data_type === 'radar') {
    return {
      grid_type: radarGridType.value,
      grid_level: Number(radarGridLevel.value),
      grid_level_mode: isGridLevelManual('radar') ? 'manual' : 'auto',
    };
  }
  if (batch?.data_type === 'product') {
    return {
      grid_type: productGridType.value,
      grid_level: Number(productGridLevel.value),
      grid_level_mode: isGridLevelManual('product') ? 'manual' : 'auto',
      target_crs: batch.normalized_payload?.target_crs || 'EPSG:4326',
    };
  }
  return {};
}

function partitionAssetRetryable(asset) {
  return ['failed', 'manual_required'].includes(asset?.status);
}

function partitionAssetSearchText(asset) {
  const payload = asset?.asset_payload || {};
  const values = [
    asset?.asset_id,
    asset?.scene_id,
    asset?.source_uri,
    asset?.status,
    asset?.last_error,
    asset?.last_run_dir,
    payload?.observation_id,
    payload?.product_name,
    payload?.product_year,
    payload?.scene_id,
    payload?.band,
    Array.isArray(payload?.bands) ? payload.bands.join(',') : '',
    payload?.acq_time,
    payload?.source_uri,
    payload?.xco2,
  ];
  return values.filter(Boolean).join(' ').toLowerCase();
}

function partitionBatchActionLabel(batch) {
  if (partitionBatchCanCancel(batch)) return '取消任务';
  if (batch?.status === 'manual_required') return '继续重试';
  if (batch?.status === 'failed') return '重试批次';
  if (batch?.status === 'cancelled') return '重新执行';
  if (batch?.status === 'succeeded') return '再次执行';
  return batch?.attempt_count ? '再次执行' : '开始执行';
}

function partitionBatchActionType(batch) {
  if (partitionBatchCanCancel(batch)) return 'danger';
  if (['failed', 'manual_required', 'cancelled'].includes(batch?.status)) return 'warning';
  return 'primary';
}

function partitionBatchActionIcon(batch) {
  if (partitionBatchCanCancel(batch)) return CircleCloseFilled;
  if (['failed', 'manual_required', 'cancelled'].includes(batch?.status)) return Refresh;
  return VideoPlay;
}

function partitionBatchSummary(batch) {
  const attemptCount = Number(batch.attempt_count || 0);
  const lastError = batch.last_error ? `最近错误：${batch.last_error}` : '暂无错误信息';
  return `${partitionStatusText(batch.status)} · 尝试 ${attemptCount} 次 · ${lastError}`;
}

function partitionBatchDetailPayloadRows(batch) {
  if (!batch) return [];
  const payload = batch.normalized_payload || {};
  const rows = [
    { label: '数据类型', value: dataLabelsByModule[batch.data_type] || batch.data_type || '-' },
    { label: '状态', value: partitionStatusText(batch.status) },
    { label: '尝试次数', value: batch.attempt_count ?? 0 },
    { label: '自动重试次数', value: batch.max_auto_retries ?? 0 },
    { label: '最后任务', value: batch.last_task_id || '-' },
    { label: '最后错误', value: batch.last_error || '-' },
    { label: '创建时间', value: formatPartitionTimestamp(batch.created_at) },
    { label: '更新时间', value: formatPartitionTimestamp(batch.updated_at) },
  ];
  if (batch.partitioned_at) {
    rows.splice(5, 0, { label: '完成时间', value: formatPartitionTimestamp(batch.partitioned_at) });
  }
  if (batch.manual_required_at) {
    rows.splice(6, 0, { label: '人工确认时间', value: formatPartitionTimestamp(batch.manual_required_at) });
  }
  if (batch.data_type === 'optical') {
    rows.splice(2, 0,
      { label: '格网类型', value: formatGridType(payload.grid_type) },
      { label: '格网层级', value: payload.grid_level ?? '-' },
      { label: '选择资产', value: Array.isArray(payload.selected_assets) ? payload.selected_assets.length : 0 },
    );
  }
  if (batch.data_type === 'carbon') {
    rows.splice(2, 0,
      { label: '产品类型', value: payload.product_type || '-' },
      { label: '选择观测', value: Array.isArray(payload.selected_observations) ? payload.selected_observations.length : 0 },
    );
  }
  if (batch.data_type === 'product') {
    rows.splice(2, 0,
      { label: '格网类型', value: formatGridType(payload.grid_type) },
      { label: '格网层级', value: payload.grid_level ?? '-' },
      { label: '目标参考系统', value: payload.target_crs || '-' },
      { label: '选择年份', value: Array.isArray(payload.selected_assets) ? payload.selected_assets.length : 0 },
    );
  }
  if (batch.data_type === 'radar') {
    rows.splice(2, 0,
      { label: '格网类型', value: formatGridType(payload.grid_type) },
      { label: '格网层级', value: payload.grid_level ?? '-' },
      { label: '目标参考系统', value: payload.target_crs || '-' },
      { label: '选择资产', value: Array.isArray(payload.selected_assets) ? payload.selected_assets.length : 0 },
    );
  }
  return rows;
}

function partitionBatchAssetRows(batch) {
  if (!batch) return [];
  return (partitionBatchDetail.value?.assets || []).map((asset) => {
    const payload = asset.asset_payload || {};
    if (batch.data_type === 'carbon') {
      return {
        asset_id: asset.asset_id,
        status: asset.status || 'pending',
        source_uri: asset.source_uri,
        title: asset.scene_id || payload.observation_id || asset.asset_id,
        subtitle: payload.observation_id || asset.scene_id || '-',
        details: [
          `时间：${payload.acq_time || '-'}`,
          `XCO2：${payload.xco2 ?? payload.xco2_value ?? '-'}`,
          `Q${payload.quality_flag ?? payload.xco2_quality_flag ?? '-'}`,
        ],
        error: asset.last_error || '',
        retryable: partitionAssetRetryable(asset),
      };
    }
    if (batch.data_type === 'product') {
      return {
        asset_id: asset.asset_id,
        status: asset.status || 'pending',
        source_uri: asset.source_uri,
        title: payload.product_year ? `${payload.product_year} 年` : asset.scene_id || asset.asset_id,
        subtitle: payload.product_name || payload.band || '-',
        details: [
          `时间：${payload.acq_time || '-'}`,
          `目标参考系统：${payload.target_crs || batch.normalized_payload?.target_crs || '-'}`,
          `波段：${payload.band || '-'}`,
        ],
        error: asset.last_error || '',
        retryable: partitionAssetRetryable(asset),
      };
    }
    return {
      asset_id: asset.asset_id,
      status: asset.status || 'pending',
      source_uri: asset.source_uri,
      title: asset.scene_id || payload.scene_id || asset.asset_id,
      subtitle: asset.asset_id,
      details: [
        `时间：${payload.acq_time || '-'}`,
        `波段：${Array.isArray(payload.bands) ? payload.bands.join(', ') : payload.band || '-'}`,
      ],
      error: asset.last_error || '',
      retryable: partitionAssetRetryable(asset),
    };
  });
}

const partitionBatchDetailAssets = computed(() => partitionBatchAssetRows(partitionBatchDetail.value));
const partitionBatchDetailAttempts = computed(() => partitionBatchDetail.value?.attempts || []);
const partitionBatchRetryableAssetCount = computed(() => partitionBatchDetailAssets.value.filter((asset) => asset.retryable).length);
const partitionBatchSelectedRetryableAssets = computed(() => (
  partitionBatchDetailAssets.value.filter((asset) => partitionBatchDetailSelectedAssetIds.value.includes(asset.asset_id) && asset.retryable)
));
const partitionBatchDetailFilteredAssets = computed(() => {
  const keyword = partitionBatchDetailSearch.value.trim().toLowerCase();
  const status = partitionBatchDetailAssetStatus.value;
  return partitionBatchDetailAssets.value.filter((asset) => {
    const matchesStatus = status === 'all' || asset.status === status;
    const matchesKeyword = !keyword || partitionAssetSearchText(asset).includes(keyword);
    return matchesStatus && matchesKeyword;
  });
});

function partitionBatchAssetSelectable(row) {
  return row.retryable;
}

function clearPartitionBatchDetailSelection() {
  partitionBatchDetailSelectedAssetIds.value = [];
}

function partitionBatchAssetSelectionChange(rows) {
  partitionBatchDetailSelectedAssetIds.value = rows.map((row) => row.asset_id);
}

function selectManagedBatchByDetail(batch) {
  setBatchSelection(batch.batch_id || batch.id, batch.data_type);
}

async function loadPartitionBatchDetail(batchId) {
  const { partitionPrefix } = apiPrefixes();
  const [batch, assetsResp, attemptsResp] = await Promise.all([
    requestGet(`${partitionPrefix}/batches/${batchId}`),
    requestGet(`${partitionPrefix}/batches/${batchId}/assets`),
    requestGet(`${partitionPrefix}/batches/${batchId}/attempts`),
  ]);
  return {
    ...batch,
    id: batch.batch_id || batch.id || batchId,
    assets: assetsResp.assets || [],
    attempts: attemptsResp.attempts || [],
  };
}

async function openPartitionBatchDetail(batch) {
  const resolved = typeof batch === 'string'
    ? [...visibleOpticalBatches.value, ...visibleCarbonBatches.value, ...visibleRadarBatches.value, ...visibleProductBatches.value].find((item) => (item.id || item.batch_id) === batch)
    : batch;
  const batchId = resolved?.id || resolved?.batch_id || batch;
  if (!batchId) return;
  partitionBatchDetailVisible.value = true;
  partitionBatchDetailLoading.value = true;
  partitionBatchDetailAction.value = '';
  partitionBatchDetailSearch.value = '';
  partitionBatchDetailAssetStatus.value = 'all';
  clearPartitionBatchDetailSelection();
  partitionBatchDetailTab.value = 'overview';
  try {
    const detail = await loadPartitionBatchDetail(batchId);
    partitionBatchDetail.value = detail;
    selectManagedBatchByDetail(detail);
    return detail;
  } catch (error) {
    partitionBatchDetail.value = null;
    ElMessage.error(error.message);
    return null;
  } finally {
    partitionBatchDetailLoading.value = false;
  }
}

async function refreshPartitionBatchDetail() {
  const batchId = partitionBatchDetail.value?.id || partitionBatchDetail.value?.batch_id;
  if (!batchId) return;
  partitionBatchDetailLoading.value = true;
  try {
    const detail = await loadPartitionBatchDetail(batchId);
    partitionBatchDetail.value = detail;
  } catch (error) {
    ElMessage.error(error.message);
  } finally {
    partitionBatchDetailLoading.value = false;
  }
}

async function runPartitionBatchFromDetail() {
  const batch = partitionBatchDetail.value;
  const batchId = batch?.id || batch?.batch_id;
  if (!batchId) return;
  const operation = partitionBatchExecutionOperation(batch);
  const configOverride = partitionBatchConfigOverride(batch);
  partitionBatchDetailAction.value = operation;
  resultLoading.value = true;
  resultRows.value = [];
  lastPartitionResult.value = null;
  ingestPreview.value = null;
  ingestResult.value = null;
  startPartitionTimer();
  resetPartitionStages();
  try {
    const { partitionPrefix } = apiPrefixes();
    const apiPath = `${partitionPrefix}/batches/${batchId}/${operation}`;
    setPartitionStage('prepare', 'done', '已锁定批次详情中的执行参数。');
    setPartitionStage('queue', 'running', operation === 'retry' ? `批次 ${batchId} 正在提交人工重试。` : `批次 ${batchId} 正在提交执行队列。`);
    const response = await requestJson(apiPath, { config_override: configOverride });
    if (!response.task_id) {
      throw new Error('批次执行任务未返回 task_id');
    }
    lastPartitionRequest.value = {
      kind: 'batch',
      batchId,
      endpoint: `batches/${batchId}`,
      payload: { config_override: configOverride },
      operation,
      apiPath,
    };
    setPartitionStage('queue', 'done', `后端任务 ${response.task_id} 已创建。`);
    setPartitionStage('partition', 'running', `后台任务 ${response.task_id} 执行中。`);
    const result = await waitForPartitionTask(partitionPrefix, response.task_id);
    setPartitionStage('partition', 'done', `已生成 ${result.rows ?? result.total_index_rows ?? 0} 条索引行。`);
    setPartitionStage('persist', 'done', result.quality_report_id ? `质检报告已保存：${result.quality_report_id}` : '执行结果已返回。');
    lastPartitionResult.value = result;
    resultRows.value = formatRows(result);
    if (activeModule.value === 'quality') {
      if (result.quality_report) {
        qualityReport.value = result.quality_report;
        selectedQualityReportId.value = result.quality_report.report_id || result.quality_report_id || '';
        await loadQualityHistory();
      } else {
        await refreshQuality();
      }
    }
    ElMessage.success(operation === 'retry' ? '批次重试完成' : '批次执行完成');
    await loadPartitionBatches();
    await refreshPartitionBatchDetail();
  } catch (error) {
    partitionStages.value = partitionStages.value.map((item) => (item.status === 'running' ? { ...item, status: 'failed' } : item));
    const failure = buildPartitionFailureResult(error, lastPartitionRequest.value || {});
    lastPartitionResult.value = failure;
    resultRows.value = formatRows(failure);
    setPartitionStage('persist', 'failed', `执行失败：${failure.error}`);
    ElMessage.error(error.message);
  } finally {
    stopPartitionTimer();
    resultLoading.value = false;
    partitionBatchDetailAction.value = '';
  }
}

async function retrySelectedPartitionAssetsFromDetail() {
  const batch = partitionBatchDetail.value;
  const batchId = batch?.id || batch?.batch_id;
  const selectedAssetIds = partitionBatchSelectedRetryableAssets.value.map((asset) => asset.asset_id);
  if (!batchId || !selectedAssetIds.length) {
    ElMessage.warning('请先选择失败或人工确认状态的资产');
    return;
  }
  partitionBatchDetailAction.value = 'assetRetry';
  resultLoading.value = true;
  resultRows.value = [];
  lastPartitionResult.value = null;
  ingestPreview.value = null;
  ingestResult.value = null;
  startPartitionTimer();
  resetPartitionStages();
  try {
    const { partitionPrefix } = apiPrefixes();
    const configOverride = partitionBatchConfigOverride(batch);
    setPartitionStage('prepare', 'done', '已锁定失败资产重试范围。');
    setPartitionStage('queue', 'running', `正在提交 ${selectedAssetIds.length} 条失败资产重试。`);
    const response = await requestJson(`${partitionPrefix}/assets/retry`, {
      asset_ids: selectedAssetIds,
      config_override: configOverride,
    });
    if (!response.task_id) {
      throw new Error('失败资产重试任务未返回 task_id');
    }
    lastPartitionRequest.value = {
      kind: 'assets',
      endpoint: 'assets',
      operation: 'retry',
      payload: { asset_ids: selectedAssetIds, config_override: configOverride },
      apiPath: `${partitionPrefix}/assets/retry`,
    };
    setPartitionStage('queue', 'done', `后端任务 ${response.task_id} 已创建。`);
    setPartitionStage('partition', 'running', `后台任务 ${response.task_id} 执行中。`);
    const result = await waitForPartitionTask(partitionPrefix, response.task_id);
    setPartitionStage('partition', 'done', `失败资产重试完成，生成 ${result.rows ?? result.total_index_rows ?? 0} 条索引行。`);
    setPartitionStage('persist', 'done', result.quality_report_id ? `质检报告已保存：${result.quality_report_id}` : '重试结果已返回。');
    lastPartitionResult.value = result;
    resultRows.value = formatRows(result);
    if (activeModule.value === 'quality') {
      if (result.quality_report) {
        qualityReport.value = result.quality_report;
        selectedQualityReportId.value = result.quality_report.report_id || result.quality_report_id || '';
        await loadQualityHistory();
      } else {
        await refreshQuality();
      }
    }
    clearPartitionBatchDetailSelection();
    ElMessage.success('失败资产重试完成');
    await loadPartitionBatches();
    await refreshPartitionBatchDetail();
  } catch (error) {
    partitionStages.value = partitionStages.value.map((item) => (item.status === 'running' ? { ...item, status: 'failed' } : item));
    const failure = buildPartitionFailureResult(error, lastPartitionRequest.value || {});
    lastPartitionResult.value = failure;
    resultRows.value = formatRows(failure);
    setPartitionStage('persist', 'failed', `重试失败：${failure.error}`);
    ElMessage.error(error.message);
  } finally {
    stopPartitionTimer();
    resultLoading.value = false;
    partitionBatchDetailAction.value = '';
  }
}

async function cancelPartitionBatchFromDetail() {
  const batch = partitionBatchDetail.value;
  const batchId = batch?.id || batch?.batch_id;
  if (!batchId) return;
  try {
    await ElMessageBox.confirm(
      '取消会立即请求执行层中断当前任务，适用于参数配置错误或任务无需继续执行的场景。',
      '取消批次任务',
      { confirmButtonText: '确认取消', cancelButtonText: '返回', type: 'warning' },
    );
  } catch {
    return;
  }
  partitionBatchDetailAction.value = 'cancel';
  try {
    const { partitionPrefix } = apiPrefixes();
    const response = await requestJson(`${partitionPrefix}/batches/${batchId}/cancel`, {});
    if (response?.status === 'cancel_requested' || response?.status === 'cancelled') {
      ElMessage.success(response.status === 'cancelled' ? '任务已取消' : '已发起取消请求');
    } else {
      ElMessage.success('已发起取消请求');
    }
    await loadPartitionBatches();
    await refreshPartitionBatchDetail();
  } catch (error) {
    ElMessage.error(error.message);
  } finally {
    partitionBatchDetailAction.value = '';
  }
}

async function handlePartitionBatchPrimaryAction(batch) {
  const detail = await openPartitionBatchDetail(batch);
  if (!detail) return;
  if (partitionBatchCanCancel(detail)) {
    await cancelPartitionBatchFromDetail();
    return;
  }
  await runPartitionBatchFromDetail();
}

const visibleDataRowsByModule = computed(() => ({
  optical: visibleOpticalBatches.value.map((batch) => ({
    id: batch.id,
    name: batch.name,
    params: `${batch.assets.length} 条资产`,
    status: batch.status,
  })),
  carbon: visibleCarbonBatches.value.map((batch) => ({
    id: batch.id,
    name: batch.name,
    params: `${batch.observations.length} 条观测 | ${batch.product_type || '-'}`,
    status: batch.status,
  })),
  radar: visibleRadarBatches.value.map((batch) => ({
    id: batch.id,
    name: batch.name,
    params: `${batch.assets.length} 条资产 | ${batch.sensor || 'sentinel1_sar'}`,
    status: batch.status,
  })),
  product: visibleProductBatches.value.map((batch) => ({
    id: batch.id,
    name: batch.name,
    params: `${batch.assets.length} 个年份 | ${batch.target_crs || 'EPSG:4326'}`,
    status: batch.status,
  })),
}));

const dataLabelsByModule = {
  optical: '光学遥感数据',
  carbon: '碳卫星数据',
  radar: '雷达遥感数据',
  product: '信息产品数据',
};

const partitionEndpointsByModule = {
  optical: 'optical',
  carbon: 'carbon',
  radar: 'radar',
  product: 'product',
};

const testModules = new Set(['optical', 'carbon', 'radar', 'product']);

const activeDataRows = computed(() => visibleDataRowsByModule.value[activeModule.value] || []);

const activeDataLabel = computed(() => dataLabelsByModule[activeModule.value] || '已载入数据');

const qualityManagedBatches = computed(() => {
  if (qualityDataType.value === 'carbon') return managedCarbonBatches.value;
  if (qualityDataType.value === 'radar') return managedRadarBatches.value;
  if (qualityDataType.value === 'product') return managedProductBatches.value;
  return managedOpticalBatches.value;
});

const qualityManualBatches = computed(() => (
  qualityManagedBatches.value.filter((batch) => ['failed', 'manual_required', 'cancelled'].includes(batch.status))
));

const qualityManualBatchStats = computed(() => {
  const batches = qualityManagedBatches.value;
  const countByStatus = (status) => batches.filter((batch) => batch.status === status).length;
  return [
    { label: '失败批次', value: countByStatus('failed'), status: 'failed' },
    { label: '人工确认', value: countByStatus('manual_required'), status: 'manual_required' },
    { label: '运行中', value: batches.filter((batch) => ['queued', 'running', 'retrying'].includes(batch.status)).length, status: 'running' },
  ];
});

const selectedDataName = computed(() => {
  if (activeModule.value === 'optical') {
    if (!selectedOpticalBatchIds.value.length) return '未选择';
    const names = visibleOpticalBatches.value
      .filter((batch) => selectedOpticalBatchIds.value.includes(batch.id))
      .map((batch) => batch.name);
    return names.join('，');
  }
  if (activeModule.value === 'carbon') {
    if (!selectedCarbonBatchIds.value.length) return '未选择';
    const names = visibleCarbonBatches.value
      .filter((batch) => selectedCarbonBatchIds.value.includes(batch.id))
      .map((batch) => batch.name);
    return names.join('，');
  }
  if (activeModule.value === 'radar') {
    if (!selectedRadarBatchIds.value.length) return '未选择';
    const names = visibleRadarBatches.value
      .filter((batch) => selectedRadarBatchIds.value.includes(batch.id))
      .map((batch) => batch.name);
    return names.join('，');
  }
  if (activeModule.value === 'product') {
    if (!selectedProductBatchIds.value.length) return '未选择';
    const names = visibleProductBatches.value
      .filter((batch) => selectedProductBatchIds.value.includes(batch.id))
      .map((batch) => batch.name);
    return names.join('，');
  }
  return '未选择';
});

const filteredQualityHistory = computed(() => {
  const keyword = qualityHistorySearch.value.trim().toLowerCase();
  return qualityHistory.value.filter((row) => {
    const matchesKeyword =
      !keyword ||
      [row.dataset, row.run_name, row.run_dir, row.report_id]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(keyword));
    const matchesStatus = !qualityHistoryStatus.value || row.status === qualityHistoryStatus.value;
    return matchesKeyword && matchesStatus;
  });
});

const selectedQualityRecord = computed(() => {
  if (!selectedQualityReportId.value) return null;
  return qualityHistory.value.find((row) => row.report_id === selectedQualityReportId.value) || null;
});

const selectedOpticalAssets = computed(() => {
  const selectedBatchSet = new Set(selectedOpticalBatchIds.value);
  const rows = [];
  visibleOpticalBatches.value.forEach((batch) => {
    if (!selectedBatchSet.has(batch.id)) return;
    batch.assets.forEach((asset) => {
      if (isOpticalAssetSelected(batch.id, asset)) {
        rows.push({ ...asset, batch_id: batch.id });
      }
    });
  });
  return rows;
});

const selectedCarbonObservations = computed(() => {
  const selectedBatchSet = new Set(selectedCarbonBatchIds.value);
  const rows = [];
  visibleCarbonBatches.value.forEach((batch) => {
    if (!selectedBatchSet.has(batch.id)) return;
    batch.observations.forEach((observation) => {
      if (isCarbonObservationSelected(batch.id, observation)) {
        rows.push({
          ...observation,
          batch_id: batch.id,
          batch_name: batch.name,
          product_type: batch.product_type,
          source_uri: batch.source_uri,
        });
      }
    });
  });
  return rows;
});

const selectedRadarAssets = computed(() => {
  const selectedBatchSet = new Set(selectedRadarBatchIds.value);
  const rows = [];
  visibleRadarBatches.value.forEach((batch) => {
    if (!selectedBatchSet.has(batch.id)) return;
    batch.assets.forEach((asset) => {
      if (isRadarAssetSelected(batch.id, asset)) {
        rows.push({
          ...asset,
          batch_id: batch.id,
          batch_name: batch.name,
          product_family: batch.product_family,
          sensor: batch.sensor,
          target_crs: batch.target_crs,
        });
      }
    });
  });
  return rows;
});

const selectedProductAssets = computed(() => {
  const selectedBatchSet = new Set(selectedProductBatchIds.value);
  const rows = [];
  visibleProductBatches.value.forEach((batch) => {
    if (!selectedBatchSet.has(batch.id)) return;
    batch.assets.forEach((asset) => {
      if (isProductAssetSelected(batch.id, asset)) {
        rows.push({
          ...asset,
          batch_id: batch.id,
          batch_name: batch.name,
          product_family: batch.product_family,
          sensor: batch.sensor,
          target_crs: batch.target_crs,
        });
      }
    });
  });
  return rows;
});

function cornersToPolygon(corners) {
  if (!Array.isArray(corners) || corners.length !== 4) return null;
  return {
    type: 'Polygon',
    coordinates: [[
      [corners[0][0], corners[0][1]],
      [corners[1][0], corners[1][1]],
      [corners[2][0], corners[2][1]],
      [corners[3][0], corners[3][1]],
      [corners[0][0], corners[0][1]],
    ]],
  };
}

function cornersToBbox(corners) {
  const lons = corners.map((c) => Number(c[0]));
  const lats = corners.map((c) => Number(c[1]));
  return [Math.min(...lons), Math.min(...lats), Math.max(...lons), Math.max(...lats)];
}

function assetBands(asset) {
  if (Array.isArray(asset?.bands) && asset.bands.length) return asset.bands;
  if (asset?.band) return [asset.band];
  return [];
}

function assetBandsText(asset) {
  const bands = assetBands(asset);
  return bands.length ? bands.join(', ') : '-';
}

const mapBatchGeometries = computed(() => selectedOpticalAssets.value
  .map((asset) => {
    const geometry = cornersToPolygon(asset.corners);
    if (!geometry) return null;
    return {
      geometry,
      label: `${asset.scene_id} / ${assetBandsText(asset)}`,
      color: '#2f91ea',
      fillColor: '#2f91ea',
      fillOpacity: 0.12,
      weight: 2,
    };
  })
  .filter(Boolean));

const productMapGeometries = computed(() => selectedProductAssets.value
  .map((asset) => {
    const geometry = cornersToPolygon(asset.corners);
    if (!geometry) return null;
    return {
      geometry,
      label: `${asset.product_year} / ${asset.product_name}`,
      color: '#3f7f5f',
      fillColor: '#3f7f5f',
      fillOpacity: 0.12,
      weight: 2,
    };
  })
  .filter(Boolean));

const radarMapGeometries = computed(() => selectedRadarAssets.value
  .map((asset) => {
    const geometry = cornersToPolygon(asset.corners);
    if (!geometry) return null;
    return {
      geometry,
      label: `${asset.scene_id} / ${(asset.polarization || asset.band || '').toUpperCase()}`,
      color: '#b06f2c',
      fillColor: '#b06f2c',
      fillOpacity: 0.12,
      weight: 2,
    };
  })
  .filter(Boolean));

const selectedMapAssets = computed(() => (
  activeModule.value === 'product'
    ? selectedProductAssets.value
    : activeModule.value === 'radar'
      ? selectedRadarAssets.value
      : selectedOpticalAssets.value
));
const selectedMapGridType = computed(() => (
  activeModule.value === 'product'
    ? productGridType.value
    : activeModule.value === 'radar'
      ? radarGridType.value
      : opticalGridType.value
));
const selectedMapGridLevel = computed(() => (
  activeModule.value === 'product' ? productGridLevel.value : activeModule.value === 'radar' ? radarGridLevel.value : opticalGridType.value === 'isea4h' ? entityGridLevel.value : opticalGridLevel.value
));
function gridLevelManualKeyFor(moduleName = activeModule.value) {
  if (moduleName === 'optical' && opticalGridType.value === 'isea4h') return 'entity';
  return moduleName;
}

function gridTypeForModule(moduleName = activeModule.value) {
  if (moduleName === 'product') return productGridType.value;
  if (moduleName === 'radar') return radarGridType.value;
  return opticalGridType.value;
}

function selectedAssetsForModule(moduleName = activeModule.value) {
  if (moduleName === 'product') return selectedProductAssets.value;
  if (moduleName === 'radar') return selectedRadarAssets.value;
  if (moduleName === 'optical') return selectedOpticalAssets.value;
  return [];
}

function isGridLevelManual(moduleName = activeModule.value) {
  const key = gridLevelManualKeyFor(moduleName);
  return Boolean(gridLevelManualOverrides.value[key]);
}

function setGridLevelManual(moduleName, value) {
  const key = gridLevelManualKeyFor(moduleName);
  gridLevelManualOverrides.value = { ...gridLevelManualOverrides.value, [key]: value };
}

function setGridLevelForModule(moduleName, level) {
  if (moduleName === 'product') {
    productGridLevel.value = level;
  } else if (moduleName === 'radar') {
    radarGridLevel.value = level;
  } else if (opticalGridType.value === 'isea4h') {
    entityGridLevel.value = level;
  } else {
    opticalGridLevel.value = level;
  }
}

function defaultGridLevelForModule(moduleName = activeModule.value) {
  const gridType = gridTypeForModule(moduleName);
  return defaultGridLevelFromAssets(selectedAssetsForModule(moduleName), gridType, defaultGridLevelForGridType(gridType));
}

function applyDefaultGridLevel(moduleName = activeModule.value) {
  if (!['optical', 'radar', 'product'].includes(moduleName) || isGridLevelManual(moduleName)) return;
  setGridLevelForModule(moduleName, defaultGridLevelForModule(moduleName));
}

function applyDefaultGridLevels() {
  applyDefaultGridLevel('optical');
  applyDefaultGridLevel('radar');
  applyDefaultGridLevel('product');
}

const activeGridLevelManual = computed(() => isGridLevelManual(activeModule.value));

async function confirmGridLevelManualEdit() {
  if (!['optical', 'radar', 'product'].includes(activeModule.value)) return;
  try {
    await ElMessageBox.confirm(
      '当前剖分层级由数据分辨率自动推荐。确认后可手动修改。',
      '修改剖分层级',
      { confirmButtonText: '确认修改', cancelButtonText: '取消', type: 'warning' },
    );
  } catch {
    return;
  }
  setGridLevelManual(activeModule.value, true);
}

function restoreDefaultGridLevel() {
  if (!['optical', 'radar', 'product'].includes(activeModule.value)) return;
  setGridLevelManual(activeModule.value, false);
  applyDefaultGridLevel(activeModule.value);
  mapGridGeometries.value = [];
}

const mapPreviewGeometries = computed(() => (
  activeModule.value === 'product' ? productMapGeometries.value : activeModule.value === 'radar' ? radarMapGeometries.value : mapBatchGeometries.value
));
const mapGeometries = computed(() => [...mapPreviewGeometries.value, ...mapGridGeometries.value]);

const partitionMetricRows = computed(() => {
  if (!lastPartitionResult.value) return [];
  const result = lastPartitionResult.value;
  const rows = [
    { label: '状态', value: result.status || '-' },
    { label: '模式', value: result.mode || '-' },
    { label: '数据类型', value: result.data_type || '-' },
    { label: '资产数', value: result.asset_count ?? '-' },
    { label: '格网任务数', value: result.grid_task_count ?? '-' },
    { label: '索引行数', value: result.rows ?? result.total_index_rows ?? '-' },
    { label: 'COG耗时(s)', value: result.cog_elapsed_sec ?? '-' },
    { label: '剖分耗时(s)', value: result.partition_elapsed_sec ?? '-' },
    { label: '总耗时(s)', value: result.total_elapsed_sec ?? '-' },
    { label: '输出路径', value: result.output_path || result.rows_path || '-' },
    { label: '质检状态', value: result.quality_status || result.quality_report?.status || '-' },
    { label: '正式入库', value: result.ingest_enabled === false ? '否' : '待确认' },
  ];
  if (result.error) {
    rows.splice(1, 0, { label: '失败原因', value: result.error });
  }
  return rows;
});

const selectedOpticalBandsText = computed(() => {
  const bands = new Set();
  selectedOpticalAssets.value.forEach((asset) => assetBands(asset).forEach((band) => bands.add(band)));
  return bands.size ? Array.from(bands).sort().join(', ') : '-';
});

const selectedOpticalTimeRange = computed(() => {
  const values = selectedOpticalAssets.value
    .map((asset) => String(asset.acq_time || '').slice(0, 10))
    .filter(Boolean)
    .sort();
  if (!values.length) return '-';
  if (values[0] === values[values.length - 1]) return values[0];
  return `${values[0]} 至 ${values[values.length - 1]}`;
});

const selectedCarbonProductTypesText = computed(() => {
  const productTypes = new Set();
  selectedCarbonObservations.value.forEach((observation) => productTypes.add(observation.product_type));
  return productTypes.size ? Array.from(productTypes).sort().join(', ') : '-';
});

const selectedCarbonTimeRange = computed(() => {
  const values = selectedCarbonObservations.value
    .map((observation) => String(observation.acq_time || '').slice(0, 10))
    .filter(Boolean)
    .sort();
  if (!values.length) return '-';
  if (values[0] === values[values.length - 1]) return values[0];
  return `${values[0]} 至 ${values[values.length - 1]}`;
});

const selectedProductYearsText = computed(() => {
  const years = selectedProductAssets.value
    .map((asset) => Number(asset.product_year))
    .filter((year) => Number.isFinite(year))
    .sort((a, b) => a - b);
  return years.length ? years.join(', ') : '-';
});

const partitionContextRows = computed(() => {
  if (activeModule.value === 'quality') return [];
  const request = lastPartitionRequest.value || {};
  const payload = request.payload || {};
  const result = lastPartitionResult.value || {};
  const operation = request.operation || (testModules.has(activeModule.value) ? 'test' : 'run');
  const endpoint = request.endpoint || partitionEndpointsByModule[activeModule.value] || activeModule.value;
  const apiPath = request.apiPath || `/v1/partition/${endpoint}/${operation}`;
  const status = resultLoading.value ? '执行中' : result.status === 'failed' ? '失败' : lastPartitionResult.value ? '已完成' : '待执行';
  const rows = [
    { label: '运行状态', value: status },
    { label: '执行接口', value: apiPath },
    { label: '开始时间', value: partitionStartedAt.value ? formatQualityTime(partitionStartedAt.value) : '-' },
    { label: '已耗时', value: `${partitionElapsedSec.value.toFixed(1)} s` },
    { label: '数据批次', value: selectedDataName.value },
    { label: '输出目录', value: result.run_dir || '-' },
  ];
  if (testModules.has(activeModule.value)) {
    const gridType = activeModule.value === 'carbon' ? 'isea4h' : payload.grid_type || opticalGridType.value;
    const gridLevel = activeModule.value === 'carbon' ? 5 : payload.grid_level || opticalGridLevel.value;
    const gridText = `${formatGridType(gridType)} / ${gridLevel} 级`;
    rows.splice(
      5,
      0,
      { label: '剖分格网', value: gridText },
      ...(activeModule.value === 'optical'
        ? [
            { label: '选择资产', value: `${selectedOpticalAssets.value.length} 条` },
            { label: '波段', value: selectedOpticalBandsText.value },
            { label: '时间范围', value: selectedOpticalTimeRange.value },
          ]
        : activeModule.value === 'carbon'
          ? [
              { label: '选择观测', value: `${selectedCarbonObservations.value.length} 条` },
              { label: '产品类型', value: selectedCarbonProductTypesText.value },
              { label: '时间范围', value: selectedCarbonTimeRange.value },
            ]
          : activeModule.value === 'product'
            ? [
                { label: '选择产品', value: `${selectedProductAssets.value.length} 个年份` },
                { label: '产品年份', value: selectedProductYearsText.value },
                { label: '目标参考系统', value: payload.target_crs || 'EPSG:4326' },
              ]
        : []),
      { label: '安全模式', value: '剖分测试不写正式库' },
    );
  }
  return rows;
});
const selectedPartitionContext = computed(() => (
  partitionContextRows.value.find((item) => item.label === selectedPartitionContextLabel.value) || null
));

const partitionResultDetailRows = computed(() => {
  const result = lastPartitionResult.value;
  if (!result) return [];
  return [
    { label: '执行引擎', value: result.execution_engine || result.partition_backend || '-' },
    { label: '后台任务 ID', value: result.partition_task_id || '-' },
    { label: '执行 ID', value: result.execution_id || result.run_task_id || result.demo_task_id || '-' },
    { label: 'Ray 任务 ID', value: result.ray_task_id || '-' },
    { label: '质检报告 ID', value: result.quality_report_id || result.quality_report?.report_id || '-' },
    { label: '索引文件', value: result.rows_path || result.output_path || '-' },
    { label: 'COG 输出', value: result.cog_output_dir || result.cog_input_dir || '-' },
    { label: '瓦片存储', value: result.asset_storage_backend || '-' },
    { label: '元数据后端', value: result.metadata_backend || '-' },
    { label: '上传瓦片', value: result.uploaded_tile_count ?? '-' },
    { label: '元数据行数', value: result.metadata_rows ?? '-' },
  ];
});

const partitionWarnNeedsRetry = computed(() => {
  const status = lastPartitionResult.value?.quality_status || lastPartitionResult.value?.quality_report?.status;
  return status === 'WARN';
});

const partitionFailureMessage = computed(() => (
  lastPartitionResult.value?.status === 'failed' ? lastPartitionResult.value.error || '剖分失败' : ''
));

const opticalIngestReady = computed(() => activeModule.value === 'optical' && Boolean(
  lastPartitionResult.value?.quality_report_id || lastPartitionResult.value?.quality_report?.report_id || lastPartitionResult.value?.run_dir,
));

const ingestPreviewRows = computed(() => {
  if (!ingestPreview.value) return [];
  const preview = ingestPreview.value;
  return [
    { label: '入库模式', value: preview.mode === 'pre_ingest_preview' ? '预入库校验' : preview.mode },
    { label: '质检状态', value: preview.quality_status || '-' },
    { label: '资产版本', value: preview.asset_version || '-' },
    { label: '立方体版本', value: preview.cube_version || '-' },
    { label: '索引行数', value: preview.input_rows ?? '-' },
    { label: '资产记录', value: `${preview.raw_asset_rows ?? 0} 条，已有 ${preview.existing_raw_asset_rows ?? 0} 条` },
    { label: '格网事实', value: `${preview.cube_fact_rows ?? 0} 条，已有 ${preview.existing_cube_fact_rows ?? 0} 条` },
  ];
});

const ingestResultRows = computed(() => {
  if (!ingestResult.value) return [];
  const result = ingestResult.value;
  return [
    { label: '入库状态', value: result.status || '-' },
    { label: '任务 ID', value: result.job_id || '-' },
    { label: '资产版本', value: result.asset_version || '-' },
    { label: '立方体版本', value: result.cube_version || '-' },
    { label: '资产记录', value: result.raw_asset_rows ?? '-' },
    { label: '格网事实', value: result.cube_fact_rows ?? '-' },
    { label: '元数据后端', value: result.metadata_backend || '-' },
  ];
});

function openDataDrawer() {
  dataSearch.value = '';
  dataDrawerVisible.value = true;
}

function openQualityHistoryDrawer() {
  qualityHistorySearch.value = '';
  qualityHistoryStatus.value = '';
  qualityHistoryDrawerVisible.value = true;
}

function qualityHistoryRowClass({ row }) {
  return row.report_id === selectedQualityReportId.value ? 'selected-quality-history-row' : '';
}

function assetKey(asset) {
  return `${asset.source_uri}|${asset.scene_id}|${assetBandsText(asset)}|${asset.acq_time}`;
}

function observationKey(observation) {
  return `${observation.source_uri}|${observation.source_index}|${observation.observation_id}`;
}

function productAssetKey(asset) {
  return `${asset.source_uri}|${asset.product_year}|${asset.band}`;
}

function radarAssetKey(asset) {
  return `${asset.source_uri}|${asset.scene_id}|${asset.polarization || asset.band}`;
}

function isOpticalAssetSelected(batchId, asset) {
  const excluded = deselectedOpticalAssetKeys.value[batchId] || [];
  return !excluded.includes(assetKey(asset));
}

function isCarbonObservationSelected(batchId, observation) {
  const excluded = deselectedCarbonObservationKeys.value[batchId] || [];
  return !excluded.includes(observationKey(observation));
}

function isProductAssetSelected(batchId, asset) {
  const excluded = deselectedProductAssetKeys.value[batchId] || [];
  return !excluded.includes(productAssetKey(asset));
}

function isRadarAssetSelected(batchId, asset) {
  const excluded = deselectedRadarAssetKeys.value[batchId] || [];
  return !excluded.includes(radarAssetKey(asset));
}

function toggleOpticalBatchSelect(batchId) {
  const exists = selectedOpticalBatchIds.value.includes(batchId);
  if (exists) {
    selectedOpticalBatchIds.value = selectedOpticalBatchIds.value.filter((id) => id !== batchId);
  } else {
    selectedOpticalBatchIds.value = [...selectedOpticalBatchIds.value, batchId];
  }
}

function toggleOpticalBatchExpand(batchId) {
  expandedOpticalBatchId.value = expandedOpticalBatchId.value === batchId ? '' : batchId;
}

function toggleCarbonBatchSelect(batchId) {
  const exists = selectedCarbonBatchIds.value.includes(batchId);
  if (exists) {
    selectedCarbonBatchIds.value = selectedCarbonBatchIds.value.filter((id) => id !== batchId);
  } else {
    selectedCarbonBatchIds.value = [...selectedCarbonBatchIds.value, batchId];
  }
}

function toggleCarbonBatchExpand(batchId) {
  expandedCarbonBatchId.value = expandedCarbonBatchId.value === batchId ? '' : batchId;
}

function toggleRadarBatchSelect(batchId) {
  const exists = selectedRadarBatchIds.value.includes(batchId);
  if (exists) {
    selectedRadarBatchIds.value = selectedRadarBatchIds.value.filter((id) => id !== batchId);
  } else {
    selectedRadarBatchIds.value = [...selectedRadarBatchIds.value, batchId];
  }
}

function toggleRadarBatchExpand(batchId) {
  expandedRadarBatchId.value = expandedRadarBatchId.value === batchId ? '' : batchId;
}

function toggleProductBatchSelect(batchId) {
  const exists = selectedProductBatchIds.value.includes(batchId);
  if (exists) {
    selectedProductBatchIds.value = selectedProductBatchIds.value.filter((id) => id !== batchId);
  } else {
    selectedProductBatchIds.value = [...selectedProductBatchIds.value, batchId];
  }
}

function toggleProductBatchExpand(batchId) {
  expandedProductBatchId.value = expandedProductBatchId.value === batchId ? '' : batchId;
}

function toggleOpticalAssetSelect(batchId, asset) {
  const key = assetKey(asset);
  const current = deselectedOpticalAssetKeys.value[batchId] || [];
  const exists = current.includes(key);
  const next = exists ? current.filter((item) => item !== key) : [...current, key];
  deselectedOpticalAssetKeys.value = { ...deselectedOpticalAssetKeys.value, [batchId]: next };
}

function toggleCarbonObservationSelect(batchId, observation) {
  const key = observationKey(observation);
  const current = deselectedCarbonObservationKeys.value[batchId] || [];
  const exists = current.includes(key);
  const next = exists ? current.filter((item) => item !== key) : [...current, key];
  deselectedCarbonObservationKeys.value = { ...deselectedCarbonObservationKeys.value, [batchId]: next };
}

function toggleRadarAssetSelect(batchId, asset) {
  const key = radarAssetKey(asset);
  const current = deselectedRadarAssetKeys.value[batchId] || [];
  const exists = current.includes(key);
  const next = exists ? current.filter((item) => item !== key) : [...current, key];
  deselectedRadarAssetKeys.value = { ...deselectedRadarAssetKeys.value, [batchId]: next };
}

function toggleProductAssetSelect(batchId, asset) {
  const key = productAssetKey(asset);
  const current = deselectedProductAssetKeys.value[batchId] || [];
  const exists = current.includes(key);
  const next = exists ? current.filter((item) => item !== key) : [...current, key];
  deselectedProductAssetKeys.value = { ...deselectedProductAssetKeys.value, [batchId]: next };
}

function opticalBatchSummary(batch) {
  const selectedCount = batch.assets.filter((asset) => isOpticalAssetSelected(batch.id, asset)).length;
  return `${selectedCount}/${batch.assets.length} 条资产已选`;
}

function carbonBatchSummary(batch) {
  const selectedCount = batch.observations.filter((observation) => isCarbonObservationSelected(batch.id, observation)).length;
  return `${selectedCount}/${batch.observations.length} 条观测已选 | schema ${batch.schema.length} 字段`;
}

function radarBatchSummary(batch) {
  const selectedCount = batch.assets.filter((asset) => isRadarAssetSelected(batch.id, asset)).length;
  return `${selectedCount}/${batch.assets.length} 条资产已选 | schema ${batch.schema.length} 字段`;
}

function productBatchSummary(batch) {
  const selectedCount = batch.assets.filter((asset) => isProductAssetSelected(batch.id, asset)).length;
  return `${selectedCount}/${batch.assets.length} 个年份已选 | schema ${batch.schema.length} 字段`;
}

function normalizeManagedBatch(batch) {
  const payload = batch.normalized_payload || {};
  const base = {
    ...batch,
    id: batch.batch_id,
    name: batch.batch_name || batch.batch_id,
    status: batch.status || '待处理',
  };
  if (batch.data_type === 'carbon') {
    return {
      ...base,
      product_type: payload.product_type || 'xco2',
      source_uri: payload.source_uri || '',
      schema: batch.source_schema?.schema || [],
      observations: payload.selected_observations || [],
    };
  }
  if (batch.data_type === 'product') {
    return {
      ...base,
      product_family: payload.product_family || 'product',
      sensor: payload.sensor || 'data_product',
      target_crs: payload.target_crs || 'EPSG:4326',
      schema: batch.source_schema?.schema || [],
      assets: payload.selected_assets || [],
    };
  }
  if (batch.data_type === 'radar') {
    return {
      ...base,
      product_family: payload.product_family || 'sentinel1',
      sensor: payload.sensor || 'sentinel1_sar',
      target_crs: payload.target_crs || 'EPSG:4326',
      schema: batch.source_schema?.schema || [],
      assets: payload.selected_assets || [],
    };
  }
  return {
    ...base,
    assets: payload.selected_assets || [],
  };
}

async function loadPartitionBatches() {
  partitionBatchLoading.value = true;
  try {
    const { partitionPrefix } = apiPrefixes();
    const response = await requestGet(`${partitionPrefix}/batches?include_succeeded=false&limit=200`);
    const batches = response.batches || [];
    managedOpticalBatches.value = batches.filter((batch) => batch.data_type === 'optical').map(normalizeManagedBatch);
    managedCarbonBatches.value = batches.filter((batch) => batch.data_type === 'carbon').map(normalizeManagedBatch);
    managedRadarBatches.value = batches.filter((batch) => batch.data_type === 'radar').map(normalizeManagedBatch);
    managedProductBatches.value = batches.filter((batch) => batch.data_type === 'product').map(normalizeManagedBatch);
    if (managedOpticalBatches.value.length && !managedOpticalBatches.value.some((batch) => selectedOpticalBatchIds.value.includes(batch.id))) {
      const batchId = preferredBatchId(managedOpticalBatches.value);
      if (batchId) {
        selectedOpticalBatchIds.value = [batchId];
        expandedOpticalBatchId.value = batchId;
      }
    }
    if (managedCarbonBatches.value.length && !managedCarbonBatches.value.some((batch) => selectedCarbonBatchIds.value.includes(batch.id))) {
      const batchId = preferredBatchId(managedCarbonBatches.value);
      if (batchId) {
        selectedCarbonBatchIds.value = [batchId];
        expandedCarbonBatchId.value = batchId;
      }
    }
    if (managedRadarBatches.value.length && !managedRadarBatches.value.some((batch) => selectedRadarBatchIds.value.includes(batch.id))) {
      const batchId = preferredBatchId(managedRadarBatches.value);
      if (batchId) {
        selectedRadarBatchIds.value = [batchId];
        expandedRadarBatchId.value = batchId;
      }
    }
    if (managedProductBatches.value.length && !managedProductBatches.value.some((batch) => selectedProductBatchIds.value.includes(batch.id))) {
      const batchId = preferredBatchId(managedProductBatches.value);
      if (batchId) {
        selectedProductBatchIds.value = [batchId];
        expandedProductBatchId.value = batchId;
      }
    }
    applyDefaultGridLevels();
  } catch (error) {
    ElMessage.error(`剖分批次加载失败：${error.message}`);
    applyDefaultGridLevels();
  } finally {
    partitionBatchLoading.value = false;
  }
}

function partitionPayloadForActiveModule() {
  if (activeModule.value === 'optical') {
    const selectedBatch = visibleOpticalBatches.value.find((batch) => selectedOpticalBatchIds.value.includes(batch.id));
    const selectedAssets = selectedOpticalAssets.value;
    const useEntityPartition = opticalGridType.value === 'isea4h';
    return {
      payload: {
        grid_type: opticalGridType.value,
        grid_level: Number(selectedMapGridLevel.value),
        grid_level_mode: isGridLevelManual('optical') || useEntityPartition ? 'manual' : 'auto',
        batch_id: selectedBatch?.id || '',
        batch_name: selectedBatch?.name || '',
        selected_assets: selectedAssets,
      },
      selectedCount: selectedAssets.length,
    };
  }
  if (activeModule.value === 'carbon') {
    const selectedBatch = visibleCarbonBatches.value.find((batch) => selectedCarbonBatchIds.value.includes(batch.id));
    const selectedObservations = selectedCarbonObservations.value;
    return {
      payload: {
        grid_type: 'isea4h',
        grid_level: 5,
        batch_id: selectedBatch?.id || '',
        batch_name: selectedBatch?.name || '',
        product_type: selectedBatch?.product_type || 'xco2',
        selected_observations: selectedObservations,
      },
      selectedCount: selectedObservations.length,
    };
  }
  if (activeModule.value === 'radar') {
    const selectedBatch = visibleRadarBatches.value.find((batch) => selectedRadarBatchIds.value.includes(batch.id));
    const selectedAssets = selectedRadarAssets.value;
    const useEntityPartition = radarGridType.value === 'isea4h';
    return {
      payload: {
        grid_type: radarGridType.value,
        grid_level: Number(radarGridLevel.value),
        grid_level_mode: isGridLevelManual('radar') || useEntityPartition ? 'manual' : 'auto',
        target_crs: selectedBatch?.target_crs || 'EPSG:4326',
        batch_id: selectedBatch?.id || '',
        batch_name: selectedBatch?.name || '',
        selected_assets: selectedAssets,
      },
      selectedCount: selectedAssets.length,
    };
  }
  if (activeModule.value === 'product') {
    const selectedBatch = visibleProductBatches.value.find((batch) => selectedProductBatchIds.value.includes(batch.id));
    const selectedAssets = selectedProductAssets.value;
    const useEntityPartition = productGridType.value === 'isea4h';
    return {
      payload: {
        grid_type: productGridType.value,
        grid_level: Number(productGridLevel.value),
        grid_level_mode: isGridLevelManual('product') || useEntityPartition ? 'manual' : 'auto',
        target_crs: selectedBatch?.target_crs || 'EPSG:4326',
        batch_id: selectedBatch?.id || '',
        batch_name: selectedBatch?.name || '',
        selected_assets: selectedAssets,
      },
      selectedCount: selectedAssets.length,
    };
  }
  return { payload: {}, selectedCount: 0 };
}

async function loadMapGridForSelectedAssets() {
  if (!['optical', 'radar', 'product'].includes(activeModule.value)) return;
  const selectedAssets = selectedMapAssets.value;
  if (!selectedAssets.length) {
    ElMessage.warning(activeModule.value === 'product' ? '请至少选择一个产品年份' : activeModule.value === 'radar' ? '请至少选择一条雷达资产' : '请至少选择一条资产');
    return;
  }
  mapGridLoading.value = true;
  mapGridGeometries.value = [];
  try {
    const { gridPrefix } = apiPrefixes();
    const requests = selectedAssets.slice(0, 30).map(async (asset) => {
      const result = await requestJson(`${gridPrefix}/cover`, {
        grid_type: selectedMapGridType.value,
        level: Number(selectedMapGridLevel.value),
        cover_mode: 'intersect',
        boundary_type: 'polygon',
        bbox: cornersToBbox(asset.corners),
        crs: 'EPSG:4326',
      });
      return (result.cells || [])
        .map((cell) => (cell.geometry ? {
          geometry: cell.geometry,
          label: cell.space_code,
          color: '#e67e22',
          fillColor: '#e67e22',
          fillOpacity: 0.06,
          weight: 1,
        } : null))
        .filter(Boolean);
    });
    const chunks = await Promise.all(requests);
    mapGridGeometries.value = chunks.flat();
    ElMessage.success(`已加载格网 ${mapGridGeometries.value.length} 个`);
  } catch (error) {
    ElMessage.error(error.message);
  } finally {
    mapGridLoading.value = false;
  }
}

function clearMapGrid() {
  mapGridGeometries.value = [];
}

function formatRows(result) {
  return Object.entries(result).map(([key, value]) => ({
    key,
    value: typeof value === 'object' ? JSON.stringify(value) : String(value),
  }));
}

function errorText(error) {
  return error?.message || String(error || '未知错误');
}

function buildPartitionFailureResult(error, request = {}) {
  const payload = request.payload || {};
  const endpoint = request.endpoint || partitionEndpointsByModule[activeModule.value] || activeModule.value;
  const operation = request.operation || (testModules.has(activeModule.value) ? 'test' : 'run');
  const apiPath = request.apiPath || `/v1/partition/${endpoint}/${operation}`;
  return {
    status: 'failed',
    mode: operation === 'test' ? 'partition_test_no_ingest' : operation === 'retry' ? 'partition_retry' : 'partition_run',
    data_type: activeModule.value,
    endpoint: apiPath,
    grid_type: payload.grid_type || selectedMapGridType.value || '-',
    grid_level: payload.grid_level || selectedMapGridLevel.value || '-',
    batch_name: selectedDataName.value,
    selected_count:
      activeModule.value === 'optical'
        ? selectedOpticalAssets.value.length
        : activeModule.value === 'carbon'
          ? selectedCarbonObservations.value.length
          : activeModule.value === 'product'
            ? selectedProductAssets.value.length
            : 0,
    error: errorText(error),
    started_at: partitionStartedAt.value || '',
    elapsed_sec: Number(partitionElapsedSec.value.toFixed(1)),
    ingest_enabled: false,
  };
}

function sleep(ms) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

async function waitForPartitionTask(partitionPrefix, taskId) {
  for (let attempt = 0; attempt < partitionTaskMaxPolls; attempt += 1) {
    const task = await requestGet(`${partitionPrefix}/tasks/${taskId}`);
    if (task.status === 'completed') {
      if (!task.result) {
        throw new Error(`剖分任务 ${taskId} 已完成但未返回结果`);
      }
      return {
        ...task.result,
        partition_task_id: task.task_id || taskId,
      };
    }
    if (task.status === 'failed') {
      throw new Error(task.error || `剖分任务 ${taskId} 执行失败`);
    }
    if (!['queued', 'running'].includes(task.status)) {
      throw new Error(`剖分任务 ${taskId} 状态异常: ${task.status || '-'}`);
    }
    await sleep(partitionTaskPollIntervalMs);
  }
  throw new Error(`剖分任务轮询超时: ${taskId}`);
}

async function requestPartitionOperation(partitionPrefix, endpoint, operation, payload) {
  const apiPath = `${partitionPrefix}/${endpoint}/tasks/${operation}`;
  const submitted = await requestJson(apiPath, payload);
  const taskId = submitted.task_id;
  if (!taskId) {
    throw new Error('剖分任务提交后未返回 task_id');
  }
  setPartitionStage('partition', 'running', `后台任务 ${taskId} 执行中。`);
  return waitForPartitionTask(partitionPrefix, taskId);
}

async function requestRetryOperation(partitionPrefix, retryRequest, retryPayload) {
  if (retryRequest.kind === 'batch') {
    const submitted = await requestJson(`${partitionPrefix}/batches/${retryRequest.batchId}/retry`, retryRequest.payload || {});
    const taskId = submitted.task_id;
    if (!taskId) {
      throw new Error('批次重试任务未返回 task_id');
    }
    setPartitionStage('partition', 'running', `后台任务 ${taskId} 执行中。`);
    return waitForPartitionTask(partitionPrefix, taskId);
  }
  if (retryRequest.kind === 'assets') {
    const submitted = await requestJson(`${partitionPrefix}/assets/retry`, retryRequest.payload || {});
    const taskId = submitted.task_id;
    if (!taskId) {
      throw new Error('失败资产重试任务未返回 task_id');
    }
    setPartitionStage('partition', 'running', `后台任务 ${taskId} 执行中。`);
    return waitForPartitionTask(partitionPrefix, taskId);
  }
  return requestPartitionOperation(partitionPrefix, retryRequest.endpoint, 'retry', retryPayload);
}

function resetPartitionStages() {
  partitionStages.value = partitionStages.value.map((item) => ({ ...item, status: 'pending' }));
}

function setPartitionStage(stageKey, status, detail = '') {
  partitionStages.value = partitionStages.value.map((item) => (
    item.key === stageKey ? { ...item, status, detail: detail || item.detail } : item
  ));
}

function stageTagType(status) {
  if (status === 'done') return 'success';
  if (status === 'running') return 'warning';
  if (status === 'failed') return 'danger';
  return 'info';
}

function stageText(status) {
  if (status === 'done') return '完成';
  if (status === 'running') return '进行中';
  if (status === 'failed') return '失败';
  return '待执行';
}

function openPartitionStageDetail(stage) {
  selectedPartitionStageKey.value = stage.key;
  partitionStageDetailVisible.value = true;
}

function openPartitionContextDetail(item) {
  selectedPartitionContextLabel.value = item.label;
  partitionContextDetailVisible.value = true;
}

function startPartitionTimer() {
  stopPartitionTimer();
  partitionStartedAt.value = new Date().toISOString();
  partitionFinishedAt.value = null;
  partitionElapsedSec.value = 0;
  const started = Date.now();
  partitionTimer = window.setInterval(() => {
    partitionElapsedSec.value = (Date.now() - started) / 1000;
  }, 200);
}

function stopPartitionTimer() {
  if (partitionTimer) {
    window.clearInterval(partitionTimer);
    partitionTimer = null;
  }
  if (partitionStartedAt.value && !partitionFinishedAt.value) {
    partitionFinishedAt.value = new Date().toISOString();
    partitionElapsedSec.value = (new Date(partitionFinishedAt.value).getTime() - new Date(partitionStartedAt.value).getTime()) / 1000;
  }
}

const qualityStatusType = computed(() => {
  const status = qualityReport.value?.status;
  if (status === 'PASS') return 'success';
  if (status === 'WARN') return 'warning';
  if (status === 'FAIL') return 'danger';
  return 'info';
});

const qualitySummaryRows = computed(() => {
  const summary = qualityReport.value?.summary || {};
  if (qualityDataType.value === 'carbon') {
    return [
      { label: '观测行数', value: summary.observation_rows ?? summary.index_rows ?? '-' },
      { label: '观测 ID 数', value: summary.distinct_observations ?? '-' },
      { label: '空间格网数', value: summary.distinct_space_codes ?? '-' },
      { label: '时空编码数', value: summary.distinct_st_codes ?? '-' },
      { label: '平均 XCO2', value: summary.avg_xco2 != null ? Number(summary.avg_xco2).toFixed(3) : '-' },
      { label: '告警项', value: summary.warning_checks ?? '-' },
      { label: '失败项', value: summary.failed_checks ?? '-' },
    ];
  }
  return [
    { label: '索引行数', value: summary.index_rows ?? '-' },
    { label: '资产数', value: summary.asset_count ?? '-' },
    { label: '空间格网数', value: summary.distinct_space_codes ?? '-' },
    { label: '时空编码数', value: summary.distinct_st_codes ?? '-' },
    { label: '通过项', value: summary.passed_checks ?? '-' },
    { label: '告警项', value: summary.warning_checks ?? '-' },
    { label: '失败项', value: summary.failed_checks ?? '-' },
  ];
});

function checkStatusType(status) {
  if (status === 'PASS') return 'success';
  if (status === 'WARN') return 'warning';
  if (status === 'FAIL') return 'danger';
  return 'info';
}

function statusText(status) {
  if (status === 'PASS') return '通过';
  if (status === 'WARN') return '告警';
  if (status === 'FAIL') return '失败';
  return '未知';
}

function checkNameText(name) {
  const names = {
    index_rows: '索引文件读取',
    index_schema: '索引字段完整性',
    time_bucket: '时间桶一致性',
    cell_bbox: '格网范围合法性',
    logical_duplicates: '逻辑资产重复',
    product_years: '产品年份完整性',
    carbon_rows: '观测行文件读取',
    carbon_schema: '观测字段完整性',
    carbon_coordinates: '观测坐标合法性',
    xco2_range: 'XCO2 数值范围',
    carbon_quality_flag: '质量标记分布',
    carbon_duplicates: '观测 ID 重复',
    carbon_footprint: '足迹几何合法性',
    asset_readability: '资产可读性',
    cog_crs: '参考系统一致性',
    window_bounds: '窗口边界合法性',
    pixel_sample: '像元抽样有效性',
  };
  return names[name] || name;
}

function checkMessageText(check) {
  const messages = {
    index_rows: '已读取剖分索引文件。',
    index_schema: '索引行字段满足入库要求。',
    time_bucket: '时间分桶与采集时间一致。',
    cell_bbox: '格网经纬度范围合法。',
    logical_duplicates: check.status === 'WARN' ? '存在同一场景同一波段对应多个资产的情况，入库前需要关注合并关系。' : '未发现逻辑资产重复。',
    product_years: check.status === 'WARN' ? '产品年份与显式期望年份不一致。' : '产品年份元数据与本次输出一致。',
    carbon_rows: '已读取碳卫星观测行文件。',
    carbon_schema: '观测行字段满足碳卫星入库要求。',
    carbon_coordinates: '观测中心点经纬度范围合法。',
    xco2_range: 'XCO2 浓度值在预期范围内。',
    carbon_quality_flag: '质量标记值符合碳卫星标准标记范围。',
    carbon_duplicates: check.status === 'WARN' ? '存在重复观测 ID，入库前需要确认是否为重复观测。' : '未发现重复观测 ID。',
    carbon_footprint: check.status === 'WARN' ? '部分观测足迹不是标准 Polygon/MultiPolygon。' : '观测足迹几何合法。',
    asset_readability: '索引引用的资产均可读取。',
    cog_crs: `资产参考系统已统一为 ${qualityReport.value?.target_crs || qualityTargetCrs.value}。`,
    window_bounds: '索引窗口未超出资产尺寸。',
    pixel_sample: check.status === 'WARN' ? '部分资产抽样像元为 0，建议结合原始影像确认是否为空值区域。' : '抽样像元有效。',
  };
  return messages[check.name] || check.message;
}

function checkDetailRows(check) {
  const metrics = check.metrics || {};
  if (check.name === 'logical_duplicates') {
    return (metrics.duplicates || []).map((item) => ({
      title: `${item.scene_id} / ${item.band}`,
      lines: (item.asset_paths || []).map((path) => path.split('/').pop()),
    }));
  }
  if (check.name === 'cog_crs') {
    return (metrics.mismatches || []).map((item) => ({
      title: item.path?.split('/').pop() || '未知资产',
      lines: [`当前参考系统：${item.crs || '未识别'}`],
    }));
  }
  if (check.name === 'window_bounds') {
    return (metrics.invalid_windows || []).map((item) => ({
      title: item.asset_path?.split('/').pop() || `索引行 ${item.line_no}`,
      lines: [`窗口：${(item.window || []).join(', ')}`, `资产尺寸：${(item.asset_size || []).join(' x ')}`],
    }));
  }
  if (check.name === 'pixel_sample') {
    return (metrics.zero_assets || []).map((item) => ({
      title: item.path?.split('/').pop() || '未知资产',
      lines: [`抽样像元：${item.sample_pixels}`, `有效像元：${item.valid_pixels}`, `非零像元：${item.nonzero_pixels}`],
    }));
  }
  if (check.name === 'product_years') {
    return [
      {
        title: '年份覆盖',
        lines: [
          `期望年份：${(metrics.expected_years || []).join(', ') || '-'}`,
          `已有年份：${(metrics.present_years || []).join(', ') || '-'}`,
          `缺少年份：${(metrics.missing_years || []).join(', ') || '无'}`,
          `非预期年份：${(metrics.unexpected_years || []).join(', ') || '无'}`,
        ],
      },
    ];
  }
  if (check.name === 'carbon_schema') {
    return (metrics.missing_rows || []).map((item) => ({
      title: `观测行 ${item.line_no}`,
      lines: [`缺失字段：${(item.missing || []).join(', ')}`],
    }));
  }
  if (check.name === 'carbon_coordinates') {
    return (metrics.invalid_rows || []).map((item) => ({
      title: `观测行 ${item.line_no}`,
      lines: [`原因：${item.reason}`, `坐标：${item.center_lon ?? '-'}, ${item.center_lat ?? '-'}`],
    }));
  }
  if (check.name === 'xco2_range') {
    const invalidRows = (metrics.invalid_rows || []).map((item) => ({
      title: `观测行 ${item.line_no}`,
      lines: [`原因：${item.reason}`, `XCO2：${item.xco2 ?? '-'}`],
    }));
    if (invalidRows.length) return invalidRows;
    return [
      {
        title: 'XCO2 统计',
        lines: [`最小值：${metrics.min_xco2 ?? '-'}`, `最大值：${metrics.max_xco2 ?? '-'}`, `平均值：${metrics.avg_xco2 ?? '-'}`],
      },
    ];
  }
  if (check.name === 'carbon_quality_flag') {
    return [
      {
        title: '质量标记分布',
        lines: Object.entries(metrics.quality_counts || {}).map(([flag, count]) => `Q${flag || '-'}：${count}`),
      },
    ];
  }
  if (check.name === 'carbon_duplicates') {
    return (metrics.duplicates || []).map((item) => ({
      title: item.observation_id,
      lines: [`重复次数：${item.count}`],
    }));
  }
  if (check.name === 'carbon_footprint') {
    return (metrics.invalid_rows || []).map((item) => ({
      title: `观测行 ${item.line_no}`,
      lines: [`几何类型：${item.type || '-'}`],
    }));
  }
  return [];
}

function qualitySourceText() {
  if (qualityDataType.value === 'product') return '数据产品自动质检';
  if (qualityDataType.value === 'carbon') return '碳卫星自动质检';
  if (qualityDataType.value === 'radar') return '雷达遥感自动质检';
  return '光学遥感自动质检';
}

function qualityBreakdownTitle() {
  if (qualityDataType.value === 'product') return '年份行数';
  if (qualityDataType.value === 'carbon') return '质量标记分布';
  return '波段行数';
}

function qualityBreakdownRows() {
  const summary = qualityReport.value?.summary || {};
  if (qualityDataType.value === 'product') return summary.rows_by_year || {};
  if (qualityDataType.value === 'carbon') return summary.quality_counts || {};
  return summary.rows_by_band || {};
}

function qualityDataTypeForEndpoint(endpoint) {
  if (endpoint === 'product') return 'product';
  if (endpoint === 'carbon') return 'carbon';
  if (endpoint === 'radar') return 'radar';
  return 'optical';
}

function formatQualityTime(value) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString('zh-CN', { hour12: false });
}

async function loadQualityHistory() {
  if (!qualityReportDataTypes.has(qualityDataType.value)) {
    qualityHistory.value = [];
    return;
  }
  qualityHistoryLoading.value = true;
  try {
    const { qualityPrefix } = apiPrefixes();
    const result = await requestJson(`${qualityPrefix}/${qualityDataType.value}/history`, {
      target_crs: qualityTargetCrs.value,
      limit: qualityHistoryLimit.value,
    });
    qualityHistory.value = result.records || [];
  } catch (error) {
    qualityError.value = error.message;
    ElMessage.error(error.message);
  } finally {
    qualityHistoryLoading.value = false;
  }
}

async function runQualityCheck(reportId = '') {
  if (!qualityReportDataTypes.has(qualityDataType.value)) {
    qualityReport.value = null;
    qualityError.value = '';
    return;
  }
  qualityLoading.value = true;
  qualityError.value = '';
  try {
    const { qualityPrefix } = apiPrefixes();
    const endpoint = reportId ? `${qualityPrefix}/${qualityDataType.value}/report` : `${qualityPrefix}/${qualityDataType.value}/latest`;
    const payload = reportId ? { report_id: reportId } : {};
    qualityReport.value = await requestJson(endpoint, payload);
    selectedQualityReportId.value = qualityReport.value.report_id || reportId;
    const message = qualityReport.value.status === 'FAIL' ? '质检结果存在失败项' : '质检结果已加载';
    ElMessage[qualityReport.value.status === 'FAIL' ? 'warning' : 'success'](message);
  } catch (error) {
    qualityError.value = error.message;
    ElMessage.error(error.message);
  } finally {
    qualityLoading.value = false;
  }
}

async function refreshQuality() {
  await runQualityCheck();
  await loadQualityHistory();
}

async function refreshQualityWorkspace() {
  await Promise.all([
    refreshQuality(),
    loadPartitionBatches(),
  ]);
}

async function selectQualityRecord(row) {
  if (row.data_type && row.data_type !== qualityDataType.value) {
    qualityDataType.value = row.data_type;
  }
  await runQualityCheck(row.report_id);
  qualityHistoryDrawerVisible.value = false;
}

async function exportQualityReport(format) {
  if (!qualityReport.value?.report_id) {
    ElMessage.warning('请先加载质检结果');
    return;
  }
  if (!['pdf', 'txt'].includes(format)) {
    ElMessage.error('不支持的导出格式');
    return;
  }
  qualityExportLoading.value = true;
  try {
    const { qualityPrefix } = apiPrefixes();
    const response = await fetch(`${qualityPrefix}/${qualityDataType.value}/report/${format}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ report_id: qualityReport.value.report_id }),
    });
    if (!response.ok) {
      const text = await response.text();
      let message = `${format.toUpperCase()}导出失败: ${response.status}`;
      try {
        const body = text ? JSON.parse(text) : {};
        message = body?.detail || body?.error?.message || message;
      } catch {
        if (text) message = text;
      }
      throw new Error(message);
    }
    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    const runName = qualityReport.value.run_name || qualityReport.value.run_dir?.split('/').filter(Boolean).pop() || 'run';
    link.href = url;
    link.download = `quality-report-${qualityDataType.value}-${runName}.${format}`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
    ElMessage.success(`${format.toUpperCase()}已导出`);
  } catch (error) {
    ElMessage.error(error.message);
  } finally {
    qualityExportLoading.value = false;
  }
}

async function changeQualityDataType() {
  qualityReport.value = null;
  qualityHistory.value = [];
  selectedQualityReportId.value = '';
  await refreshQualityWorkspace();
}

async function runDemo() {
  if (activeModule.value === 'quality') {
    await refreshQualityWorkspace();
    return;
  }
  resultLoading.value = true;
  resultRows.value = [];
  lastPartitionResult.value = null;
  ingestPreview.value = null;
  ingestResult.value = null;
  startPartitionTimer();
  resetPartitionStages();
  setPartitionStage('prepare', 'done', '已锁定当前参数与数据选择。');
  try {
    const { partitionPrefix } = apiPrefixes();
    const endpoint = partitionEndpointsByModule[activeModule.value];
    if (!endpoint) {
      throw new Error(`不支持的剖分模块: ${activeModule.value}`);
    }
    const { payload, selectedCount } = partitionPayloadForActiveModule();
    const operation = testModules.has(activeModule.value) ? 'test' : 'run';
    lastPartitionRequest.value = { endpoint, payload, operation, apiPath: `/v1/partition/${endpoint}/tasks/${operation}` };
    if (activeModule.value === 'optical' && selectedCount <= 0) {
      throw new Error('请至少选择一条影像资产');
    }
	    if (activeModule.value === 'carbon' && selectedCount <= 0) {
	      throw new Error('请至少选择一条碳卫星观测');
	    }
	    if (activeModule.value === 'radar' && selectedCount <= 0) {
	      throw new Error('请至少选择一条雷达资产');
	    }
	    if (activeModule.value === 'product' && selectedCount <= 0) {
	      throw new Error('请至少选择一个信息产品年份');
	    }
		    setPartitionStage(
	      'queue',
	      'running',
	      activeModule.value === 'optical'
	        ? `准备读取 ${selectedCount} 条影像资产。`
	        : activeModule.value === 'carbon'
	          ? `准备读取 ${selectedCount} 条碳卫星观测。`
	          : activeModule.value === 'radar'
	            ? `准备读取 ${selectedCount} 条雷达资产。`
	          : activeModule.value === 'product'
	            ? `准备读取 ${selectedCount} 个信息产品年份。`
	            : '准备读取当前队列中的数据。',
	    );
    await Promise.resolve();
    setPartitionStage('queue', 'done', '数据队列已提交到后端。');
    setPartitionStage('partition', 'running', `调用 /v1/partition/${endpoint}/tasks/${operation} 提交后台剖分任务。`);
    const result = await requestPartitionOperation(partitionPrefix, endpoint, operation, payload);
    setPartitionStage('partition', 'done', `已生成 ${result.rows ?? result.total_index_rows ?? 0} 条索引行。`);
    setPartitionStage('persist', 'running', '正在整理结果并保存质检报告。');
    lastPartitionResult.value = result;
    resultRows.value = formatRows(result);
	    setPartitionStage('persist', 'done', result.quality_report_id ? `质检报告已保存：${result.quality_report_id}` : '执行结果已返回。');
	    if (result.quality_report) {
	      qualityDataType.value = qualityDataTypeForEndpoint(endpoint);
	      qualityReport.value = result.quality_report;
	      selectedQualityReportId.value = result.quality_report.report_id || result.quality_report_id || '';
	    }
	    ElMessage.success(testModules.has(activeModule.value) ? '剖分测试完成，未写入正式库' : '剖分任务完成');
	  } catch (error) {
	    partitionStages.value = partitionStages.value.map((item) => (item.status === 'running' ? { ...item, status: 'failed' } : item));
	    const failure = buildPartitionFailureResult(error, lastPartitionRequest.value || {});
	    lastPartitionResult.value = failure;
	    resultRows.value = formatRows(failure);
	    setPartitionStage('persist', 'failed', `执行失败：${failure.error}`);
	  } finally {
    stopPartitionTimer();
    resultLoading.value = false;
  }
}

function currentIngestPayload() {
  const result = lastPartitionResult.value || {};
  const reportId = result.quality_report_id || result.quality_report?.report_id || '';
  const payload = { ...ingestDefaults.value };
  if (reportId) {
    payload.report_id = reportId;
  } else if (result.run_dir) {
    payload.run_dir = result.run_dir;
  }
  return payload;
}

async function loadManagedConfig() {
  try {
    const { configPrefix } = apiPrefixes();
    const response = await requestJson(`${configPrefix}/get`, {});
    const config = response.config || {};
    const optical = config.partition?.optical || {};
    const quality = config.quality?.optical || {};
    const ingest = config.ingest?.optical || {};
    opticalGridType.value = optical.grid_type || opticalGridType.value;
    opticalGridLevel.value = Number(optical.grid_level || opticalGridLevel.value);
    qualityTargetCrs.value = quality.target_crs || qualityTargetCrs.value;
    qualityHistoryLimit.value = Number(quality.history_limit || qualityHistoryLimit.value);
    ingestDefaults.value = { ...ingestDefaults.value, ...ingest };
  } catch (error) {
    ElMessage.warning(`配置加载失败，保留当前配置：${error.message}`);
  }
}

async function previewOpticalIngest() {
  if (!opticalIngestReady.value) {
    ElMessage.warning('请先完成一次光学剖分测试');
    return;
  }
  ingestLoading.value = true;
  ingestPreview.value = null;
  ingestResult.value = null;
  try {
    const { ingestPrefix } = apiPrefixes();
    ingestPreview.value = await requestJson(`${ingestPrefix}/optical/preview`, currentIngestPayload());
    ElMessage.success('预入库校验完成');
  } catch (error) {
    ElMessage.error(error.message);
  } finally {
    ingestLoading.value = false;
  }
}

async function confirmOpticalIngest() {
  if (!opticalIngestReady.value) {
    ElMessage.warning('请先完成一次光学剖分测试');
    return;
  }
  try {
    await ElMessageBox.confirm(
      '确认后会将当前剖分结果写入生产版本，重复执行会按唯一键覆盖同版本记录。',
      '确认入库',
      { confirmButtonText: '确认入库', cancelButtonText: '取消', type: 'warning' },
    );
  } catch {
    return;
  }
  ingestConfirmLoading.value = true;
  ingestResult.value = null;
  try {
    const { ingestPrefix } = apiPrefixes();
    ingestResult.value = await requestJson(`${ingestPrefix}/optical/confirm`, currentIngestPayload());
    ElMessage.success('生产版本入库完成');
    if (!ingestPreview.value) {
      ingestPreview.value = {
        mode: 'pre_ingest_preview',
        quality_status: ingestResult.value.quality_status,
        asset_version: ingestResult.value.asset_version,
        cube_version: ingestResult.value.cube_version,
        input_rows: ingestResult.value.input_rows,
        raw_asset_rows: ingestResult.value.raw_asset_rows,
        cube_fact_rows: ingestResult.value.cube_fact_rows,
        existing_raw_asset_rows: 0,
        existing_cube_fact_rows: 0,
      };
    }
  } catch (error) {
    ElMessage.error(error.message);
  } finally {
    ingestConfirmLoading.value = false;
  }
}

async function retryLastPartitionTask() {
  if (!lastPartitionRequest.value) {
    ElMessage.warning('暂无可重试任务，请先执行一次剖分');
    return;
  }
  resultLoading.value = true;
  resultRows.value = [];
  const retryRequest = lastPartitionRequest.value;
  const retryResult = lastPartitionResult.value || {};
  let currentRetryRequest = null;
  lastPartitionResult.value = null;
  ingestPreview.value = null;
  ingestResult.value = null;
  startPartitionTimer();
  resetPartitionStages();
  setPartitionStage('prepare', 'done', '已使用上一次请求参数准备重试。');
  try {
    const { partitionPrefix } = apiPrefixes();
    const { endpoint } = retryRequest;
    setPartitionStage('queue', 'running', '重试请求已进入后端队列。');
    await Promise.resolve();
    setPartitionStage('queue', 'done', '后端已接收重试请求。');
    const operation = 'retry';
    const retryPayload = {
      request: retryRequest,
      last_result: retryResult,
    };
    currentRetryRequest = { endpoint, operation, payload: retryPayload, apiPath: `/v1/partition/${endpoint}/tasks/retry` };
    if (retryRequest.kind === 'batch') {
      currentRetryRequest = { ...retryRequest, operation, apiPath: `/v1/partition/batches/${retryRequest.batchId}/retry` };
    } else if (retryRequest.kind === 'assets') {
      currentRetryRequest = { ...retryRequest, operation, apiPath: '/v1/partition/assets/retry' };
    }
    setPartitionStage('partition', 'running', `调用 ${currentRetryRequest.apiPath} 提交后台重试任务。`);
    const result = await requestRetryOperation(partitionPrefix, retryRequest, retryPayload);
    setPartitionStage('partition', 'done', `重试完成，生成 ${result.rows ?? result.total_index_rows ?? 0} 条索引行。`);
    setPartitionStage('persist', 'running', '正在更新结果与质检报告。');
    lastPartitionResult.value = result;
    resultRows.value = formatRows(result);
	    setPartitionStage('persist', 'done', result.quality_report_id ? `质检报告已保存：${result.quality_report_id}` : '重试结果已返回。');
	    if (activeModule.value === 'quality' && result.quality_report) {
	      qualityDataType.value = qualityDataTypeForEndpoint(retryRequest.endpoint);
	      qualityReport.value = result.quality_report;
      selectedQualityReportId.value = result.quality_report.report_id || result.quality_report_id || '';
      await loadQualityHistory();
    }
    ElMessage.success('任务已重试完成');
  } catch (error) {
    partitionStages.value = partitionStages.value.map((item) => (item.status === 'running' ? { ...item, status: 'failed' } : item));
    const failure = buildPartitionFailureResult(error, currentRetryRequest || retryRequest || {});
    lastPartitionResult.value = failure;
    resultRows.value = formatRows(failure);
    setPartitionStage('persist', 'failed', `重试失败：${failure.error}`);
  } finally {
    stopPartitionTimer();
    resultLoading.value = false;
  }
}

watch(activeModule, (moduleName) => {
  if (moduleName === 'quality' && !qualityReport.value && !qualityLoading.value) {
    refreshQualityWorkspace();
  }
});

watch(qualityDataType, () => {
  if (activeModule.value === 'quality') {
    changeQualityDataType();
  }
});

watch(opticalGridType, () => {
  applyDefaultGridLevel('optical');
  mapGridGeometries.value = [];
});

watch([selectedOpticalAssets, opticalGridType], () => {
  applyDefaultGridLevel('optical');
}, { deep: true });

watch([selectedRadarAssets, radarGridType], () => {
  applyDefaultGridLevel('radar');
}, { deep: true });

watch([selectedProductAssets, productGridType], () => {
  applyDefaultGridLevel('product');
}, { deep: true });

watch([
  selectedOpticalBatchIds,
  deselectedOpticalAssetKeys,
  opticalGridType,
  opticalGridLevel,
  entityGridLevel,
  selectedRadarBatchIds,
  deselectedRadarAssetKeys,
  radarGridType,
  radarGridLevel,
  selectedProductBatchIds,
  deselectedProductAssetKeys,
  productGridType,
  productGridLevel,
], () => {
  mapGridGeometries.value = [];
}, { deep: true });

onMounted(async () => {
  await loadManagedConfig();
  await loadPartitionBatches();
  applyDefaultGridLevels();
  if (activeModule.value === 'quality') {
    refreshQualityWorkspace();
  }
});

onUnmounted(() => {
  stopPartitionTimer();
});
</script>

<template>
  <section>
    <section class="module-nav">
      <div class="container">
        <div class="module-tabs">
          <button class="module-tab" :class="{ active: activeModule === 'optical' }" @click="activeModule = 'optical'">光学遥感</button>
          <button class="module-tab" :class="{ active: activeModule === 'carbon' }" @click="activeModule = 'carbon'">碳卫星</button>
          <button class="module-tab" :class="{ active: activeModule === 'radar' }" @click="activeModule = 'radar'">雷达遥感</button>
          <button class="module-tab" :class="{ active: activeModule === 'product' }" @click="activeModule = 'product'">信息产品</button>
          <button class="module-tab" :class="{ active: activeModule === 'quality' }" @click="activeModule = 'quality'">自动化质检</button>
          <button class="module-tab" :class="{ active: activeModule === 'config' }" @click="activeModule = 'config'">配置管理</button>
        </div>
      </div>
    </section>

    <main class="main-content-area">
      <div class="container">
        <div class="module-content active">
          <ConfigView v-if="activeModule === 'config'" />
          <div v-else class="workspace">
            <div class="workspace-sidebar">
              <div class="config-panel">
                <h3>{{ activeModule === 'optical' ? '数据配置' : '参数配置' }}</h3>

                <template v-if="activeModule === 'optical'">
                  <div class="form-group">
                    <label>数据源类型</label>
                    <div class="task-note">
                      <span>数据库载入批次</span>
                    </div>
                  </div>
                  <div class="form-group">
                    <label>待剖分数据队列</label>
                    <div class="task-note">
                      <span>任务备注：请在 ARD数据载入 子系统中完成数据接入与登记后，待剖分数据将自动出现在此队列中。</span>
                    </div>
                    <div class="data-queue-panel">
                      <button type="button" class="queue-header queue-drawer-toggle" @click="openDataDrawer">
                        <span class="queue-title">已载入数据</span>
                        <span class="queue-header-meta">
                          <span class="queue-count">{{ activeDataRows.length }} 个批次</span>
                          <span class="queue-open-text">打开列表</span>
                        </span>
                      </button>
                      <div class="queue-selected-summary">当前选择：<span>{{ selectedDataName }}</span></div>
                    </div>
                  </div>
                  <div class="form-group">
                    <label>剖分格网</label>
                    <el-select v-model="opticalGridType" class="legacy-control">
                      <el-option label="四边形格网" value="geohash" />
                      <el-option label="平面格网" value="tile_matrix" />
                      <el-option label="六边形格网" value="isea4h" />
                    </el-select>
                  </div>
                </template>

                <template v-else-if="activeModule === 'carbon'">
                  <div class="form-group">
                    <label>待剖分数据队列</label>
                    <div class="task-note">
                      <span>任务备注：请在 ARD数据载入 子系统中完成数据接入与登记后，待剖分数据将自动出现在此队列中。</span>
                    </div>
                    <div class="data-queue-panel">
                      <button type="button" class="queue-header queue-drawer-toggle" @click="openDataDrawer">
                        <span class="queue-title">已载入数据</span>
                        <span class="queue-header-meta">
                          <span class="queue-count">{{ activeDataRows.length }} 个批次</span>
                          <span class="queue-open-text">打开列表</span>
                        </span>
                      </button>
                      <div class="queue-selected-summary">当前选择：<span>{{ selectedDataName }}</span></div>
                    </div>
                  </div>
                  <div class="form-group">
                    <label>剖分格网</label>
                    <el-select v-model="radarGridType" class="legacy-control">
                      <el-option label="四边形格网" value="geohash" />
                      <el-option label="平面格网" value="tile_matrix" />
                    </el-select>
                  </div>
                </template>

                <template v-else-if="activeModule === 'radar'">
                  <div class="form-group">
                    <label>待剖分数据队列</label>
                    <div class="task-note">
                      <span>任务备注：请在 ARD数据载入 子系统中完成数据接入与登记后，待剖分数据将自动出现在此队列中。</span>
                    </div>
                    <div class="data-queue-panel">
                      <button type="button" class="queue-header queue-drawer-toggle" @click="openDataDrawer">
                        <span class="queue-title">已载入数据</span>
                        <span class="queue-header-meta">
                          <span class="queue-count">{{ activeDataRows.length }} 个批次</span>
                          <span class="queue-open-text">打开列表</span>
                        </span>
                      </button>
                      <div class="queue-selected-summary">当前选择：<span>{{ selectedDataName }}</span></div>
                    </div>
                  </div>
                  <div class="form-group">
                    <label>剖分格网</label>
                    <el-select v-model="radarGridType" class="legacy-control">
                      <el-option label="四边形格网" value="geohash" />
                      <el-option label="平面格网" value="tile_matrix" />
                      <el-option label="六边形格网" value="isea4h" />
                    </el-select>
                  </div>
                </template>

                <template v-else-if="activeModule === 'product'">
                  <div class="form-group">
                    <label>待剖分数据队列</label>
                    <div class="task-note">
                      <span>任务备注：请在 ARD数据载入 子系统中完成数据接入与登记后，待剖分数据将自动出现在此队列中。</span>
                    </div>
                    <div class="data-queue-panel">
                      <button type="button" class="queue-header queue-drawer-toggle" @click="openDataDrawer">
                        <span class="queue-title">已载入数据</span>
                        <span class="queue-header-meta">
                          <span class="queue-count">{{ activeDataRows.length }} 个批次</span>
                          <span class="queue-open-text">打开列表</span>
                        </span>
                      </button>
                      <div class="queue-selected-summary">当前选择：<span>{{ selectedDataName }}</span></div>
                    </div>
                  </div>
                  <div class="form-group">
                    <label>剖分格网</label>
                    <el-select v-model="productGridType" class="legacy-control">
                      <el-option label="四边形格网" value="geohash" />
                      <el-option label="平面格网" value="tile_matrix" />
                      <el-option label="六边形格网" value="isea4h" />
                    </el-select>
                  </div>
                </template>

                <template v-else>
                  <div class="form-group">
	                    <label>质检数据类型</label>
	                    <el-select v-model="qualityDataType" class="legacy-control">
	                      <el-option label="光学遥感" value="optical" />
	                      <el-option label="数据产品" value="product" />
	                      <el-option label="碳卫星" value="carbon" />
	                      <el-option label="雷达遥感" value="radar" />
	                    </el-select>
	                  </div>
                  <div class="form-group">
                    <label>目标参考系统</label>
                    <el-select v-model="qualityTargetCrs" class="legacy-control" @change="refreshQualityWorkspace">
                      <el-option label="EPSG:4326" value="EPSG:4326" />
                    </el-select>
                  </div>
	                  <div class="form-group">
	                    <div class="quality-rule-list">
	                      <template v-if="qualityDataType === 'carbon'">
	                        <div>观测行文件读取</div>
	                        <div>观测字段完整性</div>
	                        <div>观测坐标合法性</div>
	                        <div>XCO2 数值范围</div>
	                        <div>质量标记分布</div>
	                        <div>观测 ID 重复检查</div>
	                      </template>
	                      <template v-else>
	                        <div>索引字段完整性</div>
	                        <div>COG 可读性与 CRS</div>
	                        <div>window 越界检查</div>
	                        <div>像元抽样有效性</div>
	                        <div v-if="qualityDataType === 'product'">产品年份完整性</div>
	                      </template>
	                    </div>
	                  </div>
                  <div class="form-group">
                    <label>历史质检记录</label>
                    <button type="button" class="quality-history-drawer-toggle" @click="openQualityHistoryDrawer">
                      <span>
                        <strong>{{ qualityHistory.length }}</strong>
                        <span>条记录</span>
                      </span>
                      <span>打开列表</span>
                    </button>
                    <div class="quality-selected-record">
                      <span>当前选中</span>
                      <strong>{{ selectedQualityRecord?.run_name || qualityReport?.run_dir?.split('/').filter(Boolean).pop() || '未选择' }}</strong>
                      <small v-if="selectedQualityRecord">
                        {{ selectedQualityRecord.dataset }} · {{ statusText(selectedQualityRecord.status) }} · {{ formatQualityTime(selectedQualityRecord.generated_at || selectedQualityRecord.modified_at) }}
                      </small>
                    </div>
                  </div>
                </template>

                <div class="form-group action-buttons">
                  <el-button>重置</el-button>
                  <el-button type="primary" :loading="activeModule === 'quality' ? qualityLoading : resultLoading" @click="runDemo">
                    {{ activeModule === 'quality' ? '刷新结果' : testModules.has(activeModule) ? '剖分测试' : '开始剖分' }}
                  </el-button>
                  <el-button
                    v-if="activeModule === 'optical'"
                    :loading="ingestLoading"
                    :disabled="!opticalIngestReady || resultLoading"
                    @click="previewOpticalIngest"
                  >
                    预入库校验
                  </el-button>
                  <el-button
                    v-if="activeModule === 'optical'"
                    type="success"
                    :loading="ingestConfirmLoading"
                    :disabled="!opticalIngestReady || !ingestPreview || resultLoading || ingestLoading"
                    @click="confirmOpticalIngest"
                  >
                    确认入库
                  </el-button>
                </div>
              </div>
            </div>

            <div class="workspace-main">
              <div v-if="activeModule !== 'quality'" class="map-panel">
                <div class="panel-header">
                  <h3>{{ activeModule === 'carbon' ? '观测足迹地图分布' : activeModule === 'product' ? '产品范围地图预览' : '地图预览' }}</h3>
                  <div v-if="['optical', 'radar', 'product'].includes(activeModule)" class="map-actions">
                    <el-input-number v-if="activeModule === 'product'" v-model="productGridLevel" :min="1" :max="15" size="small" :disabled="!activeGridLevelManual" />
                    <el-input-number v-else-if="activeModule === 'radar'" v-model="radarGridLevel" :min="1" :max="15" size="small" :disabled="!activeGridLevelManual" />
                    <el-input-number v-else-if="opticalGridType === 'isea4h'" v-model="entityGridLevel" :min="1" :max="15" size="small" :disabled="!activeGridLevelManual" />
                    <el-input-number v-else v-model="opticalGridLevel" :min="1" :max="15" size="small" :disabled="!activeGridLevelManual" />
                    <el-button size="small" :icon="activeGridLevelManual ? Refresh : EditPen" @click="activeGridLevelManual ? restoreDefaultGridLevel() : confirmGridLevelManualEdit()">
                      {{ activeGridLevelManual ? '恢复默认' : '修改层级' }}
                    </el-button>
                    <el-button size="small" :loading="mapGridLoading" @click="loadMapGridForSelectedAssets">加载格网</el-button>
                    <el-button size="small" @click="clearMapGrid">清空格网</el-button>
                  </div>
                </div>
                <GlobeMap :markers="[]" :geometries="['optical', 'radar', 'product'].includes(activeModule) ? mapGeometries : []" />
              </div>
              <div v-else class="quality-overview-panel">
                <div class="panel-header">
                  <h3>质检总览</h3>
                </div>
                <div class="quality-dashboard">
                  <template v-if="qualityReport">
                    <div class="quality-status-band" :class="qualityReport.status.toLowerCase()">
                      <div>
                        <span>批次状态</span>
                        <strong>{{ statusText(qualityReport.status) }}</strong>
                      </div>
                      <el-tag :type="qualityStatusType" size="large">{{ qualityReport.target_crs }}</el-tag>
                    </div>
                    <div class="quality-metrics">
                      <div v-for="item in qualitySummaryRows" :key="item.label" class="quality-metric">
                        <span>{{ item.label }}</span>
                        <strong>{{ item.value }}</strong>
                      </div>
                    </div>
                    <div class="quality-band-table">
                      <div class="quality-section-title">当前批次</div>
                      <div class="quality-kv">
                        <span>报告 ID</span>
                        <strong>{{ qualityReport.report_id }}</strong>
                      </div>
                      <div class="quality-kv">
                        <span>来源</span>
                        <strong>{{ qualitySourceText() }}</strong>
                      </div>
                    </div>
                    <div class="quality-band-table">
                      <div class="quality-section-title">{{ qualityBreakdownTitle() }}</div>
                      <div
                        v-for="(value, band) in qualityBreakdownRows()"
                        :key="band"
                        class="quality-kv"
                      >
                        <span>{{ band }}</span>
                        <strong>{{ value }}</strong>
                      </div>
                    </div>
                  </template>
                  <div v-else-if="qualityLoading" class="quality-empty-state compact">
                    <div class="quality-empty-icon">QC</div>
                    <p>正在自动加载最新质检结果</p>
                  </div>
                  <div v-else-if="qualityError" class="quality-empty-state compact">
                    <div class="quality-empty-icon">ERR</div>
                    <p>{{ qualityError }}</p>
                  </div>
                  <div v-else class="quality-empty-state compact">
                    <div class="quality-empty-icon">QC</div>
                    <p>等待自动质检结果</p>
                  </div>

                  <div class="quality-manual-section">
                    <div class="quality-section-heading">
                      <div>
                        <strong>人工处置队列</strong>
                        <span>{{ dataLabelsByModule[qualityDataType] || qualitySourceText() }}</span>
                      </div>
                      <el-button size="small" :icon="Refresh" :loading="partitionBatchLoading" @click="loadPartitionBatches">刷新队列</el-button>
                    </div>
                    <div class="quality-manual-stats">
                      <div v-for="item in qualityManualBatchStats" :key="item.label" class="quality-manual-stat" :class="item.status">
                        <span>{{ item.label }}</span>
                        <strong>{{ item.value }}</strong>
                      </div>
                    </div>
                    <div v-if="qualityManualBatches.length" class="quality-manual-list">
                      <div v-for="batch in qualityManualBatches" :key="batch.id" class="quality-manual-batch">
                        <div class="quality-manual-batch-main">
                          <div class="quality-manual-batch-head">
                            <strong>{{ batch.name }}</strong>
                            <el-tag size="small" :type="partitionStatusType(batch.status)">{{ partitionStatusText(batch.status) }}</el-tag>
                          </div>
                          <div class="quality-manual-batch-meta">
                            <span>{{ batch.id }}</span>
                            <span>尝试 {{ batch.attempt_count || 0 }} 次</span>
                            <span v-if="batch.last_task_id">最近任务 {{ batch.last_task_id }}</span>
                          </div>
                          <div class="quality-manual-batch-error">{{ batch.last_error || batch.quality_failure_reason || '等待人工确认后继续处理' }}</div>
                        </div>
                        <div class="quality-manual-batch-actions">
                          <el-button size="small" :icon="Document" @click="openPartitionBatchDetail(batch)">详情</el-button>
                          <el-button size="small" :type="partitionBatchActionType(batch)" :icon="partitionBatchActionIcon(batch)" @click="handlePartitionBatchPrimaryAction(batch)">
                            {{ partitionBatchActionLabel(batch) }}
                          </el-button>
                        </div>
                      </div>
                    </div>
                    <div v-else class="quality-manual-empty">当前数据类型没有需要人工处置的失败批次</div>
                  </div>
                </div>
              </div>
            </div>

            <div class="workspace-result">
              <div class="result-panel">
                <div class="result-panel-header">
                  <h3>{{ activeModule === 'quality' ? '质检结果' : '执行结果' }}</h3>
                  <el-dropdown
                    v-if="activeModule === 'quality'"
                    trigger="click"
                    @command="exportQualityReport"
                  >
                    <el-button
                      size="small"
                      :loading="qualityExportLoading"
                      :disabled="!qualityReport"
                    >
                      导出报告
                    </el-button>
                    <template #dropdown>
                      <el-dropdown-menu>
                        <el-dropdown-item command="pdf">导出 PDF</el-dropdown-item>
                        <el-dropdown-item command="txt">导出 TXT</el-dropdown-item>
                      </el-dropdown-menu>
                    </template>
                  </el-dropdown>
                </div>
                <div class="results-content">
                  <template v-if="activeModule !== 'quality'">
                    <div class="partition-progress-panel">
                      <div class="quality-section-title">剖分进程</div>
                      <div class="partition-context-grid">
                        <button
                          v-for="item in partitionContextRows"
                          :key="item.label"
                          type="button"
                          class="partition-context-item"
                          :title="String(item.value)"
                          @click="openPartitionContextDetail(item)"
                        >
                          <span>{{ item.label }}</span>
                          <strong>{{ item.value }}</strong>
                        </button>
                      </div>
                      <div class="partition-stage-list">
                        <button
                          v-for="stage in partitionStages"
                          :key="stage.key"
                          type="button"
                          class="partition-stage-item"
                          :title="stage.detail"
                          @click="openPartitionStageDetail(stage)"
                        >
                          <div class="partition-stage-main">
                            <strong>{{ stage.label }}</strong>
                            <span>{{ stage.detail }}</span>
                          </div>
                          <div class="partition-stage-actions">
                            <el-tag :type="stageTagType(stage.status)" size="small">{{ stageText(stage.status) }}</el-tag>
                            <span class="partition-stage-open">详情</span>
                          </div>
                        </button>
                      </div>
                    </div>
                    <el-alert
                      v-if="partitionFailureMessage"
                      type="error"
                      :closable="false"
                      :title="partitionFailureMessage"
                      class="partition-failure-alert"
                    />
                    <el-alert
                      v-if="partitionWarnNeedsRetry"
                      type="warning"
                      :closable="false"
                      title="质检出现告警，可在“自动化质检”的人工处置队列中查看并重试。"
                      class="partition-warn-alert"
                    />
                    <div v-if="partitionMetricRows.length" class="partition-metrics">
                      <div class="quality-section-title">剖分结果</div>
                      <div v-for="item in partitionMetricRows" :key="item.label" class="quality-kv">
                        <span>{{ item.label }}</span>
                        <strong>{{ item.value }}</strong>
                      </div>
                    </div>
                    <div v-if="partitionResultDetailRows.length" class="partition-metrics">
                      <div class="quality-section-title">执行明细</div>
                      <div v-for="item in partitionResultDetailRows" :key="item.label" class="quality-kv">
                        <span>{{ item.label }}</span>
                        <strong>{{ item.value }}</strong>
                      </div>
                    </div>
                    <div v-if="activeModule === 'optical' && ingestPreviewRows.length" class="partition-metrics">
                      <div class="quality-section-title">预入库校验</div>
                      <div v-for="item in ingestPreviewRows" :key="item.label" class="quality-kv">
                        <span>{{ item.label }}</span>
                        <strong>{{ item.value }}</strong>
                      </div>
                    </div>
                    <div v-if="activeModule === 'optical' && ingestResultRows.length" class="partition-metrics">
                      <div class="quality-section-title">确认入库结果</div>
                      <div v-for="item in ingestResultRows" :key="item.label" class="quality-kv">
                        <span>{{ item.label }}</span>
                        <strong>{{ item.value }}</strong>
                      </div>
                    </div>
                  </template>
                  <template v-if="activeModule === 'quality' && qualityReport">
                    <div class="quality-check-list">
                      <div v-for="check in qualityReport.checks" :key="check.name" class="quality-check-item" :class="check.status.toLowerCase()">
                        <div class="quality-check-head">
                          <strong>{{ checkNameText(check.name) }}</strong>
                          <el-tag :type="checkStatusType(check.status)" size="small">{{ statusText(check.status) }}</el-tag>
                        </div>
                        <p>{{ checkMessageText(check) }}</p>
                        <div v-if="checkDetailRows(check).length" class="quality-check-details">
                          <div v-for="detail in checkDetailRows(check)" :key="detail.title" class="quality-check-detail">
                            <strong>{{ detail.title }}</strong>
                            <span v-for="line in detail.lines" :key="line">{{ line }}</span>
                          </div>
                        </div>
                      </div>
                    </div>
                    <div v-if="qualityReport.assets?.length" class="quality-assets">
                      <div class="quality-section-title">资产抽查</div>
                      <div v-for="asset in qualityReport.assets.slice(0, 6)" :key="asset.path" class="quality-asset-row">
                        <span>{{ asset.path.split('/').pop() }}</span>
                        <strong>{{ asset.crs }}</strong>
                      </div>
                    </div>
                  </template>
                  <template v-else-if="resultRows.length">
                    <div v-for="row in resultRows" :key="row.key" class="result-item">
                      <div class="result-label">{{ row.key }}</div>
                      <div class="result-value">{{ row.value }}</div>
                    </div>
                  </template>
                  <div v-else class="empty-state">
                    <p>{{ activeModule === 'quality' ? '尚未执行质检' : '配置参数并执行剖分' }}</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </main>

    <el-drawer v-model="dataDrawerVisible" :title="`已载入${activeDataLabel}`" size="680px" direction="rtl">
      <el-input v-model="dataSearch" :prefix-icon="Search" placeholder="按名称查询" clearable />
      <div v-loading="partitionBatchLoading && ['optical', 'carbon', 'radar', 'product'].includes(activeModule)">
        <template v-if="activeModule === 'optical'">
          <div class="batch-list">
          <div
            v-for="batch in visibleOpticalBatches.filter((item) => !dataSearch.trim() || item.name.toLowerCase().includes(dataSearch.trim().toLowerCase()) || item.id.toLowerCase().includes(dataSearch.trim().toLowerCase()))"
            :key="batch.id"
            class="batch-card"
          >
            <div class="batch-card-header">
              <el-checkbox :model-value="selectedOpticalBatchIds.includes(batch.id)" @change="toggleOpticalBatchSelect(batch.id)">
                <span class="batch-name">{{ batch.name }}</span>
              </el-checkbox>
              <div class="batch-meta">
                <span class="batch-id">{{ batch.id }}</span>
                <el-tag size="small" :type="partitionStatusType(batch.status)">{{ partitionStatusText(batch.status) }}</el-tag>
                <el-button size="small" :icon="Document" @click="openPartitionBatchDetail(batch)">详情</el-button>
                <el-button size="small" :type="partitionBatchActionType(batch)" :icon="partitionBatchActionIcon(batch)" @click="handlePartitionBatchPrimaryAction(batch)">
                  {{ partitionBatchActionLabel(batch) }}
                </el-button>
                <button type="button" class="batch-expand-btn" @click="toggleOpticalBatchExpand(batch.id)">
                  {{ expandedOpticalBatchId === batch.id ? '收起' : '展开' }}
                </button>
              </div>
            </div>
            <div class="batch-summary">{{ opticalBatchSummary(batch) }}</div>
            <div class="batch-footer">
              <span>{{ partitionBatchSummary(batch) }}</span>
              <span v-if="batch.last_task_id">最近任务 {{ batch.last_task_id }}</span>
            </div>
            <div v-if="expandedOpticalBatchId === batch.id" class="batch-assets">
              <div v-for="asset in batch.assets" :key="`${batch.id}-${asset.source_uri}-${assetBandsText(asset)}`" class="asset-row">
                <div class="asset-main">
                  <el-checkbox :model-value="isOpticalAssetSelected(batch.id, asset)" @change="toggleOpticalAssetSelect(batch.id, asset)" />
                  <strong>{{ asset.scene_id }}</strong>
                  <span>{{ assetBandsText(asset) }}</span>
                  <span>{{ asset.acq_time }}</span>
                </div>
                <div class="asset-source">{{ asset.source_uri }}</div>
                <div class="asset-corners">corners: {{ asset.corners.map((c) => `[${c[0]}, ${c[1]}]`).join(' ') }}</div>
              </div>
            </div>
          </div>
          </div>
        </template>
        <template v-else-if="activeModule === 'carbon'">
          <div class="batch-list">
          <div
            v-for="batch in visibleCarbonBatches.filter((item) => !dataSearch.trim() || item.name.toLowerCase().includes(dataSearch.trim().toLowerCase()) || item.id.toLowerCase().includes(dataSearch.trim().toLowerCase()))"
            :key="batch.id"
            class="batch-card"
          >
            <div class="batch-card-header">
              <el-checkbox :model-value="selectedCarbonBatchIds.includes(batch.id)" @change="toggleCarbonBatchSelect(batch.id)">
                <span class="batch-name">{{ batch.name }}</span>
              </el-checkbox>
              <div class="batch-meta">
                <span class="batch-id">{{ batch.id }}</span>
                <el-tag size="small" :type="partitionStatusType(batch.status)">{{ partitionStatusText(batch.status) }}</el-tag>
                <el-button size="small" :icon="Document" @click="openPartitionBatchDetail(batch)">详情</el-button>
                <el-button size="small" :type="partitionBatchActionType(batch)" :icon="partitionBatchActionIcon(batch)" @click="handlePartitionBatchPrimaryAction(batch)">
                  {{ partitionBatchActionLabel(batch) }}
                </el-button>
                <button type="button" class="batch-expand-btn" @click="toggleCarbonBatchExpand(batch.id)">
                  {{ expandedCarbonBatchId === batch.id ? '收起' : '展开' }}
                </button>
              </div>
            </div>
            <div class="batch-summary">{{ carbonBatchSummary(batch) }}</div>
            <div class="batch-footer">
              <span>{{ partitionBatchSummary(batch) }}</span>
              <span v-if="batch.last_task_id">最近任务 {{ batch.last_task_id }}</span>
            </div>
            <div v-if="expandedCarbonBatchId === batch.id" class="batch-assets">
              <div class="schema-grid">
                <div v-for="field in batch.schema" :key="`${batch.id}-${field.field}`" class="schema-item">
                  <strong>{{ field.field }}</strong>
                  <span>{{ field.type }}</span>
                  <small>{{ field.meaning }}</small>
                </div>
              </div>
              <div v-for="observation in batch.observations" :key="`${batch.id}-${observation.source_index}`" class="asset-row">
                <div class="asset-main">
                  <el-checkbox :model-value="isCarbonObservationSelected(batch.id, observation)" @change="toggleCarbonObservationSelect(batch.id, observation)" />
                  <strong>{{ observation.observation_id }}</strong>
                  <span>{{ observation.acq_time }}</span>
                  <span>xco2 {{ observation.xco2 }}</span>
                  <span>Q{{ observation.quality_flag }}</span>
                </div>
                <div class="asset-source">{{ batch.source_uri }} #{{ observation.source_index }}</div>
	                <div class="asset-corners">center: [{{ observation.lon }}, {{ observation.lat }}]</div>
	              </div>
	            </div>
	          </div>
          </div>
        </template>
        <template v-else-if="activeModule === 'radar'">
          <div class="batch-list">
            <div
              v-for="batch in visibleRadarBatches.filter((item) => !dataSearch.trim() || item.name.toLowerCase().includes(dataSearch.trim().toLowerCase()) || item.id.toLowerCase().includes(dataSearch.trim().toLowerCase()))"
              :key="batch.id"
              class="batch-card"
            >
              <div class="batch-card-header">
                <el-checkbox :model-value="selectedRadarBatchIds.includes(batch.id)" @change="toggleRadarBatchSelect(batch.id)">
                  <span class="batch-name">{{ batch.name }}</span>
                </el-checkbox>
                <div class="batch-meta">
                  <span class="batch-id">{{ batch.id }}</span>
                  <el-tag size="small" :type="partitionStatusType(batch.status)">{{ partitionStatusText(batch.status) }}</el-tag>
                  <el-button size="small" :icon="Document" @click="openPartitionBatchDetail(batch)">详情</el-button>
                  <el-button size="small" :type="partitionBatchActionType(batch)" :icon="partitionBatchActionIcon(batch)" @click="handlePartitionBatchPrimaryAction(batch)">
                    {{ partitionBatchActionLabel(batch) }}
                  </el-button>
                  <button type="button" class="batch-expand-btn" @click="toggleRadarBatchExpand(batch.id)">
                    {{ expandedRadarBatchId === batch.id ? '收起' : '展开' }}
                  </button>
                </div>
              </div>
              <div class="batch-summary">{{ radarBatchSummary(batch) }}</div>
              <div class="batch-footer">
                <span>{{ partitionBatchSummary(batch) }}</span>
                <span v-if="batch.last_task_id">最近任务 {{ batch.last_task_id }}</span>
              </div>
              <div v-if="expandedRadarBatchId === batch.id" class="batch-assets">
                <div class="schema-grid">
                  <div v-for="field in batch.schema" :key="`${batch.id}-${field.field}`" class="schema-item">
                    <strong>{{ field.field }}</strong>
                    <span>{{ field.type }}</span>
                    <small>{{ field.meaning }}</small>
                  </div>
                </div>
                <div v-for="asset in batch.assets" :key="`${batch.id}-${asset.source_uri}`" class="asset-row">
                  <div class="asset-main">
                    <el-checkbox :model-value="isRadarAssetSelected(batch.id, asset)" @change="toggleRadarAssetSelect(batch.id, asset)" />
                    <strong>{{ asset.scene_id }}</strong>
                    <span>{{ (asset.polarization || asset.band || '').toUpperCase() }}</span>
                    <span>{{ asset.resolution }}m</span>
                    <span>{{ asset.acq_time }}</span>
                  </div>
                  <div class="asset-source">{{ asset.source_uri }}</div>
                  <div class="asset-corners">bbox: {{ asset.bbox.join(', ') }}</div>
                  <div class="asset-corners">corners: {{ asset.corners.map((c) => `[${c[0]}, ${c[1]}]`).join(' ') }}</div>
                </div>
              </div>
            </div>
          </div>
        </template>
        <template v-else-if="activeModule === 'product'">
          <div class="batch-list">
            <div
              v-for="batch in visibleProductBatches.filter((item) => !dataSearch.trim() || item.name.toLowerCase().includes(dataSearch.trim().toLowerCase()) || item.id.toLowerCase().includes(dataSearch.trim().toLowerCase()))"
              :key="batch.id"
              class="batch-card"
            >
              <div class="batch-card-header">
                <el-checkbox :model-value="selectedProductBatchIds.includes(batch.id)" @change="toggleProductBatchSelect(batch.id)">
                  <span class="batch-name">{{ batch.name }}</span>
                </el-checkbox>
                <div class="batch-meta">
                  <span class="batch-id">{{ batch.id }}</span>
                  <el-tag size="small" :type="partitionStatusType(batch.status)">{{ partitionStatusText(batch.status) }}</el-tag>
                  <el-button size="small" :icon="Document" @click="openPartitionBatchDetail(batch)">详情</el-button>
                  <el-button size="small" :type="partitionBatchActionType(batch)" :icon="partitionBatchActionIcon(batch)" @click="handlePartitionBatchPrimaryAction(batch)">
                    {{ partitionBatchActionLabel(batch) }}
                  </el-button>
                  <button type="button" class="batch-expand-btn" @click="toggleProductBatchExpand(batch.id)">
                    {{ expandedProductBatchId === batch.id ? '收起' : '展开' }}
                  </button>
                </div>
              </div>
              <div class="batch-summary">{{ productBatchSummary(batch) }}</div>
              <div class="batch-footer">
                <span>{{ partitionBatchSummary(batch) }}</span>
                <span v-if="batch.last_task_id">最近任务 {{ batch.last_task_id }}</span>
              </div>
              <div v-if="expandedProductBatchId === batch.id" class="batch-assets">
                <div class="schema-grid">
                  <div v-for="field in batch.schema" :key="`${batch.id}-${field.field}`" class="schema-item">
                    <strong>{{ field.field }}</strong>
                    <span>{{ field.type }}</span>
                    <small>{{ field.meaning }}</small>
                  </div>
                </div>
                <div v-for="asset in batch.assets" :key="`${batch.id}-${asset.source_uri}`" class="asset-row">
                  <div class="asset-main">
                    <el-checkbox :model-value="isProductAssetSelected(batch.id, asset)" @change="toggleProductAssetSelect(batch.id, asset)" />
                    <strong>{{ asset.product_year }} 年</strong>
                    <span>{{ asset.band }}</span>
                    <span>{{ asset.resolution }}</span>
                    <span>{{ asset.acq_time }}</span>
                  </div>
                  <div class="asset-source">{{ asset.source_uri }}</div>
                  <div class="asset-corners">product: {{ asset.product_name }} | {{ batch.target_crs }}</div>
                  <div class="asset-corners">bbox: {{ asset.bbox.join(', ') }}</div>
                  <div class="asset-corners">corners: {{ asset.corners.map((c) => `[${c[0]}, ${c[1]}]`).join(' ') }}</div>
                </div>
              </div>
            </div>
          </div>
        </template>
      </div>
    </el-drawer>

    <el-drawer v-model="qualityHistoryDrawerVisible" title="历史质检记录" size="760px" direction="rtl">
      <div class="quality-history-filterbar">
        <el-input v-model="qualityHistorySearch" :prefix-icon="Search" placeholder="按数据集、批次或路径筛选" clearable />
        <el-select v-model="qualityHistoryStatus" placeholder="状态" clearable>
          <el-option label="通过" value="PASS" />
          <el-option label="告警" value="WARN" />
          <el-option label="失败" value="FAIL" />
        </el-select>
      </div>
      <el-table
        v-loading="qualityHistoryLoading"
        :data="filteredQualityHistory"
        class="drawer-table quality-history-table"
        highlight-current-row
        :row-class-name="qualityHistoryRowClass"
        @row-click="selectQualityRecord"
      >
        <el-table-column label="数据集" prop="dataset" min-width="130" />
        <el-table-column label="批次" prop="run_name" min-width="170" />
	        <el-table-column v-if="qualityDataType === 'product'" label="年份" min-width="150">
	          <template #default="{ row }">{{ row.summary?.product_years?.join(', ') || '-' }}</template>
	        </el-table-column>
	        <el-table-column v-if="qualityDataType === 'carbon'" label="质量标记" min-width="150">
	          <template #default="{ row }">
	            {{ Object.entries(row.summary?.quality_counts || {}).map(([flag, count]) => `Q${flag}: ${count}`).join(', ') || '-' }}
	          </template>
	        </el-table-column>
        <el-table-column label="状态" width="86">
          <template #default="{ row }">
            <el-tag :type="checkStatusType(row.status)" size="small">{{ statusText(row.status) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="索引行" width="82">
          <template #default="{ row }">{{ row.summary?.index_rows ?? 0 }}</template>
        </el-table-column>
        <el-table-column label="告警/失败" width="98">
          <template #default="{ row }">{{ row.summary?.warning_checks ?? 0 }}/{{ row.summary?.failed_checks ?? 0 }}</template>
        </el-table-column>
        <el-table-column label="质检时间" min-width="160">
          <template #default="{ row }">{{ formatQualityTime(row.generated_at || row.modified_at) }}</template>
        </el-table-column>
      </el-table>
    </el-drawer>

    <el-dialog v-model="partitionStageDetailVisible" title="剖分进程详情" width="520px">
      <div v-if="selectedPartitionStage" class="partition-stage-detail">
        <div class="partition-stage-detail-row">
          <span>阶段</span>
          <strong>{{ selectedPartitionStage.label }}</strong>
        </div>
        <div class="partition-stage-detail-row">
          <span>状态</span>
          <el-tag :type="stageTagType(selectedPartitionStage.status)" size="small">
            {{ stageText(selectedPartitionStage.status) }}
          </el-tag>
        </div>
        <div class="partition-stage-detail-message">{{ selectedPartitionStage.detail }}</div>
      </div>
    </el-dialog>

    <el-dialog v-model="partitionContextDetailVisible" title="剖分信息详情" width="520px">
      <div v-if="selectedPartitionContext" class="partition-stage-detail">
        <div class="partition-stage-detail-row">
          <span>字段</span>
          <strong>{{ selectedPartitionContext.label }}</strong>
        </div>
        <div class="partition-stage-detail-message">{{ selectedPartitionContext.value }}</div>
      </div>
    </el-dialog>

    <el-drawer
      v-model="partitionBatchDetailVisible"
      :title="partitionBatchDetailTitle(partitionBatchDetail)"
      size="920px"
      direction="rtl"
      @open="refreshPartitionBatchDetail"
    >
      <div v-loading="partitionBatchDetailLoading" class="partition-batch-detail">
        <div class="partition-batch-detail-head">
          <div>
            <div class="partition-batch-detail-subtitle">{{ partitionBatchDetailSubtitle(partitionBatchDetail) }}</div>
            <div v-if="partitionBatchDetail" class="partition-batch-detail-summary">{{ partitionBatchSummary(partitionBatchDetail) }}</div>
          </div>
          <div class="partition-batch-detail-actions">
            <el-button :icon="Document" @click="refreshPartitionBatchDetail">刷新</el-button>
            <el-button
              v-if="partitionBatchCanCancel(partitionBatchDetail)"
              type="danger"
              :icon="CircleCloseFilled"
              :loading="partitionBatchDetailAction === 'cancel'"
              @click="cancelPartitionBatchFromDetail"
            >
              取消任务
            </el-button>
            <el-button
              v-else
              type="primary"
              :icon="partitionBatchActionIcon(partitionBatchDetail)"
              :loading="partitionBatchDetailAction === 'run' || partitionBatchDetailAction === 'retry'"
              @click="runPartitionBatchFromDetail"
            >
              {{ partitionBatchActionLabel(partitionBatchDetail) }}
            </el-button>
          </div>
        </div>

        <div class="partition-batch-detail-tabs">
          <button type="button" class="partition-detail-tab" :class="{ active: partitionBatchDetailTab === 'overview' }" @click="partitionBatchDetailTab = 'overview'">概览</button>
          <button type="button" class="partition-detail-tab" :class="{ active: partitionBatchDetailTab === 'assets' }" @click="partitionBatchDetailTab = 'assets'">资产</button>
          <button type="button" class="partition-detail-tab" :class="{ active: partitionBatchDetailTab === 'attempts' }" @click="partitionBatchDetailTab = 'attempts'">尝试历史</button>
        </div>

        <div v-if="partitionBatchDetailTab === 'overview'" class="partition-detail-section">
          <div class="quality-section-title">批次信息</div>
          <div class="partition-detail-grid">
            <div v-for="item in partitionBatchDetailPayloadRows(partitionBatchDetail)" :key="item.label" class="quality-kv">
              <span>{{ item.label }}</span>
              <strong>{{ item.value }}</strong>
            </div>
          </div>
        </div>

        <div v-else-if="partitionBatchDetailTab === 'assets'" class="partition-detail-section">
          <div class="partition-detail-toolbar">
            <el-input v-model="partitionBatchDetailSearch" :prefix-icon="Search" placeholder="按资产、时间、路径筛选" clearable />
            <el-select v-model="partitionBatchDetailAssetStatus" placeholder="状态" clearable>
              <el-option label="全部" value="all" />
              <el-option label="待处理" value="pending" />
              <el-option label="已排队" value="queued" />
              <el-option label="执行中" value="running" />
              <el-option label="失败" value="failed" />
              <el-option label="人工确认" value="manual_required" />
              <el-option label="已取消" value="cancelled" />
              <el-option label="已完成" value="succeeded" />
            </el-select>
          </div>
          <div class="partition-detail-asset-summary">
            <span>可重试资产 {{ partitionBatchRetryableAssetCount }} 条</span>
            <span>已选 {{ partitionBatchSelectedRetryableAssets.length }} 条</span>
            <el-button size="small" :icon="Refresh" :disabled="!partitionBatchCanRetrySelectedAssets(partitionBatchDetail) || partitionBatchDetailAction === 'assetRetry'" :loading="partitionBatchDetailAction === 'assetRetry'" @click="retrySelectedPartitionAssetsFromDetail">
              重试失败资产
            </el-button>
          </div>
          <el-table
            :data="partitionBatchDetailFilteredAssets"
            class="drawer-table partition-asset-table"
            height="420"
            row-key="asset_id"
            @selection-change="partitionBatchAssetSelectionChange"
          >
            <el-table-column type="selection" width="42" :selectable="partitionBatchAssetSelectable" />
            <el-table-column label="状态" width="96">
              <template #default="{ row }">
                <el-tag :type="partitionAssetStatusType(row.status)" size="small">{{ partitionAssetStatusText(row.status) }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="标题" min-width="190">
              <template #default="{ row }">
                <strong>{{ row.title }}</strong>
                <div class="partition-asset-subtitle">{{ row.subtitle }}</div>
              </template>
            </el-table-column>
            <el-table-column label="来源" min-width="240">
              <template #default="{ row }">{{ row.source_uri }}</template>
            </el-table-column>
            <el-table-column label="详情" min-width="220">
              <template #default="{ row }">
                <div class="partition-asset-detail" v-for="line in row.details" :key="line">{{ line }}</div>
              </template>
            </el-table-column>
            <el-table-column label="错误" min-width="170">
              <template #default="{ row }">{{ row.error || '-' }}</template>
            </el-table-column>
          </el-table>
        </div>

        <div v-else class="partition-detail-section">
          <div class="quality-section-title">尝试历史</div>
          <div class="partition-attempt-list">
            <div v-for="attempt in partitionBatchDetailAttempts" :key="attempt.task_id" class="partition-attempt-item">
              <div class="partition-attempt-main">
                <div class="partition-attempt-head">
                  <strong>{{ attempt.task_id }}</strong>
                  <el-tag :type="partitionAttemptStatusType(attempt.status)" size="small">{{ partitionAttemptStatusText(attempt.status) }}</el-tag>
                </div>
                <div class="partition-attempt-meta">
                  <span>{{ partitionOperationText(attempt.operation) }}</span>
                  <span>第 {{ attempt.attempt_no }} 次</span>
                  <span>创建 {{ formatPartitionTimestamp(attempt.created_at) }}</span>
                  <span v-if="attempt.started_at">开始 {{ formatPartitionTimestamp(attempt.started_at) }}</span>
                  <span v-if="attempt.finished_at">结束 {{ formatPartitionTimestamp(attempt.finished_at) }}</span>
                  <span v-if="attempt.requested_by">提交者 {{ attempt.requested_by }}</span>
                </div>
                <div v-if="attempt.error_message" class="partition-attempt-error">{{ attempt.error_message }}</div>
              </div>
            </div>
            <div v-if="!partitionBatchDetailAttempts.length" class="empty-state compact">
              <p>尚无尝试记录</p>
            </div>
          </div>
        </div>
      </div>
    </el-drawer>
  </section>
</template>
