<script setup>
import { computed, defineAsyncComponent, onMounted, onUnmounted, ref, watch } from 'vue';
import { ElMessage, ElMessageBox } from 'element-plus';
import { CircleCloseFilled, Document, EditPen, FolderChecked, Refresh, Search, VideoPlay } from '@element-plus/icons-vue';

import { apiPrefixes, authHeaders, requestGet, requestJson } from '@/api/client';
import ExpandableText from '@/components/ExpandableText.vue';

const GlobeMap = defineAsyncComponent(() => import('@/components/GlobeMap.vue'));

function initialModule() {
  return 'optical';
}

const activeModule = ref(initialModule());
const dataDrawerVisible = ref(false);
const dataSearch = ref('');
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
const defaultEntityGridLevel = 6;
const gridTypeLabels = {
  geohash: 'GeoHash格网',
  mgrs: 'MGRS格网',
  isea4h: '六边形格网',
};
const partitionMethodLabels = {
  logical: '逻辑剖分',
  entity: '实体剖分',
};
const partitionGridTypes = ['geohash', 'mgrs', 'isea4h'];
const partitionMethods = ['logical', 'entity'];
const partitionArchiveableStatuses = ['failed', 'manual_required', 'cancelled'];
const partitionActiveStatuses = ['queued', 'running', 'retrying', 'cancel_requested'];
const partitionTaskPollIntervalMs = 1500;
const partitionTaskMaxPolls = 1200;
const opticalGridType = ref('geohash');
const opticalGridLevel = ref(defaultLogicalGridLevel);
const entityGridLevel = ref(defaultEntityGridLevel);
const opticalPartitionMethod = ref('logical');
const radarGridType = ref('geohash');
const radarGridLevel = ref(5);
const radarEntityGridLevel = ref(defaultEntityGridLevel);
const radarPartitionMethod = ref('logical');
const productGridType = ref('geohash');
const productGridLevel = ref(5);
const productEntityGridLevel = ref(defaultEntityGridLevel);
const productPartitionMethod = ref('logical');
const defaultOpticalSchemaFields = [
  { field: 'source_uri', type: 'string', meaning: '光学栅格源文件路径或 MinIO 对象 URL' },
  { field: 'scene_id', type: 'string', meaning: '光学场景标识' },
  { field: 'sensor', type: 'string', meaning: '光学传感器' },
  { field: 'product_family', type: 'string', meaning: '光学产品族' },
  { field: 'bands / band', type: 'string[] / string', meaning: '波段' },
  { field: 'acq_time', type: 'datetime', meaning: '采集时间' },
  { field: 'resolution', type: 'float', meaning: '空间分辨率（米）' },
  { field: 'bbox', type: 'float[4]', meaning: '覆盖范围 bbox（WGS84）' },
  { field: 'corners', type: 'float[4][2]', meaning: '覆盖范围四角点（WGS84 lon/lat）' },
];
const defaultCarbonSchemaFields = [
  { field: 'source_uri', type: 'string', meaning: '碳卫星源文件路径或 MinIO 对象 URL' },
  { field: 'observation_id / sounding_id', type: 'string', meaning: '碳卫星观测唯一标识' },
  { field: 'product_type', type: 'string', meaning: '碳卫星产品类型' },
  { field: 'acq_time / time', type: 'datetime', meaning: '观测时间' },
  { field: 'lon / lat', type: 'float', meaning: '观测中心点（WGS84 lon/lat）' },
  { field: 'xco2', type: 'float', meaning: '柱平均 CO2 浓度' },
  { field: 'quality_flag / xco2_quality_flag', type: 'int', meaning: '质量标记' },
  { field: 'footprint / vertices', type: 'float[4][2]', meaning: '观测足迹四角点（WGS84 lon/lat）' },
];
const defaultRadarSchemaFields = [
  { field: 'source_uri', type: 'string', meaning: '雷达栅格源文件路径或 MinIO 对象 URL' },
  { field: 'scene_id', type: 'string', meaning: 'Sentinel-1 场景标识' },
  { field: 'sensor', type: 'string', meaning: '雷达传感器' },
  { field: 'product_family', type: 'string', meaning: '雷达产品族' },
  { field: 'band / polarization', type: 'string', meaning: '极化方式' },
  { field: 'acq_time', type: 'datetime', meaning: '采集时间' },
  { field: 'resolution', type: 'float', meaning: '空间分辨率（米）' },
  { field: 'bbox', type: 'float[4]', meaning: '覆盖范围 bbox（WGS84）' },
  { field: 'corners', type: 'float[4][2]', meaning: '覆盖范围四角点（WGS84 lon/lat）' },
];
const defaultProductSchemaFields = [
  { field: 'source_uri', type: 'string', meaning: '信息产品栅格源文件路径或 MinIO 对象 URL' },
  { field: 'product_name', type: 'string', meaning: '信息产品名称' },
  { field: 'product_year', type: 'int', meaning: '产品年份' },
  { field: 'scene_id', type: 'string', meaning: '产品场景标识' },
  { field: 'product_family', type: 'string', meaning: '产品族' },
  { field: 'sensor', type: 'string', meaning: '数据来源/产品传感器' },
  { field: 'band', type: 'string', meaning: '产品值波段' },
  { field: 'acq_time', type: 'datetime', meaning: '产品时间' },
  { field: 'resolution', type: 'float', meaning: '空间分辨率（米）' },
  { field: 'target_crs', type: 'string', meaning: '标准化目标参考系统' },
  { field: 'bbox', type: 'float[4]', meaning: '产品覆盖范围 bbox（WGS84）' },
  { field: 'corners', type: 'float[4][2]', meaning: '产品覆盖范围四角点（WGS84 lon/lat）' },
];
const gridLevelManualOverrides = ref({
  optical: false,
  opticalEntity: false,
  radar: false,
  radarEntity: false,
  product: false,
  productEntity: false,
});
const mapGridLoading = ref(false);
const mapGridGeometries = ref([]);
const resultLoading = ref(false);
const resultRows = ref([]);
const lastPartitionResult = ref(null);
const lastPartitionRequest = ref(null);
const partitionStartedAt = ref(null);
const partitionFinishedAt = ref(null);
const partitionElapsedSec = ref(0);
let partitionTimer = null;
let partitionTaskSyncTimer = null;
let partitionTaskSyncInFlight = false;
const partitionStages = ref([
  { key: 'prepare', label: '准备任务', detail: '等待选择数据批次与剖分参数。', status: 'pending' },
  { key: 'queue', label: '读取数据队列', detail: '解析已载入资产、批次、波段与时间信息。', status: 'pending' },
  { key: 'partition', label: '执行剖分', detail: '生成 COG、按格网覆盖切分窗口并输出索引行。', status: 'pending' },
  { key: 'persist', label: '质检入库', detail: '执行自动质检并保存质检报告，同步当前批次的入库状态。', status: 'pending' },
]);
const partitionStageDetailVisible = ref(false);
const selectedPartitionStageKey = ref('');
const selectedPartitionStage = computed(() => (
  partitionStages.value.find((stage) => stage.key === selectedPartitionStageKey.value) || null
));
const partitionContextDetailVisible = ref(false);
const selectedPartitionContextLabel = ref('');
function targetCrsForGrid(gridType, fallback) {
  return fallback || 'EPSG:4326';
}

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

function partitionMethodText(partitionMethod) {
  return partitionMethodLabels[partitionMethod] || partitionMethod || '-';
}

function defaultGridLevelForGridTypeAndMethod(gridType, partitionMethod) {
  if (partitionMethod === 'entity') return defaultEntityGridLevel;
  return defaultLogicalGridLevel;
}

function formatGridType(gridType) {
  return gridTypeLabels[gridType] || gridType || '-';
}

function defaultGridLevelForResolution(
  resolution,
  gridType,
  partitionMethod,
  fallback = defaultGridLevelForGridTypeAndMethod(gridType, partitionMethod),
) {
  if (partitionMethod === 'entity') return defaultEntityGridLevel;
  if (!Number.isFinite(resolution) || resolution <= 0) return fallback;
  if (resolution < 10) return 8;
  if (resolution <= 30) return 7;
  return 6;
}

function defaultGridLevelFromAssets(
  assets,
  gridType,
  partitionMethod,
  fallback = defaultGridLevelForGridTypeAndMethod(gridType, partitionMethod),
) {
  const resolutions = (Array.isArray(assets) ? assets : [])
    .map(assetResolution)
    .filter((resolution) => resolution !== null);
  if (!resolutions.length) return fallback;
  return defaultGridLevelForResolution(Math.min(...resolutions), gridType, partitionMethod, fallback);
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
let partitionBatchDetailRequestToken = 0;
const partitionTasks = ref([]);
const partitionTasksLoading = ref(false);
const partitionTaskPage = ref(1);
const partitionTaskPageSize = ref(20);
const partitionTaskTotal = ref(0);
const activePartitionTasks = ref([]);
const activePartitionTaskPage = ref(1);
const activePartitionTaskPageSize = ref(20);
const activePartitionTaskTotal = ref(0);
const partitionTaskDrawerVisible = ref(false);

const visibleOpticalBatches = computed(() => managedOpticalBatches.value);
const visibleCarbonBatches = computed(() => managedCarbonBatches.value);
const visibleRadarBatches = computed(() => managedRadarBatches.value);
const visibleProductBatches = computed(() => managedProductBatches.value);

function partitionTaskStats(tasks) {
  const countByStatus = (statuses, options = {}) => tasks.filter((task) => {
    if (options.excludeArchivedBatch && task.batch_status === 'archived') return false;
    return statuses.includes(partitionTaskDisplayStatus(task));
  }).length;
  return [
    { label: '排队中', value: countByStatus(['queued']), status: 'queued' },
    { label: '运行中', value: countByStatus(['running', 'retrying']), status: 'running' },
    { label: '需处理', value: countByStatus(['failed', 'manual_required', 'cancel_requested'], { excludeArchivedBatch: true }), status: 'manual_required' },
    { label: '已完成', value: countByStatus(['succeeded', 'completed', 'archived']), status: 'succeeded' },
  ];
}

const partitionTaskQueueStats = computed(() => partitionTaskStats(partitionTasks.value));
const activePartitionTaskQueueStats = computed(() => partitionTaskStats(activePartitionTasks.value));
const activePartitionTaskDrawerTitle = computed(() => `${dataLabelsByModule[activeModule.value] || '当前模块'}剖分任务队列`);

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

function pruneBatchSelection(selectedIds, batches) {
  const availableIds = new Set(batches.map((batch) => batch.id));
  return selectedIds.filter((batchId) => availableIds.has(batchId));
}

function pruneExpandedBatchId(expandedBatchId, batches) {
  if (!expandedBatchId) return '';
  return batches.some((batch) => batch.id === expandedBatchId) ? expandedBatchId : '';
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
    archived: '已归档',
    succeeded: '已完成',
    completed: '已完成',
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
    archived: 'info',
    succeeded: 'success',
    completed: 'success',
  };
  return map[status] || 'info';
}

function partitionSupportsIngestStatus(dataType) {
  return ['optical', 'entity', 'radar', 'product'].includes(dataType);
}

function initialPartitionIngestStatus(dataType) {
  return partitionSupportsIngestStatus(dataType) ? 'not_ready' : 'not_supported';
}

function partitionIngestStatus(result = lastPartitionResult.value) {
  if (!result) return 'not_ready';
  const dataType = result.data_type || activeModule.value;
  if (!partitionSupportsIngestStatus(dataType)) {
    return ['completed', 'succeeded'].includes(result.status) ? 'not_supported' : 'not_ready';
  }
  if (result.ingest_status) return result.ingest_status;
  const qualityStatus = result.quality_status || '';
  if (['completed', 'succeeded'].includes(result.status) && !['FAIL', 'WARN'].includes(String(qualityStatus).toUpperCase())) {
    return result.ingest_enabled === false ? 'ready' : 'ingested';
  }
  return 'not_ready';
}

function partitionIngestStatusText(status) {
  const map = {
    not_supported: '剖分完成，暂不支持入库',
    not_ready: '未就绪',
    ready: '待补入库',
    previewed: '已预入库校验',
    ingested: '已入库',
    failed: '入库失败',
  };
  return map[status] || status || '未就绪';
}

function partitionPersistDoneText(result, fallback) {
  const prefix = fallback;
  const ingestStatus = partitionIngestStatus(result);
  if (ingestStatus === 'ingested') {
    return `${prefix}自动入库已完成。`;
  }
  if (ingestStatus === 'ready') {
    return `${prefix}当前批次未自动入库。`;
  }
  return prefix;
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
    manual_run: '手动执行',
    demo: '批次执行',
    retry: '重试',
    test: '测试',
    run: '执行',
  };
  return map[operation] || operation || '-';
}

function isExpandableErrorLabel(label) {
  return typeof label === 'string' && /错误|失败/.test(label);
}

function attemptPayload(attempt) {
  return attempt?.payload && typeof attempt.payload === 'object' ? attempt.payload : {};
}

function attemptPartitionMethod(attempt) {
  const payload = attemptPayload(attempt);
  if (payload.partition_method) return payload.partition_method;
  if (payload.grid_type === 'isea4h') return 'entity';
  if (payload.grid_type) return 'logical';
  return '';
}

function attemptPartitionMethodLabel(attempt) {
  const method = attemptPartitionMethod(attempt);
  return method ? partitionMethodText(method) : '-';
}

function attemptGridLabel(attempt) {
  const payload = attemptPayload(attempt);
  if (!payload.grid_type) return '-';
  const gridText = formatGridType(payload.grid_type);
  return payload.grid_level === undefined || payload.grid_level === null
    ? gridText
    : `${gridText} / ${payload.grid_level} 级`;
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
  return partitionActiveStatuses.includes(batch?.status);
}

function partitionBatchCanArchive(batch) {
  return partitionArchiveableStatuses.includes(batch?.status);
}

function partitionBatchCanRun(batch) {
  return Boolean(batch) && batch.status !== 'archived';
}

function partitionBatchCanRetrySelectedAssets(batch) {
  return Boolean(batch) && ['failed', 'manual_required'].includes(batch.status) && partitionBatchSelectedRetryableAssets.value.length > 0;
}

function partitionBatchExecutionOperation(batch) {
  return ['failed', 'manual_required'].includes(batch?.status) ? 'retry' : 'run';
}

function partitionBatchConfigOverride(batch) {
  const payload = batch?.normalized_payload || {};
  if (batch?.data_type === 'optical') {
    return {
      partition_method: payload.partition_method || partitionMethodForModule('optical'),
      grid_type: payload.grid_type || gridTypeForModule('optical'),
      grid_level: Number(payload.grid_level || gridLevelForModule('optical')),
      grid_level_mode: payload.grid_level_mode || gridLevelModeForModule('optical'),
    };
  }
  if (batch?.data_type === 'radar') {
    return {
      partition_method: payload.partition_method || partitionMethodForModule('radar'),
      grid_type: payload.grid_type || gridTypeForModule('radar'),
      grid_level: Number(payload.grid_level || gridLevelForModule('radar')),
      grid_level_mode: payload.grid_level_mode || gridLevelModeForModule('radar'),
    };
  }
  if (batch?.data_type === 'product') {
    const gridType = payload.grid_type || gridTypeForModule('product');
    return {
      partition_method: payload.partition_method || partitionMethodForModule('product'),
      grid_type: gridType,
      grid_level: Number(payload.grid_level || gridLevelForModule('product')),
      grid_level_mode: payload.grid_level_mode || gridLevelModeForModule('product'),
      target_crs: targetCrsForGrid(gridType, payload.target_crs),
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
  if (batch?.status === 'archived') return '已归档';
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
  if (batch?.status === 'archived') return FolderChecked;
  return VideoPlay;
}

function partitionBatchArchiveActionKey(batch) {
  const batchId = batch?.id || batch?.batch_id;
  return batchId ? `archive:${batchId}` : 'archive';
}

function partitionBatchSummary(batch) {
  const attemptCount = Number(batch.attempt_count || 0);
  const lastError = batch.last_error ? `最近错误：${batch.last_error}` : '暂无错误信息';
  const ingestSummary = partitionBatchNeedsIngestAttention(batch) ? ` · ${partitionIngestStatusText(batch.ingest_status)}` : '';
  return `${partitionStatusText(batch.status)}${ingestSummary} · 尝试 ${attemptCount} 次 · ${lastError}`;
}

function partitionSlots(batch) {
  return Array.isArray(batch?.partition_slots) ? batch.partition_slots : [];
}

function partitionSlotStatusText(status) {
  if (status === 'available') return '可剖分';
  if (status === 'completed') return '已完成';
  return partitionStatusText(status);
}

function partitionSlotStatusType(status) {
  const map = {
    available: 'primary',
    queued: 'warning',
    running: 'warning',
    retrying: 'warning',
    cancel_requested: 'warning',
    failed: 'danger',
    manual_required: 'danger',
    cancelled: 'info',
    completed: 'info',
  };
  return map[status] || 'info';
}

function partitionBatchAllSlotsCompleted(batch) {
  const slots = partitionSlots(batch);
  return slots.length === 3 && slots.every((slot) => slot.status === 'completed');
}

function partitionSlotGroups(batch) {
  if (!batch || batch.data_type === 'carbon') return [];
  return partitionSlots(batch).map((slot) => ({
    grid_type: slot.grid_type,
    grid_label: slot.grid_label || formatGridType(slot.grid_type),
    slots: [slot],
  }));
}

function partitionSlotSummary(slot) {
  const parts = [partitionSlotStatusText(slot.status)];
  if (slot.latest_task_id) parts.push(slot.latest_task_id);
  if (slot.finished_at) parts.push(formatPartitionTimestamp(slot.finished_at));
  return parts.join(' · ');
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
      { label: '剖分方式', value: partitionMethodText(payload.partition_method) },
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
      { label: '剖分方式', value: partitionMethodText(payload.partition_method) },
      { label: '格网类型', value: formatGridType(payload.grid_type) },
      { label: '格网层级', value: payload.grid_level ?? '-' },
      { label: '目标参考系统', value: payload.target_crs || '-' },
      { label: '选择年份', value: Array.isArray(payload.selected_assets) ? payload.selected_assets.length : 0 },
    );
  }
  if (batch.data_type === 'radar') {
    rows.splice(2, 0,
      { label: '剖分方式', value: partitionMethodText(payload.partition_method) },
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
  return (batch.assets || []).map((asset) => {
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
  syncModuleControlsFromBatch(batch);
}

function syncModuleControlsFromBatch(batch) {
  const dataType = batch?.data_type;
  const payload = batch?.normalized_payload || {};
  if (!['optical', 'radar', 'product'].includes(dataType)) return;
  const gridType = payload.grid_type;
  const partitionMethod = payload.partition_method;
  const gridLevel = Number(payload.grid_level);
  const gridLevelMode = payload.grid_level_mode;
  if (gridType) setGridTypeForModule(dataType, gridType);
  if (partitionMethod) setPartitionMethodForModule(dataType, partitionMethod);
  if (Number.isFinite(gridLevel) && gridLevel > 0) setGridLevelForModule(dataType, gridLevel);
  if (gridLevelMode === 'manual' || gridLevelMode === 'auto') {
    setGridLevelManual(dataType, gridLevelMode === 'manual');
  }
}

function partitionTaskDisplayStatus(task) {
  return task?.status;
}

function partitionTaskTitle(task) {
  return task.batch_name || task.batch_id || task.task_id || '-';
}

function partitionTaskTimeText(task) {
  if (task.finished_at) return `结束 ${formatPartitionTimestamp(task.finished_at)}`;
  if (task.started_at) return `开始 ${formatPartitionTimestamp(task.started_at)}`;
  return `创建 ${formatPartitionTimestamp(task.created_at)}`;
}

function partitionTaskResultText(task) {
  if (task.error_message) return task.error_message;
  const summary = task.result_summary || {};
  const rows = summary.rows ?? '-';
  const quality = summary.quality_status ? ` · 质检 ${summary.quality_status}` : '';
  return `索引行 ${rows}${quality}`;
}

function canOpenPartitionTaskBatch(task) {
  return Boolean(task?.batch_id);
}

function partitionTaskBatchProxy(task) {
  const batchStatus = task?.batch_status;
  const taskStatus = task?.status;
  const status = batchStatus === 'archived'
    ? batchStatus
    : partitionActiveStatuses.includes(batchStatus)
      ? batchStatus
    : partitionArchiveableStatuses.includes(batchStatus)
      ? batchStatus
      : partitionArchiveableStatuses.includes(taskStatus)
        ? taskStatus
        : batchStatus || taskStatus;
  return {
    id: task?.batch_id,
    batch_id: task?.batch_id,
    batch_name: task?.batch_name,
    data_type: task?.data_type,
    status,
  };
}

function partitionTaskCanArchiveBatch(task) {
  return canOpenPartitionTaskBatch(task) && partitionBatchCanArchive(partitionTaskBatchProxy(task));
}

function partitionTaskCanRequeueBatch(task) {
  return canOpenPartitionTaskBatch(task) && ['failed', 'manual_required', 'cancelled'].includes(partitionTaskBatchProxy(task).status);
}

async function archivePartitionTaskBatch(task) {
  await archivePartitionBatch(partitionTaskBatchProxy(task));
}

async function requeuePartitionTaskBatch(task) {
  const batch = partitionTaskBatchProxy(task);
  const batchId = batch?.id || batch?.batch_id;
  if (!batchId || !partitionTaskCanRequeueBatch(task)) return;
  const actionKey = `requeue:${batchId}`;
  partitionBatchDetailAction.value = actionKey;
  try {
    const { partitionPrefix } = apiPrefixes();
    const requeuedBatch = await requestJson(`${partitionPrefix}/batches/${batchId}/requeue`, {});
    applyRequeuedPartitionBatch(batchId, requeuedBatch);
    ElMessage.success('已打回已载入数据队列');
    await Promise.all([
      loadPartitionBatches(),
      loadPartitionTasks(partitionTaskPage.value),
      partitionTaskDrawerVisible.value && partitionModules.has(activeModule.value)
        ? loadActivePartitionTasks(activePartitionTaskPage.value)
        : Promise.resolve(),
    ]);
  } catch (error) {
    ElMessage.error(error.message);
  } finally {
    if (partitionBatchDetailAction.value === actionKey) {
      partitionBatchDetailAction.value = '';
    }
  }
}

function applyRequeuedPartitionBatch(batchId, requeuedBatch = null) {
  const patch = {
    ...(requeuedBatch || {}),
    id: batchId,
    batch_id: batchId,
    status: 'pending',
    last_error: null,
    manual_required_at: null,
  };
  const patchTaskRows = (items) => items.map((item) => (
    item.batch_id === batchId
      ? { ...item, batch_status: 'pending' }
      : item
  ));
  partitionTasks.value = patchTaskRows(partitionTasks.value);
  activePartitionTasks.value = patchTaskRows(activePartitionTasks.value);
  if (partitionBatchDetail.value?.batch_id === batchId || partitionBatchDetail.value?.id === batchId) {
    partitionBatchDetail.value = { ...partitionBatchDetail.value, ...patch };
  }
  if (lastPartitionResult.value?.batch_id === batchId) {
    lastPartitionResult.value = { ...lastPartitionResult.value, batch_status: 'pending' };
    resultRows.value = formatRows(lastPartitionResult.value);
  }
}

function applyArchivedPartitionBatch(batchId, archivedBatch = null) {
  const detailBatchId = partitionBatchDetail.value?.id || partitionBatchDetail.value?.batch_id;
  if (detailBatchId === batchId) {
    partitionBatchDetail.value = {
      ...partitionBatchDetail.value,
      ...(archivedBatch || {}),
      id: batchId,
      batch_id: batchId,
      status: 'archived',
    };
  }
  const result = lastPartitionResult.value;
  if (result?.batch_id === batchId) {
    lastPartitionResult.value = {
      ...result,
      batch_status: 'archived',
    };
    resultRows.value = formatRows(lastPartitionResult.value);
  }
}

const partitionResultArchiveBatch = computed(() => {
  const result = lastPartitionResult.value;
  if (!result || resultLoading.value || !['failed', 'manual_required'].includes(result.status)) return null;
  if (result.batch_status === 'archived' || partitionActiveStatuses.includes(result.batch_status)) return null;
  const request = lastPartitionRequest.value || {};
  const payload = request.payload || {};
  const batchId = result.batch_id || request.batchId || payload.batch_id;
  if (!batchId) return null;
  return {
    id: batchId,
    batch_id: batchId,
    batch_name: result.batch_name || payload.batch_name || selectedDataName.value,
    data_type: result.data_type || activeModule.value,
    status: result.batch_status || result.status,
  };
});

async function archiveLastPartitionResultBatch() {
  if (!partitionResultArchiveBatch.value) return;
  await archivePartitionBatch(partitionResultArchiveBatch.value);
}

async function openPartitionTaskBatch(task) {
  if (!canOpenPartitionTaskBatch(task)) return;
  await openPartitionBatchDetail({
    id: task.batch_id,
    batch_id: task.batch_id,
    batch_name: task.batch_name,
    data_type: task.data_type,
    status: task.batch_status || task.status,
  });
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

function partitionTaskQuery(params) {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') query.set(key, String(value));
  });
  return query.toString();
}

function applyPartitionTaskResponse(response, target) {
  target.tasks.value = response.tasks || [];
  target.total.value = Number(response.total ?? target.tasks.value.length);
  target.page.value = Number(response.page || target.page.value);
  target.pageSize.value = Number(response.page_size || target.pageSize.value);
}

async function loadPartitionTasks(page = partitionTaskPage.value) {
  partitionTasksLoading.value = true;
  try {
    const { partitionPrefix } = apiPrefixes();
    const query = partitionTaskQuery({
      page,
      page_size: partitionTaskPageSize.value,
    });
    const response = await requestGet(`${partitionPrefix}/tasks?${query}`);
    applyPartitionTaskResponse(response, {
      tasks: partitionTasks,
      total: partitionTaskTotal,
      page: partitionTaskPage,
      pageSize: partitionTaskPageSize,
    });
  } catch (error) {
    ElMessage.error(`剖分任务队列加载失败：${error.message}`);
  } finally {
    partitionTasksLoading.value = false;
  }
}

async function loadActivePartitionTasks(page = activePartitionTaskPage.value) {
  if (!partitionModules.has(activeModule.value)) {
    activePartitionTasks.value = [];
    activePartitionTaskTotal.value = 0;
    return;
  }
  partitionTasksLoading.value = true;
  try {
    const { partitionPrefix } = apiPrefixes();
    const query = partitionTaskQuery({
      data_type: activeModule.value,
      page,
      page_size: activePartitionTaskPageSize.value,
    });
    const response = await requestGet(`${partitionPrefix}/tasks?${query}`);
    applyPartitionTaskResponse(response, {
      tasks: activePartitionTasks,
      total: activePartitionTaskTotal,
      page: activePartitionTaskPage,
      pageSize: activePartitionTaskPageSize,
    });
  } catch (error) {
    ElMessage.error(`剖分任务队列加载失败：${error.message}`);
  } finally {
    partitionTasksLoading.value = false;
  }
}

function upsertPartitionTaskRow(row) {
  if (!row?.task_id) return;
  const cleanRow = Object.fromEntries(Object.entries(row).filter(([, value]) => value !== undefined));
  const replaceIn = (items) => {
    const index = items.findIndex((item) => item.task_id === cleanRow.task_id);
    if (index < 0) return [cleanRow, ...items];
    const next = [...items];
    next[index] = { ...next[index], ...cleanRow };
    return next;
  };
  partitionTasks.value = replaceIn(partitionTasks.value).slice(0, partitionTaskPageSize.value);
  if (!cleanRow.data_type || cleanRow.data_type === activeModule.value) {
    activePartitionTasks.value = replaceIn(activePartitionTasks.value).slice(0, activePartitionTaskPageSize.value);
  }
}

async function fetchPartitionTaskRow(taskId) {
  const { partitionPrefix } = apiPrefixes();
  const task = await requestGet(`${partitionPrefix}/tasks/${taskId}`);
  const result = task.result || {};
  return {
    task_id: task.task_id || taskId,
    status: task.status,
    data_type: task.data_type || result.data_type,
    operation: task.operation,
    batch_id: result.batch_id,
    batch_name: result.batch_name,
    batch_status: result.batch_status,
    quality_status: result.quality_status,
    quality_failure_reason: result.quality_failure_reason,
    ingest_status: result.ingest_status,
    ingest_job_id: result.ingest_job_id,
    ingest_error: result.ingest_error,
    ingested_at: result.ingested_at,
    error_message: task.error,
    result,
    result_summary: {
      rows: result.rows ?? result.total_index_rows ?? result.metadata_rows,
      quality_status: result.quality_status,
      run_dir: result.run_dir,
      rows_path: result.rows_path || result.output_path,
      execution_engine: result.execution_engine || result.partition_backend,
    },
  };
}

function applySyncedPartitionTaskRow(row) {
  const result = lastPartitionResult.value;
  if (!row?.task_id || !result || result.partition_task_id !== row.task_id) return;
  upsertPartitionTaskRow(row);
  const batchStatus = row.batch_status || result.batch_status;
  const batchId = row.batch_id || result.batch_id;
  const rowStatus = row.status || result.status;
  const isArchiveableFailure = !partitionActiveStatuses.includes(batchStatus)
    && (partitionArchiveableStatuses.includes(batchStatus) || rowStatus === 'failed');
  if (isArchiveableFailure) {
    const error = row.error_message || result.error || partitionTaskResultText(row);
    lastPartitionResult.value = {
      ...result,
      status: 'failed',
      batch_id: batchId,
      batch_status: batchStatus,
      error,
    };
    resultRows.value = formatRows(lastPartitionResult.value);
    partitionStages.value = partitionStages.value.map((item) => (
      item.status === 'running' || item.status === 'pending' ? { ...item, status: 'failed' } : item
    ));
    setPartitionStage('persist', 'failed', `执行失败：${error}`);
    stopPartitionTaskSync();
    loadPartitionBatches();
    return;
  }
  if (rowStatus === 'completed' || rowStatus === 'succeeded') {
    const completedResult = row.result || row.result_summary || {};
    lastPartitionResult.value = {
      ...(result || {}),
      ...completedResult,
      status: 'completed',
      batch_id: batchId,
      batch_status: batchStatus,
      ingest_status: completedResult.ingest_status || row.ingest_status || result.ingest_status,
      ingest_job_id: completedResult.ingest_job_id || row.ingest_job_id || result.ingest_job_id,
      ingested_at: completedResult.ingested_at || row.ingested_at || result.ingested_at,
      ingest_error: completedResult.ingest_error || row.ingest_error || result.ingest_error,
    };
    resultRows.value = formatRows(lastPartitionResult.value);
    setPartitionStage('persist', 'done', partitionPersistDoneText(lastPartitionResult.value, '执行结果已返回。'));
    stopPartitionTaskSync();
    loadPartitionBatches();
    return;
  }
  if (rowStatus === 'cancel_requested' || rowStatus === 'cancelled') {
    lastPartitionResult.value = {
      ...result,
      status: rowStatus,
      batch_id: batchId,
      batch_status: batchStatus,
      error: row.error_message || (rowStatus === 'cancel_requested' ? '剖分任务正在取消' : '剖分任务已取消'),
    };
    resultRows.value = formatRows(lastPartitionResult.value);
    partitionStages.value = partitionStages.value.map((item) => (
      item.status === 'running' || item.status === 'pending' ? { ...item, status: 'cancelled' } : item
    ));
    setPartitionStage('partition', 'cancelled', lastPartitionResult.value.error || '剖分任务已取消');
    setPartitionStage(
      'persist',
      'cancelled',
      rowStatus === 'cancel_requested' ? '取消请求已提交，等待后台停止任务。' : '任务取消后不整理执行结果。',
    );
    if (rowStatus === 'cancelled') {
      stopPartitionTaskSync();
    }
    loadPartitionBatches();
  }
}

async function syncSubmittedPartitionTask(taskId) {
  if (partitionTaskSyncInFlight) return;
  const result = lastPartitionResult.value;
  if (!result || result.partition_task_id !== taskId) {
    stopPartitionTaskSync();
    return;
  }
  partitionTaskSyncInFlight = true;
  try {
    const row = await fetchPartitionTaskRow(taskId);
    if (row) {
      applySyncedPartitionTaskRow(row);
    }
  } catch {
    // Keep the next interval alive; the task list can be temporarily unavailable.
  } finally {
    partitionTaskSyncInFlight = false;
  }
}

function startPartitionTaskSync(taskId) {
  stopPartitionTaskSync();
  if (!taskId) return;
  syncSubmittedPartitionTask(taskId);
  partitionTaskSyncTimer = window.setInterval(() => {
    syncSubmittedPartitionTask(taskId);
  }, partitionTaskPollIntervalMs);
}

function stopPartitionTaskSync() {
  if (partitionTaskSyncTimer) {
    window.clearInterval(partitionTaskSyncTimer);
    partitionTaskSyncTimer = null;
  }
  partitionTaskSyncInFlight = false;
}

function changePartitionTaskPageSize(size) {
  partitionTaskPageSize.value = size;
  loadPartitionTasks(1);
}

function changeActivePartitionTaskPageSize(size) {
  activePartitionTaskPageSize.value = size;
  loadActivePartitionTasks(1);
}

async function openActivePartitionTaskDrawer() {
  partitionTaskDrawerVisible.value = true;
  await loadActivePartitionTasks(1);
}

async function openPartitionBatchDetail(batch) {
  const resolved = typeof batch === 'string'
    ? [...visibleOpticalBatches.value, ...visibleCarbonBatches.value, ...visibleRadarBatches.value, ...visibleProductBatches.value].find((item) => (item.id || item.batch_id) === batch)
    : batch;
  const batchId = resolved?.id || resolved?.batch_id || batch;
  if (!batchId) return;
  dataDrawerVisible.value = false;
  partitionBatchDetail.value = resolved ? { ...resolved, id: batchId, batch_id: batchId } : { id: batchId, batch_id: batchId };
  partitionBatchDetailVisible.value = true;
  partitionBatchDetailLoading.value = true;
  partitionBatchDetailAction.value = '';
  partitionBatchDetailSearch.value = '';
  partitionBatchDetailAssetStatus.value = 'all';
  clearPartitionBatchDetailSelection();
  partitionBatchDetailTab.value = 'overview';
  const requestToken = ++partitionBatchDetailRequestToken;
  try {
    const detail = await loadPartitionBatchDetail(batchId);
    if (requestToken !== partitionBatchDetailRequestToken) return null;
    partitionBatchDetail.value = detail;
    selectManagedBatchByDetail(detail);
    return detail;
  } catch (error) {
    if (requestToken !== partitionBatchDetailRequestToken) return null;
    partitionBatchDetail.value = null;
    ElMessage.error(error.message);
    return null;
  } finally {
    if (requestToken === partitionBatchDetailRequestToken) {
      partitionBatchDetailLoading.value = false;
    }
  }
}

async function refreshPartitionBatchDetail() {
  const batchId = partitionBatchDetail.value?.id || partitionBatchDetail.value?.batch_id;
  if (!batchId) return;
  partitionBatchDetailLoading.value = true;
  const requestToken = ++partitionBatchDetailRequestToken;
  try {
    const detail = await loadPartitionBatchDetail(batchId);
    if (requestToken !== partitionBatchDetailRequestToken) return;
    partitionBatchDetail.value = detail;
  } catch (error) {
    if (requestToken !== partitionBatchDetailRequestToken) return;
    ElMessage.error(error.message);
  } finally {
    if (requestToken === partitionBatchDetailRequestToken) {
      partitionBatchDetailLoading.value = false;
    }
  }
}

async function runPartitionBatchFromDetail() {
  const batch = partitionBatchDetail.value;
  const batchId = batch?.id || batch?.batch_id;
  if (!batchId || !partitionBatchCanRun(batch)) return;
  stopPartitionTaskSync();
  const operation = partitionBatchExecutionOperation(batch);
  const configOverride = partitionBatchConfigOverride(batch);
  partitionBatchDetailAction.value = operation;
  resultLoading.value = true;
  resultRows.value = [];
  lastPartitionResult.value = null;
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
    await loadPartitionTasks();
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
    if (isPartitionCancelledResult(result)) {
      applyPartitionCancelledResult(result, operation === 'retry' ? '批次重试已取消' : '批次执行已取消');
    } else {
      setPartitionStage('partition', 'done', `已生成 ${result.rows ?? result.total_index_rows ?? 0} 条索引行。`);
      setPartitionStage('persist', 'done', partitionPersistDoneText(result, '执行结果已返回。'));
      lastPartitionResult.value = result;
      resultRows.value = formatRows(result);
      ElMessage.success(operation === 'retry' ? '批次重试完成' : '批次执行完成');
    }
    await loadPartitionBatches();
    await loadPartitionTasks();
    await refreshPartitionBatchDetail();
  } catch (error) {
    partitionStages.value = partitionStages.value.map((item) => (item.status === 'running' ? { ...item, status: 'failed' } : item));
    const failure = buildPartitionFailureResult(error, lastPartitionRequest.value || {});
    lastPartitionResult.value = failure;
    resultRows.value = formatRows(failure);
    setPartitionStage('persist', 'failed', `执行失败：${failure.error}`);
    await loadPartitionTasks();
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
  stopPartitionTaskSync();
  partitionBatchDetailAction.value = 'assetRetry';
  resultLoading.value = true;
  resultRows.value = [];
  lastPartitionResult.value = null;
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
    await loadPartitionTasks();
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
    if (isPartitionCancelledResult(result)) {
      applyPartitionCancelledResult(result, '失败资产重试已取消');
    } else {
      setPartitionStage('partition', 'done', `失败资产重试完成，生成 ${result.rows ?? result.total_index_rows ?? 0} 条索引行。`);
      setPartitionStage('persist', 'done', partitionPersistDoneText(result, '重试结果已返回。'));
      lastPartitionResult.value = result;
      resultRows.value = formatRows(result);
      clearPartitionBatchDetailSelection();
      ElMessage.success('失败资产重试完成');
    }
    await loadPartitionBatches();
    await loadPartitionTasks();
    await refreshPartitionBatchDetail();
  } catch (error) {
    partitionStages.value = partitionStages.value.map((item) => (item.status === 'running' ? { ...item, status: 'failed' } : item));
    const failure = buildPartitionFailureResult(error, lastPartitionRequest.value || {});
    lastPartitionResult.value = failure;
    resultRows.value = formatRows(failure);
    setPartitionStage('persist', 'failed', `重试失败：${failure.error}`);
    await loadPartitionTasks();
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
    await loadPartitionTasks();
    await refreshPartitionBatchDetail();
  } catch (error) {
    ElMessage.error(error.message);
  } finally {
    partitionBatchDetailAction.value = '';
  }
}

async function archivePartitionBatch(batch) {
  const batchId = batch?.id || batch?.batch_id;
  if (!batchId || !partitionBatchCanArchive(batch)) return;
  try {
    await ElMessageBox.confirm(
      '该批次会归档并从待处理队列移除，不会继续提交重试或执行；历史详情仍保留原始错误信息。',
      '不再处理批次',
      { confirmButtonText: '确认不再处理', cancelButtonText: '返回', type: 'warning' },
    );
  } catch {
    return;
  }
  const actionKey = partitionBatchArchiveActionKey(batch);
  partitionBatchDetailAction.value = actionKey;
  try {
    const { partitionPrefix } = apiPrefixes();
    const archivedBatch = await requestJson(`${partitionPrefix}/batches/${batchId}/archive`, {});
    applyArchivedPartitionBatch(batchId, archivedBatch);
    ElMessage.success('批次已归档，不再处理');
    await Promise.all([
      loadPartitionBatches(),
      loadPartitionTasks(partitionTaskPage.value),
      partitionTaskDrawerVisible.value && partitionModules.has(activeModule.value)
        ? loadActivePartitionTasks(activePartitionTaskPage.value)
        : Promise.resolve(),
    ]);
    const detailBatchId = partitionBatchDetail.value?.id || partitionBatchDetail.value?.batch_id;
    if (partitionBatchDetailVisible.value && detailBatchId === batchId) {
      await refreshPartitionBatchDetail();
    }
  } catch (error) {
    ElMessage.error(error.message);
  } finally {
    if (partitionBatchDetailAction.value === actionKey) {
      partitionBatchDetailAction.value = '';
    }
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

const partitionModules = new Set(['optical', 'carbon', 'radar', 'product']);

const activeDataRows = computed(() => visibleDataRowsByModule.value[activeModule.value] || []);

const activeDataLabel = computed(() => dataLabelsByModule[activeModule.value] || '已载入数据');

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

function normalizedBboxKey(corners) {
  if (!Array.isArray(corners) || corners.length !== 4) return '';
  const bbox = cornersToBbox(corners);
  if (!bbox.every((value) => Number.isFinite(value))) return '';
  return bbox.map((value) => value.toFixed(6)).join(',');
}

function normalizedCornersKey(corners) {
  if (!Array.isArray(corners) || corners.length !== 4) return '';
  const values = corners.flatMap((corner) => [Number(corner?.[0]), Number(corner?.[1])]);
  if (!values.every((value) => Number.isFinite(value))) return '';
  return values.map((value) => value.toFixed(6)).join(',');
}

function bboxText(bbox) {
  return Array.isArray(bbox) ? bbox.join(', ') : '-';
}

function cornersText(corners) {
  return Array.isArray(corners) ? corners.map((c) => `[${c?.[0]}, ${c?.[1]}]`).join(' ') : '-';
}

function joinUniqueLabels(values, limit = 4) {
  const labels = Array.from(new Set(values.map((value) => String(value || '').trim()).filter(Boolean)));
  if (labels.length <= limit) return labels.join(', ');
  return `${labels.slice(0, limit).join(', ')} 等 ${labels.length} 项`;
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

function uniqueFootprintAssets(assets, labelForAsset) {
  const groups = new Map();
  assets.forEach((asset) => {
    const key = normalizedCornersKey(asset.corners);
    if (!key) return;
    const group = groups.get(key);
    if (group) {
      group.assets.push(asset);
      group.labels.push(labelForAsset(asset));
      return;
    }
    groups.set(key, {
      key,
      corners: asset.corners,
      assets: [asset],
      labels: [labelForAsset(asset)],
    });
  });
  return Array.from(groups.values()).map((group) => ({
    ...group,
    label: joinUniqueLabels(group.labels),
    assetCount: group.assets.length,
  }));
}

function mapGeometryItemsFromFootprints(footprints, style) {
  return footprints
    .map((footprint) => {
      const geometry = cornersToPolygon(footprint.corners);
      if (!geometry) return null;
      return {
        geometry,
        label: footprint.assetCount > 1 ? `${footprint.label} / ${footprint.assetCount} 条资产` : footprint.label,
        ...style,
      };
    })
    .filter(Boolean);
}

const opticalMapFootprints = computed(() => uniqueFootprintAssets(
  selectedOpticalAssets.value,
  (asset) => `${asset.scene_id} / ${assetBandsText(asset)}`,
));

const productMapFootprints = computed(() => uniqueFootprintAssets(
  selectedProductAssets.value,
  (asset) => `${asset.product_year} / ${asset.product_name}`,
));

const radarMapFootprints = computed(() => uniqueFootprintAssets(
  selectedRadarAssets.value,
  (asset) => `${asset.scene_id} / ${(asset.polarization || asset.band || '').toUpperCase()}`,
));

const mapBatchGeometries = computed(() => mapGeometryItemsFromFootprints(opticalMapFootprints.value, {
  color: '#2f91ea',
  fillColor: '#2f91ea',
  fillOpacity: 0.12,
  weight: 2,
}));

const productMapGeometries = computed(() => mapGeometryItemsFromFootprints(productMapFootprints.value, {
  color: '#3f7f5f',
  fillColor: '#3f7f5f',
  fillOpacity: 0.12,
  weight: 2,
}));

const radarMapGeometries = computed(() => mapGeometryItemsFromFootprints(radarMapFootprints.value, {
  color: '#b06f2c',
  fillColor: '#b06f2c',
  fillOpacity: 0.12,
  weight: 2,
}));

function selectedMapFootprintsForModule() {
  if (activeModule.value === 'product') return productMapFootprints.value;
  if (activeModule.value === 'radar') return radarMapFootprints.value;
  return opticalMapFootprints.value;
}

function uniqueGridCoverFootprints(footprints) {
  const groups = new Map();
  footprints.forEach((footprint) => {
    const key = normalizedBboxKey(footprint.corners);
    if (key && !groups.has(key)) groups.set(key, footprint);
  });
  return Array.from(groups.values());
}

function uniqueGridGeometryItems(chunks, gridType, level) {
  const cellsByCode = new Map();
  chunks.flat().forEach((cell) => {
    if (!cell?.geometry || !cell.space_code) return;
    const key = `${gridType}:${level}:${cell.space_code}`;
    if (!cellsByCode.has(key)) cellsByCode.set(key, cell);
  });
  return Array.from(cellsByCode.values()).map((cell) => ({
    geometry: cell.geometry,
    label: cell.space_code,
    color: '#e67e22',
    fillColor: '#e67e22',
    fillOpacity: 0.06,
    weight: 1,
  }));
}

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

function gridLevelManualKeyFor(moduleName = activeModule.value) {
  if (moduleName === 'optical' && partitionMethodForModule('optical') === 'entity') return 'opticalEntity';
  if (moduleName === 'radar' && partitionMethodForModule('radar') === 'entity') return 'radarEntity';
  if (moduleName === 'product' && partitionMethodForModule('product') === 'entity') return 'productEntity';
  return moduleName;
}

function gridLevelForModule(moduleName = activeModule.value) {
  if (moduleName === 'product') return partitionMethodForModule('product') === 'entity' ? productEntityGridLevel.value : productGridLevel.value;
  if (moduleName === 'radar') return partitionMethodForModule('radar') === 'entity' ? radarEntityGridLevel.value : radarGridLevel.value;
  return partitionMethodForModule('optical') === 'entity' ? entityGridLevel.value : opticalGridLevel.value;
}

const selectedMapGridLevel = computed({
  get: () => gridLevelForModule(activeModule.value),
  set: (level) => {
    if (!['optical', 'radar', 'product'].includes(activeModule.value)) return;
    setGridLevelForModule(activeModule.value, level);
  },
});

function gridTypeForModule(moduleName = activeModule.value) {
  if (moduleName === 'product') return productGridType.value;
  if (moduleName === 'radar') return radarGridType.value;
  return opticalGridType.value;
}

function setGridTypeForModule(moduleName, gridType) {
  if (moduleName === 'product') {
    productGridType.value = gridType;
  } else if (moduleName === 'radar') {
    radarGridType.value = gridType;
  } else if (moduleName === 'optical') {
    opticalGridType.value = gridType;
  }
}

function partitionMethodForModule(moduleName = activeModule.value) {
  if (moduleName === 'product') return productPartitionMethod.value;
  if (moduleName === 'radar') return radarPartitionMethod.value;
  if (moduleName === 'optical') return opticalPartitionMethod.value;
  return 'logical';
}

function setPartitionMethodForModule(moduleName, partitionMethod) {
  if (moduleName === 'product') {
    productPartitionMethod.value = partitionMethod;
  } else if (moduleName === 'radar') {
    radarPartitionMethod.value = partitionMethod;
  } else if (moduleName === 'optical') {
    opticalPartitionMethod.value = partitionMethod;
  }
}

function syncPartitionMethodForGrid(moduleName) {
  setPartitionMethodForModule(moduleName, gridTypeForModule(moduleName) === 'isea4h' ? 'entity' : 'logical');
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

function gridLevelModeForModule(moduleName = activeModule.value) {
  return isGridLevelManual(moduleName) ? 'manual' : 'auto';
}

function setGridLevelForModule(moduleName, level) {
  if (moduleName === 'product' && partitionMethodForModule('product') === 'entity') {
    productEntityGridLevel.value = level;
  } else if (moduleName === 'product') {
    productGridLevel.value = level;
  } else if (moduleName === 'radar' && partitionMethodForModule('radar') === 'entity') {
    radarEntityGridLevel.value = level;
  } else if (moduleName === 'radar') {
    radarGridLevel.value = level;
  } else if (partitionMethodForModule('optical') === 'entity') {
    entityGridLevel.value = level;
  } else {
    opticalGridLevel.value = level;
  }
}

function defaultGridLevelForModule(moduleName = activeModule.value) {
  const gridType = gridTypeForModule(moduleName);
  const partitionMethod = partitionMethodForModule(moduleName);
  return defaultGridLevelFromAssets(
    selectedAssetsForModule(moduleName),
    gridType,
    partitionMethod,
    defaultGridLevelForGridTypeAndMethod(gridType, partitionMethod),
  );
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
    { label: '质检状态', value: result.quality_status || '-' },
    { label: '入库状态', value: partitionIngestStatusText(partitionIngestStatus(result)) },
  ];
  if (result.error) {
    rows.splice(1, 0, { label: '失败原因', value: result.error });
  }
  if (result.batch_status) {
    rows.splice(1, 0, { label: '批次状态', value: partitionStatusText(result.batch_status) });
  }
  if (result.partition_method || result.partition_type) {
    rows.splice(3, 0, { label: '剖分方式', value: partitionMethodText(result.partition_method || result.partition_type) });
  }
  if (result.grid_type) {
    rows.splice(4, 0, { label: '格网类型', value: formatGridType(result.grid_type) });
  }
  if (result.grid_level !== undefined && result.grid_level !== null) {
    rows.splice(5, 0, { label: '格网层级', value: result.grid_level });
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
  const request = lastPartitionRequest.value || {};
  const payload = request.payload || {};
  const result = lastPartitionResult.value || {};
  const operation = request.operation || 'run';
  const endpoint = request.endpoint || partitionEndpointsByModule[activeModule.value] || activeModule.value;
  const apiPath = request.apiPath || `/v1/partition/${endpoint}/${operation}`;
  const status = resultLoading.value ? '执行中' : result.status ? partitionStatusText(result.status) : lastPartitionResult.value ? '已完成' : '待执行';
  const rows = [
    { label: '运行状态', value: status },
    { label: '执行接口', value: apiPath },
    { label: '开始时间', value: partitionStartedAt.value ? formatQualityTime(partitionStartedAt.value) : '-' },
    { label: '已耗时', value: `${partitionElapsedSec.value.toFixed(1)} s` },
    { label: '数据批次', value: selectedDataName.value },
    { label: '输出目录', value: result.run_dir || '-' },
  ];
  if (partitionModules.has(activeModule.value)) {
    const gridType = activeModule.value === 'carbon' ? 'isea4h' : payload.grid_type || opticalGridType.value;
    const gridLevel = activeModule.value === 'carbon' ? 5 : payload.grid_level || gridLevelForModule(activeModule.value);
    const partitionMethod = activeModule.value === 'carbon'
      ? 'logical'
      : payload.partition_method || result.partition_method || result.partition_type || partitionMethodForModule(activeModule.value);
    const gridText = `${formatGridType(gridType)} / ${gridLevel} 级`;
    rows.splice(
      5,
      0,
      { label: '剖分方式', value: partitionMethodText(partitionMethod) },
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
      { label: '运行模式', value: '生产剖分运行' },
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
    { label: '剖分方式', value: partitionMethodText(result.partition_method || result.partition_type) },
    { label: '格网类型', value: formatGridType(result.grid_type) },
    { label: '格网层级', value: result.grid_level ?? '-' },
    { label: '执行引擎', value: result.execution_engine || result.partition_backend || '-' },
    { label: '后台任务 ID', value: result.partition_task_id || '-' },
    { label: '执行 ID', value: result.execution_id || result.run_task_id || result.demo_task_id || '-' },
    { label: 'Ray 任务 ID', value: result.ray_task_id || '-' },
    { label: '索引文件', value: result.rows_path || result.output_path || '-' },
    { label: 'COG 输出', value: result.cog_output_dir || result.cog_input_dir || '-' },
    { label: '瓦片存储', value: result.asset_storage_backend || '-' },
    { label: '元数据后端', value: result.metadata_backend || '-' },
    { label: '上传瓦片', value: result.uploaded_tile_count ?? '-' },
    { label: '元数据行数', value: result.metadata_rows ?? '-' },
  ];
});

const partitionWarnNeedsRetry = computed(() => {
  const status = lastPartitionResult.value?.quality_status;
  return status === 'WARN';
});

const partitionFailureMessage = computed(() => (
  ['failed', 'manual_required'].includes(lastPartitionResult.value?.status)
    ? lastPartitionResult.value.error || (lastPartitionResult.value?.status === 'manual_required' ? '需要人工确认后继续处理' : '剖分失败')
    : ''
));

async function openDataDrawer() {
  dataSearch.value = '';
  dataDrawerVisible.value = true;
  if (partitionModules.has(activeModule.value)) {
    await loadPartitionBatches();
  }
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

function isSelectedPartitionSlot(batch, slot) {
  return activeModule.value === batch?.data_type
    && gridTypeForModule(batch.data_type) === slot.grid_type
    && partitionMethodForModule(batch.data_type) === slot.partition_method;
}

function selectPartitionSlot(batch, slot) {
  if (!batch || !slot || slot.disabled) return;
  const batchId = batch.id || batch.batch_id;
  if (batchId) setBatchSelection(batchId, batch.data_type);
  setGridTypeForModule(batch.data_type, slot.grid_type);
  setPartitionMethodForModule(batch.data_type, slot.partition_method);
  applyDefaultGridLevel(batch.data_type);
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
  return `${selectedCount}/${batch.assets.length} 条资产已选 | schema ${schemaForBatch(batch).length} 字段`;
}

function carbonBatchSummary(batch) {
  const selectedCount = batch.observations.filter((observation) => isCarbonObservationSelected(batch.id, observation)).length;
  return `${selectedCount}/${batch.observations.length} 条观测已选 | schema ${schemaForBatch(batch).length} 字段`;
}

function radarBatchSummary(batch) {
  const selectedCount = batch.assets.filter((asset) => isRadarAssetSelected(batch.id, asset)).length;
  return `${selectedCount}/${batch.assets.length} 条资产已选 | schema ${schemaForBatch(batch).length} 字段`;
}

function productBatchSummary(batch) {
  const selectedCount = batch.assets.filter((asset) => isProductAssetSelected(batch.id, asset)).length;
  return `${selectedCount}/${batch.assets.length} 个年份已选 | schema ${schemaForBatch(batch).length} 字段`;
}

function fallbackSchemaForDataType(dataType) {
  if (dataType === 'carbon') return defaultCarbonSchemaFields;
  if (dataType === 'radar') return defaultRadarSchemaFields;
  if (dataType === 'product') return defaultProductSchemaFields;
  return defaultOpticalSchemaFields;
}

function schemaForBatch(batch) {
  return Array.isArray(batch?.schema) && batch.schema.length ? batch.schema : fallbackSchemaForDataType(batch?.data_type);
}

function schemaCollapseTitle(batch) {
  return `Schema 字段（${schemaForBatch(batch).length}）`;
}

function partitionSlotCollapseTitle(batch) {
  return `剖分槽位（${partitionSlots(batch).length || partitionGridTypes.length * partitionMethods.length}）`;
}

function schemaFromManagedBatch(batch) {
  if (Array.isArray(batch?.source_schema?.schema) && batch.source_schema.schema.length) return batch.source_schema.schema;
  if (Array.isArray(batch?.schema) && batch.schema.length) return batch.schema;
  return fallbackSchemaForDataType(batch?.data_type);
}

function normalizeManagedBatch(batch) {
  const payload = batch.normalized_payload || {};
  const base = {
    ...batch,
    id: batch.batch_id,
    name: batch.batch_name || batch.batch_id,
    status: batch.status || '待处理',
    schema: schemaFromManagedBatch(batch),
    asset_count: batch.asset_count ?? (
      batch.data_type === 'carbon'
        ? (Array.isArray(payload.selected_observations) ? payload.selected_observations.length : 0)
        : (Array.isArray(payload.selected_assets) ? payload.selected_assets.length : 0)
    ),
  };
  if (batch.data_type === 'carbon') {
    return {
      ...base,
      product_type: payload.product_type || 'xco2',
      source_uri: payload.source_uri || '',
      observations: payload.selected_observations || [],
    };
  }
  if (batch.data_type === 'product') {
    return {
      ...base,
      product_family: payload.product_family || 'product',
      sensor: payload.sensor || 'data_product',
      target_crs: targetCrsForGrid(payload.grid_type, payload.target_crs),
      assets: payload.selected_assets || [],
    };
  }
  if (batch.data_type === 'radar') {
    return {
      ...base,
      product_family: payload.product_family || 'sentinel1',
      sensor: payload.sensor || 'sentinel1_sar',
      target_crs: targetCrsForGrid(payload.grid_type, payload.target_crs),
      assets: payload.selected_assets || [],
    };
  }
  return {
    ...base,
    assets: payload.selected_assets || [],
  };
}

function partitionBatchNeedsIngestAttention(batch) {
  return batch?.status === 'succeeded' && ['ready', 'previewed', 'failed'].includes(batch?.ingest_status);
}

function shouldDisplayManagedBatch(batch) {
  if (batch?.status === 'archived') return false;
  if (batch?.status !== 'succeeded') return true;
  return !partitionBatchAllSlotsCompleted(batch);
}

async function loadPartitionBatches() {
  partitionBatchLoading.value = true;
  try {
    const { partitionPrefix } = apiPrefixes();
    const response = await requestGet(`${partitionPrefix}/batches?limit=500`);
    const batches = (response.batches || []).filter(shouldDisplayManagedBatch);
    managedOpticalBatches.value = batches.filter((batch) => batch.data_type === 'optical').map(normalizeManagedBatch);
    managedCarbonBatches.value = batches.filter((batch) => batch.data_type === 'carbon').map(normalizeManagedBatch);
    managedRadarBatches.value = batches.filter((batch) => batch.data_type === 'radar').map(normalizeManagedBatch);
    managedProductBatches.value = batches.filter((batch) => batch.data_type === 'product').map(normalizeManagedBatch);
    selectedOpticalBatchIds.value = pruneBatchSelection(selectedOpticalBatchIds.value, managedOpticalBatches.value);
    expandedOpticalBatchId.value = pruneExpandedBatchId(expandedOpticalBatchId.value, managedOpticalBatches.value);
    selectedCarbonBatchIds.value = pruneBatchSelection(selectedCarbonBatchIds.value, managedCarbonBatches.value);
    expandedCarbonBatchId.value = pruneExpandedBatchId(expandedCarbonBatchId.value, managedCarbonBatches.value);
    selectedRadarBatchIds.value = pruneBatchSelection(selectedRadarBatchIds.value, managedRadarBatches.value);
    expandedRadarBatchId.value = pruneExpandedBatchId(expandedRadarBatchId.value, managedRadarBatches.value);
    selectedProductBatchIds.value = pruneBatchSelection(selectedProductBatchIds.value, managedProductBatches.value);
    expandedProductBatchId.value = pruneExpandedBatchId(expandedProductBatchId.value, managedProductBatches.value);
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
    return {
      payload: {
        partition_method: partitionMethodForModule('optical'),
        grid_type: gridTypeForModule('optical'),
        grid_level: Number(gridLevelForModule('optical')),
        grid_level_mode: gridLevelModeForModule('optical'),
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
    const gridType = gridTypeForModule('radar');
    return {
      payload: {
        partition_method: partitionMethodForModule('radar'),
        grid_type: gridType,
        grid_level: Number(gridLevelForModule('radar')),
        grid_level_mode: gridLevelModeForModule('radar'),
        target_crs: targetCrsForGrid(gridType, selectedBatch?.target_crs),
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
    const gridType = gridTypeForModule('product');
    return {
      payload: {
        partition_method: partitionMethodForModule('product'),
        grid_type: gridType,
        grid_level: Number(gridLevelForModule('product')),
        grid_level_mode: gridLevelModeForModule('product'),
        target_crs: targetCrsForGrid(gridType, selectedBatch?.target_crs),
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
  const mapFootprints = selectedMapFootprintsForModule();
  const footprints = uniqueGridCoverFootprints(mapFootprints).slice(0, 30);
  if (!footprints.length) {
    ElMessage.warning('所选资产缺少有效空间范围');
    return;
  }
  mapGridLoading.value = true;
  mapGridGeometries.value = [];
  try {
    const { gridPrefix } = apiPrefixes();
    const gridType = selectedMapGridType.value;
    const gridLevel = Number(selectedMapGridLevel.value);
    const requests = footprints.map(async (footprint) => {
      const result = await requestJson(`${gridPrefix}/cover`, {
        grid_type: gridType,
        requested_grid_level: gridLevel,
        cover_mode: 'intersect',
        boundary_type: 'polygon',
        bbox: cornersToBbox(footprint.corners),
        crs: 'EPSG:4326',
      });
      return result.cells || [];
    });
    const chunks = await Promise.all(requests);
    mapGridGeometries.value = uniqueGridGeometryItems(chunks, gridType, gridLevel);
    const mergedAssetCount = selectedAssets.length - mapFootprints.length;
    const mergedCoverCount = mapFootprints.length - footprints.length;
    const mergedParts = [
      mergedAssetCount > 0 ? `合并重复资产 ${mergedAssetCount} 条` : '',
      mergedCoverCount > 0 ? `合并重复格网范围 ${mergedCoverCount} 个` : '',
    ].filter(Boolean);
    const mergedText = mergedParts.length ? `，${mergedParts.join('，')}` : '';
    ElMessage.success(`已加载格网 ${mapGridGeometries.value.length} 个${mergedText}`);
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
  const operation = request.operation || 'run';
  const apiPath = request.apiPath || `/v1/partition/${endpoint}/${operation}`;
  const dataType = payload.data_type || activeModule.value;
  const message = errorText(error);
  const manualRequired = /需要人工处理|人工确认|manual_required/i.test(message);
  return {
    status: manualRequired ? 'manual_required' : 'failed',
    mode: operation === 'test' ? 'partition_test_no_ingest' : operation === 'retry' ? 'partition_retry' : 'partition_run',
    data_type: dataType,
    endpoint: apiPath,
    partition_method: payload.partition_method || partitionMethodForModule(dataType),
    grid_type: payload.grid_type || selectedMapGridType.value || '-',
    grid_level: payload.grid_level || selectedMapGridLevel.value || '-',
    batch_id: request.batchId || payload.batch_id || '',
    batch_name: selectedDataName.value,
    selected_count:
      activeModule.value === 'optical'
        ? selectedOpticalAssets.value.length
        : activeModule.value === 'carbon'
          ? selectedCarbonObservations.value.length
        : activeModule.value === 'product'
            ? selectedProductAssets.value.length
            : 0,
    error: message,
    started_at: partitionStartedAt.value || '',
    elapsed_sec: Number(partitionElapsedSec.value.toFixed(1)),
    ingest_status: initialPartitionIngestStatus(dataType),
  };
}

function buildPartitionCancelledResult(task, taskId) {
  const dataType = task.data_type || activeModule.value;
  return {
    status: task.status === 'cancel_requested' ? 'cancel_requested' : 'cancelled',
    mode: 'partition_cancelled',
    data_type: dataType,
    partition_task_id: task.task_id || taskId,
    error: task.error || (task.status === 'cancel_requested' ? '剖分任务正在取消' : '剖分任务已取消'),
    started_at: partitionStartedAt.value || '',
    elapsed_sec: Number(partitionElapsedSec.value.toFixed(1)),
    ingest_status: initialPartitionIngestStatus(dataType),
  };
}

function isPartitionCancelledResult(result) {
  return ['cancel_requested', 'cancelled'].includes(result?.status);
}

function applyPartitionCancelledResult(result, message) {
  partitionStages.value = partitionStages.value.map((item) => (item.status === 'running' ? { ...item, status: 'cancelled' } : item));
  setPartitionStage('partition', 'cancelled', result.error || message);
  setPartitionStage('persist', 'cancelled', '任务取消后不整理执行结果。');
  lastPartitionResult.value = result;
  resultRows.value = formatRows(result);
  ElMessage.info(message);
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
    if (task.status === 'manual_required') {
      return {
        ...(task.result || {}),
        status: 'manual_required',
        partition_task_id: task.task_id || taskId,
        error: task.error || task.result?.quality_failure_reason || `剖分任务 ${taskId} 需要人工处理`,
      };
    }
    if (task.status === 'cancel_requested') {
      await sleep(partitionTaskPollIntervalMs);
      continue;
    }
    if (task.status === 'cancelled') {
      return buildPartitionCancelledResult(task, taskId);
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
  await loadPartitionTasks();
  setPartitionStage('partition', 'running', `后台任务 ${taskId} 执行中。`);
  try {
    return await waitForPartitionTask(partitionPrefix, taskId);
  } finally {
    await loadPartitionTasks();
  }
}

async function submitPartitionOperation(partitionPrefix, endpoint, operation, payload) {
  const apiPath = `${partitionPrefix}/${endpoint}/tasks/${operation}`;
  const submitted = await requestJson(apiPath, payload);
  const taskId = submitted.task_id;
  if (!taskId) {
    throw new Error('剖分任务提交后未返回 task_id');
  }
  await loadPartitionTasks(partitionTaskPage.value);
  await loadActivePartitionTasks(1);
  await loadPartitionBatches();
  return { ...submitted, task_id: taskId, api_path: apiPath };
}

function buildPartitionSubmittedResult(submitted, request, selectedCount) {
  const payload = request.payload || {};
  const dataType = submitted.data_type || payload.data_type || activeModule.value;
  return {
    status: submitted.status || 'queued',
    mode: 'partition_task_submitted',
    data_type: dataType,
    operation: submitted.operation || request.operation || 'run',
    endpoint: request.apiPath,
    partition_task_id: submitted.task_id,
    partition_method: payload.partition_method || partitionMethodForModule(dataType),
    grid_type: payload.grid_type || selectedMapGridType.value || '-',
    grid_level: payload.grid_level || selectedMapGridLevel.value || '-',
    batch_id: payload.batch_id || '',
    batch_name: payload.batch_name || selectedDataName.value,
    selected_count: selectedCount,
    submitted_at: new Date().toISOString(),
    ingest_status: initialPartitionIngestStatus(dataType),
    message: '剖分任务已提交，后台将连接 Ray 集群异步执行。',
  };
}

async function requestRetryOperation(partitionPrefix, retryRequest, retryPayload) {
  if (retryRequest.kind === 'batch') {
    const submitted = await requestJson(`${partitionPrefix}/batches/${retryRequest.batchId}/retry`, retryRequest.payload || {});
    const taskId = submitted.task_id;
    if (!taskId) {
      throw new Error('批次重试任务未返回 task_id');
    }
    await loadPartitionTasks();
    setPartitionStage('partition', 'running', `后台任务 ${taskId} 执行中。`);
    try {
      return await waitForPartitionTask(partitionPrefix, taskId);
    } finally {
      await loadPartitionTasks();
    }
  }
  if (retryRequest.kind === 'assets') {
    const submitted = await requestJson(`${partitionPrefix}/assets/retry`, retryRequest.payload || {});
    const taskId = submitted.task_id;
    if (!taskId) {
      throw new Error('失败资产重试任务未返回 task_id');
    }
    await loadPartitionTasks();
    setPartitionStage('partition', 'running', `后台任务 ${taskId} 执行中。`);
    try {
      return await waitForPartitionTask(partitionPrefix, taskId);
    } finally {
      await loadPartitionTasks();
    }
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
  if (status === 'cancelled') return 'info';
  return 'info';
}

function stageText(status) {
  if (status === 'done') return '完成';
  if (status === 'running') return '进行中';
  if (status === 'failed') return '失败';
  if (status === 'cancelled') return '已取消';
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

async function runDemo() {
  stopPartitionTaskSync();
  resultLoading.value = true;
  resultRows.value = [];
  lastPartitionResult.value = null;
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
    const operation = 'run';
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
    const submitted = await submitPartitionOperation(partitionPrefix, endpoint, operation, payload);
    const result = buildPartitionSubmittedResult(submitted, lastPartitionRequest.value, selectedCount);
    setPartitionStage('partition', 'done', `后台任务 ${submitted.task_id} 已提交，Ray 集群将异步执行。`);
    setPartitionStage('persist', 'pending', '等待后台任务完成后生成质检报告。');
    lastPartitionResult.value = result;
    resultRows.value = formatRows(result);
    startPartitionTaskSync(submitted.task_id);
    partitionTaskDrawerVisible.value = true;
    ElMessage.success('剖分任务已提交');
  } catch (error) {
    partitionStages.value = partitionStages.value.map((item) => (item.status === 'running' ? { ...item, status: 'failed' } : item));
    const failure = buildPartitionFailureResult(error, lastPartitionRequest.value || {});
    lastPartitionResult.value = failure;
    resultRows.value = formatRows(failure);
    setPartitionStage('persist', 'failed', `提交失败：${failure.error}`);
  } finally {
    stopPartitionTimer();
    resultLoading.value = false;
  }
}

async function loadManagedConfig() {
  try {
    const { configPrefix } = apiPrefixes();
    const response = await requestJson(`${configPrefix}/get`, {});
    const config = response.config || {};
    const optical = config.partition?.optical || {};
    opticalGridType.value = optical.grid_type || opticalGridType.value;
    opticalGridLevel.value = Number(optical.grid_level || opticalGridLevel.value);
  } catch (error) {
    ElMessage.warning(`配置加载失败，保留当前配置：${error.message}`);
  }
}

async function retryLastPartitionTask() {
  if (!lastPartitionRequest.value) {
    ElMessage.warning('暂无可重试任务，请先执行一次剖分');
    return;
  }
  stopPartitionTaskSync();
  resultLoading.value = true;
  resultRows.value = [];
  const retryRequest = lastPartitionRequest.value;
  const retryResult = lastPartitionResult.value || {};
  let currentRetryRequest = null;
  lastPartitionResult.value = null;
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
    if (isPartitionCancelledResult(result)) {
      applyPartitionCancelledResult(result, '重试任务已取消');
    } else {
      setPartitionStage('partition', 'done', `重试完成，生成 ${result.rows ?? result.total_index_rows ?? 0} 条索引行。`);
      setPartitionStage('persist', 'running', '正在更新结果与质检报告。');
      lastPartitionResult.value = result;
      resultRows.value = formatRows(result);
      setPartitionStage('persist', 'done', partitionPersistDoneText(result, '重试结果已返回。'));
      ElMessage.success('任务已重试完成');
    }
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
  if (dataDrawerVisible.value && partitionModules.has(moduleName)) {
    loadPartitionBatches();
  }
  if (partitionTaskDrawerVisible.value && partitionModules.has(moduleName)) {
    loadActivePartitionTasks(1);
  }
});

watch(opticalGridType, () => {
  syncPartitionMethodForGrid('optical');
  applyDefaultGridLevel('optical');
  mapGridGeometries.value = [];
});

watch(opticalPartitionMethod, () => {
  applyDefaultGridLevel('optical');
  mapGridGeometries.value = [];
});
watch(radarGridType, () => syncPartitionMethodForGrid('radar'));
watch(productGridType, () => syncPartitionMethodForGrid('product'));

watch([selectedOpticalAssets, opticalGridType, opticalPartitionMethod], () => {
  applyDefaultGridLevel('optical');
}, { deep: true });

watch([selectedRadarAssets, radarGridType, radarPartitionMethod], () => {
  applyDefaultGridLevel('radar');
}, { deep: true });

watch([selectedProductAssets, productGridType, productPartitionMethod], () => {
  applyDefaultGridLevel('product');
}, { deep: true });

watch([
  selectedOpticalBatchIds,
  deselectedOpticalAssetKeys,
  opticalGridType,
  opticalGridLevel,
  entityGridLevel,
  opticalPartitionMethod,
  selectedRadarBatchIds,
  deselectedRadarAssetKeys,
  radarGridType,
  radarGridLevel,
  radarEntityGridLevel,
  radarPartitionMethod,
  selectedProductBatchIds,
  deselectedProductAssetKeys,
  productGridType,
  productGridLevel,
  productEntityGridLevel,
  productPartitionMethod,
], () => {
  mapGridGeometries.value = [];
}, { deep: true });

onMounted(async () => {
  await loadManagedConfig();
  await loadPartitionBatches();
  await loadPartitionTasks();
  applyDefaultGridLevels();
});

onUnmounted(() => {
  stopPartitionTimer();
  stopPartitionTaskSync();
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
          <button class="module-tab" :class="{ active: activeModule === 'tasks' }" @click="activeModule = 'tasks'; loadPartitionTasks(1)">剖分任务队列</button>
        </div>
      </div>
    </section>

    <main class="main-content-area">
      <div class="container">
        <div class="module-content active">
          <div v-if="activeModule === 'tasks'" class="partition-task-workspace">
            <div class="partition-task-page-header">
              <div>
                <h3>剖分任务队列</h3>
                <span>共 {{ partitionTaskTotal }} 个任务</span>
              </div>
              <el-button :icon="Refresh" :loading="partitionTasksLoading" @click="loadPartitionTasks(partitionTaskPage)">刷新</el-button>
            </div>
            <div class="partition-task-stats">
              <div v-for="item in partitionTaskQueueStats" :key="item.label" class="partition-task-stat" :class="item.status">
                <span>{{ item.label }}</span>
                <strong>{{ item.value }}</strong>
              </div>
            </div>
            <div class="partition-task-table-panel">
              <el-table v-loading="partitionTasksLoading" :data="partitionTasks" class="drawer-table partition-task-table" row-key="task_id">
                <el-table-column label="状态" width="96">
                  <template #default="{ row }">
                    <el-tag size="small" :type="partitionStatusType(partitionTaskDisplayStatus(row))">{{ partitionStatusText(partitionTaskDisplayStatus(row)) }}</el-tag>
                  </template>
                </el-table-column>
                <el-table-column label="任务" min-width="230">
                  <template #default="{ row }">
                    <strong>{{ row.task_id }}</strong>
                    <div class="partition-asset-subtitle">{{ partitionTaskTitle(row) }}</div>
                  </template>
                </el-table-column>
                <el-table-column label="数据类型" width="110">
                  <template #default="{ row }">{{ dataLabelsByModule[row.data_type] || row.data_type || '-' }}</template>
                </el-table-column>
                <el-table-column label="操作" width="120">
                  <template #default="{ row }">{{ partitionOperationText(row.operation) }}</template>
                </el-table-column>
                <el-table-column label="批次" min-width="180">
                  <template #default="{ row }">{{ row.batch_id || '-' }}</template>
                </el-table-column>
                <el-table-column label="资产" width="80">
                  <template #default="{ row }">{{ row.asset_count ?? 0 }}</template>
                </el-table-column>
                <el-table-column label="时间" min-width="180">
                  <template #default="{ row }">{{ partitionTaskTimeText(row) }}</template>
                </el-table-column>
                <el-table-column label="结果" min-width="180">
                  <template #default="{ row }">
                    <ExpandableText :text="partitionTaskResultText(row)" :threshold="80" />
                  </template>
                </el-table-column>
                <el-table-column label="操作" width="300" fixed="right">
                  <template #default="{ row }">
                    <div class="partition-task-actions">
                      <el-button size="small" :icon="Document" :disabled="!canOpenPartitionTaskBatch(row)" @click="openPartitionTaskBatch(row)">详情</el-button>
                      <el-button
                        v-if="partitionTaskCanRequeueBatch(row)"
                        size="small"
                        type="warning"
                        :icon="Refresh"
                        :loading="partitionBatchDetailAction === `requeue:${row.batch_id}`"
                        @click="requeuePartitionTaskBatch(row)"
                      >
                        打回队列
                      </el-button>
                      <el-button
                        v-if="partitionTaskCanArchiveBatch(row)"
                        size="small"
                        type="info"
                        :icon="FolderChecked"
                        :loading="partitionBatchDetailAction === partitionBatchArchiveActionKey(partitionTaskBatchProxy(row))"
                        @click="archivePartitionTaskBatch(row)"
                      >
                        不再处理
                      </el-button>
                    </div>
                  </template>
                </el-table-column>
              </el-table>
              <el-pagination
                v-model:current-page="partitionTaskPage"
                v-model:page-size="partitionTaskPageSize"
                :total="partitionTaskTotal"
                :page-sizes="[10, 20, 50, 100]"
                layout="total, sizes, prev, pager, next"
                class="partition-task-pagination"
                @current-change="loadPartitionTasks"
                @size-change="changePartitionTaskPageSize"
              />
            </div>
          </div>
          <div v-else class="workspace">
            <div class="workspace-sidebar">
              <div class="config-panel">
                <h3>{{ activeModule === 'optical' ? '数据配置' : '参数配置' }}</h3>

                <template v-if="activeModule === 'optical'">
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
                      <el-option label="GeoHash格网" value="geohash" />
                      <el-option label="MGRS格网" value="mgrs" />
                      <el-option label="六边形格网" value="isea4h" />
                    </el-select>
                  </div>
                  <div class="form-group">
                    <label>剖分方式</label>
                    <el-radio-group v-model="opticalPartitionMethod" class="legacy-control partition-method-group">
                      <el-radio-button label="logical" :disabled="opticalGridType === 'isea4h'">逻辑剖分</el-radio-button>
                      <el-radio-button label="entity" :disabled="opticalGridType !== 'isea4h'">实体剖分</el-radio-button>
                    </el-radio-group>
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
                      <el-option label="GeoHash格网" value="geohash" />
                      <el-option label="MGRS格网" value="mgrs" />
                      <el-option label="六边形格网" value="isea4h" />
                    </el-select>
                  </div>
                  <div class="form-group">
                    <label>剖分方式</label>
                    <el-radio-group v-model="radarPartitionMethod" class="legacy-control partition-method-group">
                      <el-radio-button label="logical" :disabled="radarGridType === 'isea4h'">逻辑剖分</el-radio-button>
                      <el-radio-button label="entity" :disabled="radarGridType !== 'isea4h'">实体剖分</el-radio-button>
                    </el-radio-group>
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
                      <el-option label="GeoHash格网" value="geohash" />
                      <el-option label="MGRS格网" value="mgrs" />
                      <el-option label="六边形格网" value="isea4h" />
                    </el-select>
                  </div>
                  <div class="form-group">
                    <label>剖分方式</label>
                    <el-radio-group v-model="productPartitionMethod" class="legacy-control partition-method-group">
                      <el-radio-button label="logical" :disabled="productGridType === 'isea4h'">逻辑剖分</el-radio-button>
                      <el-radio-button label="entity" :disabled="productGridType !== 'isea4h'">实体剖分</el-radio-button>
                    </el-radio-group>
                  </div>
                </template>

                <div class="form-group action-buttons">
                  <el-button>重置</el-button>
                  <el-button type="primary" :loading="resultLoading" @click="runDemo">提交剖分任务</el-button>
                </div>
              </div>
            </div>

            <div class="workspace-main">
              <div class="map-panel">
                <div v-if="['optical', 'radar', 'product'].includes(activeModule)" class="panel-header">
                  <div class="map-actions">
                    <el-input-number v-model="selectedMapGridLevel" :min="gridTypeForModule() === 'geohash' ? 1 : 0" :max="gridTypeForModule() === 'mgrs' ? 5 : 15" size="small" :disabled="!activeGridLevelManual" />
                    <el-button size="small" :icon="activeGridLevelManual ? Refresh : EditPen" @click="activeGridLevelManual ? restoreDefaultGridLevel() : confirmGridLevelManualEdit()">
                      {{ activeGridLevelManual ? '恢复默认' : '修改层级' }}
                    </el-button>
                    <el-button size="small" :loading="mapGridLoading" @click="loadMapGridForSelectedAssets">加载格网</el-button>
                    <el-button size="small" @click="clearMapGrid">清空格网</el-button>
                  </div>
                </div>
                <GlobeMap :markers="[]" :geometries="['optical', 'radar', 'product'].includes(activeModule) ? mapGeometries : []" />
              </div>
            </div>

            <div class="workspace-result">
                <div class="result-panel">
                  <div class="result-panel-header">
                    <h3>执行结果</h3>
                    <el-button
                      size="small"
                      :icon="Document"
                      :loading="partitionTasksLoading"
                      @click="openActivePartitionTaskDrawer"
                    >
                      任务队列
                    </el-button>
                    <el-button
                      v-if="partitionResultArchiveBatch"
                      size="small"
                      type="info"
                      :icon="FolderChecked"
                      :loading="partitionBatchDetailAction === partitionBatchArchiveActionKey(partitionResultArchiveBatch)"
                      @click="archiveLastPartitionResultBatch"
                    >
                      不再处理
                    </el-button>
                </div>
                <div class="results-content">
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
                      title="执行失败"
                      class="partition-failure-alert"
                    >
                      <ExpandableText :text="partitionFailureMessage" :lines="3" :threshold="100" />
                    </el-alert>
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
                        <strong v-if="!isExpandableErrorLabel(item.label)">{{ item.value }}</strong>
                        <ExpandableText v-else :text="item.value" :threshold="80" />
                      </div>
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
                <el-button
                  v-if="partitionBatchCanArchive(batch)"
                  size="small"
                  type="info"
                  :icon="FolderChecked"
                  :loading="partitionBatchDetailAction === partitionBatchArchiveActionKey(batch)"
                  @click="archivePartitionBatch(batch)"
                >
                  不再处理
                </el-button>
                <button type="button" class="batch-expand-btn" @click="toggleOpticalBatchExpand(batch.id)">
                  {{ expandedOpticalBatchId === batch.id ? '收起' : '展开' }}
                </button>
              </div>
            </div>
            <div class="batch-summary">{{ opticalBatchSummary(batch) }}</div>
            <el-collapse v-if="partitionSlotGroups(batch).length" class="batch-schema-collapse">
              <el-collapse-item :title="partitionSlotCollapseTitle(batch)" :name="`${batch.id}-slots`">
                <div class="partition-slot-grid">
                  <div v-for="group in partitionSlotGroups(batch)" :key="`${batch.id}-${group.grid_type}`" class="partition-slot-group">
                    <span class="partition-slot-group-title">{{ group.grid_label }}</span>
                    <button
                      v-for="slot in group.slots"
                      :key="`${batch.id}-${slot.grid_type}-${slot.partition_method}`"
                      type="button"
                      class="partition-slot-chip"
                      :class="{ active: isSelectedPartitionSlot(batch, slot), disabled: slot.disabled }"
                      :disabled="slot.disabled"
                      :title="partitionSlotSummary(slot)"
                      @click="selectPartitionSlot(batch, slot)"
                    >
                      <span>{{ partitionMethodText(slot.partition_method) }}</span>
                      <el-tag size="small" :type="partitionSlotStatusType(slot.status)">{{ partitionSlotStatusText(slot.status) }}</el-tag>
                    </button>
                  </div>
                </div>
              </el-collapse-item>
            </el-collapse>
            <el-collapse class="batch-schema-collapse">
              <el-collapse-item :title="schemaCollapseTitle(batch)" :name="`${batch.id}-schema`">
                <div class="schema-grid">
                  <div v-for="field in schemaForBatch(batch)" :key="`${batch.id}-${field.field}`" class="schema-item">
                    <strong>{{ field.field }}</strong>
                    <span>{{ field.type }}</span>
                    <small>{{ field.meaning }}</small>
                  </div>
                </div>
              </el-collapse-item>
            </el-collapse>
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
                <div class="asset-corners">corners: {{ cornersText(asset.corners) }}</div>
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
                <el-button
                  v-if="partitionBatchCanArchive(batch)"
                  size="small"
                  type="info"
                  :icon="FolderChecked"
                  :loading="partitionBatchDetailAction === partitionBatchArchiveActionKey(batch)"
                  @click="archivePartitionBatch(batch)"
                >
                  不再处理
                </el-button>
                <button type="button" class="batch-expand-btn" @click="toggleCarbonBatchExpand(batch.id)">
                  {{ expandedCarbonBatchId === batch.id ? '收起' : '展开' }}
                </button>
              </div>
            </div>
            <div class="batch-summary">{{ carbonBatchSummary(batch) }}</div>
            <el-collapse class="batch-schema-collapse">
              <el-collapse-item :title="schemaCollapseTitle(batch)" :name="`${batch.id}-schema`">
                <div class="schema-grid">
                  <div v-for="field in schemaForBatch(batch)" :key="`${batch.id}-${field.field}`" class="schema-item">
                    <strong>{{ field.field }}</strong>
                    <span>{{ field.type }}</span>
                    <small>{{ field.meaning }}</small>
                  </div>
                </div>
              </el-collapse-item>
            </el-collapse>
            <div class="batch-footer">
              <span>{{ partitionBatchSummary(batch) }}</span>
              <span v-if="batch.last_task_id">最近任务 {{ batch.last_task_id }}</span>
            </div>
            <div v-if="expandedCarbonBatchId === batch.id" class="batch-assets">
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
                  <el-button
                    v-if="partitionBatchCanArchive(batch)"
                    size="small"
                    type="info"
                    :icon="FolderChecked"
                    :loading="partitionBatchDetailAction === partitionBatchArchiveActionKey(batch)"
                    @click="archivePartitionBatch(batch)"
                  >
                    不再处理
                  </el-button>
                  <button type="button" class="batch-expand-btn" @click="toggleRadarBatchExpand(batch.id)">
                    {{ expandedRadarBatchId === batch.id ? '收起' : '展开' }}
                  </button>
                </div>
              </div>
            <div class="batch-summary">{{ radarBatchSummary(batch) }}</div>
            <el-collapse class="batch-schema-collapse">
              <el-collapse-item :title="partitionSlotCollapseTitle(batch)" :name="`${batch.id}-slots`">
                <div class="partition-slot-grid">
                  <div v-for="group in partitionSlotGroups(batch)" :key="`${batch.id}-${group.grid_type}`" class="partition-slot-group">
                    <span class="partition-slot-group-title">{{ group.grid_label }}</span>
                    <button
                      v-for="slot in group.slots"
                      :key="`${batch.id}-${slot.grid_type}-${slot.partition_method}`"
                      type="button"
                      class="partition-slot-chip"
                      :class="{ active: isSelectedPartitionSlot(batch, slot), disabled: slot.disabled }"
                      :disabled="slot.disabled"
                      :title="partitionSlotSummary(slot)"
                      @click="selectPartitionSlot(batch, slot)"
                    >
                      <span>{{ partitionMethodText(slot.partition_method) }}</span>
                      <el-tag size="small" :type="partitionSlotStatusType(slot.status)">{{ partitionSlotStatusText(slot.status) }}</el-tag>
                    </button>
                  </div>
                </div>
              </el-collapse-item>
            </el-collapse>
            <el-collapse class="batch-schema-collapse">
                <el-collapse-item :title="schemaCollapseTitle(batch)" :name="`${batch.id}-schema`">
                  <div class="schema-grid">
                    <div v-for="field in schemaForBatch(batch)" :key="`${batch.id}-${field.field}`" class="schema-item">
                      <strong>{{ field.field }}</strong>
                      <span>{{ field.type }}</span>
                      <small>{{ field.meaning }}</small>
                    </div>
                  </div>
                </el-collapse-item>
              </el-collapse>
              <div class="batch-footer">
                <span>{{ partitionBatchSummary(batch) }}</span>
                <span v-if="batch.last_task_id">最近任务 {{ batch.last_task_id }}</span>
              </div>
              <div v-if="expandedRadarBatchId === batch.id" class="batch-assets">
                <div v-for="asset in batch.assets" :key="`${batch.id}-${asset.source_uri}`" class="asset-row">
                  <div class="asset-main">
                    <el-checkbox :model-value="isRadarAssetSelected(batch.id, asset)" @change="toggleRadarAssetSelect(batch.id, asset)" />
                    <strong>{{ asset.scene_id }}</strong>
                    <span>{{ (asset.polarization || asset.band || '').toUpperCase() }}</span>
                    <span>{{ asset.resolution }}m</span>
                    <span>{{ asset.acq_time }}</span>
                  </div>
                  <div class="asset-source">{{ asset.source_uri }}</div>
                  <div class="asset-corners">bbox: {{ bboxText(asset.bbox) }}</div>
                  <div class="asset-corners">corners: {{ cornersText(asset.corners) }}</div>
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
                  <el-button
                    v-if="partitionBatchCanArchive(batch)"
                    size="small"
                    type="info"
                    :icon="FolderChecked"
                    :loading="partitionBatchDetailAction === partitionBatchArchiveActionKey(batch)"
                    @click="archivePartitionBatch(batch)"
                  >
                    不再处理
                  </el-button>
                  <button type="button" class="batch-expand-btn" @click="toggleProductBatchExpand(batch.id)">
                    {{ expandedProductBatchId === batch.id ? '收起' : '展开' }}
                  </button>
                </div>
              </div>
            <div class="batch-summary">{{ productBatchSummary(batch) }}</div>
            <el-collapse class="batch-schema-collapse">
              <el-collapse-item :title="partitionSlotCollapseTitle(batch)" :name="`${batch.id}-slots`">
                <div class="partition-slot-grid">
                  <div v-for="group in partitionSlotGroups(batch)" :key="`${batch.id}-${group.grid_type}`" class="partition-slot-group">
                    <span class="partition-slot-group-title">{{ group.grid_label }}</span>
                    <button
                      v-for="slot in group.slots"
                      :key="`${batch.id}-${slot.grid_type}-${slot.partition_method}`"
                      type="button"
                      class="partition-slot-chip"
                      :class="{ active: isSelectedPartitionSlot(batch, slot), disabled: slot.disabled }"
                      :disabled="slot.disabled"
                      :title="partitionSlotSummary(slot)"
                      @click="selectPartitionSlot(batch, slot)"
                    >
                      <span>{{ partitionMethodText(slot.partition_method) }}</span>
                      <el-tag size="small" :type="partitionSlotStatusType(slot.status)">{{ partitionSlotStatusText(slot.status) }}</el-tag>
                    </button>
                  </div>
                </div>
              </el-collapse-item>
            </el-collapse>
            <el-collapse class="batch-schema-collapse">
                <el-collapse-item :title="schemaCollapseTitle(batch)" :name="`${batch.id}-schema`">
                  <div class="schema-grid">
                    <div v-for="field in schemaForBatch(batch)" :key="`${batch.id}-${field.field}`" class="schema-item">
                      <strong>{{ field.field }}</strong>
                      <span>{{ field.type }}</span>
                      <small>{{ field.meaning }}</small>
                    </div>
                  </div>
                </el-collapse-item>
              </el-collapse>
              <div class="batch-footer">
                <span>{{ partitionBatchSummary(batch) }}</span>
                <span v-if="batch.last_task_id">最近任务 {{ batch.last_task_id }}</span>
              </div>
              <div v-if="expandedProductBatchId === batch.id" class="batch-assets">
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
                  <div class="asset-corners">bbox: {{ bboxText(asset.bbox) }}</div>
                  <div class="asset-corners">corners: {{ cornersText(asset.corners) }}</div>
                </div>
              </div>
            </div>
          </div>
        </template>
      </div>
    </el-drawer>

    <el-drawer v-model="partitionTaskDrawerVisible" :title="activePartitionTaskDrawerTitle" size="860px" direction="rtl">
      <div class="partition-task-page-header">
        <div>
          <h3>{{ activePartitionTaskDrawerTitle }}</h3>
          <span>共 {{ activePartitionTaskTotal }} 个任务</span>
        </div>
        <el-button :icon="Refresh" :loading="partitionTasksLoading" @click="loadActivePartitionTasks(activePartitionTaskPage)">刷新</el-button>
      </div>
      <div class="partition-task-stats partition-task-drawer-stats">
        <div v-for="item in activePartitionTaskQueueStats" :key="item.label" class="partition-task-stat" :class="item.status">
          <span>{{ item.label }}</span>
          <strong>{{ item.value }}</strong>
        </div>
      </div>
      <div class="partition-task-table-panel">
        <el-table
          v-loading="partitionTasksLoading"
          :data="activePartitionTasks"
          class="drawer-table partition-task-table"
          row-key="task_id"
          empty-text="当前类别暂无剖分任务"
        >
          <el-table-column label="状态" width="96">
            <template #default="{ row }">
              <el-tag size="small" :type="partitionStatusType(partitionTaskDisplayStatus(row))">{{ partitionStatusText(partitionTaskDisplayStatus(row)) }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="任务" min-width="220">
            <template #default="{ row }">
              <strong>{{ row.task_id }}</strong>
              <div class="partition-asset-subtitle">{{ partitionTaskTitle(row) }}</div>
            </template>
          </el-table-column>
          <el-table-column label="操作" width="110">
            <template #default="{ row }">{{ partitionOperationText(row.operation) }}</template>
          </el-table-column>
          <el-table-column label="批次" min-width="170">
            <template #default="{ row }">{{ row.batch_id || '-' }}</template>
          </el-table-column>
          <el-table-column label="资产" width="72">
            <template #default="{ row }">{{ row.asset_count ?? 0 }}</template>
          </el-table-column>
          <el-table-column label="时间" min-width="170">
            <template #default="{ row }">{{ partitionTaskTimeText(row) }}</template>
          </el-table-column>
          <el-table-column label="结果" min-width="170">
            <template #default="{ row }">
              <ExpandableText :text="partitionTaskResultText(row)" :threshold="80" />
            </template>
          </el-table-column>
          <el-table-column label="操作" width="300" fixed="right">
            <template #default="{ row }">
              <div class="partition-task-actions">
                <el-button size="small" :icon="Document" :disabled="!canOpenPartitionTaskBatch(row)" @click="openPartitionTaskBatch(row)">详情</el-button>
                <el-button
                  v-if="partitionTaskCanRequeueBatch(row)"
                  size="small"
                  type="warning"
                  :icon="Refresh"
                  :loading="partitionBatchDetailAction === `requeue:${row.batch_id}`"
                  @click="requeuePartitionTaskBatch(row)"
                >
                  打回队列
                </el-button>
                <el-button
                  v-if="partitionTaskCanArchiveBatch(row)"
                  size="small"
                  type="info"
                  :icon="FolderChecked"
                  :loading="partitionBatchDetailAction === partitionBatchArchiveActionKey(partitionTaskBatchProxy(row))"
                  @click="archivePartitionTaskBatch(row)"
                >
                  不再处理
                </el-button>
              </div>
            </template>
          </el-table-column>
        </el-table>
        <el-pagination
          v-model:current-page="activePartitionTaskPage"
          v-model:page-size="activePartitionTaskPageSize"
          :total="activePartitionTaskTotal"
          :page-sizes="[10, 20, 50, 100]"
          layout="total, sizes, prev, pager, next"
          class="partition-task-pagination"
          @current-change="loadActivePartitionTasks"
          @size-change="changeActivePartitionTaskPageSize"
        />
      </div>
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
        <div class="partition-stage-detail-message">
          <ExpandableText :text="selectedPartitionStage.detail" :lines="4" :threshold="120" />
        </div>
      </div>
    </el-dialog>

    <el-dialog v-model="partitionContextDetailVisible" title="剖分信息详情" width="520px">
      <div v-if="selectedPartitionContext" class="partition-stage-detail">
        <div class="partition-stage-detail-row">
          <span>字段</span>
          <strong>{{ selectedPartitionContext.label }}</strong>
        </div>
        <div class="partition-stage-detail-message">
          <ExpandableText :text="selectedPartitionContext.value" :lines="4" :threshold="120" />
        </div>
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
            <div v-if="partitionBatchDetail" class="partition-batch-detail-summary">
              <ExpandableText :text="partitionBatchSummary(partitionBatchDetail)" :lines="3" :threshold="100" />
            </div>
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
              v-else-if="partitionBatchCanRun(partitionBatchDetail)"
              type="primary"
              :icon="partitionBatchActionIcon(partitionBatchDetail)"
              :loading="partitionBatchDetailAction === 'run' || partitionBatchDetailAction === 'retry'"
              @click="runPartitionBatchFromDetail"
            >
              {{ partitionBatchActionLabel(partitionBatchDetail) }}
            </el-button>
            <el-button
              v-if="partitionBatchCanArchive(partitionBatchDetail)"
              type="info"
              :icon="FolderChecked"
              :loading="partitionBatchDetailAction === partitionBatchArchiveActionKey(partitionBatchDetail)"
              @click="archivePartitionBatch(partitionBatchDetail)"
            >
              不再处理
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
              <strong v-if="!isExpandableErrorLabel(item.label)">{{ item.value }}</strong>
              <ExpandableText v-else :text="item.value" :threshold="80" />
            </div>
          </div>
          <el-collapse v-if="partitionSlotGroups(partitionBatchDetail).length" class="batch-schema-collapse">
            <el-collapse-item :title="partitionSlotCollapseTitle(partitionBatchDetail)" name="detail-slots">
              <div class="partition-slot-grid detail">
                <div v-for="group in partitionSlotGroups(partitionBatchDetail)" :key="`detail-${group.grid_type}`" class="partition-slot-group">
                  <span class="partition-slot-group-title">{{ group.grid_label }}</span>
                  <div
                    v-for="slot in group.slots"
                    :key="`detail-${slot.grid_type}-${slot.partition_method}`"
                    class="partition-slot-chip static"
                    :class="{ active: isSelectedPartitionSlot(partitionBatchDetail, slot), disabled: slot.disabled }"
                  >
                    <span>{{ partitionMethodText(slot.partition_method) }}</span>
                    <el-tag size="small" :type="partitionSlotStatusType(slot.status)">{{ partitionSlotStatusText(slot.status) }}</el-tag>
                    <small>{{ partitionSlotSummary(slot) }}</small>
                  </div>
                </div>
              </div>
            </el-collapse-item>
          </el-collapse>
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
              <el-option label="已归档" value="archived" />
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
              <template #default="{ row }">
                <ExpandableText :text="row.error || '-'" :threshold="60" />
              </template>
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
                  <el-tag size="small" effect="plain">{{ attemptPartitionMethodLabel(attempt) }}</el-tag>
                  <el-tag size="small" effect="plain">{{ attemptGridLabel(attempt) }}</el-tag>
                  <span>第 {{ attempt.attempt_no }} 次</span>
                  <span>创建 {{ formatPartitionTimestamp(attempt.created_at) }}</span>
                  <span v-if="attempt.started_at">开始 {{ formatPartitionTimestamp(attempt.started_at) }}</span>
                  <span v-if="attempt.finished_at">结束 {{ formatPartitionTimestamp(attempt.finished_at) }}</span>
                  <span v-if="attempt.requested_by">提交者 {{ attempt.requested_by }}</span>
                </div>
                <div v-if="attempt.error_message" class="partition-attempt-error">
                  <ExpandableText :text="attempt.error_message" :lines="3" :threshold="100" />
                </div>
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
