<script setup>
import { computed, onMounted, onUnmounted, ref, watch } from 'vue';
import { ElMessage, ElMessageBox } from 'element-plus';
import { Search } from '@element-plus/icons-vue';

import LeafletMap from '@/components/LeafletMap.vue';
import ConfigView from '@/views/ConfigView.vue';
import { apiPrefixes, requestJson } from '@/api/client';

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
const selectedBatches = ref({
  optical: 'OPTICAL_BATCH_20260522_135546',
  carbon: 'C20240307001',
  radar: 'R20240307001',
  product: 'P20240307001',
});
const selectedOpticalBatchIds = ref(['OPTICAL_BATCH_20260522_135546']);
const expandedOpticalBatchId = ref('OPTICAL_BATCH_20260522_135546');
const deselectedOpticalAssetKeys = ref({});
const opticalGridType = ref('geohash');
const opticalGridLevel = ref(5);
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
const qualityLoading = ref(false);
const qualityHistoryLoading = ref(false);
const qualityPdfLoading = ref(false);
const qualityReport = ref(null);
const qualityHistory = ref([]);
const qualityError = ref('');
const qualityTargetCrs = ref('EPSG:4326');
const qualityHistoryLimit = ref(30);
const selectedQualityReportId = ref('');
const qualityDataType = ref('optical');
const ingestDefaults = ref({
  dataset: 'demo_optical',
  sensor: 'optical_mosaic',
  quality_rule: 'best_quality_wins',
  allow_failed_quality: false,
});

const opticalBatches = [
  {
    id: 'OPTICAL_BATCH_20260522_135546',
    name: 'Shandong_mosaic_optocal',
    status: '就绪',
    assets: [
      { source_uri: 'Shandong_mosaic_2015Q3_sr_band3_cut/Shandong_mosaic_2015Q3_sr_band3_cut.tif', scene_id: 'Shandong_mosaic_2015Q3', acq_time: '2015-07-01T00:00:00Z', bands: ['sr_band3'], corners: [[114.757377, 38.503521], [122.774914, 38.503521], [122.774914, 33.857041], [114.757377, 33.857041]] },
      { source_uri: 'Shandong_mosaic_2017Q2_sr_band2_cut/Shandong_mosaic_2017Q2_sr_band2_cut.tif', scene_id: 'Shandong_mosaic_2017Q2', acq_time: '2017-04-01T00:00:00Z', bands: ['sr_band2'], corners: [[114.757377, 38.503521], [122.774914, 38.503521], [122.774914, 33.857041], [114.757377, 33.857041]] },
      { source_uri: 'Shandong_mosaic_202008_sr_band2/Shandong_mosaic_202008_sr_band2.tif', scene_id: 'Shandong_mosaic_202008', acq_time: '2020-08-01T00:00:00Z', bands: ['sr_band2'], corners: [[108.227954, 38.75], [128.544672, 38.75], [128.544672, 33.499766], [108.227954, 33.499766]] },
      { source_uri: 'Shandong_mosaic_2020Q3_sr_band4_cut/Shandong_mosaic_2020Q3_sr_band4_cut.tif', scene_id: 'Shandong_mosaic_2020Q3', acq_time: '2020-07-01T00:00:00Z', bands: ['sr_band4'], corners: [[114.757377, 38.503521], [122.774914, 38.503521], [122.774914, 33.857041], [114.757377, 33.857041]] },
    ],
  },
  {
    id: 'OPTICAL_BATCH_20260522_091000',
    name: 'Shandong_mosaic_2020Q3_rgb_batch',
    status: '就绪',
    assets: [
      { source_uri: 'Shandong_mosaic_2020Q3_sr_band2_cut/Shandong_mosaic_2020Q3_sr_band2_cut.tif', scene_id: 'Shandong_mosaic_2020Q3', acq_time: '2020-07-01T00:00:00Z', bands: ['sr_band2'], corners: [[114.757377, 38.503521], [122.774914, 38.503521], [122.774914, 33.857041], [114.757377, 33.857041]] },
      { source_uri: 'Shandong_mosaic_2020Q3_sr_band3_cut/Shandong_mosaic_2020Q3_sr_band3_cut.tif', scene_id: 'Shandong_mosaic_2020Q3', acq_time: '2020-07-01T00:00:00Z', bands: ['sr_band3'], corners: [[114.757377, 38.503521], [122.774914, 38.503521], [122.774914, 33.857041], [114.757377, 33.857041]] },
      { source_uri: 'Shandong_mosaic_2020Q3_sr_band4_cut/Shandong_mosaic_2020Q3_sr_band4_cut.tif', scene_id: 'Shandong_mosaic_2020Q3', acq_time: '2020-07-01T00:00:00Z', bands: ['sr_band4'], corners: [[114.757377, 38.503521], [122.774914, 38.503521], [122.774914, 33.857041], [114.757377, 33.857041]] },
    ],
  },
  {
    id: 'OPTICAL_BATCH_20260521_181500',
    name: 'Shandong_mosaic_2017Q3_batch',
    status: '就绪',
    assets: [
      { source_uri: 'Shandong_mosaic_2017Q3_sr_band3_cut/Shandong_mosaic_2017Q3_sr_band3_cut.tif', scene_id: 'Shandong_mosaic_2017Q3', acq_time: '2017-07-01T00:00:00Z', bands: ['sr_band3'], corners: [[114.757377, 38.503521], [122.774914, 38.503521], [122.774914, 33.857041], [114.757377, 33.857041]] },
      { source_uri: 'Shandong_mosaic_2017Q3_sr_band4_cut/Shandong_mosaic_2017Q3_sr_band4_cut.tif', scene_id: 'Shandong_mosaic_2017Q3', acq_time: '2017-07-01T00:00:00Z', bands: ['sr_band4'], corners: [[114.757377, 38.503521], [122.774914, 38.503521], [122.774914, 33.857041], [114.757377, 33.857041]] },
    ],
  },
];
const opticalRows = opticalBatches.map((batch) => ({
  id: batch.id,
  name: batch.name,
  params: `${batch.assets.length} 条资产`,
  status: batch.status,
}));

const carbonRows = [
  { id: 'C20240307001', name: 'OCO-2_XCO2_20240307', params: '1,247 footprints', status: '就绪' },
  { id: 'C20240307002', name: 'GOSAT_CH4_20240306', params: '684 footprints', status: '就绪' },
];

const radarRows = [
  { id: 'R20240307001', name: 'Sentinel-1A_GRD_20240307', params: 'VV/VH | 10m', status: '就绪' },
  { id: 'R20240307002', name: 'GF3_SAR_20240306', params: 'HH/HV | 8m', status: '就绪' },
];

const productRows = [
  { id: 'P20240307001', name: 'LandCover_GLC_2023', params: '30m | 分类产品', status: '就绪' },
  { id: 'P20240307002', name: 'NDVI_MODIS_202403', params: '250m | 月度产品', status: '就绪' },
  { id: 'P20240307003', name: 'DEM_SRTM_90m', params: '90m | 高程产品', status: '就绪' },
];

const dataRowsByModule = {
  optical: opticalRows,
  carbon: carbonRows,
  radar: radarRows,
  product: productRows,
};

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

const testModules = new Set(['optical', 'carbon']);

const activeDataRows = computed(() => dataRowsByModule[activeModule.value] || []);

const activeDataLabel = computed(() => dataLabelsByModule[activeModule.value] || '已载入数据');

const selectedDataName = computed(() => {
  if (activeModule.value === 'optical') {
    if (!selectedOpticalBatchIds.value.length) return '未选择';
    const names = opticalBatches
      .filter((batch) => selectedOpticalBatchIds.value.includes(batch.id))
      .map((batch) => batch.name);
    return names.join('，');
  }
  const rows = activeDataRows.value;
  const selectedId = selectedBatches.value[activeModule.value];
  return rows.find((row) => row.id === selectedId)?.name || '未选择';
});

const filteredDataRows = computed(() => {
  const keyword = dataSearch.value.trim().toLowerCase();
  const rows = activeDataRows.value;
  if (!keyword) return rows;
  return rows.filter((row) => row.name.toLowerCase().includes(keyword));
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
  opticalBatches.forEach((batch) => {
    if (!selectedBatchSet.has(batch.id)) return;
    batch.assets.forEach((asset) => {
      if (isOpticalAssetSelected(batch.id, asset)) {
        rows.push({ ...asset, batch_id: batch.id });
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

const mapGeometries = computed(() => [...mapBatchGeometries.value, ...mapGridGeometries.value]);

const partitionMetricRows = computed(() => {
  if (!lastPartitionResult.value) return [];
  const result = lastPartitionResult.value;
  return [
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

const partitionContextRows = computed(() => {
  if (activeModule.value === 'quality') return [];
  const request = lastPartitionRequest.value || {};
  const payload = request.payload || {};
  const result = lastPartitionResult.value || {};
  const operation = request.operation || (testModules.has(activeModule.value) ? 'test' : 'demo');
  const endpoint = request.endpoint || partitionEndpointsByModule[activeModule.value] || activeModule.value;
  const status = resultLoading.value ? '执行中' : lastPartitionResult.value ? '已完成' : '待执行';
  const rows = [
    { label: '运行状态', value: status },
    { label: '执行接口', value: `/v1/partition/${endpoint}/${operation}` },
    { label: '开始时间', value: partitionStartedAt.value ? formatQualityTime(partitionStartedAt.value) : '-' },
    { label: '已耗时', value: `${partitionElapsedSec.value.toFixed(1)} s` },
    { label: '数据批次', value: selectedDataName.value },
    { label: '输出目录', value: result.run_dir || '-' },
  ];
  if (testModules.has(activeModule.value)) {
    rows.splice(
      5,
      0,
      { label: '剖分格网', value: activeModule.value === 'optical' ? `${payload.grid_type || opticalGridType.value} / ${payload.grid_level || opticalGridLevel.value} 级` : 'isea4h / 5 级' },
      ...(activeModule.value === 'optical'
        ? [
            { label: '选择资产', value: `${selectedOpticalAssets.value.length} 条` },
            { label: '波段', value: selectedOpticalBandsText.value },
            { label: '时间范围', value: selectedOpticalTimeRange.value },
          ]
        : []),
      { label: '安全模式', value: '剖分测试不写正式库' },
    );
  }
  return rows;
});

const partitionResultDetailRows = computed(() => {
  const result = lastPartitionResult.value;
  if (!result) return [];
  return [
    { label: '执行引擎', value: result.execution_engine || result.partition_backend || '-' },
    { label: '演示任务 ID', value: result.demo_task_id || '-' },
    { label: 'Ray 任务 ID', value: result.ray_task_id || '-' },
    { label: '质检报告 ID', value: result.quality_report_id || result.quality_report?.report_id || '-' },
    { label: '索引文件', value: result.rows_path || result.output_path || '-' },
    { label: 'COG 输出', value: result.cog_output_dir || result.cog_input_dir || '-' },
  ];
});

const partitionWarnNeedsRetry = computed(() => {
  const status = lastPartitionResult.value?.quality_status || lastPartitionResult.value?.quality_report?.status;
  return status === 'WARN';
});

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

function selectData(row) {
  if (activeModule.value === 'optical') return;
  if (!dataRowsByModule[activeModule.value]) return;
  selectedBatches.value[activeModule.value] = row.id;
  dataDrawerVisible.value = false;
}

function assetKey(asset) {
  return `${asset.source_uri}|${asset.scene_id}|${assetBandsText(asset)}|${asset.acq_time}`;
}

function isOpticalAssetSelected(batchId, asset) {
  const excluded = deselectedOpticalAssetKeys.value[batchId] || [];
  return !excluded.includes(assetKey(asset));
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

function selectSingleOpticalBatch(batchId) {
  selectedOpticalBatchIds.value = [batchId];
}

async function runDemoForBatch(batchId) {
  selectSingleOpticalBatch(batchId);
  dataDrawerVisible.value = false;
  await runDemo();
}

function toggleOpticalAssetSelect(batchId, asset) {
  const key = assetKey(asset);
  const current = deselectedOpticalAssetKeys.value[batchId] || [];
  const exists = current.includes(key);
  const next = exists ? current.filter((item) => item !== key) : [...current, key];
  deselectedOpticalAssetKeys.value = { ...deselectedOpticalAssetKeys.value, [batchId]: next };
}

function opticalBatchSummary(batch) {
  const selectedCount = batch.assets.filter((asset) => isOpticalAssetSelected(batch.id, asset)).length;
  return `${selectedCount}/${batch.assets.length} 条资产已选`;
}

async function loadMapGridForSelectedAssets() {
  if (activeModule.value !== 'optical') return;
  if (!selectedOpticalAssets.value.length) {
    ElMessage.warning('请至少选择一条资产');
    return;
  }
  mapGridLoading.value = true;
  mapGridGeometries.value = [];
  try {
    const { gridPrefix } = apiPrefixes();
    const requests = selectedOpticalAssets.value.slice(0, 30).map(async (asset) => {
      const result = await requestJson(`${gridPrefix}/cover`, {
        grid_type: opticalGridType.value,
        level: Number(opticalGridLevel.value),
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
    product_years: check.status === 'WARN' ? '产品年份不完整或存在非预期年份。' : '产品年份覆盖完整。',
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
  return [];
}

function formatQualityTime(value) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString('zh-CN', { hour12: false });
}

async function loadQualityHistory() {
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

async function selectQualityRecord(row) {
  if (row.data_type && row.data_type !== qualityDataType.value) {
    qualityDataType.value = row.data_type;
  }
  await runQualityCheck(row.report_id);
  qualityHistoryDrawerVisible.value = false;
}

async function exportQualityPdf() {
  if (!qualityReport.value?.report_id) {
    ElMessage.warning('请先加载质检结果');
    return;
  }
  qualityPdfLoading.value = true;
  try {
    const { qualityPrefix } = apiPrefixes();
    const response = await fetch(`${qualityPrefix}/${qualityDataType.value}/report/pdf`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ report_id: qualityReport.value.report_id }),
    });
    if (!response.ok) {
      const text = await response.text();
      let message = `PDF导出失败: ${response.status}`;
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
    link.download = `quality-report-${qualityDataType.value}-${runName}.pdf`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
    ElMessage.success('PDF已导出');
  } catch (error) {
    ElMessage.error(error.message);
  } finally {
    qualityPdfLoading.value = false;
  }
}

async function changeQualityDataType() {
  qualityReport.value = null;
  qualityHistory.value = [];
  selectedQualityReportId.value = '';
  await refreshQuality();
}

async function runDemo() {
  if (activeModule.value === 'quality') {
    await refreshQuality();
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
    const selectedBatch = activeModule.value === 'optical'
      ? opticalBatches.find((batch) => selectedOpticalBatchIds.value.includes(batch.id))
      : null;
    const selectedAssets = activeModule.value === 'optical'
      ? selectedOpticalAssets.value
      : [];
    const payload = activeModule.value === 'optical'
      ? {
          grid_type: opticalGridType.value,
          grid_level: Number(opticalGridLevel.value),
          batch_id: selectedBatch?.id || '',
          batch_name: selectedBatch?.name || '',
          selected_assets: selectedAssets,
        }
      : {};
    const operation = testModules.has(activeModule.value) ? 'test' : 'demo';
    lastPartitionRequest.value = { endpoint, payload, operation };
    setPartitionStage('queue', 'running', activeModule.value === 'optical' ? `准备读取 ${selectedAssets.length} 条影像资产。` : '准备读取当前队列中的数据。');
    await Promise.resolve();
    setPartitionStage('queue', 'done', '数据队列已提交到后端。');
    setPartitionStage('partition', 'running', `调用 /v1/partition/${endpoint}/${operation} 执行剖分。`);
    const result = await requestJson(`${partitionPrefix}/${endpoint}/${operation}`, payload);
    setPartitionStage('partition', 'done', `已生成 ${result.rows ?? result.total_index_rows ?? 0} 条索引行。`);
    setPartitionStage('persist', 'running', '正在整理结果并保存质检报告。');
    lastPartitionResult.value = result;
    resultRows.value = formatRows(result);
    setPartitionStage('persist', 'done', result.quality_report_id ? `质检报告已保存：${result.quality_report_id}` : '执行结果已返回。');
    ElMessage.success(testModules.has(activeModule.value) ? '剖分测试完成，未写入正式库' : '剖分任务完成');
  } catch (error) {
    partitionStages.value = partitionStages.value.map((item) => (item.status === 'running' ? { ...item, status: 'failed' } : item));
    ElMessage.error(error.message);
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
    ElMessage.warning(`配置加载失败，使用页面默认值：${error.message}`);
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
      '确认后会将当前剖分结果写入演示版本，重复执行会按唯一键覆盖同版本记录。',
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
    ElMessage.success('演示版本入库完成');
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
    setPartitionStage('partition', 'running', `调用 /v1/partition/${endpoint}/retry 执行重试。`);
    const result = await requestJson(`${partitionPrefix}/${endpoint}/retry`, {
      request: retryRequest,
      last_result: retryResult,
    });
    setPartitionStage('partition', 'done', `重试完成，生成 ${result.rows ?? result.total_index_rows ?? 0} 条索引行。`);
    setPartitionStage('persist', 'running', '正在更新结果与质检报告。');
    lastPartitionResult.value = result;
    resultRows.value = formatRows(result);
    setPartitionStage('persist', 'done', result.quality_report_id ? `质检报告已保存：${result.quality_report_id}` : '重试结果已返回。');
    if (activeModule.value === 'quality' && result.quality_report) {
      qualityDataType.value = retryRequest.endpoint === 'product' ? 'product' : 'optical';
      qualityReport.value = result.quality_report;
      selectedQualityReportId.value = result.quality_report.report_id || result.quality_report_id || '';
      await loadQualityHistory();
    }
    ElMessage.success('任务已重试完成');
  } catch (error) {
    partitionStages.value = partitionStages.value.map((item) => (item.status === 'running' ? { ...item, status: 'failed' } : item));
    ElMessage.error(error.message);
  } finally {
    stopPartitionTimer();
    resultLoading.value = false;
  }
}

watch(activeModule, (moduleName) => {
  if (moduleName === 'quality' && !qualityReport.value && !qualityLoading.value) {
    refreshQuality();
  }
});

watch(qualityDataType, () => {
  if (activeModule.value === 'quality') {
    changeQualityDataType();
  }
});

watch([selectedOpticalBatchIds, deselectedOpticalAssetKeys, opticalGridType, opticalGridLevel], () => {
  mapGridGeometries.value = [];
}, { deep: true });

onMounted(async () => {
  await loadManagedConfig();
  if (activeModule.value === 'quality') {
    refreshQuality();
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
                    <el-select model-value="loaded" class="legacy-control">
                      <el-option label="从载入子系统获取" value="loaded" />
                      <el-option label="本地数据源" value="local" />
                    </el-select>
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
                      <el-option label="Geohash (逻辑剖分)" value="geohash" />
                      <el-option label="MGRS (逻辑剖分)" value="mgrs" />
                      <el-option label="ISEA4H (实体剖分)" value="isea4h" />
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
                    <label>观测足迹匹配</label>
                    <el-select model-value="centroid" class="legacy-control">
                      <el-option label="重心落入" value="centroid" />
                      <el-option label="面积加权" value="weighted" />
                      <el-option label="最近邻" value="nearest" />
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
                </template>

                <template v-else>
                  <div class="form-group">
                    <label>质检数据类型</label>
                    <el-select v-model="qualityDataType" class="legacy-control">
                      <el-option label="光学遥感" value="optical" />
                      <el-option label="数据产品" value="product" />
                    </el-select>
                  </div>
                  <div class="form-group">
                    <label>目标参考系统</label>
                    <el-select v-model="qualityTargetCrs" class="legacy-control" @change="refreshQuality">
                      <el-option label="EPSG:4326" value="EPSG:4326" />
                    </el-select>
                  </div>
                  <div class="form-group">
                    <div class="quality-rule-list">
                      <div>索引字段完整性</div>
                      <div>COG 可读性与 CRS</div>
                      <div>window 越界检查</div>
                      <div>像元抽样有效性</div>
                      <div v-if="qualityDataType === 'product'">产品年份完整性</div>
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
                  <el-button
                    v-if="activeModule === 'quality'"
                    :disabled="!lastPartitionRequest || resultLoading"
                    :type="partitionWarnNeedsRetry ? 'warning' : 'default'"
                    @click="retryLastPartitionTask"
                  >
                    {{ partitionWarnNeedsRetry ? '告警后重试任务' : '手动重试' }}
                  </el-button>
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
                  <h3>{{ activeModule === 'carbon' ? '观测足迹地图分布' : '地图预览' }}</h3>
                  <div v-if="activeModule === 'optical'" class="map-actions">
                    <el-input-number v-model="opticalGridLevel" :min="1" :max="15" size="small" />
                    <el-button size="small" :loading="mapGridLoading" @click="loadMapGridForSelectedAssets">加载格网</el-button>
                    <el-button size="small" @click="clearMapGrid">清空格网</el-button>
                  </div>
                </div>
                <LeafletMap :markers="[]" :geometries="activeModule === 'optical' ? mapGeometries : []" />
              </div>
              <div v-else class="quality-overview-panel">
                <div class="panel-header">
                  <h3>质检总览</h3>
                </div>
                <div v-if="qualityReport" class="quality-dashboard">
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
                      <strong>{{ qualityDataType === 'product' ? '数据产品自动质检' : '光学遥感自动质检' }}</strong>
                    </div>
                  </div>
                  <div class="quality-band-table">
                    <div class="quality-section-title">{{ qualityDataType === 'product' ? '年份行数' : '波段行数' }}</div>
                    <div
                      v-for="(value, band) in (qualityDataType === 'product' ? qualityReport.summary.rows_by_year : qualityReport.summary.rows_by_band)"
                      :key="band"
                      class="quality-kv"
                    >
                      <span>{{ band }}</span>
                      <strong>{{ value }}</strong>
                    </div>
                  </div>
                </div>
                <div v-else-if="qualityLoading" class="quality-empty-state">
                  <div class="quality-empty-icon">QC</div>
                  <p>正在自动加载最新质检结果</p>
                </div>
                <div v-else-if="qualityError" class="quality-empty-state">
                  <div class="quality-empty-icon">ERR</div>
                  <p>{{ qualityError }}</p>
                </div>
                <div v-else class="quality-empty-state">
                  <div class="quality-empty-icon">QC</div>
                  <p>等待自动质检结果</p>
                </div>
              </div>
            </div>

            <div class="workspace-result">
              <div class="result-panel">
                <div class="result-panel-header">
                  <h3>{{ activeModule === 'quality' ? '质检结果' : '执行结果' }}</h3>
                  <el-button
                    v-if="activeModule === 'quality'"
                    size="small"
                    :loading="qualityPdfLoading"
                    :disabled="!qualityReport"
                    @click="exportQualityPdf"
                  >
                    导出PDF
                  </el-button>
                </div>
                <div class="results-content">
                  <template v-if="activeModule !== 'quality'">
                    <div class="partition-progress-panel">
                      <div class="quality-section-title">剖分进程</div>
                      <div class="partition-context-grid">
                        <div v-for="item in partitionContextRows" :key="item.label" class="partition-context-item">
                          <span>{{ item.label }}</span>
                          <strong>{{ item.value }}</strong>
                        </div>
                      </div>
                      <div class="partition-stage-list">
                        <div v-for="stage in partitionStages" :key="stage.key" class="partition-stage-item">
                          <div class="partition-stage-main">
                            <strong>{{ stage.label }}</strong>
                            <span>{{ stage.detail }}</span>
                          </div>
                          <el-tag :type="stageTagType(stage.status)" size="small">{{ stageText(stage.status) }}</el-tag>
                        </div>
                      </div>
                    </div>
                    <el-alert
                      v-if="partitionWarnNeedsRetry"
                      type="warning"
                      :closable="false"
                      title="质检出现告警，建议点击“告警后重试任务”重新执行剖分。"
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
      <template v-if="activeModule === 'optical'">
        <div class="batch-list">
          <div
            v-for="batch in opticalBatches.filter((item) => !dataSearch.trim() || item.name.toLowerCase().includes(dataSearch.trim().toLowerCase()) || item.id.toLowerCase().includes(dataSearch.trim().toLowerCase()))"
            :key="batch.id"
            class="batch-card"
          >
            <div class="batch-card-header">
              <el-checkbox :model-value="selectedOpticalBatchIds.includes(batch.id)" @change="toggleOpticalBatchSelect(batch.id)">
                <span class="batch-name">{{ batch.name }}</span>
              </el-checkbox>
              <div class="batch-meta">
                <span class="batch-id">{{ batch.id }}</span>
                <el-tag size="small" type="success">{{ batch.status }}</el-tag>
                <el-button size="small" type="primary" @click="runDemoForBatch(batch.id)">测试该批次</el-button>
                <button type="button" class="batch-expand-btn" @click="toggleOpticalBatchExpand(batch.id)">
                  {{ expandedOpticalBatchId === batch.id ? '收起' : '展开' }}
                </button>
              </div>
            </div>
            <div class="batch-summary">{{ opticalBatchSummary(batch) }}</div>
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
      <el-table v-else :data="filteredDataRows" class="drawer-table" highlight-current-row @row-click="selectData">
        <el-table-column prop="id" label="批次ID" min-width="190" />
        <el-table-column prop="name" label="名称" min-width="260" />
        <el-table-column prop="params" label="参数" min-width="180" />
        <el-table-column prop="status" label="状态" width="90">
          <template #default="{ row }">
            <el-tag type="success">{{ row.status }}</el-tag>
          </template>
        </el-table-column>
      </el-table>
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
  </section>
</template>
