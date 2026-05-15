<script setup>
import { computed, onMounted, ref, watch } from 'vue';
import { ElMessage } from 'element-plus';
import { Search } from '@element-plus/icons-vue';

import LeafletMap from '@/components/LeafletMap.vue';
import { apiPrefixes, requestJson } from '@/api/client';

const activeModule = ref(window.location.pathname === '/quality' ? 'quality' : 'optical');
const dataDrawerVisible = ref(false);
const qualityHistoryDrawerVisible = ref(false);
const dataSearch = ref('');
const qualityHistorySearch = ref('');
const qualityHistoryStatus = ref('');
const selectedBatches = ref({
  optical: 'B20240307001',
  carbon: 'C20240307001',
  radar: 'R20240307001',
  product: 'P20240307001',
});
const resultLoading = ref(false);
const resultRows = ref([]);
const qualityLoading = ref(false);
const qualityHistoryLoading = ref(false);
const qualityPdfLoading = ref(false);
const qualityReport = ref(null);
const qualityHistory = ref([]);
const qualityError = ref('');
const qualityTargetCrs = ref('EPSG:4326');
const selectedQualityRunDir = ref('');
const qualityDataType = ref('optical');

const opticalRows = [
  { id: 'B20240307001', name: 'LC08_L2SP_120030_20260204_20260217_02_T1', params: 'Landsat 8 | SR_B2/SR_B3/SR_B4', status: '就绪' },
  { id: 'B20240307002', name: 'Sentinel-2_MSI_20240307', params: '10980x10980 像素 | 10m', status: '就绪' },
  { id: 'B20240307003', name: '高分一号_PMS_20240306', params: '12000x12000 像素 | 2m', status: '就绪' },
];

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

const activeDataRows = computed(() => dataRowsByModule[activeModule.value] || []);

const activeDataLabel = computed(() => dataLabelsByModule[activeModule.value] || '已载入数据');

const selectedDataName = computed(() => {
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
      [row.dataset, row.run_name, row.run_dir]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(keyword));
    const matchesStatus = !qualityHistoryStatus.value || row.status === qualityHistoryStatus.value;
    return matchesKeyword && matchesStatus;
  });
});

const selectedQualityRecord = computed(() => {
  if (!selectedQualityRunDir.value) return null;
  return qualityHistory.value.find((row) => row.run_dir === selectedQualityRunDir.value) || null;
});

const mapMarkers = [
  { position: [39.9042, 116.4074], label: '北京样例区域' },
  { position: [31.2304, 121.4737], label: '上海样例区域' },
];

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
  return row.run_dir === selectedQualityRunDir.value ? 'selected-quality-history-row' : '';
}

function selectData(row) {
  if (!dataRowsByModule[activeModule.value]) return;
  selectedBatches.value[activeModule.value] = row.id;
  dataDrawerVisible.value = false;
}

function formatRows(result) {
  return Object.entries(result).map(([key, value]) => ({
    key,
    value: typeof value === 'object' ? JSON.stringify(value) : String(value),
  }));
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
      limit: 30,
    });
    qualityHistory.value = result.records || [];
  } catch (error) {
    qualityError.value = error.message;
    ElMessage.error(error.message);
  } finally {
    qualityHistoryLoading.value = false;
  }
}

async function runQualityCheck(runDir = '') {
  qualityLoading.value = true;
  qualityError.value = '';
  try {
    const { qualityPrefix } = apiPrefixes();
    const endpoint = runDir ? `${qualityPrefix}/${qualityDataType.value}/report` : `${qualityPrefix}/${qualityDataType.value}/latest`;
    const payload = {
      target_crs: qualityTargetCrs.value,
    };
    if (runDir) payload.run_dir = runDir;
    qualityReport.value = await requestJson(endpoint, payload);
    selectedQualityRunDir.value = qualityReport.value.run_dir || runDir;
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
  await runQualityCheck(row.run_dir);
  qualityHistoryDrawerVisible.value = false;
}

async function exportQualityPdf() {
  if (!qualityReport.value?.run_dir) {
    ElMessage.warning('请先加载质检结果');
    return;
  }
  qualityPdfLoading.value = true;
  try {
    const { qualityPrefix } = apiPrefixes();
    const response = await fetch(`${qualityPrefix}/${qualityDataType.value}/report/pdf`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ run_dir: qualityReport.value.run_dir }),
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
    const runName = qualityReport.value.run_dir.split('/').filter(Boolean).pop() || 'run';
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
  selectedQualityRunDir.value = '';
  await refreshQuality();
}

async function runDemo() {
  if (activeModule.value === 'quality') {
    await refreshQuality();
    return;
  }
  resultLoading.value = true;
  resultRows.value = [];
  try {
    const { partitionPrefix } = apiPrefixes();
    const endpoint = activeModule.value === 'carbon' ? 'carbon' : 'optical';
    const result = await requestJson(`${partitionPrefix}/${endpoint}/demo`, {});
    resultRows.value = formatRows(result);
    ElMessage.success('剖分任务完成');
  } catch (error) {
    ElMessage.error(error.message);
  } finally {
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

onMounted(() => {
  if (activeModule.value === 'quality') {
    refreshQuality();
  }
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
        </div>
      </div>
    </section>

    <main class="main-content-area">
      <div class="container">
        <div class="module-content active">
          <div class="workspace">
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
                    <el-select model-value="geohash" class="legacy-control">
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
                  <el-button type="primary" :loading="activeModule === 'quality' ? qualityLoading : resultLoading" @click="runDemo">
                    {{ activeModule === 'quality' ? '刷新结果' : '开始剖分' }}
                  </el-button>
                </div>
              </div>
            </div>

            <div class="workspace-main">
              <div v-if="activeModule !== 'quality'" class="map-panel">
                <div class="panel-header">
                  <h3>{{ activeModule === 'carbon' ? '观测足迹地图分布' : '地图预览' }}</h3>
                </div>
                <LeafletMap :markers="mapMarkers" />
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
                      <span>run_dir</span>
                      <strong>{{ qualityReport.run_dir?.split('/').slice(-2).join('/') }}</strong>
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
      <el-table :data="filteredDataRows" class="drawer-table" highlight-current-row @row-click="selectData">
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
