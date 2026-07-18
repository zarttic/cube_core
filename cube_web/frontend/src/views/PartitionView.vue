<script setup>
import { computed, defineAsyncComponent, onMounted, ref } from 'vue';
import { ElMessage } from 'element-plus';
import { RefreshLeft } from '@element-plus/icons-vue';

import { requestJson } from '@/api/client';
import { usePartitionStore } from '@/stores/partition';
import { derivedPartitionMethod, gridDefinition, nativeLevelLabel, withFixedPartitionOptions } from '@/utils/grid';
import DataManagementView from '@/views/DataManagementView.vue';
import QualityView from '@/views/QualityView.vue';
import BatchAssetsPanel from '@/views/partition/BatchAssetsPanel.vue';
import ExecutionResultPanel from '@/views/partition/ExecutionResultPanel.vue';
import GridParameters from '@/views/partition/GridParameters.vue';
import TaskQueuePanel from '@/views/partition/TaskQueuePanel.vue';

const GlobeMap = defineAsyncComponent(() => import('@/components/GlobeMap.vue'));

const productModules = Object.freeze([
  { value: 'optical', label: '光学遥感', title: '光学遥感数据剖分' },
  { value: 'carbon', label: '碳卫星', title: '碳卫星数据剖分' },
  { value: 'radar', label: '雷达遥感', title: '雷达遥感数据剖分' },
  { value: 'product', label: '信息产品', title: '信息产品数据剖分' },
]);
const modules = Object.freeze([
  ...productModules,
  { value: 'quality', label: '自动化质检' },
  { value: 'ingest', label: '数据管理与入库' },
  { value: 'tasks', label: '剖分任务队列' },
]);

const store = usePartitionStore();
const activeModule = ref('optical');
const datasetDrawerVisible = ref(false);
const gridPreviewLoading = ref(false);
const gridGeometriesByModule = ref({});
const gridPreviewMetaByModule = ref({});
const resultsByModule = ref({});
const errorsByModule = ref({});
let gridPreviewGeneration = 0;
const gridPreviewColors = Object.freeze({ geohash: '#2f73d9', mgrs: '#16836f', isea4h: '#d97706' });
const MAX_RENDERED_GRID_CELLS = 2000;
const moduleForms = ref(Object.fromEntries(productModules.map(({ value }) => [value, {
  gridType: value === 'carbon' ? 'isea4h' : 'geohash',
  requestedGridLevel: value === 'carbon' ? 5 : 4,
}])));

const activeProduct = computed(() => productModules.find((item) => item.value === activeModule.value) || null);
const activeDatasets = computed(() => store.form.datasets.filter((dataset) => dataset.data_type === activeModule.value));
const gridLevelLocked = computed(() => activeDatasets.value.length > 0
  && activeDatasets.value.some((dataset) => dataset.grid_level_unlocked !== true));

function fallbackGridLevel(gridType) {
  if (gridType === 'mgrs') return 1;
  if (gridType === 'isea4h') return activeModule.value === 'carbon' ? 5 : 6;
  return 4;
}

function recommendedGridLevel(dataset, gridType) {
  const definition = gridDefinition(gridType);
  const suggested = Number(dataset?.suggested_grid_levels?.[gridType]);
  return Number.isInteger(suggested) && suggested >= definition.minLevel && suggested <= definition.maxLevel
    ? suggested
    : fallbackGridLevel(gridType);
}

const formModel = computed({
  get: () => ({ ...store.form, ...moduleForms.value[activeModule.value] }),
  set: (value) => {
    if (activeProduct.value) {
      const gridTypeChanged = value.gridType !== moduleForms.value[activeModule.value].gridType;
      if (!gridTypeChanged && gridLevelLocked.value) return;
      const definition = gridDefinition(value.gridType);
      const requestedLevel = Number(value.requestedGridLevel);
      const requestedGridLevel = Number.isInteger(requestedLevel)
        && requestedLevel >= definition.minLevel
        && requestedLevel <= definition.maxLevel
        ? requestedLevel
        : definition.minLevel;
      const settings = {
        gridType: value.gridType,
        requestedGridLevel: gridTypeChanged && activeDatasets.value.length
          ? recommendedGridLevel(activeDatasets.value[0], value.gridType)
          : requestedGridLevel,
      };
      moduleForms.value[activeModule.value] = settings;
      store.form.datasets = store.form.datasets.map((dataset) => dataset.data_type === activeModule.value
        ? {
          ...dataset,
          grid_level_unlocked: gridTypeChanged ? false : dataset.grid_level_unlocked,
          partition: withFixedPartitionOptions({
            ...dataset.partition,
            grid_type: settings.gridType,
            requested_grid_level: gridTypeChanged
              ? recommendedGridLevel(dataset, settings.gridType)
              : settings.requestedGridLevel,
            partition_method: derivedPartitionMethod(settings.gridType),
          }),
        }
        : dataset);
    }
  },
});

function bboxGeometry(bbox) {
  if (!Array.isArray(bbox) || bbox.length !== 4) return null;
  const [west, south, east, north] = bbox.map(Number);
  if (![west, south, east, north].every(Number.isFinite) || west >= east || south >= north) return null;
  return {
    type: 'Polygon',
    coordinates: [[[west, south], [east, south], [east, north], [west, north], [west, south]]],
  };
}

const selectedGeometries = computed(() => store.form.datasets.flatMap((dataset, datasetIndex) => (
  (dataset.assets || []).map((asset, assetIndex) => {
    const geometry = bboxGeometry(asset.bbox);
    if (!geometry) return null;
    return {
      geometry,
      label: dataset.dataset_title || dataset.dataset_code || dataset.dataset_id,
      color: ['#2f73d9', '#16836f', '#d97706', '#7c3aed'][(datasetIndex + assetIndex) % 4],
      fillOpacity: 0.18,
      weight: 2,
    };
  }).filter(Boolean)
)));
const activeGridGeometries = computed(() => gridGeometriesByModule.value[activeModule.value] || []);
const gridGeometries = computed(() => Object.values(gridGeometriesByModule.value).flat());
const gridPreviewLimited = computed(() => Boolean(gridPreviewMetaByModule.value[activeModule.value]?.limited));
const mapGeometries = computed(() => [...selectedGeometries.value, ...gridGeometries.value]);
const activeGridLegends = computed(() => {
  const legends = new Map();
  store.form.datasets.forEach((dataset) => {
    const partition = dataset.partition;
    if (!partition?.grid_type) return;
    const key = `${partition.grid_type}:${partition.requested_grid_level}`;
    if (!legends.has(key)) legends.set(key, {
      key,
      color: gridPreviewColors[partition.grid_type] || '#7c3aed',
      label: `${gridDefinition(partition.grid_type)?.label || partition.grid_type} · ${nativeLevelLabel(partition.grid_type, partition.requested_grid_level)}`,
    });
  });
  return [...legends.values()];
});
const mapHint = computed(() => {
  if (!activeDatasets.value.length) return '从' + (activeProduct.value?.label || '') + '待剖分队列选择数据后可预览范围';
  if (gridPreviewLoading.value) return '正在加载真实格网覆盖';
  if (activeGridGeometries.value.length) {
    const total = gridPreviewMetaByModule.value[activeModule.value]?.total || activeGridGeometries.value.length;
    return '已加载 ' + activeGridGeometries.value.length + ' 个格网单元'
      + (total > activeGridGeometries.value.length ? `（共 ${total} 个）` : '')
      + (gridPreviewLimited.value ? '（仅预览前 30 个数据单元范围）' : '');
  }
  return '已显示 ' + selectedGeometries.value.length + ' 个数据单元范围';
});
const activeSceneCount = computed(() => activeDatasets.value.reduce((total, dataset) => total + (dataset.scenes || []).length, 0));
const selectedSourceBatchIds = computed(() => [...new Set(activeDatasets.value.flatMap((dataset) => (
  (dataset.scenes || []).flatMap((scene) => [
    ...(Array.isArray(scene.source_batch_ids) ? scene.source_batch_ids : []),
    scene.load_batch_id,
  ])
)).map((value) => String(value || '').trim()).filter(Boolean))]);
const activeTaskCount = computed(() => store.tasks.filter((task) => ['queued', 'running', 'retrying', 'cancel_requested'].includes(task.status)).length);
const activeResult = computed(() => resultsByModule.value[activeModule.value] || null);
const activeError = computed(() => errorsByModule.value[activeModule.value] || '');

function selectModule(moduleName) {
  activeModule.value = moduleName;
  datasetDrawerVisible.value = false;
  gridPreviewLoading.value = false;
  if (moduleName === 'tasks') store.loadTasks(1, store.taskPage.pageSize);
}

function setModuleGridPreview(moduleName, geometries, meta = {}) {
  gridGeometriesByModule.value = { ...gridGeometriesByModule.value, [moduleName]: geometries };
  gridPreviewMetaByModule.value = { ...gridPreviewMetaByModule.value, [moduleName]: meta };
}

async function submit() {
  const moduleName = activeModule.value;
  const submittedModules = [...new Set(store.form.datasets.map((dataset) => dataset.data_type))];
  try {
    errorsByModule.value[moduleName] = '';
    Object.assign(store.form, moduleForms.value[moduleName]);
    const result = await store.submit();
    (submittedModules.length ? submittedModules : [moduleName]).forEach((item) => {
      resultsByModule.value[item] = result;
      errorsByModule.value[item] = '';
    });
    ElMessage.success('剖分任务已提交。');
  } catch (error) {
    errorsByModule.value[moduleName] = error.message || '提交剖分失败。';
    ElMessage.error(error.message || '提交剖分失败。');
  }
}

function reset() {
  const currentType = activeModule.value;
  store.form.datasets = store.form.datasets.filter((dataset) => dataset.data_type !== currentType);
  resultsByModule.value[currentType] = null;
  errorsByModule.value[currentType] = '';
  gridPreviewGeneration += 1;
  setModuleGridPreview(currentType, []);
}

async function loadGridPreview(partition, bbox) {
  const response = await requestJson('/v1/grid/cover', {
    grid_type: partition.grid_type,
    requested_grid_level: Number(partition.requested_grid_level),
    cover_mode: 'intersect',
    boundary_type: 'polygon',
    geometry: null,
    bbox,
    crs: 'EPSG:4326',
  });
  return (response.cells || []).map((cell) => ({ ...cell, preview_grid_type: partition.grid_type }));
}

async function loadMap() {
  if (!activeDatasets.value.length) {
    ElMessage.warning('请先选择' + (activeProduct.value?.label || '') + '数据集。');
    return;
  }
  if (!selectedGeometries.value.length) {
    ElMessage.warning('所选数据集没有可用于地图预览的范围。');
    return;
  }

  const generation = ++gridPreviewGeneration;
  const moduleName = activeModule.value;
  const requests = new Map();
  activeDatasets.value.forEach((dataset) => {
    const partition = dataset.partition || {
      grid_type: formModel.value.gridType,
      requested_grid_level: Number(formModel.value.requestedGridLevel),
    };
    (dataset.assets || []).forEach((asset) => {
      const bbox = Array.isArray(asset.bbox) ? asset.bbox.map(Number) : [];
      if (!bboxGeometry(bbox)) return;
      const key = [partition.grid_type, partition.requested_grid_level, bbox.join(',')].join(':');
      if (!requests.has(key)) requests.set(key, { partition, bbox });
    });
  });

  gridPreviewLoading.value = true;
  setModuleGridPreview(moduleName, []);
  try {
    const previewRequests = [...requests.values()].slice(0, 30);
    const settled = await Promise.allSettled(previewRequests.map(({ partition, bbox }) => loadGridPreview(partition, bbox)));
    if (generation !== gridPreviewGeneration) return;
    const successful = settled.filter((item) => item.status === 'fulfilled').map((item) => item.value);
    const failures = settled.filter((item) => item.status === 'rejected');
    if (!successful.length && failures.length) throw failures[0].reason;
    const cells = successful.flat();
    const stride = Math.max(1, Math.ceil(cells.length / MAX_RENDERED_GRID_CELLS));
    const uniqueKeys = new Set();
    const rendered = new Map();
    cells.forEach((cell) => {
      const geometry = cell.geometry || bboxGeometry(cell.bbox);
      if (!geometry || !cell.space_code) return;
      const key = [cell.preview_grid_type, cell.grid_level, cell.space_code].join(':');
      if (uniqueKeys.has(key)) return;
      uniqueKeys.add(key);
      if ((uniqueKeys.size - 1) % stride !== 0 || rendered.size >= MAX_RENDERED_GRID_CELLS) return;
      rendered.set(key, {
        geometry,
        label: cell.space_code,
        color: gridPreviewColors[cell.preview_grid_type] || '#7c3aed',
        fillColor: gridPreviewColors[cell.preview_grid_type] || '#7c3aed',
        fillOpacity: 0.07,
        weight: 1.5,
      });
    });
    const geometries = [...rendered.values()];
    const limited = requests.size > previewRequests.length || uniqueKeys.size > geometries.length;
    setModuleGridPreview(moduleName, geometries, { limited, total: uniqueKeys.size });
    if (failures.length) {
      ElMessage.warning(`已加载 ${geometries.length} 个格网单元，${failures.length} 个范围加载失败。`);
    } else {
      ElMessage.success('已加载 ' + geometries.length + ' 个格网单元' + (limited ? `（总计 ${uniqueKeys.size} 个）` : '') + '。');
    }
  } catch (error) {
    if (generation !== gridPreviewGeneration) return;
    ElMessage.error(error.message || '加载格网预览失败。');
  } finally {
    if (generation === gridPreviewGeneration) gridPreviewLoading.value = false;
  }
}

function resetGridPreview() {
  gridPreviewGeneration += 1;
  gridPreviewLoading.value = false;
  setModuleGridPreview(activeModule.value, []);
}

function updateDatasets(datasets) {
  store.form.datasets = datasets;
  const partitions = datasets
    .filter((dataset) => dataset.data_type === activeModule.value && dataset.partition)
    .map((dataset) => dataset.partition);
  if (partitions.length && partitions.every((partition) => (
    partition.grid_type === partitions[0].grid_type
    && Number(partition.requested_grid_level) === Number(partitions[0].requested_grid_level)
  ))) {
    moduleForms.value[activeModule.value] = {
      gridType: partitions[0].grid_type,
      requestedGridLevel: Number(partitions[0].requested_grid_level),
    };
  }
  gridPreviewGeneration += 1;
  setModuleGridPreview(activeModule.value, []);
}

onMounted(() => {
  store.loadBatches();
  store.loadTasks();
});
</script>

<template>
  <section>
    <section class="module-nav">
      <div class="container">
        <div class="module-tabs" aria-label="剖分页面导航">
        <button
          v-for="module in modules"
          :key="module.value"
          type="button"
          class="module-tab"
          :class="{ active: activeModule === module.value }"
          :data-testid="'partition-module-' + module.value"
          @click="selectModule(module.value)"
        >
          {{ module.label }}
        </button>
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
                <span>共 {{ store.taskPage.total }} 个任务</span>
              </div>
              <el-button :loading="store.loading.tasks" @click="store.loadTasks(1, store.taskPage.pageSize)">刷新</el-button>
            </div>
            <TaskQueuePanel
              :tasks="store.tasks"
              :page="store.taskPage"
              :loading="store.loading.tasks"
              @page-change="store.loadTasks($event, store.taskPage.pageSize)"
              @page-size-change="store.loadTasks(1, $event)"
            />
          </div>

          <div v-else-if="activeModule === 'quality'" class="module-page">
            <QualityView embedded />
          </div>

          <div v-else-if="activeModule === 'ingest'" class="module-page">
            <DataManagementView embedded />
          </div>

          <div v-else class="workspace">
            <div class="workspace-sidebar">
            <GridParameters
              v-model="formModel"
              :loading="store.loading.submit"
              :data-type-label="activeProduct.label"
              :selected-count="activeSceneCount"
              :selected-dataset-count="activeDatasets.length"
              :source-batch-ids="selectedSourceBatchIds"
              @open-datasets="datasetDrawerVisible = true"
              @reset="reset"
              @submit="submit"
            />
            </div>

            <div class="workspace-main">
              <div class="map-panel" aria-label="剖分范围地图">
                <div class="panel-header">
                  <div class="panel-header-main">
                    <h3>{{ activeProduct.label }}空间预览</h3>
                    <div class="map-actions">
                      <el-tag v-for="legend in activeGridLegends" :key="legend.key" size="small" class="grid-legend-tag">
                        <span class="grid-legend-dot" :style="{ backgroundColor: legend.color }" />{{ legend.label }}
                      </el-tag>
                      <el-button data-testid="load-map" size="small" :loading="gridPreviewLoading" @click="loadMap">加载格网</el-button>
                      <el-button data-testid="reset-grid" size="small" :icon="RefreshLeft" :disabled="!activeGridGeometries.length && !gridPreviewLoading" @click="resetGridPreview">重置</el-button>
                    </div>
                  </div>
                  <span class="map-hint">{{ mapHint }}</span>
                </div>
                <GlobeMap :geometries="mapGeometries" :zoom="4" />
              </div>
            </div>

            <div class="workspace-result">
              <div class="result-panel">
                <div class="result-panel-header">
                  <h3>执行结果</h3>
                  <el-button size="small" @click="selectModule('tasks')">任务队列</el-button>
                </div>
                <div class="results-content">
                  <div class="quality-section-title">剖分进程</div>
                  <div class="partition-context-grid">
                    <div class="partition-context-item"><span>当前数据集</span><strong>{{ activeDatasets.length }}</strong></div>
                    <div class="partition-context-item"><span>当前数据单元</span><strong>{{ activeSceneCount }}</strong></div>
                    <div class="partition-context-item"><span>来源批次</span><strong>{{ selectedSourceBatchIds.length }}</strong></div>
                    <div class="partition-context-item"><span>活动任务</span><strong>{{ activeTaskCount }}</strong></div>
                  </div>
                  <ExecutionResultPanel :result="activeResult" :error="activeError" />
                  <div v-if="!activeResult && !activeError" class="empty-state">
                    <p>配置参数并提交剖分任务</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <el-drawer
          v-if="activeProduct"
          v-model="datasetDrawerVisible"
          :title="activeProduct.label + '待剖分数据队列'"
          size="min(1100px, 94vw)"
          destroy-on-close
          class="partition-data-drawer"
        >
          <BatchAssetsPanel
            :model-value="store.form.datasets"
            :data-type-filter="activeModule"
            :data-type-label="activeProduct.label"
            :default-grid-type="formModel.gridType"
            :default-requested-grid-level="Number(formModel.requestedGridLevel)"
            @update:model-value="updateDatasets"
          />
        </el-drawer>
      </div>
    </main>
  </section>
</template>
