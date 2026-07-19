<script setup>
import { computed, defineAsyncComponent, onMounted, ref } from 'vue';
import { ElMessage } from 'element-plus';
import { RefreshLeft } from '@element-plus/icons-vue';

import { requestGet, requestJson, requestPost } from '@/api/client';
import router from '@/router';
import { usePartitionStore } from '@/stores/partition';
import { takePartitionSelection } from '@/stores/partitionTransfer';
import { derivedPartitionMethod, gridDefinition, nativeLevelLabel, withFixedPartitionOptions } from '@/utils/grid';
import DataManagementView from '@/views/DataManagementView.vue';
import QualityView from '@/views/QualityView.vue';
import BatchAssetsPanel from '@/views/partition/BatchAssetsPanel.vue';
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
const pendingDrafts = ref([]);
const activeDraftId = ref('');
let gridPreviewGeneration = 0;
const gridPreviewColors = Object.freeze({ geohash: '#2f73d9', mgrs: '#16836f', isea4h: '#d97706' });
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

function normalizeBbox(bbox) {
  if (!Array.isArray(bbox) || bbox.length !== 4) return null;
  const [rawWest, rawSouth, rawEast, rawNorth] = bbox.map(Number);
  if (![rawWest, rawSouth, rawEast, rawNorth].every(Number.isFinite)) return null;
  const west = Math.max(-180, Math.min(180, rawWest));
  const south = Math.max(-90, Math.min(90, rawSouth));
  const east = Math.max(-180, Math.min(180, rawEast));
  const north = Math.max(-90, Math.min(90, rawNorth));
  if (![west, south, east, north].every(Number.isFinite) || west >= east || south >= north) return null;
  return [west, south, east, north];
}

function bboxGeometry(bbox) {
  const normalized = normalizeBbox(bbox);
  if (!normalized) return null;
  const [west, south, east, north] = normalized;
  const displaySouth = Math.max(-89.999, south);
  const displayNorth = Math.min(89.999, north);
  // Cesium rejects a ground polygon edge when its endpoints are nearly antipodal.
  if (west <= -179.999 && east >= 179.999 && south <= -89.999 && north >= 89.999) {
    const longitudes = [-180, -90, 0, 90, 180];
    const latitudes = [displaySouth, 0, displayNorth];
    return {
      type: 'MultiPolygon',
      coordinates: longitudes.slice(0, -1).flatMap((sliceWest, longitudeIndex) => (
        latitudes.slice(0, -1).map((sliceSouth, latitudeIndex) => {
          const sliceEast = longitudes[longitudeIndex + 1];
          const sliceNorth = latitudes[latitudeIndex + 1];
          return [[
            [sliceWest, sliceSouth],
            [sliceEast, sliceSouth],
            [sliceEast, sliceNorth],
            [sliceWest, sliceNorth],
            [sliceWest, sliceSouth],
          ]];
        })
      )),
    };
  }
  return {
    type: 'Polygon',
    coordinates: [[
      [west, displaySouth], [east, displaySouth], [east, displayNorth],
      [west, displayNorth], [west, displaySouth],
    ]],
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
// The grid preview already covers the selected source extent. Keeping repeated
// global footprints in the same Cesium ground batch can cause mismatched
// attribute lists and adds no visual information.
const mapGeometries = computed(() => (
  gridGeometries.value.length ? gridGeometries.value : selectedGeometries.value
));
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
const activeBandUnitCount = computed(() => activeDatasets.value.reduce((total, dataset) => (
  total + (Array.isArray(dataset.band_unit_ids) ? dataset.band_unit_ids.length : (dataset.scenes || []).length)
), 0));
const selectedSourceBatchIds = computed(() => [...new Set(activeDatasets.value.flatMap((dataset) => (
  (dataset.scenes || []).flatMap((scene) => [
    ...(Array.isArray(scene.source_batch_ids) ? scene.source_batch_ids : []),
    scene.load_batch_id,
  ])
)).map((value) => String(value || '').trim()).filter(Boolean))]);

function selectModule(moduleName) {
  activeModule.value = moduleName;
  datasetDrawerVisible.value = false;
  gridPreviewLoading.value = false;
  if (moduleName === 'tasks') store.loadTasks(1, store.taskPage.pageSize);
}

function selectDraft(draft) {
  const datasets = draft?.selection?.datasets || [];
  if (!datasets.length) return;
  store.form.datasets = datasets;
  activeDraftId.value = draft.draft_id;
  activeModule.value = draft.data_type;
  const partition = datasets[0]?.partition;
  moduleForms.value[draft.data_type] = {
    gridType: partition.grid_type,
    requestedGridLevel: Number(partition.requested_grid_level),
  };
  datasetDrawerVisible.value = false;
  gridPreviewGeneration += 1;
  setModuleGridPreview(draft.data_type, []);
  ElMessage.success('已载入待剖分批次，请确认后提交。');
}

function queueManagedPartition(draft) {
  selectDraft(draft);
}

async function loadDrafts() {
  const response = await requestGet('/v1/partition/drafts?limit=100');
  pendingDrafts.value = response.items || [];
}

function setModuleGridPreview(moduleName, geometries, meta = {}) {
  gridGeometriesByModule.value = { ...gridGeometriesByModule.value, [moduleName]: geometries };
  gridPreviewMetaByModule.value = { ...gridPreviewMetaByModule.value, [moduleName]: meta };
}

async function submit() {
  const moduleName = activeModule.value;
  try {
    Object.assign(store.form, moduleForms.value[moduleName]);
    const response = await store.submit();
    if (activeDraftId.value) {
      await requestPost(`/v1/partition/drafts/${encodeURIComponent(activeDraftId.value)}/submitted`, { partition_run_id: response.partition_run_id });
      activeDraftId.value = '';
      await loadDrafts();
    }
    ElMessage.success('剖分任务已提交。');
  } catch (error) {
    ElMessage.error(error.message || '提交剖分失败。');
  }
}

function reset() {
  const currentType = activeModule.value;
  store.form.datasets = store.form.datasets.filter((dataset) => dataset.data_type !== currentType);
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
      const bbox = normalizeBbox(asset.bbox);
      if (!bbox) return;
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
    const uniqueKeys = new Set();
    const rendered = new Map();
    cells.forEach((cell) => {
      const geometry = cell.geometry || bboxGeometry(cell.bbox);
      if (!geometry || !cell.space_code) return;
      const key = [cell.preview_grid_type, cell.grid_level, cell.space_code].join(':');
      if (uniqueKeys.has(key)) return;
      uniqueKeys.add(key);
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
    const limited = requests.size > previewRequests.length;
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
  const pendingSelection = takePartitionSelection();
  if (pendingSelection) queueManagedPartition(pendingSelection);
  const requestedModule = String(router.currentRoute.value.query.module || pendingSelection?.data_type || '');
  if (productModules.some((item) => item.value === requestedModule)) activeModule.value = requestedModule;
  const queued = store.form.datasets.find((dataset) => dataset.data_type === activeModule.value && dataset.partition);
  if (queued) {
    moduleForms.value[activeModule.value] = {
      gridType: queued.partition.grid_type,
      requestedGridLevel: Number(queued.partition.requested_grid_level),
    };
  }
  store.loadBatches();
  store.loadTasks();
  loadDrafts().then(() => {
    const requestedDraftId = String(router.currentRoute.value.query.draft_id || '');
    const draft = pendingSelection || pendingDrafts.value.find((item) => item.draft_id === requestedDraftId);
    if (draft) selectDraft(draft);
  }).catch((error) => { ElMessage.error(error.message || '待剖分批次加载失败。'); });
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
            <DataManagementView embedded @queue-partition="queueManagedPartition" />
          </div>

          <div v-else class="workspace">
            <div class="workspace-sidebar">
            <GridParameters
              v-model="formModel"
              :loading="store.loading.submit"
              :data-type-label="activeProduct.label"
              :selected-count="activeBandUnitCount"
              :selected-dataset-count="activeDatasets.length"
              :source-batch-ids="selectedSourceBatchIds"
              @open-datasets="datasetDrawerVisible = true"
              @reset="reset"
              @submit="submit"
            />
            </div>

            <div class="workspace-main">
              <div class="map-panel" aria-label="剖分范围地图">
                <div class="panel-header"><h3>地图</h3></div>
                <div class="map-canvas-wrap">
                  <GlobeMap :geometries="mapGeometries" :zoom="4" />
                  <div class="map-overlay-actions">
                    <div class="map-actions">
                      <el-tag v-for="legend in activeGridLegends" :key="legend.key" size="small" class="grid-legend-tag">
                        <span class="grid-legend-dot" :style="{ backgroundColor: legend.color }" />{{ legend.label }}
                      </el-tag>
                      <el-button data-testid="load-map" size="small" :loading="gridPreviewLoading" @click="loadMap">加载格网</el-button>
                      <el-button data-testid="reset-grid" size="small" :icon="RefreshLeft" :disabled="!activeGridGeometries.length && !gridPreviewLoading" @click="resetGridPreview">重置</el-button>
                    </div>
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
            :partition-drafts="pendingDrafts.filter((draft) => draft.data_type === activeModule)"
            :active-partition-draft-id="activeDraftId"
            @update:model-value="updateDatasets"
            @activate-partition-draft="activeDraftId = $event"
          />
        </el-drawer>
      </div>
    </main>
  </section>
</template>
