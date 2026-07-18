<script setup>
import { computed, onBeforeUnmount, onMounted, ref } from 'vue';
import { Collection, FolderOpened, Picture, Search, Unlock } from '@element-plus/icons-vue';

import { requestGet } from '@/api/client';
import { bandDisplayLabel, dataUnitTypeLabel, sceneBands, sceneMatchesBand } from '@/utils/bands';
import { derivedPartitionMethod, gridDefinition, gridDefinitions, nativeLevelLabel, withFixedPartitionOptions } from '@/utils/grid';

const props = defineProps({
  modelValue: { type: Array, default: () => [] },
  defaultGridType: { type: String, default: 'geohash' },
  defaultRequestedGridLevel: { type: Number, default: 4 },
  dataTypeFilter: { type: String, default: '' },
  dataTypeLabel: { type: String, default: '' },
});
const emit = defineEmits(['update:modelValue']);
const loading = ref(false);
const error = ref('');
const availableBatches = ref([]);
const selectedBatchIds = ref([]);
const availableDatasets = ref([]);
const availableBatchGroups = ref([]);
const selectedSceneIds = ref([]);
const bandKeyword = ref('');
let sceneRequestGeneration = 0;
let sceneRequestController = null;
let committedBatchState = { batchIds: [], datasets: [], batchGroups: [], sceneIds: [] };

function sceneSourceBatchIds(scene) {
  return [...new Set([
    ...(Array.isArray(scene?.source_batch_ids) ? scene.source_batch_ids : []),
    scene?.load_batch_id,
  ].map((value) => String(value || '').trim()).filter(Boolean))];
}

function normalizedPartition(gridType, requestedGridLevel) {
  const definition = gridDefinition(gridType) || gridDefinition('geohash');
  const level = Number(requestedGridLevel);
  const fallbackLevel = fallbackGridLevel(null, definition.value);
  const safeLevel = Number.isInteger(level) && level >= definition.minLevel && level <= definition.maxLevel
    ? level
    : fallbackLevel;
  return withFixedPartitionOptions({
    grid_type: definition.value,
    requested_grid_level: safeLevel,
    partition_method: derivedPartitionMethod(definition.value),
  });
}

function suggestedLevel(dataset, gridType) {
  if (dataset?.data_type === 'carbon') return null;
  const level = Number(dataset?.suggested_grid_levels?.[gridType]);
  const definition = gridDefinition(gridType);
  return definition && Number.isInteger(level) && level >= definition.minLevel && level <= definition.maxLevel
    ? level
    : null;
}

function fallbackGridLevel(dataset, gridType) {
  if (gridType === 'mgrs') return 1;
  if (gridType === 'isea4h') return dataset?.data_type === 'carbon' ? 5 : 6;
  return 4;
}

function defaultPartition(dataset) {
  return normalizedPartition(
    props.defaultGridType,
    suggestedLevel(dataset, props.defaultGridType) ?? fallbackGridLevel(dataset, props.defaultGridType),
  );
}

function selectedBatchesFromModel() {
  return [...new Set(props.modelValue.flatMap((dataset) => (
    Array.isArray(dataset.scenes) ? dataset.scenes.flatMap(sceneSourceBatchIds) : []
  )))];
}

function selectedScenesFromModel() {
  return props.modelValue
    .filter((dataset) => !props.dataTypeFilter || dataset.data_type === props.dataTypeFilter)
    .flatMap((dataset) => (dataset.scenes || []).map((scene) => scene.scene_id));
}

async function loadAvailable() {
  loading.value = true;
  error.value = '';
  try {
    const query = new URLSearchParams({ limit: '100', status: 'succeeded' });
    if (props.dataTypeFilter) query.set('data_type', props.dataTypeFilter);
    const response = await requestGet(`/v1/partition/load-batches?${query.toString()}`);
    availableBatches.value = Array.isArray(response?.load_batches)
      ? response.load_batches.filter((batch) => batch.status === 'succeeded')
      : [];
    selectedBatchIds.value = selectedBatchesFromModel().filter((batchId) => (
      availableBatches.value.some((batch) => batch.load_batch_id === batchId)
    ));
    selectedSceneIds.value = selectedScenesFromModel();
    if (selectedBatchIds.value.length) await loadSelectedBatches(selectedBatchIds.value);
  } catch (caught) {
    error.value = caught.message || '载入批次失败。';
  } finally {
    loading.value = false;
  }
}

function mergeBatchDatasets(responses) {
  const datasets = new Map();
  responses.forEach(({ batchId, response }) => {
    (response?.datasets || []).forEach((dataset) => {
      const existing = datasets.get(dataset.dataset_id) || { ...dataset, scenes: new Map() };
      const incomingResolution = Number(dataset.resolution_m);
      const existingResolution = Number(existing.resolution_m);
      if (Number.isFinite(incomingResolution) && incomingResolution > 0
          && (!Number.isFinite(existingResolution) || existingResolution <= 0 || incomingResolution < existingResolution)) {
        existing.resolution_m = incomingResolution;
        existing.suggested_grid_levels = dataset.suggested_grid_levels;
      }
      (dataset.scenes || []).forEach((scene) => {
        const prior = existing.scenes.get(scene.scene_id);
        if (prior && prior.dataset_id && scene.dataset_id && prior.dataset_id !== scene.dataset_id) {
          throw new Error(`数据单元 ${scene.scene_id} 在不同数据集中重复出现。`);
        }
        const sourceBatchIds = [...new Set([
          ...sceneSourceBatchIds(prior),
          ...sceneSourceBatchIds(scene),
          batchId,
        ])];
        const sourceLoadStatuses = {
          ...(prior?.source_load_statuses || {}),
          [batchId]: scene.load_status || scene.status || 'pending',
        };
        const eligibleSourceBatchIds = sourceBatchIds.filter((sourceBatchId) => (
          ['succeeded', 'duplicate'].includes(sourceLoadStatuses[sourceBatchId])
        ));
        existing.scenes.set(scene.scene_id, {
          ...prior,
          ...scene,
          source_batch_ids: sourceBatchIds,
          eligible_source_batch_ids: eligibleSourceBatchIds,
          source_load_statuses: sourceLoadStatuses,
        });
      });
      datasets.set(dataset.dataset_id, existing);
    });
  });
  return [...datasets.values()].map((dataset) => ({ ...dataset, scenes: [...dataset.scenes.values()] }));
}

function buildBatchGroups(responses, mergedDatasets) {
  const mergedScenes = new Map(mergedDatasets.flatMap((dataset) => (
    dataset.scenes.map((scene) => [scene.scene_id, scene])
  )));
  const batchesById = new Map(availableBatches.value.map((batch) => [batch.load_batch_id, batch]));
  return responses.map(({ batchId, response }) => ({
    ...(batchesById.get(batchId) || {}),
    ...(response?.load_batch || {}),
    load_batch_id: batchId,
    datasets: (response?.datasets || []).map((dataset) => ({
      ...dataset,
      scenes: (dataset.scenes || []).map((scene) => {
        const merged = mergedScenes.get(scene.scene_id);
        return merged ? {
          ...merged,
          ...scene,
          source_batch_ids: merged.source_batch_ids,
          eligible_source_batch_ids: merged.eligible_source_batch_ids,
          source_load_statuses: merged.source_load_statuses,
        } : scene;
      }),
    })),
  }));
}

async function loadSelectedBatches(batchIds) {
  const ids = [...new Set(batchIds)];
  selectedBatchIds.value = ids;
  const generation = ++sceneRequestGeneration;
  sceneRequestController?.abort();
  sceneRequestController = new AbortController();
  loading.value = true;
  error.value = '';
  try {
    const query = props.dataTypeFilter ? `?data_type=${encodeURIComponent(props.dataTypeFilter)}` : '';
    const responses = await Promise.all(ids.map(async (batchId) => ({
      batchId,
      response: await requestGet(
        `/v1/partition/load-batches/${encodeURIComponent(batchId)}/scenes${query}`,
        { signal: sceneRequestController.signal },
      ),
    })));
    if (generation !== sceneRequestGeneration) return;
    availableDatasets.value = mergeBatchDatasets(responses);
    availableBatchGroups.value = buildBatchGroups(responses, availableDatasets.value);
    const availableSceneIds = new Set(availableDatasets.value.flatMap((dataset) => dataset.scenes.map((scene) => scene.scene_id)));
    selectedSceneIds.value = selectedSceneIds.value.filter((sceneId) => availableSceneIds.has(sceneId));
    committedBatchState = {
      batchIds: [...ids],
      datasets: availableDatasets.value,
      batchGroups: availableBatchGroups.value,
      sceneIds: [...selectedSceneIds.value],
    };
    updateSceneSelection(selectedSceneIds.value);
  } catch (caught) {
    if (generation !== sceneRequestGeneration || caught?.name === 'AbortError') return;
    error.value = caught.message || '载入批次数据单元失败。';
    selectedBatchIds.value = [...committedBatchState.batchIds];
    availableDatasets.value = committedBatchState.datasets;
    availableBatchGroups.value = committedBatchState.batchGroups;
    selectedSceneIds.value = [...committedBatchState.sceneIds];
  } finally {
    if (generation === sceneRequestGeneration) loading.value = false;
  }
}

function scenePreviewAsset(scene) {
  return {
    source_asset_id: scene.source_asset_id || scene.scene_id,
    source_uri: scene.source_uri,
    bbox: scene.bbox,
    crs: scene.crs,
    time_start: scene.acquisition_time,
    time_end: scene.acquisition_time,
  };
}

function updateSceneSelection(sceneIds) {
  selectedSceneIds.value = [...new Set(sceneIds)];
  if (
    selectedBatchIds.value.length === committedBatchState.batchIds.length
    && selectedBatchIds.value.every((batchId, index) => batchId === committedBatchState.batchIds[index])
  ) {
    committedBatchState = { ...committedBatchState, sceneIds: [...selectedSceneIds.value] };
  }
  const selected = new Set(selectedSceneIds.value);
  const existingById = new Map(props.modelValue.map((dataset) => [dataset.dataset_id, dataset]));
  const retained = props.dataTypeFilter
    ? props.modelValue.filter((dataset) => dataset.data_type !== props.dataTypeFilter)
    : [];
  const current = availableDatasets.value.flatMap((dataset) => {
    const scenes = dataset.scenes.filter((scene) => selected.has(scene.scene_id));
    if (!scenes.length) return [];
    const existing = existingById.get(dataset.dataset_id);
    return [{
      dataset_id: dataset.dataset_id,
      dataset_code: dataset.dataset_code,
      dataset_title: dataset.dataset_title,
      data_type: dataset.data_type,
      product_type: dataset.product_type ?? null,
      resolution_m: dataset.resolution_m ?? null,
      suggested_grid_levels: dataset.suggested_grid_levels ?? {},
      grid_level_unlocked: existing?.grid_level_unlocked === true,
      scenes,
      assets: scenes.map(scenePreviewAsset),
      partition: existing?.partition ? withFixedPartitionOptions(existing.partition) : defaultPartition(dataset),
    }];
  });
  emit('update:modelValue', [...retained, ...current]);
}

const selectedCount = computed(() => selectedSceneIds.value.length);
const selectedDatasetsById = computed(() => new Map(props.modelValue.map((dataset) => [dataset.dataset_id, dataset])));
const visibleBatchGroups = computed(() => availableBatchGroups.value.map((batch) => ({
  ...batch,
  datasets: batch.datasets.map((dataset) => ({
    ...dataset,
    scenes: dataset.scenes.filter((scene) => sceneMatchesBand(scene, dataset.data_type, bandKeyword.value)),
  })).filter((dataset) => dataset.scenes.length),
})).filter((batch) => batch.datasets.length));

function bandsFor(scene, dataType) {
  return sceneBands(scene, dataType);
}

function partitionFor(dataset) {
  const partition = selectedDatasetsById.value.get(dataset.dataset_id)?.partition;
  return partition ? withFixedPartitionOptions(partition) : defaultPartition(dataset);
}

function gridLevelLocked(datasetId) {
  return selectedDatasetsById.value.has(datasetId)
    && selectedDatasetsById.value.get(datasetId)?.grid_level_unlocked !== true;
}

function unlockGridLevel(datasetId) {
  emit('update:modelValue', props.modelValue.map((dataset) => (
    dataset.dataset_id === datasetId ? { ...dataset, grid_level_unlocked: true } : dataset
  )));
}

function levelOptions(gridType) {
  const definition = gridDefinition(gridType);
  if (!definition) return [];
  return Array.from({ length: definition.maxLevel - definition.minLevel + 1 }, (_, index) => definition.minLevel + index);
}

function updatePartition(datasetId, patch) {
  emit('update:modelValue', props.modelValue.map((dataset) => {
    if (dataset.dataset_id !== datasetId) return dataset;
    if (patch.requested_grid_level !== undefined && gridLevelLocked(datasetId)) return dataset;
    const partition = { ...partitionFor(dataset), ...patch };
    let gridLevelUnlocked = dataset.grid_level_unlocked === true;
    if (patch.grid_type) {
      const recommendedLevel = suggestedLevel(dataset, partition.grid_type);
      if (recommendedLevel !== null) {
        partition.requested_grid_level = recommendedLevel;
      } else {
        partition.requested_grid_level = fallbackGridLevel(dataset, partition.grid_type);
      }
      partition.partition_method = derivedPartitionMethod(partition.grid_type);
      gridLevelUnlocked = false;
    }
    return { ...dataset, partition, grid_level_unlocked: gridLevelUnlocked };
  }));
}

function eligibleScene(scene) {
  return Array.isArray(scene.eligible_source_batch_ids)
    ? scene.eligible_source_batch_ids.length > 0
    : ['succeeded', 'duplicate'].includes(scene.load_status || 'succeeded');
}

onMounted(loadAvailable);
onBeforeUnmount(() => {
  sceneRequestGeneration += 1;
  sceneRequestController?.abort();
});
</script>

<template>
  <section class="partition-data-list">
    <div class="partition-drawer-heading">
      <h3>{{ dataTypeLabel || '' }}待剖分数据</h3>
      <span>{{ selectedBatchIds.length }} 个批次 · {{ availableDatasets.length }} 个数据集 · 已选 {{ selectedCount }} 个数据单元</span>
    </div>
    <el-alert v-if="error" :title="error" type="error" :closable="false" show-icon />

    <div class="partition-batch-picker">
      <div class="partition-section-title"><strong>来源载入批次</strong><span>{{ availableBatches.length }} 个可用批次</span></div>
      <el-checkbox-group :model-value="selectedBatchIds" data-testid="load-batch-selector" @update:model-value="loadSelectedBatches">
        <el-checkbox v-for="batch in availableBatches" :key="batch.load_batch_id" :value="batch.load_batch_id" :data-testid="`load-batch-${batch.load_batch_id}`">
          <span class="partition-batch-option">
            <strong>{{ batch.batch_name || batch.load_batch_id }}</strong>
            <span>{{ batch.load_batch_id }} · {{ batch.dataset_count || 0 }} 个数据集 · {{ batch.scene_count || 0 }} 个数据单元</span>
          </span>
        </el-checkbox>
      </el-checkbox-group>
      <div v-if="!loading && !availableBatches.length" class="empty-state">暂无已完成且包含当前产品数据的载入批次</div>
    </div>

    <div class="band-filter-toolbar">
      <div><strong>波段筛选</strong></div>
      <el-input v-model="bandKeyword" :prefix-icon="Search" clearable placeholder="例如 B04、VV、NDVI 或 ppm" />
    </div>

    <div v-loading="loading" class="partition-batch-tree-list">
      <section v-for="batch in visibleBatchGroups" :key="batch.load_batch_id" class="partition-batch-tree" :data-testid="`batch-tree-${batch.load_batch_id}`">
        <header class="partition-batch-tree-header">
          <div class="partition-tree-identity">
            <el-icon><FolderOpened /></el-icon>
            <div><strong>{{ batch.batch_name || batch.load_batch_id }}</strong><span>{{ batch.load_batch_id }}</span></div>
          </div>
          <span>{{ batch.datasets.length }} 个数据集 · {{ batch.datasets.reduce((total, dataset) => total + dataset.scenes.length, 0) }} 个数据单元</span>
        </header>

        <div class="partition-dataset-tree-list">
          <section v-for="dataset in batch.datasets" :key="`${batch.load_batch_id}-${dataset.dataset_id}`" class="partition-dataset-tree" :data-testid="`dataset-tree-${batch.load_batch_id}-${dataset.dataset_id}`">
            <div class="partition-scene-group-header">
              <div class="partition-tree-identity">
                <el-icon><Collection /></el-icon>
                <div><strong>{{ dataset.dataset_title || dataset.dataset_code || dataset.dataset_id }}</strong><span>{{ dataset.dataset_code || dataset.dataset_id }} · {{ dataset.scenes.length }} 个数据单元<span v-if="dataset.resolution_m"> · {{ dataset.resolution_m }} m</span></span></div>
              </div>
              <div v-if="selectedDatasetsById.has(dataset.dataset_id)" class="partition-dataset-grid">
                <el-select
                  :data-testid="`dataset-grid-${dataset.dataset_id}`"
                  :model-value="partitionFor(dataset).grid_type"
                  @update:model-value="updatePartition(dataset.dataset_id, { grid_type: $event })"
                >
                  <el-option v-for="grid in gridDefinitions" :key="grid.value" :label="grid.label" :value="grid.value" />
                </el-select>
                <el-select
                  :data-testid="`dataset-grid-level-${dataset.dataset_id}`"
                  :disabled="gridLevelLocked(dataset.dataset_id)"
                  :model-value="Number(partitionFor(dataset).requested_grid_level)"
                  @update:model-value="updatePartition(dataset.dataset_id, { requested_grid_level: Number($event) })"
                >
                  <el-option v-for="level in levelOptions(partitionFor(dataset).grid_type)" :key="level" :label="nativeLevelLabel(partitionFor(dataset).grid_type, level)" :value="level" />
                </el-select>
                <el-tooltip v-if="gridLevelLocked(dataset.dataset_id)" content="解锁格网层级" placement="top">
                  <el-button :data-testid="`unlock-grid-level-${dataset.dataset_id}`" :icon="Unlock" aria-label="解锁格网层级" @click="unlockGridLevel(dataset.dataset_id)" />
                </el-tooltip>
              </div>
            </div>
            <el-checkbox-group class="partition-scene-list" :model-value="selectedSceneIds" @update:model-value="updateSceneSelection">
              <el-checkbox
                v-for="scene in dataset.scenes"
                :key="scene.scene_id"
                :value="scene.scene_id"
                :disabled="!eligibleScene(scene)"
                :data-testid="`scene-${scene.scene_id}`"
              >
                <span class="partition-scene-option">
                  <el-icon><Picture /></el-icon>
                  <span>
                    <strong>{{ scene.scene_key || scene.scene_id }}</strong>
                    <small>{{ dataUnitTypeLabel(dataset.data_type) }} · {{ scene.acquisition_time || '采集时间未登记' }}</small>
                    <span v-if="bandsFor(scene, dataset.data_type).length" class="scene-band-list">
                      <span v-for="(band, bandIndex) in bandsFor(scene, dataset.data_type)" :key="`${scene.scene_id}-${band.band_code || 'invalid'}-${band.display_order}-${bandIndex}`" class="scene-band-chip">{{ bandDisplayLabel(band) }}</span>
                    </span>
                    <small v-else class="band-missing">波段信息未登记</small>
                  </span>
                </span>
              </el-checkbox>
            </el-checkbox-group>
          </section>
        </div>
      </section>
      <div v-if="!loading && selectedBatchIds.length && !availableDatasets.length" class="empty-state">所选批次没有当前产品可剖分的数据单元</div>
      <div v-else-if="!loading && selectedBatchIds.length && !visibleBatchGroups.length" class="empty-state">没有匹配当前波段筛选条件的数据单元</div>
    </div>
  </section>
</template>

<style scoped>
.partition-batch-picker { padding: 12px 0 18px; border-bottom: 1px solid var(--el-border-color-lighter); }
.partition-section-title { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 10px; }
.partition-section-title span { color: var(--el-text-color-secondary); font-size: 12px; }
.partition-batch-picker :deep(.el-checkbox-group) { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 8px 16px; }
.partition-batch-picker :deep(.el-checkbox) { align-items: flex-start; height: auto; margin: 0; padding: 8px 0; }
.partition-batch-picker :deep(.el-checkbox__label), .partition-scene-list :deep(.el-checkbox__label) { min-width: 0; white-space: normal; }
.partition-batch-option { display: flex; min-width: 0; flex-direction: column; gap: 3px; }
.partition-batch-option strong { color: var(--el-text-color-primary); font-size: 13px; line-height: 1.4; }
.partition-batch-option span { color: var(--el-text-color-secondary); font-size: 12px; overflow-wrap: anywhere; }
.partition-batch-tree-list { min-height: 180px; padding-top: 14px; }
.band-filter-toolbar { display: flex; align-items: flex-end; justify-content: space-between; gap: 18px; padding: 14px 0 2px; }
.band-filter-toolbar > div { display: flex; flex-direction: column; gap: 3px; }
.band-filter-toolbar strong { color: var(--el-text-color-primary); font-size: 13px; }
.band-filter-toolbar span { color: var(--el-text-color-secondary); font-size: 12px; }
.band-filter-toolbar .el-input { width: min(360px, 48%); }
.partition-batch-tree { border-left: 3px solid var(--el-color-primary); }
.partition-batch-tree + .partition-batch-tree { margin-top: 18px; }
.partition-batch-tree-header { display: flex; align-items: center; justify-content: space-between; gap: 16px; min-height: 48px; padding: 8px 12px; background: var(--el-fill-color-light); border-bottom: 1px solid var(--el-border-color-lighter); }
.partition-batch-tree-header > span { flex: none; color: var(--el-text-color-secondary); font-size: 12px; }
.partition-dataset-tree-list { padding-left: 22px; }
.partition-dataset-tree { padding: 14px 0 14px 14px; border-bottom: 1px solid var(--el-border-color-lighter); }
.partition-tree-identity { display: flex; min-width: 0; align-items: center; gap: 9px; }
.partition-tree-identity > .el-icon { flex: none; color: var(--el-color-primary); font-size: 17px; }
.partition-tree-identity > div { display: flex; min-width: 0; flex-direction: column; gap: 2px; }
.partition-tree-identity strong { overflow-wrap: anywhere; }
.partition-tree-identity span { color: var(--el-text-color-secondary); font-size: 12px; overflow-wrap: anywhere; }
.partition-scene-group-header { display: flex; align-items: center; justify-content: space-between; gap: 16px; margin-bottom: 10px; }
.partition-dataset-grid { display: grid; grid-template-columns: minmax(130px, 1fr) minmax(170px, 1fr) 32px; gap: 8px; width: min(420px, 56%); }
.partition-dataset-grid :deep(.el-button) { width: 32px; padding: 0; }
.partition-scene-list { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 6px 16px; }
.partition-scene-list :deep(.el-checkbox) { align-items: flex-start; height: auto; margin: 0; padding: 7px 0; }
.partition-scene-option { display: flex; min-width: 0; align-items: flex-start; gap: 8px; }
.partition-scene-option > .el-icon { flex: none; margin-top: 2px; color: var(--el-text-color-secondary); }
.partition-scene-option > span { display: flex; min-width: 0; flex-direction: column; gap: 2px; }
.partition-scene-option strong { color: var(--el-text-color-primary); font-size: 13px; font-weight: 500; overflow-wrap: anywhere; }
.partition-scene-option small { color: var(--el-text-color-secondary); font-size: 11px; line-height: 1.4; overflow-wrap: anywhere; }
.scene-band-list { display: flex; flex-wrap: wrap; gap: 4px; margin-top: 3px; }
.scene-band-chip { padding: 2px 6px; border: 1px solid #bfd5e8; border-radius: 4px; background: #f3f8fc; color: #2d628d; font-size: 11px; line-height: 1.35; }
.band-missing { color: #a16a1c !important; }
@media (max-width: 720px) {
  .partition-drawer-heading { align-items: flex-start; flex-direction: column; gap: 4px; }
  .partition-batch-picker :deep(.el-checkbox-group), .partition-scene-list { grid-template-columns: 1fr; }
  .partition-batch-tree-header, .partition-scene-group-header { align-items: stretch; flex-direction: column; }
  .partition-batch-tree-header > span { padding-left: 26px; }
  .partition-dataset-tree-list { padding-left: 10px; }
  .partition-dataset-tree { padding-left: 10px; }
  .partition-dataset-grid { grid-template-columns: minmax(0, 1fr) 32px; width: 100%; }
  .partition-dataset-grid > :first-child { grid-column: 1 / -1; }
  .band-filter-toolbar { align-items: stretch; flex-direction: column; }
  .band-filter-toolbar .el-input { width: 100%; }
}
</style>
