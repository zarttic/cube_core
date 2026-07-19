<script setup>
import { computed, onBeforeUnmount, onMounted, ref } from 'vue';
import { Collection, FolderOpened, Picture, Refresh, Search, Unlock } from '@element-plus/icons-vue';

import { requestGet } from '@/api/client';
import { bandDisplayLabel, dataUnitTypeLabel, sceneBands, sceneMatchesBand } from '@/utils/bands';
import { derivedPartitionMethod, gridDefinition, gridDefinitions, nativeLevelLabel, withFixedPartitionOptions } from '@/utils/grid';
import { formatShanghaiTime } from '@/utils/time';

const props = defineProps({
  modelValue: { type: Array, default: () => [] },
  defaultGridType: { type: String, default: 'geohash' },
  defaultRequestedGridLevel: { type: Number, default: 4 },
  dataTypeFilter: { type: String, default: '' },
  dataTypeLabel: { type: String, default: '' },
  partitionDrafts: { type: Array, default: () => [] },
  activePartitionDraftId: { type: String, default: '' },
});
const emit = defineEmits(['update:modelValue', 'activate-partition-draft']);
const loading = ref(false);
const error = ref('');
const availableBatches = ref([]);
const selectedBatchIds = ref([]);
const availableDatasets = ref([]);
const availableBatchGroups = ref([]);
const selectedSceneIds = ref([]);
const selectedBandUnitIds = ref([]);
const bandKeyword = ref('');
const collapsedBatches = ref(new Set());
const collapsedDatasets = ref(new Set());
const collapsedScenes = ref(new Set());
let sceneRequestGeneration = 0;
let sceneRequestController = null;
let refreshTimer = null;
let committedBatchState = { batchIds: [], datasets: [], batchGroups: [], sceneIds: [], bandUnitIds: [] };
const REFRESH_INTERVAL_MS = 20_000;

function sceneSourceBatchIds(scene) {
  return [...new Set([
    ...(Array.isArray(scene?.source_batch_ids) ? scene.source_batch_ids : []),
    scene?.load_batch_id,
  ].map((value) => String(value || '').trim()).filter(Boolean))];
}

function batchGroupKey(batch) { return String(batch.load_batch_id); }
function datasetGroupKey(batch, dataset) { return `${batch.load_batch_id}:${dataset.dataset_id}`; }
function toggleBatchGroup(batch) {
  const key = batchGroupKey(batch);
  const next = new Set(collapsedBatches.value);
  if (next.has(key)) next.delete(key); else next.add(key);
  collapsedBatches.value = next;
}
function toggleDatasetGroup(batch, dataset) {
  const key = datasetGroupKey(batch, dataset);
  const next = new Set(collapsedDatasets.value);
  if (next.has(key)) next.delete(key); else next.add(key);
  collapsedDatasets.value = next;
}
function sceneGroupKey(dataset, scene) { return `${dataset.dataset_id}:${scene.scene_id}`; }
function toggleSceneGroup(dataset, scene) {
  const key = sceneGroupKey(dataset, scene);
  const next = new Set(collapsedScenes.value);
  if (next.has(key)) next.delete(key); else next.add(key);
  collapsedScenes.value = next;
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
  const recommendedGridType = ['geohash', 'mgrs'].includes(dataset?.suggested_grid_type)
    ? dataset.suggested_grid_type
    : props.defaultGridType;
  return normalizedPartition(
    recommendedGridType,
    suggestedLevel(dataset, recommendedGridType) ?? fallbackGridLevel(dataset, recommendedGridType),
  );
}

function resolutionLabel(dataset) {
  const resolution = Number(dataset?.resolution_native);
  if (!Number.isFinite(resolution) || resolution <= 0) return '';
  return dataset.resolution_unit === 'degree' ? `${resolution}°` : `${resolution} m`;
}

function selectedBatchesFromModel() {
  if (props.activePartitionDraftId) return [props.activePartitionDraftId];
  return [...new Set(props.modelValue.flatMap((dataset) => (
    Array.isArray(dataset.scenes) ? dataset.scenes.flatMap(sceneSourceBatchIds) : []
  )))];
}

function selectedScenesFromModel() {
  return props.modelValue
    .filter((dataset) => !props.dataTypeFilter || dataset.data_type === props.dataTypeFilter)
    .flatMap((dataset) => (dataset.scenes || []).map((scene) => scene.scene_id));
}

function selectedBandsFromModel() {
  return props.modelValue
    .filter((dataset) => !props.dataTypeFilter || dataset.data_type === props.dataTypeFilter)
    .flatMap((dataset) => Array.isArray(dataset.band_unit_ids) ? dataset.band_unit_ids : []);
}

function draftDatasets(draft) {
  return Array.isArray(draft?.selection?.datasets) ? draft.selection.datasets : [];
}

function draftBatch(draft) {
  const datasets = draftDatasets(draft);
  return {
    load_batch_id: draft.draft_id,
    batch_name: draft.draft_name || draft.draft_id,
    status: 'succeeded',
    dataset_count: datasets.length,
    scene_count: datasets.reduce((count, dataset) => count + (dataset.scenes || []).length, 0),
    partition_draft: draft,
  };
}

const queueBatches = computed(() => [
  ...props.partitionDrafts.map(draftBatch),
  ...availableBatches.value,
]);

function draftForBatchId(batchId) {
  return props.partitionDrafts.find((draft) => draft.draft_id === batchId) || null;
}

async function loadAvailable({ preserveSelection = false, preserveExpansion = false } = {}) {
  if (loading.value) return;
  const priorBatchIds = [...selectedBatchIds.value];
  const priorSceneIds = [...selectedSceneIds.value];
  const priorBandUnitIds = [...selectedBandUnitIds.value];
  loading.value = true;
  error.value = '';
  try {
    const query = new URLSearchParams({ limit: '100', status: 'succeeded' });
    if (props.dataTypeFilter) query.set('data_type', props.dataTypeFilter);
    const response = await requestGet(`/v1/partition/load-batches?${query.toString()}`);
    availableBatches.value = Array.isArray(response?.load_batches)
      ? response.load_batches.filter((batch) => batch.status === 'succeeded')
      : [];
    const requestedBatchIds = preserveSelection ? priorBatchIds : selectedBatchesFromModel();
    selectedBatchIds.value = requestedBatchIds.filter((batchId) => (
      queueBatches.value.some((batch) => batch.load_batch_id === batchId)
    ));
    selectedSceneIds.value = preserveSelection ? priorSceneIds : selectedScenesFromModel();
    selectedBandUnitIds.value = preserveSelection ? priorBandUnitIds : selectedBandsFromModel();
    if (selectedBatchIds.value.length) await loadSelectedBatches(selectedBatchIds.value, { preserveExpansion });
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
        existing.resolution_native = dataset.resolution_native;
        existing.resolution_unit = dataset.resolution_unit;
        existing.crs = dataset.crs;
        existing.suggested_grid_type = dataset.suggested_grid_type;
        existing.suggested_grid_levels = dataset.suggested_grid_levels;
      }
      (dataset.scenes || []).forEach((scene) => {
        const queueBatch = queueBatches.value.find((batch) => batch.load_batch_id === batchId);
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
          [batchId]: queueBatch?.partition_draft ? 'succeeded' : (scene.load_status || scene.status || 'pending'),
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
  const batchesById = new Map(queueBatches.value.map((batch) => [batch.load_batch_id, batch]));
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

async function loadSelectedBatches(batchIds, { preserveExpansion = false } = {}) {
  const ids = [...new Set(batchIds)];
  const selectedDrafts = ids.map(draftForBatchId).filter(Boolean);
  const priorCollapsedBatches = new Set(collapsedBatches.value);
  const priorCollapsedDatasets = new Set(collapsedDatasets.value);
  const priorCollapsedScenes = new Set(collapsedScenes.value);
  const priorBatchKeys = new Set(availableBatchGroups.value.map(batchGroupKey));
  const priorDatasetKeys = new Set(availableBatchGroups.value.flatMap((batch) => batch.datasets.map((dataset) => datasetGroupKey(batch, dataset))));
  const priorSceneKeys = new Set(availableBatchGroups.value.flatMap((batch) => batch.datasets.flatMap((dataset) => dataset.scenes.map((scene) => sceneGroupKey(dataset, scene)))));
  selectedBatchIds.value = ids;
  emit('activate-partition-draft', selectedDrafts.length ? selectedDrafts[0].draft_id : '');
  if (selectedDrafts.length) {
    selectedSceneIds.value = selectedDrafts.flatMap(draftDatasets).flatMap((dataset) => (
      (dataset.scenes || []).map((scene) => scene.scene_id)
    ));
    selectedBandUnitIds.value = selectedDrafts.flatMap(draftDatasets).flatMap((dataset) => dataset.band_unit_ids || []);
  }
  const generation = ++sceneRequestGeneration;
  sceneRequestController?.abort();
  sceneRequestController = new AbortController();
  loading.value = true;
  error.value = '';
  try {
    const query = props.dataTypeFilter ? `?data_type=${encodeURIComponent(props.dataTypeFilter)}` : '';
    const responses = await Promise.all(ids.map(async (batchId) => {
      const draft = draftForBatchId(batchId);
      return {
        batchId,
        response: draft
          ? { load_batch: draftBatch(draft), datasets: draftDatasets(draft) }
          : await requestGet(
            `/v1/partition/load-batches/${encodeURIComponent(batchId)}/scenes${query}`,
            { signal: sceneRequestController.signal },
          ),
      };
    }));
    if (generation !== sceneRequestGeneration) return;
    availableDatasets.value = mergeBatchDatasets(responses);
    availableBatchGroups.value = buildBatchGroups(responses, availableDatasets.value);
    const batchKeys = availableBatchGroups.value.map(batchGroupKey);
    const datasetKeys = availableBatchGroups.value.flatMap((batch) => batch.datasets.map((dataset) => datasetGroupKey(batch, dataset)));
    const sceneKeys = availableBatchGroups.value.flatMap((batch) => batch.datasets.flatMap((dataset) => dataset.scenes.map((scene) => sceneGroupKey(dataset, scene))));
    collapsedBatches.value = preserveExpansion
      ? new Set(batchKeys.filter((key) => !priorBatchKeys.has(key) || priorCollapsedBatches.has(key)))
      : new Set(batchKeys);
    collapsedDatasets.value = preserveExpansion
      ? new Set(datasetKeys.filter((key) => !priorDatasetKeys.has(key) || priorCollapsedDatasets.has(key)))
      : new Set(datasetKeys);
    collapsedScenes.value = preserveExpansion
      ? new Set(sceneKeys.filter((key) => !priorSceneKeys.has(key) || priorCollapsedScenes.has(key)))
      : new Set(sceneKeys);
    const bands = availableDatasets.value.flatMap((dataset) => dataset.scenes.flatMap((scene) => (
      bandsFor(scene, dataset.data_type).map((band) => ({ ...band, scene_id: scene.scene_id }))
    )));
    const availableBandUnitIds = new Set(bands
      .filter((band) => !bandConsumedByLoadBatch(band))
      .map((band) => band.band_unit_id)
      .filter(Boolean));
    if (!selectedBandUnitIds.value.length && selectedSceneIds.value.length) {
      const legacySceneIds = new Set(selectedSceneIds.value);
      selectedBandUnitIds.value = bands
        .filter((band) => legacySceneIds.has(band.scene_id) && band.band_unit_id && !bandConsumedByLoadBatch(band))
        .map((band) => band.band_unit_id);
    } else {
      selectedBandUnitIds.value = selectedBandUnitIds.value.filter((bandUnitId) => availableBandUnitIds.has(bandUnitId));
    }
    committedBatchState = {
      batchIds: [...ids],
      datasets: availableDatasets.value,
      batchGroups: availableBatchGroups.value,
      sceneIds: [...selectedSceneIds.value],
      bandUnitIds: [...selectedBandUnitIds.value],
    };
    updateBandSelection(selectedBandUnitIds.value);
  } catch (caught) {
    if (generation !== sceneRequestGeneration || caught?.name === 'AbortError') return;
    error.value = caught.message || '载入批次数据单元失败。';
    selectedBatchIds.value = [...committedBatchState.batchIds];
    availableDatasets.value = committedBatchState.datasets;
    availableBatchGroups.value = committedBatchState.batchGroups;
    selectedSceneIds.value = [...committedBatchState.sceneIds];
    selectedBandUnitIds.value = [...committedBatchState.bandUnitIds];
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

function updateBandSelection(bandUnitIds) {
  selectedBandUnitIds.value = [...new Set(bandUnitIds)];
  const selectedBands = new Set(selectedBandUnitIds.value);
  selectedSceneIds.value = availableDatasets.value.flatMap((dataset) => dataset.scenes)
    .filter((scene) => bandsFor(scene, availableDatasets.value.find((dataset) => dataset.dataset_id === scene.dataset_id)?.data_type)
      .some((band) => selectedBands.has(band.band_unit_id)))
    .map((scene) => scene.scene_id);
  selectedSceneIds.value = [...new Set(selectedSceneIds.value)];
  if (
    selectedBatchIds.value.length === committedBatchState.batchIds.length
    && selectedBatchIds.value.every((batchId, index) => batchId === committedBatchState.batchIds[index])
  ) {
    committedBatchState = {
      ...committedBatchState,
      sceneIds: [...selectedSceneIds.value],
      bandUnitIds: [...selectedBandUnitIds.value],
    };
  }
  const existingById = new Map(props.modelValue.map((dataset) => [dataset.dataset_id, dataset]));
  const retained = props.dataTypeFilter
    ? props.modelValue.filter((dataset) => dataset.data_type !== props.dataTypeFilter)
    : [];
  const current = availableDatasets.value.flatMap((dataset) => {
    const datasetBandUnitIds = dataset.scenes.flatMap((scene) => bandsFor(scene, dataset.data_type))
      .map((band) => band.band_unit_id)
      .filter((bandUnitId) => bandUnitId && selectedBands.has(bandUnitId));
    const scenes = dataset.scenes.filter((scene) => bandsFor(scene, dataset.data_type)
      .some((band) => selectedBands.has(band.band_unit_id)));
    if (!scenes.length) return [];
    const existing = existingById.get(dataset.dataset_id);
    return [{
      dataset_id: dataset.dataset_id,
      dataset_code: dataset.dataset_code,
      dataset_title: dataset.dataset_title,
      data_type: dataset.data_type,
      product_type: dataset.product_type ?? null,
      resolution_m: dataset.resolution_m ?? null,
      resolution_native: dataset.resolution_native ?? null,
      resolution_unit: dataset.resolution_unit ?? null,
      crs: dataset.crs ?? null,
      suggested_grid_type: dataset.suggested_grid_type ?? null,
      suggested_grid_levels: dataset.suggested_grid_levels ?? {},
      grid_level_unlocked: existing?.grid_level_unlocked === true,
      selection_source: existing?.selection_source || dataset.selection_source || 'load_batch',
      scenes,
      band_unit_ids: datasetBandUnitIds,
      assets: scenes.map(scenePreviewAsset),
      partition: existing?.partition || dataset.partition
        ? withFixedPartitionOptions(existing?.partition || dataset.partition)
        : defaultPartition(dataset),
    }];
  });
  emit('update:modelValue', [...retained, ...current]);
}

const selectedCount = computed(() => selectedBandUnitIds.value.length);
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

function ingestedGridStatus(band) {
  return (band.grid_statuses || []).find((status) => (
    status.partition_status === 'completed' && status.ingest_status === 'completed'
  )) || null;
}

function ingestedGridLabel(band) {
  return gridStatusLabel(ingestedGridStatus(band));
}

function gridStatusLabel(status) {
  if (!status) return '';
  const definition = gridDefinition(status.grid_type);
  return `${definition?.label || status.grid_type} · 层级 ${Number(status.grid_level)}`;
}

function bandStatusLabel(band) {
  const statuses = band.grid_statuses || [];
  const status = statuses.find((item) => item.ingest_status === 'completed') || statuses[0];
  if (!status) return '待剖分';
  const gridLabel = gridStatusLabel(status);
  const withGrid = (label) => gridLabel ? `${label} · ${gridLabel}` : label;
  if (status.ingest_status === 'completed') return withGrid('已入库');
  if (status.ingest_status === 'running') return withGrid('入库中');
  if (status.ingest_status === 'queued') return withGrid('等待入库');
  if (status.ingest_status === 'failed') return withGrid('入库失败');
  if (status.quality_status === 'running') return withGrid('质检中');
  if (['fail', 'error'].includes(status.quality_status)) return withGrid('质检未通过');
  if (status.partition_status === 'running') return withGrid('剖分中');
  if (status.partition_status === 'queued') return withGrid('等待剖分');
  if (status.partition_status === 'failed') return withGrid('剖分失败');
  if (status.partition_status === 'completed' && ['pass', 'warn'].includes(status.quality_status)) return withGrid('待入库');
  if (status.partition_status === 'completed') return withGrid('待质检');
  return withGrid('待剖分');
}

function bandStatusClass(band) {
  const label = bandStatusLabel(band);
  if (label === '已入库' || label.startsWith('已入库 ·')) return 'status-complete';
  if (label.includes('失败') || label === '质检未通过') return 'status-failed';
  if (label === '待入库') return 'status-ready';
  if (label.includes('中') || label.includes('等待')) return 'status-running';
  return 'status-pending';
}

function bandConsumedByLoadBatch(band) {
  return Boolean(ingestedGridStatus(band));
}

function selectableBandIdsForScene(scene, dataType, dataset = null) {
  if (!eligibleScene(scene)) return [];
  return bandsFor(scene, dataType)
    .filter((band) => band.band_unit_id && band.contract_errors.length === 0 && (!dataset || !bandConsumedByLoadBatch(band)))
    .map((band) => band.band_unit_id);
}

function selectableBandIdsForDataset(datasetId) {
  const dataset = availableDatasets.value.find((item) => item.dataset_id === datasetId);
  return dataset ? dataset.scenes.flatMap((scene) => selectableBandIdsForScene(scene, dataset.data_type, dataset)) : [];
}

function groupSelectionState(bandUnitIds) {
  const selected = new Set(selectedBandUnitIds.value);
  const count = bandUnitIds.filter((bandUnitId) => selected.has(bandUnitId)).length;
  return { checked: bandUnitIds.length > 0 && count === bandUnitIds.length, indeterminate: count > 0 && count < bandUnitIds.length };
}

function toggleBandGroup(bandUnitIds, checked) {
  const next = new Set(selectedBandUnitIds.value);
  bandUnitIds.forEach((bandUnitId) => checked ? next.add(bandUnitId) : next.delete(bandUnitId));
  updateBandSelection([...next]);
}

function toggleDatasetSelection(datasetId, checked) {
  toggleBandGroup(selectableBandIdsForDataset(datasetId), checked);
}

function toggleSceneSelection(scene, dataType, checked) {
  const dataset = availableDatasets.value.find((item) => item.scenes.some((candidate) => candidate.scene_id === scene.scene_id));
  toggleBandGroup(selectableBandIdsForScene(scene, dataType, dataset), checked);
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
  if (!props.modelValue.some((dataset) => dataset.dataset_id === datasetId)) return;
  emit('update:modelValue', props.modelValue.map((dataset) => ({ ...dataset, grid_level_unlocked: true })));
}

function levelOptions(gridType) {
  const definition = gridDefinition(gridType);
  if (!definition) return [];
  return Array.from({ length: definition.maxLevel - definition.minLevel + 1 }, (_, index) => definition.minLevel + index);
}

function updatePartition(datasetId, patch) {
  const sourceDataset = props.modelValue.find((dataset) => dataset.dataset_id === datasetId);
  if (!sourceDataset) return;
  let sharedPatch = { ...patch };
  const sourcePartition = { ...partitionFor(sourceDataset), ...patch };
  if (patch.grid_type) {
    const recommendedLevel = suggestedLevel(sourceDataset, sourcePartition.grid_type);
    sourcePartition.requested_grid_level = recommendedLevel ?? fallbackGridLevel(sourceDataset, sourcePartition.grid_type);
    sourcePartition.partition_method = derivedPartitionMethod(sourcePartition.grid_type);
    sharedPatch = sourcePartition;
  }
  emit('update:modelValue', props.modelValue.map((dataset) => {
    if (patch.requested_grid_level !== undefined && gridLevelLocked(datasetId)) return dataset;
    const partition = { ...partitionFor(dataset), ...sharedPatch };
    let gridLevelUnlocked = props.modelValue.every((item) => item.grid_level_unlocked === true);
    if (patch.grid_type) gridLevelUnlocked = false;
    return { ...dataset, partition, grid_level_unlocked: gridLevelUnlocked };
  }));
}

function eligibleScene(scene) {
  return Array.isArray(scene.eligible_source_batch_ids)
    ? scene.eligible_source_batch_ids.length > 0
    : ['succeeded', 'duplicate'].includes(scene.load_status || 'succeeded');
}

function refreshAvailable() {
  return loadAvailable({ preserveSelection: true, preserveExpansion: true });
}

onMounted(() => {
  loadAvailable();
  refreshTimer = window.setInterval(refreshAvailable, REFRESH_INTERVAL_MS);
});
onBeforeUnmount(() => {
  if (refreshTimer !== null) window.clearInterval(refreshTimer);
  sceneRequestGeneration += 1;
  sceneRequestController?.abort();
});
</script>

<template>
  <section class="partition-data-list">
    <div class="partition-drawer-heading">
      <h3>{{ dataTypeLabel || '' }}待剖分数据</h3>
      <div><span>{{ selectedBatchIds.length }} 个批次 · {{ availableDatasets.length }} 个数据集 · 已选 {{ selectedCount }} 个波段</span><el-button :icon="Refresh" circle size="small" :loading="loading" aria-label="刷新已载入数据" @click="refreshAvailable" /></div>
    </div>
    <el-alert v-if="error" :title="error" type="error" :closable="false" show-icon />

    <div class="partition-batch-picker">
      <div class="partition-section-title"><strong>待剖分批次</strong><span>{{ queueBatches.length }} 个可用批次</span></div>
      <el-checkbox-group :model-value="selectedBatchIds" data-testid="load-batch-selector" @update:model-value="loadSelectedBatches">
        <el-checkbox v-for="batch in queueBatches" :key="batch.load_batch_id" :value="batch.load_batch_id" :data-testid="`load-batch-${batch.load_batch_id}`">
          <span class="partition-batch-option">
            <strong>{{ batch.batch_name || batch.load_batch_id }}</strong>
            <span>{{ batch.load_batch_id }} · {{ batch.dataset_count || 0 }} 个数据集 · {{ batch.scene_count || 0 }} 景</span>
          </span>
        </el-checkbox>
      </el-checkbox-group>
      <div v-if="!loading && !queueBatches.length" class="empty-state">暂无可用待剖分批次</div>
    </div>

    <div class="band-filter-toolbar">
      <div><strong>波段筛选</strong></div>
      <el-input v-model="bandKeyword" :prefix-icon="Search" clearable placeholder="例如 B04、VV、NDVI 或 ppm" />
    </div>

    <div v-loading="loading" class="partition-batch-tree-list">
      <section v-for="batch in visibleBatchGroups" :key="batch.load_batch_id" class="partition-batch-tree" :data-testid="`batch-tree-${batch.load_batch_id}`">
        <header class="partition-batch-tree-header" :aria-expanded="!collapsedBatches.has(batchGroupKey(batch))" @click="toggleBatchGroup(batch)">
          <div class="partition-tree-identity">
            <el-icon><FolderOpened /></el-icon>
            <div><strong>{{ batch.batch_name || batch.load_batch_id }}</strong><span>{{ batch.load_batch_id }}</span></div>
          </div>
          <span>{{ batch.datasets.length }} 个数据集 · {{ batch.datasets.reduce((total, dataset) => total + dataset.scenes.length, 0) }} 景</span>
        </header>

        <div v-show="!collapsedBatches.has(batchGroupKey(batch))" class="partition-dataset-tree-list">
          <section v-for="dataset in batch.datasets" :key="`${batch.load_batch_id}-${dataset.dataset_id}`" class="partition-dataset-tree" :data-testid="`dataset-tree-${batch.load_batch_id}-${dataset.dataset_id}`">
            <div class="partition-scene-group-header" :aria-expanded="!collapsedDatasets.has(datasetGroupKey(batch, dataset))" @click="toggleDatasetGroup(batch, dataset)">
              <div class="partition-tree-identity">
                <el-icon><Collection /></el-icon>
                <div><strong>{{ dataset.dataset_title || dataset.dataset_code || dataset.dataset_id }}</strong><span>{{ dataset.dataset_code || dataset.dataset_id }} · {{ dataset.scenes.length }} 景 · {{ dataset.scenes.reduce((total, scene) => total + bandsFor(scene, dataset.data_type).length, 0) }} 波段<span v-if="dataset.crs"> · {{ dataset.crs }}</span><span v-if="resolutionLabel(dataset)"> · {{ resolutionLabel(dataset) }}</span></span></div>
              </div>
              <el-checkbox
                class="dataset-select-all"
                @click.stop
                :model-value="groupSelectionState(selectableBandIdsForDataset(dataset.dataset_id)).checked"
                :indeterminate="groupSelectionState(selectableBandIdsForDataset(dataset.dataset_id)).indeterminate"
                :disabled="selectableBandIdsForDataset(dataset.dataset_id).length === 0"
                :data-testid="`select-dataset-${dataset.dataset_id}`"
                @change="toggleDatasetSelection(dataset.dataset_id, $event)"
              >全选数据集</el-checkbox>
              <div v-if="selectedDatasetsById.has(dataset.dataset_id)" class="partition-dataset-grid" @click.stop>
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
            <div v-show="!collapsedDatasets.has(datasetGroupKey(batch, dataset))" class="partition-scene-list">
              <section v-for="scene in dataset.scenes" :key="scene.scene_id" class="partition-scene-option" :data-testid="`scene-${scene.scene_id}`">
                <div class="partition-scene-identity" :aria-expanded="!collapsedScenes.has(sceneGroupKey(dataset, scene))" @click="toggleSceneGroup(dataset, scene)">
                  <el-icon><Picture /></el-icon>
                  <span>
                    <strong>{{ scene.scene_key || scene.scene_id }}</strong>
                    <small>景 · {{ formatShanghaiTime(scene.acquisition_time, '采集时间未登记') }}</small>
                  </span>
                  <el-checkbox
                    class="scene-select-all"
                    @click.stop
                    :model-value="groupSelectionState(selectableBandIdsForScene(scene, dataset.data_type, dataset)).checked"
                    :indeterminate="groupSelectionState(selectableBandIdsForScene(scene, dataset.data_type, dataset)).indeterminate"
                    :disabled="selectableBandIdsForScene(scene, dataset.data_type, dataset).length === 0"
                    :data-testid="`select-scene-${scene.scene_id}`"
                    @change="toggleSceneSelection(scene, dataset.data_type, $event)"
                  >全选该景</el-checkbox>
                </div>
                <el-checkbox-group
                  v-show="!collapsedScenes.has(sceneGroupKey(dataset, scene)) && bandsFor(scene, dataset.data_type).length"
                  class="scene-band-list"
                  :model-value="selectedBandUnitIds"
                  @update:model-value="updateBandSelection"
                >
                  <el-checkbox
                    v-for="(band, bandIndex) in bandsFor(scene, dataset.data_type)"
                    :key="`${scene.scene_id}-${band.band_unit_id || band.band_code || 'invalid'}-${band.display_order}-${bandIndex}`"
                    :value="band.band_unit_id"
                    :disabled="!eligibleScene(scene) || !band.band_unit_id || band.contract_errors.length > 0 || bandConsumedByLoadBatch(band)"
                    :data-testid="band.band_unit_id ? `band-unit-${band.band_unit_id}` : undefined"
                  >
                    <span class="scene-band-chip"><small>{{ dataUnitTypeLabel(dataset.data_type) }}</small>{{ bandDisplayLabel(band) }}<em :class="bandStatusClass(band)">{{ bandStatusLabel(band) }}</em></span>
                  </el-checkbox>
                </el-checkbox-group>
                  <small v-if="!bandsFor(scene, dataset.data_type).length" class="band-missing">波段信息未登记</small>
              </section>
            </div>
          </section>
        </div>
      </section>
      <div v-if="!loading && selectedBatchIds.length && !availableDatasets.length" class="empty-state">所选批次没有当前产品可剖分的数据单元</div>
      <div v-else-if="!loading && selectedBatchIds.length && !visibleBatchGroups.length" class="empty-state">没有匹配当前波段筛选条件的数据单元</div>
    </div>
  </section>
</template>

<style scoped>
.partition-drawer-heading > div { display: flex; align-items: center; gap: 8px; }
.partition-batch-picker { padding: 12px 0 18px; border-bottom: 1px solid var(--el-border-color-lighter); }
.partition-section-title { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 10px; }
.partition-section-title span { color: var(--el-text-color-secondary); font-size: 12px; }
.partition-batch-picker :deep(.el-checkbox-group) { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 8px 16px; }
.partition-batch-picker :deep(.el-checkbox) { align-items: flex-start; height: auto; margin: 0; padding: 8px 0; }
.partition-batch-picker :deep(.el-checkbox__label), .scene-band-list :deep(.el-checkbox__label) { min-width: 0; white-space: normal; }
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
.partition-batch-tree-header { display: flex; align-items: center; justify-content: space-between; gap: 16px; min-height: 48px; padding: 8px 12px; background: var(--el-fill-color-light); border-bottom: 1px solid var(--el-border-color-lighter); cursor: pointer; }
.partition-batch-tree-header > span { flex: none; color: var(--el-text-color-secondary); font-size: 12px; }
.partition-dataset-tree-list { padding-left: 22px; }
.partition-dataset-tree { padding: 14px 0 14px 14px; border-bottom: 1px solid var(--el-border-color-lighter); }
.partition-tree-identity { display: flex; min-width: 0; align-items: center; gap: 9px; }
.partition-tree-identity > .el-icon { flex: none; color: var(--el-color-primary); font-size: 17px; }
.partition-tree-identity > div { display: flex; min-width: 0; flex-direction: column; gap: 2px; }
.partition-tree-identity strong { overflow-wrap: anywhere; }
.partition-tree-identity span { color: var(--el-text-color-secondary); font-size: 12px; overflow-wrap: anywhere; }
.partition-scene-group-header { display: flex; align-items: center; justify-content: space-between; gap: 16px; margin-bottom: 10px; cursor: pointer; }
.dataset-select-all { flex: none; margin-left: auto; }
.partition-dataset-grid { display: grid; grid-template-columns: minmax(130px, 1fr) minmax(170px, 1fr) 32px; gap: 8px; width: min(420px, 56%); }
.partition-dataset-grid :deep(.el-button) { width: 32px; padding: 0; }
.partition-scene-list { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 10px 16px; }
.partition-scene-option { min-width: 0; padding: 9px 10px; border-left: 2px solid var(--el-border-color); background: var(--el-fill-color-extra-light); }
.partition-scene-identity { display: flex; min-width: 0; align-items: flex-start; gap: 8px; cursor: pointer; }
.partition-scene-identity > .el-icon { flex: none; margin-top: 2px; color: var(--el-text-color-secondary); }
.partition-scene-identity > span { display: flex; min-width: 0; flex-direction: column; gap: 2px; }
.scene-select-all { flex: none; margin-left: auto; }
.partition-scene-identity strong { color: var(--el-text-color-primary); font-size: 13px; font-weight: 500; overflow-wrap: anywhere; }
.partition-scene-identity small { color: var(--el-text-color-secondary); font-size: 11px; line-height: 1.4; overflow-wrap: anywhere; }
.scene-band-list { display: flex; flex-wrap: wrap; gap: 5px 10px; margin-top: 8px; padding-left: 25px; }
.scene-band-list :deep(.el-checkbox) { height: auto; margin: 0; }
.scene-band-chip em { margin-left: 6px; font-size: 11px; font-style: normal; }
.scene-band-chip em.status-complete { color: var(--el-color-success); }
.scene-band-chip em.status-ready { color: #1769aa; }
.scene-band-chip em.status-running { color: #9a6700; }
.scene-band-chip em.status-failed { color: var(--el-color-danger); }
.scene-band-chip em.status-pending { color: var(--el-text-color-secondary); }
.scene-band-chip { display: flex; flex-direction: column; padding: 3px 7px; border: 1px solid #bfd5e8; border-radius: 4px; background: #f3f8fc; color: #2d628d; font-size: 11px; line-height: 1.35; }
.scene-band-chip small { color: var(--el-text-color-secondary); font-size: 10px; }
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
