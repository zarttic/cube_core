import { reactive, ref } from 'vue';
import { defineStore } from 'pinia';

import { requestGet, requestPost } from '@/api/client';
import { normalizePageResponse, pageQuery } from '@/api/pagination';
import { createRequestScope } from '@/api/requestScope';
import { derivedPartitionMethod, gridDefinition, withFixedPartitionOptions } from '@/utils/grid';

const initialForm = () => ({
  datasets: [],
  gridType: 'geohash',
  requestedGridLevel: 4,
});

const partitionFields = new Set([
  'grid_type',
  'requested_grid_level',
  'partition_method',
  'cover_mode',
  'time_granularity',
  'max_cells_per_asset',
  'max_observations',
]);

function invalidRequest(message) {
  const error = new Error(message);
  error.code = 'invalid_partition_request';
  return error;
}

function validatePartition(partition) {
  if (!partition || typeof partition !== 'object' || Array.isArray(partition)
    || Object.keys(partition).some((key) => !partitionFields.has(key))) {
    throw invalidRequest('数据集格网参数无效。');
  }
  const definition = gridDefinition(partition.grid_type);
  const level = Number(partition.requested_grid_level);
  if (!definition || !Number.isInteger(level) || level < definition.minLevel || level > definition.maxLevel
    || partition.partition_method !== derivedPartitionMethod(partition.grid_type)) {
    throw invalidRequest('数据集格网层级或剖分方式无效。');
  }
  const maxCells = Number(partition.max_cells_per_asset ?? 0);
  if (!Number.isInteger(maxCells) || maxCells < 0) {
    throw invalidRequest('每数据单元最大格网单元数必须是非负整数。');
  }
}

function normalizeSceneBatchIds(scene) {
  if (Array.isArray(scene?.eligible_source_batch_ids)) {
    return [...new Set(scene.eligible_source_batch_ids.map((value) => String(value || '').trim()).filter(Boolean))];
  }
  return [...new Set([
    ...(Array.isArray(scene?.source_batch_ids) ? scene.source_batch_ids : []),
    scene?.load_batch_id,
  ].map((value) => String(value || '').trim()).filter(Boolean))];
}

function buildDatasetSelection(dataset) {
  if (!String(dataset?.dataset_id || '').trim()) throw invalidRequest('数据集必须包含有效标识。');
  if (!Array.isArray(dataset.scenes) || !dataset.scenes.length) throw invalidRequest('每个数据集至少选择一个景。');
  const partition = withFixedPartitionOptions(dataset.partition);
  validatePartition(partition);
  const sceneIds = dataset.scenes.map((scene) => String(scene?.scene_id || '').trim());
  if (sceneIds.some((sceneId) => !sceneId) || new Set(sceneIds).size !== sceneIds.length) {
    throw invalidRequest('数据集包含无效或重复的数据单元。');
  }
  const bandUnitIds = Array.isArray(dataset.band_unit_ids)
    ? dataset.band_unit_ids.map((value) => String(value || '').trim())
    : null;
  if (bandUnitIds && (!bandUnitIds.length || bandUnitIds.some((value) => !value)
    || new Set(bandUnitIds).size !== bandUnitIds.length)) {
    throw invalidRequest('数据集包含无效或重复的波段数据单元。');
  }
  return {
    dataset_id: dataset.dataset_id,
    scene_ids: sceneIds,
    ...(bandUnitIds ? { band_unit_ids: bandUnitIds } : {}),
    partition: Object.fromEntries(Object.entries(partition).filter(([key, value]) => partitionFields.has(key) && value != null)),
  };
}

export function createPartitionRunId() {
  const suffix = globalThis.crypto?.randomUUID?.()
    || `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 12)}`;
  return `partition-run-${suffix}`;
}

export const usePartitionStore = defineStore('partition', () => {
  const form = reactive(initialForm());
  const batches = ref([]);
  const tasks = ref([]);
  const taskPage = reactive({ page: 1, pageSize: 20, total: 0 });
  const result = ref(null);
  const error = ref('');
  const loading = reactive({ batches: false, tasks: false, submit: false });
  const batchScope = createRequestScope();
  const taskScope = createRequestScope();
  const submitScope = createRequestScope();

  function resetForm() {
    Object.assign(form, initialForm());
    result.value = null;
    error.value = '';
  }

  function buildRequest(partitionRunId) {
    const runId = String(partitionRunId || '').trim();
    if (!runId) throw invalidRequest('缺少剖分执行 ID。');
    if (!Array.isArray(form.datasets) || !form.datasets.length) throw invalidRequest('请至少选择一个数据单元。');
    const datasets = form.datasets.map(buildDatasetSelection);
    const allSceneIds = datasets.flatMap((dataset) => dataset.scene_ids);
    if (new Set(allSceneIds).size !== allSceneIds.length) throw invalidRequest('同一数据单元不能重复归入多个数据集。');
    const allBandUnitIds = datasets.flatMap((dataset) => dataset.band_unit_ids || []);
    if (new Set(allBandUnitIds).size !== allBandUnitIds.length) throw invalidRequest('同一波段数据单元不能重复归入多个数据集。');
    const sourceBatchIds = [...new Set(form.datasets.flatMap((dataset) => (
      dataset.scenes.flatMap(normalizeSceneBatchIds)
    )))];
    if (!sourceBatchIds.length) throw invalidRequest('所选数据单元缺少来源载入批次。');
    if (sourceBatchIds.includes(runId)) throw invalidRequest('剖分执行 ID 不能复用载入批次 ID。');
    return {
      partition_run_id: runId,
      source_batch_ids: sourceBatchIds,
      datasets,
    };
  }

  async function loadBatches() {
    const request = batchScope.begin();
    loading.batches = true;
    try {
      const response = await requestGet('/v1/partition/load-batches?limit=100&status=succeeded', { signal: request.signal });
      if (batchScope.isCurrent(request.token)) {
        batches.value = Array.isArray(response?.load_batches)
          ? response.load_batches.filter((batch) => batch.status === 'succeeded')
          : [];
      }
      return batches.value;
    } catch (caught) {
      if (batchScope.isCurrent(request.token) && caught?.name !== 'AbortError') error.value = caught.message || '加载载入批次失败。';
      return [];
    } finally {
      if (batchScope.isCurrent(request.token)) loading.batches = false;
    }
  }

  async function loadTasks(page = taskPage.page, pageSize = taskPage.pageSize) {
    const request = taskScope.begin();
    loading.tasks = true;
    try {
      const query = pageQuery({ page, page_size: pageSize });
      const response = await requestGet(`/v1/partition/tasks?${query}`, { signal: request.signal });
      if (taskScope.isCurrent(request.token)) {
        const normalized = normalizePageResponse({ ...response, items: response?.tasks }, page, pageSize);
        tasks.value = normalized.items;
        Object.assign(taskPage, { page: normalized.page, pageSize: normalized.pageSize, total: normalized.total });
      }
      return tasks.value;
    } catch (caught) {
      if (taskScope.isCurrent(request.token) && caught?.name !== 'AbortError') error.value = caught.message || '加载任务失败。';
      return [];
    } finally {
      if (taskScope.isCurrent(request.token)) loading.tasks = false;
    }
  }

  async function submit() {
    const partitionRunId = createPartitionRunId();
    const body = buildRequest(partitionRunId);
    const request = submitScope.begin();
    error.value = '';
    loading.submit = true;
    try {
      const response = await requestPost('/v1/partition/runs', body, { signal: request.signal });
      if (submitScope.isCurrent(request.token)) {
        result.value = response;
        await Promise.all([loadBatches(), loadTasks(1, taskPage.pageSize)]);
      }
      return response;
    } catch (caught) {
      if (submitScope.isCurrent(request.token) && caught?.name !== 'AbortError') error.value = caught.message || '提交剖分失败。';
      throw caught;
    } finally {
      if (submitScope.isCurrent(request.token)) loading.submit = false;
    }
  }

  return { form, batches, tasks, taskPage, result, error, loading, loadBatches, loadTasks, submit, resetForm, buildRequest };
});
