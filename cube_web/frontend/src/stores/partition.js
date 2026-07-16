import { reactive, ref } from 'vue';
import { defineStore } from 'pinia';

import { requestGet, requestPost } from '@/api/client';
import { normalizePageResponse, pageQuery } from '@/api/pagination';
import { createRequestScope } from '@/api/requestScope';
import { derivedPartitionMethod, gridDefinition } from '@/utils/grid';

const initialForm = () => ({
  batchId: '',
  datasets: [],
  gridType: 'geohash',
  requestedGridLevel: 6,
  coverMode: 'intersect',
  timeGranularity: 'day',
  maxCellsPerAsset: 0,
});

const datasetFields = new Set(['dataset_id', 'dataset_code', 'dataset_title', 'data_type', 'product_type', 'assets', 'bands', 'attributes', 'partition']);
const assetFields = new Set(['source_asset_id', 'cog_uri', 'source_uri', 'source_kind', 'source_format', 'checksum', 'bbox', 'crs', 'time_start', 'time_end', 'attributes']);
const bandFields = new Set(['source_asset_id', 'band_code', 'band_name', 'band_type', 'unit', 'display_order', 'attributes']);
const datasetPartitionFields = new Set(['grid_type', 'requested_grid_level', 'partition_method']);
const forbiddenRequestFields = new Set(['dataset_ids', 'datasetIds', 'grid_level', 'grid_level_mode', 'selected_assets']);

function invalidRequest(message) {
  const error = new Error(message);
  error.code = 'invalid_partition_request';
  return error;
}

function validateDataset(dataset) {
  if (!dataset || typeof dataset !== 'object') throw invalidRequest('存在无效的数据集。');
  if (!String(dataset.dataset_id || '').trim() || !String(dataset.dataset_code || '').trim() || !String(dataset.dataset_title || '').trim()) {
    throw invalidRequest('数据集必须包含完整标识与名称。');
  }
  if (!Array.isArray(dataset.assets) || !dataset.assets.length) throw invalidRequest('每个数据集至少需要一项资产。');
  if (!Array.isArray(dataset.bands) || !dataset.bands.length) throw invalidRequest('每个数据集至少需要一个波段。');
  if (Object.keys(dataset).some((key) => !datasetFields.has(key))) throw invalidRequest('数据集包含不允许的请求字段。');
  if (!dataset.partition || typeof dataset.partition !== 'object' || Array.isArray(dataset.partition)
    || Object.keys(dataset.partition).some((key) => !datasetPartitionFields.has(key))) {
    throw invalidRequest('数据集格网参数无效。');
  }
  const gridType = dataset.partition.grid_type;
  const requestedGridLevel = Number(dataset.partition.requested_grid_level);
  const definition = gridDefinition(gridType);
  if (!definition || !Number.isInteger(requestedGridLevel)
    || requestedGridLevel < definition.minLevel || requestedGridLevel > definition.maxLevel
    || dataset.partition.partition_method !== derivedPartitionMethod(gridType)) {
    throw invalidRequest('数据集格网层级或剖分方式无效。');
  }
  if (dataset.assets.some((asset) => !asset || Object.keys(asset).some((key) => !assetFields.has(key)))) {
    throw invalidRequest('资产包含不允许的请求字段。');
  }
  for (const asset of dataset.assets) {
    const sourceUri = String(asset.source_uri || '');
    if (dataset.data_type === 'carbon') {
      if (asset.cog_uri || asset.source_kind !== 'raw' || !sourceUri.startsWith('s3://') || !['netcdf', 'hdf5'].includes(asset.source_format)) {
        throw invalidRequest('碳卫星资产必须使用 NetCDF/HDF5 原始 source_uri。');
      }
      const suffixes = asset.source_format === 'netcdf' ? ['.nc', '.nc4'] : ['.h5', '.hdf', '.hdf5'];
      if (!suffixes.some((suffix) => sourceUri.toLowerCase().endsWith(suffix))) {
        throw invalidRequest('碳卫星 source_uri 与 source_format 不匹配。');
      }
    } else if (!String(asset.cog_uri || '').startsWith('s3://') || asset.source_uri || asset.source_kind !== 'cog' || asset.source_format !== 'cog') {
      throw invalidRequest('非碳卫星资产必须使用 COG cog_uri。');
    }
  }
  if (dataset.bands.some((band) => !band || Object.keys(band).some((key) => !bandFields.has(key)))) {
    throw invalidRequest('波段包含不允许的请求字段。');
  }
  const assetIds = new Set(dataset.assets.map((asset) => asset?.source_asset_id));
  if (assetIds.has(undefined) || dataset.bands.some((band) => !assetIds.has(band?.source_asset_id))) {
    throw invalidRequest('波段必须关联到已选资产。');
  }
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

  function buildRequest() {
    if (Object.keys(form).some((key) => forbiddenRequestFields.has(key))) {
      throw invalidRequest('请求包含已退役字段。');
    }
    const batchId = String(form.batchId || '').trim();
    if (!batchId) throw invalidRequest('请输入批次 ID。');
    if (!Array.isArray(form.datasets) || !form.datasets.length) throw invalidRequest('请选择至少一个完整数据集。');
    form.datasets.forEach(validateDataset);
    const dataTypes = [...new Set(form.datasets.map((dataset) => dataset.data_type))];
    if (dataTypes.some((dataType) => !dataType)) throw invalidRequest('数据集必须包含数据类型。');
    const definition = gridDefinition(form.gridType);
    const requestedGridLevel = Number(form.requestedGridLevel);
    if (!definition || !Number.isInteger(requestedGridLevel) || requestedGridLevel < definition.minLevel || requestedGridLevel > definition.maxLevel) {
      throw invalidRequest('所选格网层级不在该格网支持范围内。');
    }
    const maxCellsPerAsset = Number(form.maxCellsPerAsset);
    if (!Number.isInteger(maxCellsPerAsset) || maxCellsPerAsset < 0) throw invalidRequest('每资产最大格网单元数必须是非负整数。');
    return {
      dataTypes,
      body: {
        batch_id: batchId,
        datasets: form.datasets,
        grid_type: form.gridType,
        requested_grid_level: requestedGridLevel,
        partition_method: derivedPartitionMethod(form.gridType),
        cover_mode: form.coverMode,
        time_granularity: form.timeGranularity,
        max_cells_per_asset: maxCellsPerAsset,
      },
    };
  }

  async function loadBatches() {
    const request = batchScope.begin();
    loading.batches = true;
    try {
      const response = await requestGet('/v1/partition/batches?include_succeeded=true&limit=100', { signal: request.signal });
      if (batchScope.isCurrent(request.token)) batches.value = Array.isArray(response?.batches) ? response.batches : [];
      return batches.value;
    } catch (caught) {
      if (batchScope.isCurrent(request.token) && caught?.name !== 'AbortError') error.value = caught.message || '加载批次失败。';
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
    const { body, dataTypes } = buildRequest();
    const request = submitScope.begin();
    error.value = '';
    loading.submit = true;
    try {
      const path = dataTypes.length > 1
        ? '/v1/partition/tasks/run'
        : `/v1/partition/${encodeURIComponent(dataTypes[0])}/tasks/run`;
      const response = await requestPost(path, body, { signal: request.signal });
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
