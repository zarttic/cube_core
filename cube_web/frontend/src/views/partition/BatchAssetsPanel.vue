<script setup>
import { computed, onMounted, ref } from 'vue';

import { requestGet } from '@/api/client';
import { normalizePageResponse } from '@/api/pagination';
import { derivedPartitionMethod, gridDefinition, gridDefinitions, nativeLevelLabel } from '@/utils/grid';

const props = defineProps({
  modelValue: { type: Array, default: () => [] },
  defaultGridType: { type: String, default: 'geohash' },
  defaultRequestedGridLevel: { type: Number, default: 6 },
});
const emit = defineEmits(['update:modelValue']);
const loading = ref(false);
const error = ref('');
const available = ref([]);
const selectedIds = ref([]);
const pageSize = 500;
let selectionGeneration = 0;

async function loadAllPages(path, sortBy) {
  const items = [];
  let page = 1;
  let total = null;
  do {
    const response = await requestGet(`${path}?page=${page}&page_size=${pageSize}&sort_by=${sortBy}&sort_order=asc`);
    const normalized = normalizePageResponse(response, page, pageSize);
    items.push(...normalized.items);
    total = normalized.total;
    if (!normalized.items.length && items.length < total) {
      throw new Error('数据集分页响应不完整，无法提交剖分。');
    }
    page += 1;
  } while (items.length < total);
  return items;
}

function normalizedPartition(gridType, requestedGridLevel) {
  const definition = gridDefinition(gridType) || gridDefinition('geohash');
  const level = Number(requestedGridLevel);
  const safeLevel = Number.isInteger(level) && level >= definition.minLevel && level <= definition.maxLevel
    ? level
    : definition.minLevel;
  return {
    grid_type: definition.value,
    requested_grid_level: safeLevel,
    partition_method: derivedPartitionMethod(definition.value),
  };
}

function asDatasetInput(dataset, assets, bands, defaults) {
  const carbon = dataset.data_type === 'carbon';
  return {
    dataset_id: dataset.dataset_id,
    dataset_code: dataset.dataset_code,
    dataset_title: dataset.dataset_title,
    data_type: dataset.data_type,
    product_type: dataset.product_type ?? null,
    assets: assets.map((asset) => ({
      source_asset_id: asset.source_asset_id,
      ...(carbon
        ? { source_uri: asset.source_uri, source_kind: 'raw', source_format: asset.source_format }
        : { cog_uri: asset.cog_uri || asset.source_uri, source_kind: 'cog', source_format: 'cog' }),
      checksum: asset.checksum,
      bbox: asset.bbox,
      crs: asset.crs,
      time_start: asset.time_start,
      time_end: asset.time_end,
      attributes: asset.attributes || {},
    })),
    bands: bands.map((band) => ({
      source_asset_id: band.source_asset_id,
      band_code: band.band_code,
      band_name: band.band_name,
      band_type: band.band_type,
      unit: band.unit ?? null,
      display_order: band.display_order,
      attributes: band.attributes || {},
    })),
    attributes: dataset.attributes || {},
    partition: normalizedPartition(defaults.gridType, defaults.requestedGridLevel),
  };
}

async function loadAvailable() {
  loading.value = true;
  error.value = '';
  try {
    const response = await requestGet('/v1/partition/datasets?page=1&page_size=100&sort_by=updated_at&sort_order=desc');
    available.value = normalizePageResponse(response).items;
  } catch (caught) {
    error.value = caught.message || '载入可用数据集失败。';
  } finally {
    loading.value = false;
  }
}

async function updateSelected(ids) {
  const generation = ++selectionGeneration;
  selectedIds.value = [...ids];
  const selected = available.value.filter((dataset) => ids.includes(dataset.dataset_id));
  const existingById = new Map(props.modelValue.map((dataset) => [dataset.dataset_id, dataset]));
  try {
    const complete = await Promise.all(selected.map(async (dataset) => {
      const existing = existingById.get(dataset.dataset_id);
      if (existing) return existing;
      const [assetsResponse, bandsResponse] = await Promise.all([
        loadAllPages(`/v1/partition/datasets/${encodeURIComponent(dataset.dataset_id)}/assets`, 'source_asset_id'),
        loadAllPages(`/v1/partition/datasets/${encodeURIComponent(dataset.dataset_id)}/bands`, 'display_order'),
      ]);
      return asDatasetInput(dataset, assetsResponse, bandsResponse, {
        gridType: props.defaultGridType,
        requestedGridLevel: props.defaultRequestedGridLevel,
      });
    }));
    if (generation !== selectionGeneration) return;
    emit('update:modelValue', complete);
  } catch (caught) {
    if (generation !== selectionGeneration) return;
    error.value = caught.message || '载入数据集资产或波段失败。';
  }
}

const selectedCount = computed(() => props.modelValue.length);
const selectedById = computed(() => new Map(props.modelValue.map((dataset) => [dataset.dataset_id, dataset])));

function partitionFor(dataset) {
  return selectedById.value.get(dataset.dataset_id)?.partition || normalizedPartition(props.defaultGridType, props.defaultRequestedGridLevel);
}

function levelOptions(gridType) {
  const definition = gridDefinition(gridType);
  if (!definition) return [];
  return Array.from({ length: definition.maxLevel - definition.minLevel + 1 }, (_, index) => definition.minLevel + index);
}

function updatePartition(datasetId, patch) {
  emit('update:modelValue', props.modelValue.map((dataset) => {
    if (dataset.dataset_id !== datasetId) return dataset;
    const current = partitionFor(dataset);
    const partition = { ...current, ...patch };
    if (patch.grid_type) {
      const definition = gridDefinition(partition.grid_type);
      const level = Number(partition.requested_grid_level);
      if (!Number.isInteger(level) || level < definition.minLevel || level > definition.maxLevel) {
        partition.requested_grid_level = definition.minLevel;
      }
      partition.partition_method = derivedPartitionMethod(partition.grid_type);
    }
    return { ...dataset, partition };
  }));
}

onMounted(loadAvailable);
</script>

<template>
  <section class="partition-section">
    <div class="section-heading"><h2>已载入数据集</h2><span>每个数据集可使用独立格网参数；剖分方式由所选格网确定</span></div>
    <el-alert v-if="error" :title="error" type="error" :closable="false" show-icon />
    <el-table v-loading="loading" :data="available" row-key="dataset_id" @selection-change="updateSelected($event.map((row) => row.dataset_id))">
      <el-table-column type="selection" width="48" />
      <el-table-column prop="dataset_code" label="数据集编码" min-width="160" />
      <el-table-column prop="dataset_title" label="数据集名称" min-width="200" />
      <el-table-column prop="data_type" label="数据类型" width="110" />
      <el-table-column prop="product_type" label="产品类型" min-width="120" />
      <el-table-column label="格网类型" min-width="150">
        <template #default="{ row }">
          <el-select
            v-if="selectedById.has(row.dataset_id)"
            :data-testid="`dataset-grid-${row.dataset_id}`"
            :model-value="partitionFor(row).grid_type"
            @click.stop
            @update:model-value="updatePartition(row.dataset_id, { grid_type: $event })"
          >
            <el-option v-for="grid in gridDefinitions" :key="grid.value" :label="grid.label" :value="grid.value" />
          </el-select>
          <span v-else>-</span>
        </template>
      </el-table-column>
      <el-table-column label="原生层级" min-width="150">
        <template #default="{ row }">
          <el-select
            v-if="selectedById.has(row.dataset_id)"
            :data-testid="`dataset-grid-level-${row.dataset_id}`"
            :model-value="Number(partitionFor(row).requested_grid_level)"
            @click.stop
            @update:model-value="updatePartition(row.dataset_id, { requested_grid_level: Number($event) })"
          >
            <el-option
              v-for="level in levelOptions(partitionFor(row).grid_type)"
              :key="level"
              :label="nativeLevelLabel(partitionFor(row).grid_type, level)"
              :value="level"
            />
          </el-select>
          <span v-else>-</span>
        </template>
      </el-table-column>
      <el-table-column label="剖分方式" min-width="110">
        <template #default="{ row }">{{ selectedById.has(row.dataset_id) ? (partitionFor(row).partition_method === 'entity' ? '实体剖分' : '逻辑剖分') : '-' }}</template>
      </el-table-column>
      <el-table-column prop="partition_status" label="剖分状态" width="110" />
    </el-table>
    <p class="partition-hint">已选择 {{ selectedCount }} 个完整数据集。资产或波段缺失的数据集不能提交；各数据集的格网参数会分别提交。</p>
  </section>
</template>
