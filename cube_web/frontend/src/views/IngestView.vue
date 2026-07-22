<script setup>
import { computed, onMounted, onUnmounted, ref } from 'vue';
import { Collection, Picture, Refresh, Search } from '@element-plus/icons-vue';
import { ElMessage } from 'element-plus';

import AppTable from '@/components/AppTable.vue';
import StatusTag from '@/components/StatusTag.vue';
import { useIngestRunsStore } from '@/stores/ingestRuns';
import { formatShanghaiTime } from '@/utils/time';
import { bandDisplayLabel, dataUnitTypeLabel } from '@/utils/bands';
import { gridDefinition, nativeLevelLabel } from '@/utils/grid';
import IngestRunDetailDrawer from '@/views/ingest/IngestRunDetailDrawer.vue';

defineProps({ embedded: Boolean, title: { type: String, default: '数据入库' } });
const store = useIngestRunsStore();
const manualDialogVisible = ref(false);
const manualCollectionId = ref('');
const manualBandUnitIds = ref([]);
const collapsedManualDatasets = ref(new Set());
const collapsedManualScenes = ref(new Set());

async function refresh() {
  store.pageState.page = 1;
  await Promise.all([store.loadList(), store.loadManualCandidates()]);
}
function setPage(page) { store.pageState.page = page; return store.loadList(); }
function setPageSize(pageSize) { Object.assign(store.pageState, { page: 1, pageSize }); return store.loadList(); }

onMounted(() => { refresh().catch(() => {}); });
onUnmounted(() => store.dispose());

const selectedCollection = computed(() => store.manualCandidates.find((item) => item.partition_run_id === manualCollectionId.value));
const selectableManualUnits = computed(() => (selectedCollection.value?.units || []).filter((unit) => (
  ['pass', 'warn'].includes(unit.quality_status) && unit.ingest_status !== 'completed'
)));
const manualDatasetGroups = computed(() => {
  const datasets = new Map();
  selectableManualUnits.value.forEach((unit) => {
    const dataset = datasets.get(unit.dataset_id) || {
      dataset_id: unit.dataset_id,
      dataset_code: unit.dataset_code,
      dataset_title: unit.dataset_title,
      data_type: unit.data_type,
      grid_configs: new Map(),
      scenes: new Map(),
    };
    const scene = dataset.scenes.get(unit.scene_id) || {
      scene_id: unit.scene_id,
      scene_key: unit.scene_key,
      grid_configs: new Map(),
      bands: [],
    };
    const configKey = `${unit.grid_type}:${unit.grid_level}`;
    const config = { grid_type: unit.grid_type, grid_level: unit.grid_level };
    dataset.grid_configs.set(configKey, config);
    scene.grid_configs.set(configKey, config);
    scene.bands.push(unit);
    dataset.scenes.set(unit.scene_id, scene);
    datasets.set(unit.dataset_id, dataset);
  });
  return [...datasets.values()].map((dataset) => ({
    ...dataset,
    grid_configs: [...dataset.grid_configs.values()],
    scenes: [...dataset.scenes.values()].map((scene) => ({
      ...scene,
      grid_configs: [...scene.grid_configs.values()],
      bands: scene.bands.sort((left, right) => Number(left.display_order || 0) - Number(right.display_order || 0)),
    })),
  }));
});

function toggleManualDataset(datasetId) {
  const next = new Set(collapsedManualDatasets.value);
  if (next.has(datasetId)) next.delete(datasetId); else next.add(datasetId);
  collapsedManualDatasets.value = next;
}

function toggleManualScene(dataset, scene) {
  const key = `${dataset.dataset_id}:${scene.scene_id}`;
  const next = new Set(collapsedManualScenes.value);
  if (next.has(key)) next.delete(key); else next.add(key);
  collapsedManualScenes.value = next;
}

function manualSelectionState(units = []) {
  const ids = units.map((unit) => unit.band_unit_id).filter(Boolean);
  const selected = new Set(manualBandUnitIds.value);
  const selectedCount = ids.filter((id) => selected.has(id)).length;
  return {
    checked: ids.length > 0 && selectedCount === ids.length,
    indeterminate: selectedCount > 0 && selectedCount < ids.length,
    disabled: ids.length === 0,
  };
}

function toggleManualSelection(units, checked) {
  const next = new Set(manualBandUnitIds.value);
  units.map((unit) => unit.band_unit_id).filter(Boolean).forEach((id) => {
    if (checked) next.add(id); else next.delete(id);
  });
  manualBandUnitIds.value = [...next];
}

function manualBandLabel(unit) {
  return bandDisplayLabel(unit);
}

function gridConfigLabel(configs = []) {
  return configs.filter(Boolean).map((config) => {
    const gridType = config.grid_type;
    const rawLevel = config.grid_level;
    const grid = gridDefinition(gridType)?.label || gridType;
    return grid ? `${grid} · ${nativeLevelLabel(gridType, rawLevel)}` : '';
  }).filter(Boolean).join('；') || '格网信息缺失';
}

function collectionGridLabel(collection) {
  const configs = collection?.grid_configs?.length
    ? collection.grid_configs
    : [...new Map((collection?.units || []).map((unit) => [
      `${unit.grid_type}:${unit.grid_level}`,
      { grid_type: unit.grid_type, grid_level: unit.grid_level },
    ])).values()];
  return gridConfigLabel(configs);
}

function unitGridLabel(unit) {
  return gridConfigLabel([{ grid_type: unit.grid_type, grid_level: unit.grid_level }]);
}

function changeManualCollection() {
  manualBandUnitIds.value = [];
  collapsedManualDatasets.value = new Set();
  collapsedManualScenes.value = new Set();
}
async function submitManualIngest() {
  try {
    await store.requestManualCollection(manualCollectionId.value, manualBandUnitIds.value);
    manualDialogVisible.value = false;
    manualCollectionId.value = '';
    manualBandUnitIds.value = [];
    ElMessage.success('已提交手动入库');
  } catch (requestError) {
    ElMessage.error(requestError.message || '手动入库提交失败');
  }
}

async function openManualIngest(partitionRunId = '') {
  manualDialogVisible.value = true;
  manualCollectionId.value = partitionRunId;
  manualBandUnitIds.value = [];
  collapsedManualDatasets.value = new Set();
  collapsedManualScenes.value = new Set();
  try { await store.loadManualCandidates(); } catch (requestError) { ElMessage.error(requestError.message || '可入库数据加载失败'); }
}
</script>

<template>
  <section class="ingest-view" :class="{ embedded }">
    <header class="view-header"><div><h2>{{ title }}</h2></div><div class="header-actions"><el-button :icon="Refresh" :loading="store.loading" @click="refresh">刷新</el-button></div></header>
    <section class="pending-ingest-panel" aria-label="待手动入库">
      <div class="pending-ingest-heading"><div><h3>待手动入库</h3><span>剖分完成且质检通过的剖分批次数据集合</span></div><el-button link type="primary" :loading="store.manualCandidatesLoading" @click="store.loadManualCandidates">刷新队列</el-button></div>
      <div v-if="store.manualCandidatesLoading" class="pending-state">正在加载待入库集合</div>
      <div v-else-if="!store.manualCandidates.length" class="pending-state">暂无满足入库条件的剖分批次数据集合</div>
      <div v-else class="pending-ingest-list">
        <div v-for="collection in store.manualCandidates" :key="collection.partition_run_id" class="pending-ingest-row">
          <div class="run-cell"><strong>{{ collection.partition_run_id }}</strong><span>{{ collectionGridLabel(collection) }} · {{ collection.dataset_count }} 个数据集 · {{ collection.scene_count }} 景 · {{ collection.quality_pass_count - collection.ingested_count }} 个波段待入库</span></div>
          <el-button type="primary" size="small" @click="openManualIngest(collection.partition_run_id)">选择数据入库</el-button>
        </div>
      </div>
    </section>
    <details class="ingest-history">
      <summary>入库记录 <span>{{ store.pageState.total }} 条</span></summary>
      <div class="ingest-history-body">
    <el-form class="filter-bar" label-position="top" @submit.prevent="refresh">
      <el-form-item label="关键词"><el-input v-model="store.filters.keyword" :prefix-icon="Search" clearable placeholder="运行 ID 或数据集" /></el-form-item>
      <el-form-item label="数据集 ID"><el-input v-model="store.filters.datasetId" clearable /></el-form-item>
      <el-form-item label="运行状态"><el-select v-model="store.filters.status" clearable><el-option label="已排队" value="queued" /><el-option label="运行中" value="running" /><el-option label="已完成" value="completed" /><el-option label="部分失败" value="partial_failure" /><el-option label="失败" value="failed" /><el-option label="已取消" value="cancelled" /></el-select></el-form-item>
      <el-form-item class="filter-action"><el-button native-type="submit" type="primary" :icon="Search">查询</el-button></el-form-item>
    </el-form>
    <el-alert v-if="store.error" :title="store.error" type="error" :closable="false" show-icon />
    <div class="summary-strip" aria-label="入库统计">
      <div><strong>{{ store.summary.run_count || store.pageState.total }}</strong><span>运行</span></div>
      <div><strong>{{ store.summary.band_count || 0 }}</strong><span>波段总数</span></div>
      <div><strong>{{ store.summary.completed_band_count || 0 }}</strong><span>已完成波段</span></div>
      <div><strong>{{ store.summary.failed_band_count || 0 }}</strong><span>失败波段</span></div>
    </div>
    <AppTable :data="store.records" :loading="store.loading" row-key="ingest_run_id" :page="store.pageState.page" :page-size="store.pageState.pageSize" :total="store.pageState.total" @current-change="setPage" @size-change="setPageSize" @row-click="(row) => store.openDetail(row.ingest_run_id).catch(() => {})">
      <el-table-column label="数据入库" min-width="240"><template #default="{ row }"><div class="run-cell"><strong>{{ row.ingest_run_id }}</strong><span>{{ row.dataset_code || row.dataset_id }}</span></div></template></el-table-column>
      <el-table-column prop="partition_run_id" label="剖分运行" min-width="180" show-overflow-tooltip />
      <el-table-column label="状态" width="115"><template #default="{ row }"><StatusTag domain="ingest" :value="row.status" size="small" /></template></el-table-column>
      <el-table-column label="进度" min-width="170"><template #default="{ row }"><div class="run-progress"><el-progress :percentage="row.band_count ? Math.round((row.completed_band_count || 0) * 100 / row.band_count) : 0" :stroke-width="7" /><span>{{ row.completed_band_count || 0 }}/{{ row.band_count || 0 }} 波段<span v-if="row.failed_band_count"> · {{ row.failed_band_count }} 失败</span></span></div></template></el-table-column>
      <el-table-column label="创建时间" min-width="170"><template #default="{ row }">{{ formatShanghaiTime(row.created_at) }}</template></el-table-column>
      <el-table-column label="操作" width="80" fixed="right"><template #default="{ row }"><el-button :data-testid="`ingest-row-${row.ingest_run_id}`" link type="primary" @click.stop="store.openDetail(row.ingest_run_id).catch(() => {})">详情</el-button></template></el-table-column>
    </AppTable>
    <IngestRunDetailDrawer :visible="store.detailVisible" :run-id="store.selectedRunId" :detail="store.detail" :loading="store.detailLoading" :action-loading="store.actionLoading" @close="store.closeDetail" @retry-band-units="(bandUnitIds) => store.retryFailedBandUnits(bandUnitIds).catch(() => {})" @cancel="(reason) => store.cancelRun(reason).catch(() => {})" />
      </div>
    </details>
    <el-dialog v-model="manualDialogVisible" title="手动入库" width="760px" append-to-body>
      <el-form label-width="92px" @submit.prevent="submitManualIngest">
        <el-form-item label="剖分集合"><el-select v-model="manualCollectionId" filterable clearable :loading="store.manualCandidatesLoading" placeholder="选择剖分批次数据集合" style="width: 100%" @change="changeManualCollection">
          <el-option v-for="collection in store.manualCandidates" :key="collection.partition_run_id" :label="collection.partition_run_id" :value="collection.partition_run_id"><span>{{ collection.partition_run_id }}</span><small class="candidate-meta">{{ collectionGridLabel(collection) }} · {{ collection.dataset_count }} 个数据集 · {{ collection.scene_count }} 景 · {{ collection.quality_pass_count }} 个质检通过</small></el-option>
        </el-select></el-form-item>
        <el-form-item v-if="selectedCollection" label="入库数据">
          <div class="manual-ingest-tree" data-testid="manual-ingest-tree">
            <div class="manual-tree-summary">{{ collectionGridLabel(selectedCollection) }} · {{ manualDatasetGroups.length }} 个数据集 · {{ selectableManualUnits.length }} 个可入库波段 · 已选 {{ manualBandUnitIds.length }} 个波段</div>
            <section v-for="dataset in manualDatasetGroups" :key="dataset.dataset_id" class="manual-dataset-tree">
              <div class="manual-tree-header-row">
                <button type="button" class="manual-tree-header" :aria-expanded="!collapsedManualDatasets.has(dataset.dataset_id)" @click="toggleManualDataset(dataset.dataset_id)">
                  <el-icon><Collection /></el-icon>
                  <span><strong>{{ dataset.dataset_title || dataset.dataset_code || dataset.dataset_id }}</strong><small>{{ gridConfigLabel(dataset.grid_configs) }} · {{ dataset.dataset_code || dataset.dataset_id }} · {{ dataset.scenes.length }} 景 · {{ dataset.scenes.reduce((total, scene) => total + scene.bands.length, 0) }} 个波段</small></span>
                </button>
                <el-checkbox
                  class="manual-select-all"
                  :model-value="manualSelectionState(dataset.scenes.flatMap((scene) => scene.bands)).checked"
                  :indeterminate="manualSelectionState(dataset.scenes.flatMap((scene) => scene.bands)).indeterminate"
                  :disabled="manualSelectionState(dataset.scenes.flatMap((scene) => scene.bands)).disabled"
                  :data-testid="`manual-select-dataset-${dataset.dataset_id}`"
                  @change="toggleManualSelection(dataset.scenes.flatMap((scene) => scene.bands), $event)"
                >全选数据集</el-checkbox>
              </div>
              <div v-show="!collapsedManualDatasets.has(dataset.dataset_id)" class="manual-scene-list">
                <section v-for="scene in dataset.scenes" :key="scene.scene_id" class="manual-scene-tree">
                  <div class="manual-scene-header-row">
                    <button type="button" class="manual-scene-header" :aria-expanded="!collapsedManualScenes.has(`${dataset.dataset_id}:${scene.scene_id}`)" @click="toggleManualScene(dataset, scene)">
                      <el-icon><Picture /></el-icon><span>{{ scene.scene_key || scene.scene_id }}<small>{{ gridConfigLabel(scene.grid_configs) }}</small></span>
                    </button>
                    <el-checkbox
                      class="manual-select-all"
                      :model-value="manualSelectionState(scene.bands).checked"
                      :indeterminate="manualSelectionState(scene.bands).indeterminate"
                      :disabled="manualSelectionState(scene.bands).disabled"
                      :data-testid="`manual-select-scene-${scene.scene_id}`"
                      @change="toggleManualSelection(scene.bands, $event)"
                    >全选该景</el-checkbox>
                  </div>
                  <el-checkbox-group v-show="!collapsedManualScenes.has(`${dataset.dataset_id}:${scene.scene_id}`)" v-model="manualBandUnitIds" class="manual-band-list">
                    <el-checkbox v-for="unit in scene.bands" :key="unit.band_unit_id" :value="unit.band_unit_id" :data-testid="`manual-ingest-band-${unit.band_unit_id}`">
                      <span class="manual-band-chip"><small>{{ dataUnitTypeLabel(dataset.data_type) }} · {{ unitGridLabel(unit) }}</small>{{ manualBandLabel(unit) }}</span>
                    </el-checkbox>
                  </el-checkbox-group>
                </section>
              </div>
            </section>
            <div v-if="!manualDatasetGroups.length" class="manual-tree-empty">当前集合没有可手动入库的波段</div>
          </div>
        </el-form-item>
      </el-form>
      <template #footer><el-button @click="manualDialogVisible = false">取消</el-button><el-button type="primary" :loading="store.actionLoading" :disabled="!manualCollectionId || !manualBandUnitIds.length" @click="submitManualIngest">提交入库</el-button></template>
    </el-dialog>
  </section>
</template>

<style scoped>
.ingest-view { padding: 24px; }
.ingest-view.embedded { padding: 0; }
.view-header { display: flex; align-items: center; justify-content: space-between; gap: 16px; margin-bottom: 18px; }
.view-header h2 { margin: 0 0 3px; color: #172033; font-size: 18px; letter-spacing: 0; }
.view-header span { color: #748095; font-size: 13px; }
.header-actions { display: flex; align-items: center; gap: 8px; }
.candidate-meta { display: block; margin-top: 2px; color: #8993a4; font-size: 12px; }
.pending-ingest-panel { margin-bottom: 18px; padding: 16px; border: 1px solid #dfe4ec; border-radius: 6px; background: #fff; }
.pending-ingest-heading { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 12px; }
.pending-ingest-heading h3 { margin: 0 0 3px; color: #263247; font-size: 15px; }
.pending-ingest-heading span, .pending-state { color: #8993a4; font-size: 12px; }
.pending-state { padding: 18px 0 4px; text-align: center; }
.pending-ingest-list { display: flex; flex-direction: column; gap: 8px; }
.pending-ingest-row { display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 10px 12px; border: 1px solid #e6eaf0; border-radius: 5px; background: #fafbfd; }
.ingest-history { margin-top: 18px; border: 1px solid #dfe4ec; border-radius: 6px; background: #fff; }
.ingest-history summary { padding: 13px 16px; color: #263247; font-size: 14px; font-weight: 600; cursor: pointer; list-style: none; }
.ingest-history summary::-webkit-details-marker { display: none; }
.ingest-history summary span { margin-left: 8px; color: #8993a4; font-size: 12px; font-weight: 400; }
.ingest-history-body { padding: 0 16px 16px; }
.filter-bar { display: grid; grid-template-columns: repeat(4, minmax(150px, 1fr)); gap: 0 12px; margin-bottom: 16px; }
.filter-bar :deep(.el-form-item) { margin-bottom: 12px; }
.filter-action { align-self: end; }
.summary-strip { display: grid; grid-template-columns: repeat(4, minmax(120px, 1fr)); border: 1px solid #dfe4ec; border-radius: 6px; margin-bottom: 18px; background: #fff; overflow: hidden; }
.summary-strip div { display: flex; align-items: baseline; gap: 8px; padding: 12px 16px; border-right: 1px solid var(--el-border-color-lighter); }
.summary-strip div:last-child { border-right: 0; }
.summary-strip strong { font-size: 22px; color: #1769aa; }
.summary-strip span { color: var(--el-text-color-secondary); }
.run-cell { display: flex; flex-direction: column; min-width: 0; gap: 3px; }
.run-cell strong { overflow: hidden; color: #263247; font-weight: 600; text-overflow: ellipsis; white-space: nowrap; }
.run-cell span, .run-progress > span { color: #8993a4; font-size: 12px; }
.run-progress { min-width: 130px; }
.run-progress :deep(.el-progress__text) { min-width: 36px; font-size: 12px !important; }
.manual-ingest-tree { width: 100%; max-height: 480px; overflow: auto; border: 1px solid #dfe4ec; border-radius: 5px; }
.manual-tree-summary { padding: 9px 12px; color: #748095; font-size: 12px; background: #fafbfd; border-bottom: 1px solid #e6eaf0; }
.manual-dataset-tree + .manual-dataset-tree { border-top: 1px solid #e6eaf0; }
.manual-tree-header-row, .manual-scene-header-row { display: flex; align-items: center; gap: 10px; }
.manual-tree-header, .manual-scene-header { display: flex; min-width: 0; flex: 1; align-items: center; gap: 8px; border: 0; background: transparent; color: #263247; cursor: pointer; text-align: left; }
.manual-tree-header { padding: 10px 12px; }
.manual-tree-header .el-icon { color: #1769aa; font-size: 17px; }
.manual-tree-header span { display: flex; min-width: 0; flex-direction: column; gap: 2px; }
.manual-tree-header strong, .manual-tree-header small { overflow-wrap: anywhere; }
.manual-tree-header small { color: #8993a4; font-size: 12px; }
.manual-scene-list { padding: 0 12px 10px 38px; }
.manual-scene-tree + .manual-scene-tree { margin-top: 8px; }
.manual-scene-header { padding: 5px 0; font-size: 13px; }
.manual-scene-header span { display: flex; flex-direction: column; gap: 1px; }
.manual-scene-header small { color: #8993a4; font-size: 11px; }
.manual-scene-header .el-icon { color: #748095; }
.manual-select-all { flex: none; margin-left: auto; }
.manual-band-list { display: flex; flex-wrap: wrap; gap: 6px 12px; padding: 4px 0 0 24px; }
.manual-band-list :deep(.el-checkbox) { height: auto; margin: 0; }
.manual-band-list :deep(.el-checkbox__label) { min-width: 0; white-space: normal; }
.manual-band-chip { display: flex; flex-direction: column; padding: 3px 7px; border: 1px solid #bfd5e8; border-radius: 4px; background: #f3f8fc; color: #2d628d; font-size: 11px; line-height: 1.35; }
.manual-band-chip small { color: #748095; font-size: 10px; }
.manual-tree-empty { padding: 18px; color: #8993a4; text-align: center; font-size: 12px; }
@media (max-width: 760px) { .ingest-view { padding: 16px; } .filter-bar { grid-template-columns: 1fr; } .summary-strip { grid-template-columns: repeat(2, 1fr); } .summary-strip div:nth-child(2) { border-right: 0; } .manual-tree-header-row, .manual-scene-header-row { align-items: stretch; flex-direction: column; gap: 2px; } .manual-select-all { margin-left: 0; } }
</style>
