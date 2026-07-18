<script setup>
import { computed, onMounted, onUnmounted, ref } from 'vue';
import { Refresh, Search } from '@element-plus/icons-vue';
import { ElMessage } from 'element-plus';

import AppTable from '@/components/AppTable.vue';
import StatusTag from '@/components/StatusTag.vue';
import { useIngestRunsStore } from '@/stores/ingestRuns';
import { formatShanghaiTime } from '@/utils/time';
import IngestRunDetailDrawer from '@/views/ingest/IngestRunDetailDrawer.vue';

defineProps({ embedded: Boolean, title: { type: String, default: '数据入库' } });
const store = useIngestRunsStore();
const manualDialogVisible = ref(false);
const manualCollectionId = ref('');
const manualSceneIds = ref([]);

function refresh() { store.pageState.page = 1; return store.loadList(); }
function setPage(page) { store.pageState.page = page; return store.loadList(); }
function setPageSize(pageSize) { Object.assign(store.pageState, { page: 1, pageSize }); return store.loadList(); }

onMounted(() => { refresh().catch(() => {}); store.loadManualCandidates().catch(() => {}); });
onUnmounted(() => store.dispose());

const selectedCollection = computed(() => store.manualCandidates.find((item) => item.partition_run_id === manualCollectionId.value));
async function submitManualIngest() {
  try {
    await store.requestManualCollection(manualCollectionId.value, manualSceneIds.value);
    manualDialogVisible.value = false;
    manualCollectionId.value = '';
    manualSceneIds.value = [];
    ElMessage.success('已提交手动入库');
  } catch (requestError) {
    ElMessage.error(requestError.message || '手动入库提交失败');
  }
}

async function openManualIngest(partitionRunId = '') {
  manualDialogVisible.value = true;
  manualCollectionId.value = partitionRunId;
  manualSceneIds.value = [];
  try { await store.loadManualCandidates(); } catch (requestError) { ElMessage.error(requestError.message || '可入库数据加载失败'); }
}
</script>

<template>
  <section class="ingest-view" :class="{ embedded }">
    <header class="view-header"><div><h2>{{ title }}</h2><span>{{ store.pageState.total }} 条数据入库记录</span></div><div class="header-actions"><el-button :icon="Refresh" :loading="store.loading" @click="refresh">刷新</el-button></div></header>
    <section class="pending-ingest-panel" aria-label="待手动入库">
      <div class="pending-ingest-heading"><div><h3>待手动入库</h3><span>剖分完成且质检通过的剖分批次数据集合</span></div><el-button link type="primary" :loading="store.manualCandidatesLoading" @click="store.loadManualCandidates">刷新队列</el-button></div>
      <div v-if="store.manualCandidatesLoading" class="pending-state">正在加载待入库集合</div>
      <div v-else-if="!store.manualCandidates.length" class="pending-state">暂无满足入库条件的剖分批次数据集合</div>
      <div v-else class="pending-ingest-list">
        <div v-for="collection in store.manualCandidates" :key="collection.partition_run_id" class="pending-ingest-row">
          <div class="run-cell"><strong>{{ collection.partition_run_id }}</strong><span>{{ collection.dataset_count }} 个数据集 · {{ collection.scene_count }} 景 · {{ collection.quality_pass_count - collection.ingested_count }} 个数据单元待入库</span></div>
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
      <div><strong>{{ store.summary.scene_count || 0 }}</strong><span>景总数</span></div>
      <div><strong>{{ store.summary.completed_scene_count || 0 }}</strong><span>已完成景</span></div>
      <div><strong>{{ store.summary.failed_scene_count || 0 }}</strong><span>失败景</span></div>
    </div>
    <AppTable :data="store.records" :loading="store.loading" row-key="ingest_run_id" :page="store.pageState.page" :page-size="store.pageState.pageSize" :total="store.pageState.total" @current-change="setPage" @size-change="setPageSize" @row-click="(row) => store.openDetail(row.ingest_run_id).catch(() => {})">
      <el-table-column label="数据入库" min-width="240"><template #default="{ row }"><div class="run-cell"><strong>{{ row.ingest_run_id }}</strong><span>{{ row.dataset_code || row.dataset_id }}</span></div></template></el-table-column>
      <el-table-column prop="partition_run_id" label="剖分运行" min-width="180" show-overflow-tooltip />
      <el-table-column label="状态" width="115"><template #default="{ row }"><StatusTag domain="ingest" :value="row.status" size="small" /></template></el-table-column>
      <el-table-column label="进度" min-width="170"><template #default="{ row }"><div class="run-progress"><el-progress :percentage="row.scene_count ? Math.round((row.completed_scene_count || 0) * 100 / row.scene_count) : 0" :stroke-width="7" /><span>{{ row.completed_scene_count || 0 }}/{{ row.scene_count || 0 }} 景<span v-if="row.failed_scene_count"> · {{ row.failed_scene_count }} 失败</span></span></div></template></el-table-column>
      <el-table-column label="创建时间" min-width="170"><template #default="{ row }">{{ formatShanghaiTime(row.created_at) }}</template></el-table-column>
      <el-table-column label="操作" width="80" fixed="right"><template #default="{ row }"><el-button :data-testid="`ingest-row-${row.ingest_run_id}`" link type="primary" @click.stop="store.openDetail(row.ingest_run_id).catch(() => {})">详情</el-button></template></el-table-column>
    </AppTable>
    <IngestRunDetailDrawer :visible="store.detailVisible" :run-id="store.selectedRunId" :detail="store.detail" :loading="store.detailLoading" :action-loading="store.actionLoading" @close="store.closeDetail" @retry-scenes="(sceneIds) => store.retryFailedScenes(sceneIds).catch(() => {})" @cancel="(reason) => store.cancelRun(reason).catch(() => {})" />
      </div>
    </details>
    <el-dialog v-model="manualDialogVisible" title="手动入库" width="460px" append-to-body>
      <el-form label-width="92px" @submit.prevent="submitManualIngest">
        <el-form-item label="剖分集合"><el-select v-model="manualCollectionId" filterable clearable :loading="store.manualCandidatesLoading" placeholder="选择剖分批次数据集合" style="width: 100%">
          <el-option v-for="collection in store.manualCandidates" :key="collection.partition_run_id" :label="collection.partition_run_id" :value="collection.partition_run_id"><span>{{ collection.partition_run_id }}</span><small class="candidate-meta">{{ collection.dataset_count }} 个数据集 · {{ collection.scene_count }} 景 · {{ collection.quality_pass_count }} 个质检通过</small></el-option>
        </el-select></el-form-item>
        <el-form-item v-if="selectedCollection" label="入库数据"><el-select v-model="manualSceneIds" multiple collapse-tags filterable placeholder="选择要入库的数据单元" style="width: 100%"><el-option v-for="unit in selectedCollection.units.filter((item) => item.quality_status && item.ingest_status !== 'completed')" :key="unit.scene_id" :label="unit.dataset_code + ' / ' + unit.scene_id" :value="unit.scene_id" /></el-select></el-form-item>
      </el-form>
      <template #footer><el-button @click="manualDialogVisible = false">取消</el-button><el-button type="primary" :loading="store.actionLoading" :disabled="!manualCollectionId || !manualSceneIds.length" @click="submitManualIngest">提交入库</el-button></template>
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
@media (max-width: 760px) { .ingest-view { padding: 16px; } .filter-bar { grid-template-columns: 1fr; } .summary-strip { grid-template-columns: repeat(2, 1fr); } .summary-strip div:nth-child(2) { border-right: 0; } }
</style>
