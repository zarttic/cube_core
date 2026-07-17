<script setup>
import { onMounted, onUnmounted } from 'vue';
import { Refresh, Search } from '@element-plus/icons-vue';

import AppTable from '@/components/AppTable.vue';
import StatusTag from '@/components/StatusTag.vue';
import { m6WritesEnabled } from '@/config';
import { useIngestRunsStore } from '@/stores/ingestRuns';
import IngestRunDetailDrawer from '@/views/ingest/IngestRunDetailDrawer.vue';

defineProps({ embedded: Boolean, title: { type: String, default: '数据入库' } });
const store = useIngestRunsStore();

function refresh() { store.pageState.page = 1; return store.loadList(); }
function setPage(page) { store.pageState.page = page; return store.loadList(); }
function setPageSize(pageSize) { Object.assign(store.pageState, { page: 1, pageSize }); return store.loadList(); }

onMounted(() => { refresh().catch(() => {}); });
onUnmounted(() => store.dispose());
</script>

<template>
  <section class="ingest-view" :class="{ embedded }">
    <header class="view-header"><div><h2>{{ title }}</h2><span>{{ store.pageState.total }} 个入库运行</span></div><el-button :icon="Refresh" :loading="store.loading" @click="refresh">刷新</el-button></header>
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
      <el-table-column label="入库运行" min-width="240"><template #default="{ row }"><div class="run-cell"><strong>{{ row.ingest_run_id }}</strong><span>{{ row.dataset_code || row.dataset_id }}</span></div></template></el-table-column>
      <el-table-column prop="partition_run_id" label="剖分运行" min-width="180" show-overflow-tooltip />
      <el-table-column label="状态" width="115"><template #default="{ row }"><StatusTag domain="ingest" :value="row.status" size="small" /></template></el-table-column>
      <el-table-column label="进度" min-width="170"><template #default="{ row }"><div class="run-progress"><el-progress :percentage="row.scene_count ? Math.round((row.completed_scene_count || 0) * 100 / row.scene_count) : 0" :stroke-width="7" /><span>{{ row.completed_scene_count || 0 }}/{{ row.scene_count || 0 }} 景<span v-if="row.failed_scene_count"> · {{ row.failed_scene_count }} 失败</span></span></div></template></el-table-column>
      <el-table-column prop="created_at" label="创建时间" min-width="170" />
      <el-table-column label="操作" width="80" fixed="right"><template #default="{ row }"><el-button :data-testid="`ingest-row-${row.ingest_run_id}`" link type="primary" @click.stop="store.openDetail(row.ingest_run_id).catch(() => {})">详情</el-button></template></el-table-column>
    </AppTable>
    <IngestRunDetailDrawer :visible="store.detailVisible" :run-id="store.selectedRunId" :detail="store.detail" :loading="store.detailLoading" :action-loading="store.actionLoading" :write-enabled="m6WritesEnabled()" @close="store.closeDetail" @retry-scenes="(sceneIds) => store.retryFailedScenes(sceneIds).catch(() => {})" @cancel="(reason) => store.cancelRun(reason).catch(() => {})" />
  </section>
</template>

<style scoped>
.ingest-view { padding: 24px; }
.ingest-view.embedded { padding: 0; }
.view-header { display: flex; align-items: center; justify-content: space-between; gap: 16px; margin-bottom: 18px; }
.view-header h2 { margin: 0 0 3px; color: #172033; font-size: 18px; letter-spacing: 0; }
.view-header span { color: #748095; font-size: 13px; }
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
