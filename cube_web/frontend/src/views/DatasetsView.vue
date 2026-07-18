<script setup>
import { onMounted, onUnmounted } from 'vue';
import { Refresh, Search } from '@element-plus/icons-vue';

import AppTable from '@/components/AppTable.vue';
import StatusTag from '@/components/StatusTag.vue';
import { useDatasetsStore } from '@/stores/datasets';
import DatasetDetailDrawer from '@/views/datasets/DatasetDetailDrawer.vue';

const props = defineProps({
  embedded: Boolean,
  title: { type: String, default: '数据集' },
});
const store = useDatasetsStore();
const dataTypeLabels = {
  optical: '光学遥感',
  radar: '雷达遥感',
  carbon: '碳卫星',
  product: '信息产品',
};

function dataTypeLabel(value) {
  return dataTypeLabels[value] || value || '-';
}

function refresh() {
  store.pageState.page = 1;
  return store.loadList();
}

function setPage(page) {
  store.pageState.page = page;
  return store.loadList();
}

function setPageSize(pageSize) {
  store.pageState.pageSize = pageSize;
  store.pageState.page = 1;
  return store.loadList();
}

onMounted(() => { refresh().catch(() => {}); });
onUnmounted(() => store.dispose());
</script>

<template>
  <section class="datasets-view" :class="{ embedded }">
    <header class="view-header">
      <div>
        <h2>{{ title }}</h2>
        <span>{{ store.pageState.total }} 个数据集 · {{ store.summary.scene_count || 0 }} 景</span>
      </div>
      <el-button :icon="Refresh" :loading="store.loading" @click="refresh">刷新</el-button>
    </header>

    <el-form class="filter-bar" label-position="top" @submit.prevent="refresh">
      <el-form-item label="关键词"><el-input v-model="store.filters.keyword" :prefix-icon="Search" clearable placeholder="数据集编码、名称或关键词" /></el-form-item>
      <el-form-item label="数据类型"><el-select v-model="store.filters.dataType" clearable placeholder="全部类型"><el-option v-for="(label, value) in dataTypeLabels" :key="value" :label="label" :value="value" /></el-select></el-form-item>
      <el-form-item label="产品类型"><el-input v-model="store.filters.productType" clearable /></el-form-item>
      <el-form-item label="入库状态"><el-select v-model="store.filters.ingestStatus" clearable><el-option label="运行中" value="running" /><el-option label="已完成" value="completed" /><el-option label="部分失败" value="partial_failure" /><el-option label="失败" value="failed" /></el-select></el-form-item>
      <el-form-item label="质量状态"><el-select v-model="store.filters.qualityStatus" clearable><el-option label="通过" value="pass" /><el-option label="告警" value="warn" /><el-option label="失败" value="fail" /><el-option label="异常" value="error" /></el-select></el-form-item>
      <el-form-item label="发布状态"><el-select v-model="store.filters.publishStatus" clearable><el-option label="未发布" value="unpublished" /><el-option label="已发布" value="active" /><el-option label="已撤回" value="withdrawn" /></el-select></el-form-item>
      <el-form-item label="归档状态"><el-select v-model="store.filters.archived" clearable><el-option label="使用中" value="false" /><el-option label="已归档" value="true" /></el-select></el-form-item>
      <el-form-item class="filter-action"><el-button native-type="submit" type="primary" :icon="Search">查询</el-button></el-form-item>
    </el-form>

    <el-alert v-if="store.error" :title="store.error" type="error" :closable="false" show-icon />
    <div class="summary-strip" aria-label="数据集统计">
      <div><strong>{{ store.summary.dataset_count || store.pageState.total }}</strong><span>数据集</span></div>
      <div><strong>{{ store.summary.scene_count || 0 }}</strong><span>景总数</span></div>
      <div><strong>{{ store.summary.ready_scene_count || 0 }}</strong><span>可用景</span></div>
      <div><strong>{{ store.summary.failed_scene_count || 0 }}</strong><span>异常景</span></div>
    </div>
    <AppTable
      :data="store.records"
      :loading="store.loading"
      row-key="dataset_id"
      :page="store.pageState.page"
      :page-size="store.pageState.pageSize"
      :total="store.pageState.total"
      @current-change="setPage"
      @size-change="setPageSize"
      @row-click="(row) => store.openDetail(row.dataset_id).catch(() => {})"
    >
      <el-table-column label="数据集" min-width="250">
        <template #default="{ row }"><div class="dataset-cell"><strong>{{ row.dataset_title || row.dataset_code }}</strong><span>{{ row.dataset_code || row.dataset_id }}</span></div></template>
      </el-table-column>
      <el-table-column label="数据类型" width="120"><template #default="{ row }">{{ dataTypeLabel(row.data_type) }}</template></el-table-column>
      <el-table-column prop="product_type" label="产品类型" min-width="170" show-overflow-tooltip />
      <el-table-column prop="scene_count" label="景数量" width="90" />
      <el-table-column label="时间范围" min-width="180"><template #default="{ row }">{{ row.time_start || '-' }} 至 {{ row.time_end || '-' }}</template></el-table-column>
      <el-table-column prop="current_output_version" label="当前版本" min-width="130" show-overflow-tooltip />
      <el-table-column label="质检" width="110"><template #default="{ row }"><StatusTag domain="quality" :value="row.quality_status" size="small" /></template></el-table-column>
      <el-table-column label="入库" width="110"><template #default="{ row }"><StatusTag domain="ingest" :value="row.ingest_status" size="small" /></template></el-table-column>
      <el-table-column label="发布" width="110"><template #default="{ row }"><StatusTag domain="publication" :value="row.publish_status" size="small" /></template></el-table-column>
      <el-table-column label="操作" width="86" fixed="right"><template #default="{ row }"><el-button :data-testid="`dataset-row-${row.dataset_id}`" link type="primary" @click.stop="store.openDetail(row.dataset_id).catch(() => {})">详情</el-button></template></el-table-column>
    </AppTable>

    <DatasetDetailDrawer
      :visible="store.detailVisible"
      test-id="dataset-detail-drawer"
      :dataset-id="store.selectedDatasetId"
      :detail="store.detail"
      :loading="store.detailLoading"
      :action-loading="store.actionLoading"
      :active-tab="store.activeTab"
      :tab-pages="store.tabPages"
      @close="store.closeDetail"
      @tab-change="(tab) => store.setActiveTab(tab).catch(() => {})"
      @tab-page-change="({ tab, page }) => store.setTabPage(tab, page).catch(() => {})"
      @tab-page-size-change="({ tab, pageSize }) => store.setTabPageSize(tab, pageSize).catch(() => {})"
      @update-metadata="(payload) => store.updateMetadata(payload).catch(() => {})"
      @reassign-scene="({ scene_id, target_dataset_id, reason }) => store.reassignScene(scene_id, target_dataset_id, reason).catch(() => {})"
      @rerun-quality="store.rerunQuality().catch(() => {})"
      @retry-scene-ingest="(sceneId) => store.retrySceneIngest(sceneId).catch(() => {})"
      @publish="store.publish().catch(() => {})"
      @withdraw="(publicationId) => store.withdraw(publicationId).catch(() => {})"
      @archive="(reason) => store.archive(reason).catch(() => {})"
    />
  </section>
</template>

<style scoped>
.datasets-view { padding: 24px; }
.datasets-view.embedded { padding: 0; }
.view-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; margin-bottom: 18px; }
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
.dataset-cell { display: flex; flex-direction: column; min-width: 0; gap: 3px; }
.dataset-cell strong { overflow: hidden; color: #263247; font-weight: 600; text-overflow: ellipsis; white-space: nowrap; }
.dataset-cell span { overflow: hidden; color: #8993a4; font-size: 12px; text-overflow: ellipsis; white-space: nowrap; }
@media (max-width: 880px) { .filter-bar { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (max-width: 560px) { .datasets-view { padding: 16px; } .filter-bar { grid-template-columns: 1fr; } .view-header { align-items: stretch; flex-direction: column; } .summary-strip { grid-template-columns: repeat(2, 1fr); } .summary-strip div:nth-child(2) { border-right: 0; } }
</style>
