<script setup>
import { computed, onMounted, onUnmounted } from 'vue';
import { RefreshOutline, SearchOutline } from '@vicons/ionicons5';

import AppTable from '@/components/AppTable.vue';
import { useDatasetsStore } from '@/stores/datasets';
import { formatShanghaiRange } from '@/utils/time';
import DatasetDetailDrawer from '@/views/datasets/DatasetDetailDrawer.vue';

const props = defineProps({
  embedded: Boolean,
  title: { type: String, default: '数据集' },
});
const emit = defineEmits(['queue-partition']);
const store = useDatasetsStore();
const dataTypeLabels = {
  optical: '光学遥感',
  radar: '雷达遥感',
  carbon: '碳卫星',
  product: '信息产品',
};

const timeRange = computed({
  get: () => (store.filters.timeStart && store.filters.timeEnd
    ? [store.filters.timeStart, store.filters.timeEnd]
    : []),
  set: (value) => {
    store.filters.timeStart = value?.[0] || '';
    store.filters.timeEnd = value?.[1] || '';
  },
});

function dataTypeLabel(value) {
  return dataTypeLabels[value] || value || '-';
}

function refresh() {
  store.pageState.page = 1;
  return store.loadList();
}


function resetFilters() {
  Object.assign(store.filters, { keyword: '', dataType: '', timeStart: '', timeEnd: '' });
  return refresh();
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

function queuePartition(payload) {
  emit('queue-partition', payload);
}

onMounted(() => { refresh().catch(() => {}); });
onUnmounted(() => store.dispose());
</script>

<template>
  <section class="datasets-view" :class="{ embedded }">
    <header v-if="title" class="view-header">
      <div>
        <h2>{{ title }}</h2>
      </div>
    </header>

    <el-form class="filter-bar" label-position="top" @submit.prevent="refresh">
      <el-form-item label="关键词"><el-input v-model="store.filters.keyword" :prefix-icon="SearchOutline" clearable placeholder="数据集编码、名称或关键词" /></el-form-item>
      <el-form-item label="数据类型"><el-select v-model="store.filters.dataType" clearable placeholder="全部类型"><el-option v-for="(label, value) in dataTypeLabels" :key="value" :label="label" :value="value" /></el-select></el-form-item>
      <el-form-item label="数据时间"><el-date-picker v-model="timeRange" type="daterange" format="YYYY年M月D日" value-format="YYYY-MM-DD" range-separator="至" start-placeholder="开始日期" end-placeholder="结束日期" clearable style="width: 100%" /></el-form-item>
      <el-form-item class="filter-action">
        <el-button native-type="submit" type="primary" :icon="SearchOutline">查询</el-button>
        <el-button @click="resetFilters">重置</el-button>
        <div class="filter-actions-end">
          <el-button :icon="RefreshOutline" :loading="store.loading" title="刷新" aria-label="刷新" @click="refresh" />
        </div>
      </el-form-item>
    </el-form>

    <el-alert v-if="store.error" :title="store.error" type="error" :closable="false" show-icon />
    <div class="summary-strip" aria-label="数据集统计">
      <div><strong>{{ store.summary.dataset_count || store.pageState.total }}</strong><span>数据集</span></div>
      <div><strong>{{ store.summary.scene_count || 0 }}</strong><span>景总数</span></div>
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
      <el-table-column prop="scene_count" label="景数量" width="90" />
      <el-table-column label="时间范围" min-width="180"><template #default="{ row }">{{ formatShanghaiRange(row.time_start, row.time_end) }}</template></el-table-column>
      <el-table-column label="操作" width="86" fixed="right"><template #default="{ row }"><el-button :data-testid="`dataset-row-${row.dataset_id}`" link type="primary" @click.stop="store.openDetail(row.dataset_id).catch(() => {})">详情</el-button></template></el-table-column>
    </AppTable>

    <DatasetDetailDrawer
      :visible="store.detailVisible"
      test-id="dataset-detail-drawer"
      :dataset-id="store.selectedDatasetId"
      :detail="store.detail"
      :loading="store.detailLoading"
      :action-loading="store.actionLoading"
      :pending-grid-deletes="store.pendingGridDeletes"
      :hidden-roles="store.hiddenRoles"
      :role-restrictions-loading="store.roleRestrictionsLoading"
      :active-tab="store.activeTab"
      :tab-pages="store.tabPages"
      @close="store.closeDetail"
      @tab-change="(tab) => store.setActiveTab(tab).catch(() => {})"
      @tab-page-change="({ tab, page }) => store.setTabPage(tab, page).catch(() => {})"
      @tab-page-size-change="({ tab, pageSize }) => store.setTabPageSize(tab, pageSize).catch(() => {})"
      @update-metadata="(payload) => store.updateMetadata(payload).catch(() => {})"
      @update-role-restrictions="(roles) => store.updateRoleRestrictions(roles).catch(() => {})"
      @reassign-scene="({ scene_id, target_dataset_id, reason }) => store.reassignScene(scene_id, target_dataset_id, reason).catch(() => {})"
      @retry-band-ingest="(bandUnitId) => store.retryBandIngest(bandUnitId).catch(() => {})"
      @delete-band-grid="({ band_unit_id, grid_type }) => store.deleteBandGrid(band_unit_id, grid_type).catch(() => {})"
      @delete-dataset="store.deleteDataset().catch(() => {})"
      @queue-partition="queuePartition"
      @withdraw="(publicationId) => store.withdraw(publicationId).catch(() => {})"
    />
  </section>
</template>

<style scoped>
.datasets-view { padding: 24px; }
.datasets-view.embedded { padding: 0; }
.view-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; margin-bottom: 18px; }
.view-header h2 { margin: 0 0 3px; color: #172033; font-size: 18px; letter-spacing: 0; }
.filter-bar { display: grid; grid-template-columns: repeat(3, minmax(160px, 1fr)) minmax(240px, 1fr); gap: 0 12px; margin-bottom: 16px; }
.filter-bar :deep(.el-form-item) { margin-bottom: 12px; }
.filter-action { align-self: end; }
.filter-action :deep(.el-form-item__content) { flex-wrap: nowrap; width: 100%; }
.filter-action :deep(.el-button) { margin-left: 0; }
.filter-action :deep(.el-button + .el-button) { margin-left: 8px; }
.filter-actions-end { display: flex; align-items: center; gap: 8px; margin-left: auto; }
.summary-strip { display: grid; grid-template-columns: repeat(2, minmax(120px, 1fr)); border: 1px solid #dfe4ec; border-radius: 6px; margin-bottom: 18px; background: #fff; overflow: hidden; }
.summary-strip div { display: flex; align-items: baseline; gap: 8px; padding: 12px 16px; border-right: 1px solid var(--el-border-color-lighter); }
.summary-strip div:last-child { border-right: 0; }
.summary-strip strong { font-size: 22px; color: #1769aa; }
.summary-strip span { color: var(--el-text-color-secondary); }
.dataset-cell { display: flex; flex-direction: column; min-width: 0; gap: 3px; }
.dataset-cell strong { overflow: hidden; color: #263247; font-weight: 600; text-overflow: ellipsis; white-space: nowrap; }
.dataset-cell span { overflow: hidden; color: #8993a4; font-size: 12px; text-overflow: ellipsis; white-space: nowrap; }
@media (max-width: 880px) { .filter-bar { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (max-width: 560px) { .datasets-view { padding: 16px; } .filter-bar { grid-template-columns: 1fr; } .view-header { align-items: stretch; flex-direction: column; } }
</style>
