<script setup>
import { onMounted, onUnmounted } from 'vue';
import { Refresh, Search } from '@element-plus/icons-vue';

import AppTable from '@/components/AppTable.vue';
import StatusTag from '@/components/StatusTag.vue';
import { useDatasetsStore } from '@/stores/datasets';
import DatasetDetailDrawer from '@/views/datasets/DatasetDetailDrawer.vue';

const store = useDatasetsStore();

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
  <section class="datasets-view">
    <header class="view-header">
      <div>
        <h1>剖分数据集</h1>
      </div>
      <el-button :icon="Refresh" :loading="store.loading" @click="refresh">刷新</el-button>
    </header>

    <el-form class="filter-bar" label-position="top" @submit.prevent="refresh">
      <el-form-item label="关键词"><el-input v-model="store.filters.keyword" :prefix-icon="Search" clearable placeholder="数据集、标题或批次" /></el-form-item>
      <el-form-item label="数据类型"><el-input v-model="store.filters.dataType" clearable placeholder="例如 optical" /></el-form-item>
      <el-form-item label="产品类型"><el-input v-model="store.filters.productType" clearable /></el-form-item>
      <el-form-item label="批次"><el-input v-model="store.filters.batchId" clearable /></el-form-item>
      <el-form-item label="格网"><el-select v-model="store.filters.gridType" clearable><el-option label="Geohash" value="geohash" /><el-option label="MGRS" value="mgrs" /><el-option label="ISEA4H" value="isea4h" /></el-select></el-form-item>
      <el-form-item label="剖分状态"><el-input v-model="store.filters.partitionStatus" clearable /></el-form-item>
      <el-form-item label="质量状态"><el-select v-model="store.filters.qualityStatus" clearable><el-option label="通过" value="pass" /><el-option label="告警" value="warn" /><el-option label="失败" value="fail" /><el-option label="异常" value="error" /></el-select></el-form-item>
      <el-form-item label="发布状态"><el-select v-model="store.filters.publishStatus" clearable><el-option label="未发布" value="unpublished" /><el-option label="已发布" value="active" /><el-option label="已撤回" value="withdrawn" /></el-select></el-form-item>
      <el-form-item class="filter-action"><el-button native-type="submit" type="primary">查询</el-button></el-form-item>
    </el-form>

    <el-alert v-if="store.error" :title="store.error" type="error" :closable="false" show-icon />
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
      <el-table-column prop="dataset_code" label="数据集" min-width="170" show-overflow-tooltip />
      <el-table-column prop="dataset_title" label="名称" min-width="190" show-overflow-tooltip />
      <el-table-column prop="batch_id" label="批次" min-width="160" show-overflow-tooltip />
      <el-table-column prop="data_type" label="数据类型" width="120" />
      <el-table-column prop="product_type" label="产品类型" min-width="130" />
      <el-table-column prop="grid_type" label="格网" width="110" />
      <el-table-column prop="requested_grid_level" label="请求层级" width="100" />
      <el-table-column label="剖分" width="110"><template #default="{ row }"><StatusTag domain="partition" :value="row.partition_status" size="small" /></template></el-table-column>
      <el-table-column label="质检" width="110"><template #default="{ row }"><StatusTag domain="quality" :value="row.quality_status" size="small" /></template></el-table-column>
      <el-table-column label="发布" width="110"><template #default="{ row }"><StatusTag domain="publication" :value="row.publish_status" size="small" /></template></el-table-column>
      <el-table-column label="操作" width="86" fixed="right"><template #default="{ row }"><el-button :data-testid="`dataset-row-${row.dataset_id}`" link type="primary" @click.stop="store.openDetail(row.dataset_id).catch(() => {})">详情</el-button></template></el-table-column>
    </AppTable>

    <DatasetDetailDrawer
      :visible="store.detailVisible"
      test-id="dataset-detail-drawer"
      :dataset-id="store.selectedDatasetId"
      :detail="store.detail"
      :loading="store.detailLoading"
      :active-tab="store.activeTab"
      :tab-pages="store.tabPages"
      @close="store.closeDetail"
      @tab-change="(tab) => store.setActiveTab(tab).catch(() => {})"
      @tab-page-change="({ tab, page }) => store.setTabPage(tab, page).catch(() => {})"
      @tab-page-size-change="({ tab, pageSize }) => store.setTabPageSize(tab, pageSize).catch(() => {})"
    />
  </section>
</template>

<style scoped>
.datasets-view { padding: 24px; }
.view-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; margin-bottom: 18px; }
.view-header h1 { margin: 0; font-size: 22px; letter-spacing: 0; }
.filter-bar { display: grid; grid-template-columns: repeat(4, minmax(150px, 1fr)); gap: 0 12px; margin-bottom: 16px; }
.filter-bar :deep(.el-form-item) { margin-bottom: 12px; }
.filter-action { align-self: end; }
@media (max-width: 880px) { .filter-bar { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (max-width: 560px) { .datasets-view { padding: 16px; } .filter-bar { grid-template-columns: 1fr; } .view-header { align-items: stretch; flex-direction: column; } }
</style>
