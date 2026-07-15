<script setup>
import { onMounted, onUnmounted } from 'vue';
import { Refresh, Search } from '@element-plus/icons-vue';
import { ElMessage } from 'element-plus';

import AppTable from '@/components/AppTable.vue';
import StatusTag from '@/components/StatusTag.vue';
import { useQualityStore } from '@/stores/quality';
import QualityDetailDrawer from '@/views/quality/QualityDetailDrawer.vue';

const store = useQualityStore();

function refresh() {
  store.pageState.page = 1;
  return store.loadList();
}

async function openDetail(row) {
  try {
    await store.openDetail(row.quality_run_id);
    await store.loadResults();
  } catch (_error) {
    // The store keeps the user-facing request error for the alert.
  }
}

async function rerun(row) {
  try {
    await store.rerun(row.dataset_id, row.output_version);
    ElMessage.success('已提交重新质检请求');
  } catch (requestError) {
    ElMessage.error(requestError.message || '重新质检提交失败');
  }
}

function setPage(page) { store.pageState.page = page; return store.loadList(); }
function setPageSize(pageSize) { store.pageState.pageSize = pageSize; store.pageState.page = 1; return store.loadList(); }

onMounted(() => { refresh().catch(() => {}); });
onUnmounted(() => store.dispose());
</script>

<template>
  <section class="quality-view">
    <header class="view-header">
      <div><h1>质量记录</h1></div>
      <el-button :icon="Refresh" :loading="store.loading" @click="refresh">刷新</el-button>
    </header>

    <el-form class="filter-bar" label-position="top" @submit.prevent="refresh">
      <el-form-item label="关键词"><el-input v-model="store.filters.keyword" :prefix-icon="Search" clearable /></el-form-item>
      <el-form-item label="数据集 ID"><el-input v-model="store.filters.datasetId" clearable /></el-form-item>
      <el-form-item label="输出版本"><el-input v-model="store.filters.outputVersion" clearable /></el-form-item>
      <el-form-item label="数据类型"><el-input v-model="store.filters.dataType" clearable /></el-form-item>
      <el-form-item label="质量状态"><el-select v-model="store.filters.status" clearable><el-option label="通过" value="pass" /><el-option label="告警" value="warn" /><el-option label="失败" value="fail" /><el-option label="异常" value="error" /></el-select></el-form-item>
      <el-form-item label="触发方式"><el-select v-model="store.filters.trigger" clearable><el-option label="自动" value="automatic" /><el-option label="手动" value="manual" /></el-select></el-form-item>
      <el-form-item label="当前质量"><el-select v-model="store.filters.currentOnly" clearable><el-option label="仅当前" :value="true" /></el-select></el-form-item>
      <el-form-item class="filter-action"><el-button native-type="submit" type="primary">查询</el-button></el-form-item>
    </el-form>

    <el-alert v-if="store.error" :title="store.error" type="error" :closable="false" show-icon />
    <AppTable :data="store.records" :loading="store.loading" row-key="quality_run_id" :page="store.pageState.page" :page-size="store.pageState.pageSize" :total="store.pageState.total" @current-change="setPage" @size-change="setPageSize" @row-click="openDetail">
      <el-table-column prop="dataset_code" label="数据集" min-width="160" show-overflow-tooltip>
        <template #default="{ row }"><span :data-testid="`quality-row-${row.quality_run_id}`">{{ row.dataset_code }}</span></template>
      </el-table-column>
      <el-table-column prop="output_version" label="输出版本" min-width="190" show-overflow-tooltip />
      <el-table-column prop="quality_sequence" label="序列" width="80" />
      <el-table-column prop="trigger" label="触发" width="90" />
      <el-table-column label="状态" width="100"><template #default="{ row }"><StatusTag domain="quality" :value="row.status" size="small" /></template></el-table-column>
      <el-table-column prop="error_count" label="错误" width="80" />
      <el-table-column prop="warning_count" label="告警" width="80" />
      <el-table-column label="当前" width="80"><template #default="{ row }">{{ row.is_current ? '是' : '否' }}</template></el-table-column>
      <el-table-column prop="completed_at" label="完成时间" min-width="170" show-overflow-tooltip />
      <el-table-column label="操作" width="150" fixed="right"><template #default="{ row }"><el-button link type="primary" @click.stop="openDetail(row)">详情</el-button><el-button link type="primary" @click.stop="rerun(row)">重新质检</el-button></template></el-table-column>
    </AppTable>

    <QualityDetailDrawer
      :visible="store.detailVisible"
      test-id="quality-detail-drawer"
      :loading="store.detailLoading"
      :detail="store.detail"
      :results="store.results"
      :results-page="store.resultsPage"
      :errors="store.errors"
      :error-page="store.errorPage"
      :error-page-size="store.errorPageSize"
      :error-total="store.errorTotal"
      :error-filters="store.errorFilters"
      :active-tab="store.activeTab"
      :exporting="store.exporting"
      :rerunning="store.rerunning"
      :export-filename="store.exportFilename"
      @close="store.closeDetail"
      @tab-change="(tab) => store.setActiveTab(tab).catch(() => {})"
      @load-errors="store.loadErrors().catch(() => {})"
      @error-page-change="(page) => { store.errorPage = page; store.loadErrors().catch(() => {}); }"
      @error-page-size-change="(pageSize) => { store.errorPageSize = pageSize; store.errorPage = 1; store.loadErrors().catch(() => {}); }"
      @export="({ format, filtered }) => store.exportErrors(format, filtered).catch(() => {})"
      @rerun="rerun"
    />
  </section>
</template>

<style scoped>
.quality-view { padding: 24px; }
.view-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; margin-bottom: 18px; }
.view-header h1 { margin: 0; font-size: 22px; letter-spacing: 0; }
.filter-bar { display: grid; grid-template-columns: repeat(4, minmax(150px, 1fr)); gap: 0 12px; margin-bottom: 16px; }
.filter-bar :deep(.el-form-item) { margin-bottom: 12px; }
.filter-action { align-self: end; }
@media (max-width: 880px) { .filter-bar { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (max-width: 560px) { .quality-view { padding: 16px; } .filter-bar { grid-template-columns: 1fr; } .view-header { align-items: stretch; flex-direction: column; } }
</style>
