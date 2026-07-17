<script setup>
import { computed, onMounted, onUnmounted, ref } from 'vue';
import { Download, List, Refresh, Search } from '@element-plus/icons-vue';
import { ElMessage } from 'element-plus';

import AppTable from '@/components/AppTable.vue';
import StatusTag from '@/components/StatusTag.vue';
import { useQualityStore } from '@/stores/quality';
import QualityDetailDrawer from '@/views/quality/QualityDetailDrawer.vue';

const props = defineProps({ embedded: Boolean });
const store = useQualityStore();
const ruleDrawerVisible = ref(false);
const dataTypeLabels = { optical: '光学遥感', radar: '雷达遥感', product: '信息产品', carbon: '碳卫星' };
const pageErrorCount = computed(() => store.records.reduce((total, row) => total + Number(row.error_count || 0), 0));
const pageWarningCount = computed(() => store.records.reduce((total, row) => total + Number(row.warning_count || 0), 0));

function dataTypeLabel(value) { return dataTypeLabels[value] || value || '-'; }
function qualityTime(row) { return row.completed_at || row.started_at || row.created_at || '-'; }

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

async function exportAll(row) {
  try {
    await store.exportRunErrors(row, 'csv');
    ElMessage.success('全部错误结果已导出');
  } catch (requestError) {
    ElMessage.error(requestError.message || '错误结果导出失败');
  }
}

async function openRuleCatalog() {
  ruleDrawerVisible.value = true;
  try {
    await store.loadRuleCatalog();
  } catch (requestError) {
    ElMessage.error(requestError.message || '质检规则加载失败');
  }
}

function setPage(page) { store.pageState.page = page; return store.loadList(); }
function setPageSize(pageSize) { store.pageState.pageSize = pageSize; store.pageState.page = 1; return store.loadList(); }

onMounted(() => { refresh().catch(() => {}); });
onUnmounted(() => store.dispose());
</script>

<template>
  <section class="quality-view" :class="{ embedded }">
    <header class="view-header">
      <div><h1>全部质检记录</h1><span>共 {{ store.pageState.total }} 条质检记录</span></div>
      <div class="header-actions"><el-button :icon="List" @click="openRuleCatalog">质检规则</el-button><el-button :icon="Refresh" :loading="store.loading" @click="refresh">刷新</el-button></div>
    </header>

    <el-form class="filter-bar" label-position="top" @submit.prevent="refresh">
      <el-form-item label="关键词"><el-input v-model="store.filters.keyword" :prefix-icon="Search" clearable /></el-form-item>
      <el-form-item label="数据集 ID"><el-input v-model="store.filters.datasetId" clearable /></el-form-item>
      <el-form-item label="输出版本"><el-input v-model="store.filters.outputVersion" clearable /></el-form-item>
      <el-form-item label="数据类型"><el-select v-model="store.filters.dataType" clearable placeholder="全部类型"><el-option v-for="(label, value) in dataTypeLabels" :key="value" :label="label" :value="value" /></el-select></el-form-item>
      <el-form-item label="质量状态"><el-select v-model="store.filters.status" clearable><el-option label="通过" value="pass" /><el-option label="告警" value="warn" /><el-option label="失败" value="fail" /><el-option label="异常" value="error" /></el-select></el-form-item>
      <el-form-item label="触发方式"><el-select v-model="store.filters.trigger" clearable><el-option label="自动" value="automatic" /><el-option label="手动" value="manual" /></el-select></el-form-item>
      <el-form-item label="当前质量"><el-select v-model="store.filters.currentOnly" clearable><el-option label="仅当前" :value="true" /></el-select></el-form-item>
      <el-form-item class="filter-action"><el-button native-type="submit" type="primary" :icon="Search">查询</el-button></el-form-item>
    </el-form>

    <el-alert v-if="store.error" :title="store.error" type="error" :closable="false" show-icon />
    <div class="quality-summary" aria-label="质检记录统计">
      <div><span>全部记录</span><strong>{{ store.pageState.total }}</strong></div>
      <div><span>当前页错误</span><strong class="error-value">{{ pageErrorCount }}</strong></div>
      <div><span>当前页告警</span><strong class="warning-value">{{ pageWarningCount }}</strong></div>
    </div>
    <AppTable :data="store.records" :loading="store.loading" row-key="quality_run_id" :page="store.pageState.page" :page-size="store.pageState.pageSize" :total="store.pageState.total" @current-change="setPage" @size-change="setPageSize" @row-click="openDetail">
      <el-table-column label="数据集" min-width="190">
        <template #default="{ row }"><div class="quality-identity" :data-testid="`quality-row-${row.quality_run_id}`"><strong>{{ row.dataset_code }}</strong><span>{{ row.quality_run_id }}</span></div></template>
      </el-table-column>
      <el-table-column prop="batch_id" label="批次" min-width="150" show-overflow-tooltip />
      <el-table-column label="产品类型" min-width="140"><template #default="{ row }">{{ row.product_type || dataTypeLabel(row.data_type) }}</template></el-table-column>
      <el-table-column label="剖分状态" width="105"><template #default="{ row }"><StatusTag domain="partition" :value="row.partition_status" size="small" /></template></el-table-column>
      <el-table-column label="质检状态" width="105"><template #default="{ row }"><StatusTag domain="quality" :value="row.status" size="small" /></template></el-table-column>
      <el-table-column prop="error_count" label="错误数" width="80" />
      <el-table-column prop="warning_count" label="告警数" width="80" />
      <el-table-column label="质检时间" min-width="180" show-overflow-tooltip><template #default="{ row }">{{ qualityTime(row) }}</template></el-table-column>
      <el-table-column label="操作" width="210" fixed="right"><template #default="{ row }"><el-button link type="primary" @click.stop="openDetail(row)">详情</el-button><el-button :data-testid="`quality-export-row-${row.quality_run_id}`" link type="primary" :icon="Download" @click.stop="exportAll(row)">导出错误</el-button><el-button link type="primary" @click.stop="rerun(row)">重检</el-button></template></el-table-column>
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

    <el-drawer v-model="ruleDrawerVisible" title="质检规则" size="min(900px, 94vw)" destroy-on-close>
      <div class="rule-version">规则集版本 <strong>{{ store.ruleCatalog?.rule_set_version || '-' }}</strong></div>
      <AppTable :data="store.ruleCatalog?.items || []" :loading="store.ruleCatalogLoading" :pagination="false" row-key="code">
        <el-table-column prop="name" label="质检项" min-width="180"><template #default="{ row }"><div class="rule-name"><strong>{{ row.name }}</strong><span>{{ row.code }}</span></div></template></el-table-column>
        <el-table-column label="级别" width="90"><template #default="{ row }"><el-tag :type="row.mandatory ? 'danger' : 'info'" effect="plain" size="small">{{ row.mandatory ? '必选' : '可选' }}</el-tag></template></el-table-column>
        <el-table-column label="适用产品" min-width="230"><template #default="{ row }"><div class="applicability-tags"><el-tag v-for="type in row.applicability?.data_types || []" :key="type" effect="plain" size="small">{{ dataTypeLabel(type) }}</el-tag></div></template></el-table-column>
        <el-table-column prop="implementation_version" label="实现版本" width="110" />
      </AppTable>
    </el-drawer>
  </section>
</template>

<style scoped>
.quality-view { padding: 24px; }
.quality-view.embedded { padding: 0; }
.view-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; margin-bottom: 18px; }
.view-header h1 { margin: 0 0 3px; color: #172033; font-size: 22px; letter-spacing: 0; }
.view-header span { color: #748095; font-size: 13px; }
.header-actions { display: flex; gap: 8px; }
.filter-bar { display: grid; grid-template-columns: repeat(4, minmax(150px, 1fr)); gap: 0 12px; margin-bottom: 16px; }
.filter-bar :deep(.el-form-item) { margin-bottom: 12px; }
.filter-action { align-self: end; }
.quality-summary { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); border: 1px solid #dfe4ec; border-radius: 6px; margin-bottom: 18px; background: #fff; overflow: hidden; }
.quality-summary div { display: flex; align-items: center; justify-content: space-between; min-height: 62px; padding: 0 18px; border-right: 1px solid #e7ebf1; }
.quality-summary div:last-child { border-right: 0; }
.quality-summary span { color: #667085; font-size: 13px; }
.quality-summary strong { color: #1769aa; font-size: 23px; }
.quality-summary .error-value { color: #b84040; }
.quality-summary .warning-value { color: #a96a13; }
.quality-identity, .rule-name { display: flex; flex-direction: column; min-width: 0; gap: 3px; }
.quality-identity strong, .rule-name strong { overflow: hidden; color: #263247; text-overflow: ellipsis; white-space: nowrap; }
.quality-identity span, .rule-name span { overflow: hidden; color: #8993a4; font-size: 12px; text-overflow: ellipsis; white-space: nowrap; }
.rule-version { margin-bottom: 14px; color: #667085; font-size: 13px; }
.applicability-tags { display: flex; flex-wrap: wrap; gap: 5px; }
@media (max-width: 880px) { .filter-bar { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (max-width: 560px) { .quality-view { padding: 16px; } .filter-bar { grid-template-columns: 1fr; } .view-header { align-items: stretch; flex-direction: column; } .header-actions { display: grid; grid-template-columns: 1fr 1fr; } .quality-summary { grid-template-columns: 1fr; } .quality-summary div { border-right: 0; border-bottom: 1px solid #e7ebf1; } }
</style>
