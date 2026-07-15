<script setup>
import { computed } from 'vue';
import { Download, Refresh } from '@element-plus/icons-vue';

import AppTable from '@/components/AppTable.vue';
import DetailDrawer from '@/components/DetailDrawer.vue';
import StatusTag from '@/components/StatusTag.vue';

const props = defineProps({
  testId: { type: String, default: '' },
  visible: Boolean,
  loading: Boolean,
  detail: { type: Object, default: null },
  results: { type: Array, default: () => [] },
  resultsPage: { type: Object, default: () => ({ page: 1, pageSize: 20, total: 0 }) },
  errors: { type: Array, default: () => [] },
  errorPage: { type: Number, default: 1 },
  errorPageSize: { type: Number, default: 20 },
  errorTotal: { type: Number, default: 0 },
  errorFilters: { type: Object, default: () => ({}) },
  activeTab: { type: String, default: 'results' },
  exporting: Boolean,
  rerunning: Boolean,
  exportFilename: { type: String, default: '' },
});
const emit = defineEmits(['close', 'tab-change', 'load-errors', 'error-page-change', 'error-page-size-change', 'export', 'rerun']);
const title = computed(() => props.detail?.dataset_code ? `${props.detail.dataset_code} 质量详情` : '质量详情');

function rerun() {
  if (props.detail?.dataset_id && props.detail?.output_version) {
    emit('rerun', props.detail);
  }
}
</script>

<template>
  <DetailDrawer :visible="visible" :test-id="testId" :title="title" :loading="loading" @update:visible="(value) => !value && emit('close')" @closed="emit('close')">
    <div class="drawer-close-row"><el-button data-testid="quality-detail-close" link type="primary" @click="emit('close')">关闭</el-button></div>
    <template v-if="detail">
      <el-descriptions :column="1" border class="run-overview">
        <el-descriptions-item label="质量运行 ID">{{ detail.quality_run_id }}</el-descriptions-item>
        <el-descriptions-item label="输出版本">{{ detail.output_version }}</el-descriptions-item>
        <el-descriptions-item label="运行序列">{{ detail.quality_sequence }}</el-descriptions-item>
        <el-descriptions-item label="规则集版本">{{ detail.rule_set_version }}</el-descriptions-item>
        <el-descriptions-item label="当前质量">{{ detail.is_current ? '是（当前质量）' : '否（历史质量）' }}</el-descriptions-item>
        <el-descriptions-item label="状态"><StatusTag domain="quality" :value="detail.status" size="small" /></el-descriptions-item>
      </el-descriptions>
      <div class="drawer-actions">
        <el-button :icon="Refresh" :loading="rerunning" @click="rerun">重新质检</el-button>
        <el-button data-testid="quality-export-all" :icon="Download" :loading="exporting" @click="emit('export', { format: 'csv', filtered: false })">导出全部 CSV</el-button>
        <el-button :icon="Download" :loading="exporting" @click="emit('export', { format: 'json', filtered: false })">导出全部 JSON</el-button>
      </div>
      <p v-if="exportFilename" class="export-name">已导出：{{ exportFilename }}</p>

      <el-tabs :model-value="activeTab" @tab-change="emit('tab-change', $event)">
        <el-tab-pane name="results"><template #label><span data-testid="quality-detail-tab-results">规则结果</span></template>
          <AppTable :data="results" :page="resultsPage.page" :page-size="resultsPage.pageSize" :total="resultsPage.total" :pagination="false" row-key="rule_code">
            <el-table-column prop="rule_code" label="规则" min-width="170" />
            <el-table-column label="状态" width="100"><template #default="{ row }"><StatusTag domain="quality" :value="row.status" size="small" /></template></el-table-column>
            <el-table-column prop="finding_count" label="发现" width="80" />
            <el-table-column prop="error_count" label="错误" width="80" />
            <el-table-column prop="warning_count" label="告警" width="80" />
          </AppTable>
        </el-tab-pane>
        <el-tab-pane name="errors"><template #label><span data-testid="quality-detail-tab-errors">错误明细</span></template>
          <el-form class="error-filters" inline @submit.prevent="emit('load-errors')">
            <el-form-item label="规则"><el-input v-model="errorFilters.ruleCode" clearable /></el-form-item>
            <el-form-item label="错误码"><el-input v-model="errorFilters.errorCode" clearable /></el-form-item>
            <el-form-item label="字段"><el-input v-model="errorFilters.field" clearable /></el-form-item>
            <el-form-item><el-button type="primary" @click="emit('load-errors')">筛选</el-button></el-form-item>
          </el-form>
          <div class="drawer-actions">
            <el-button data-testid="quality-export-filtered" :icon="Download" :loading="exporting" @click="emit('export', { format: 'csv', filtered: true })">导出当前筛选结果 CSV</el-button>
            <el-button :icon="Download" :loading="exporting" @click="emit('export', { format: 'json', filtered: true })">导出当前筛选结果 JSON</el-button>
          </div>
          <AppTable :data="errors" :page="errorPage" :page-size="errorPageSize" :total="errorTotal" row-key="quality_error_id" @current-change="emit('error-page-change', $event)" @size-change="emit('error-page-size-change', $event)">
            <el-table-column prop="rule_code" label="规则" min-width="150" />
            <el-table-column prop="error_code" label="错误码" min-width="150" />
            <el-table-column prop="field" label="字段" min-width="120" />
            <el-table-column prop="message" label="说明" min-width="230" show-overflow-tooltip />
          </AppTable>
        </el-tab-pane>
      </el-tabs>
    </template>
    <el-empty v-else description="选择一条质量记录查看详情" />
  </DetailDrawer>
</template>

<style scoped>
.run-overview { margin-bottom: 14px; }
.drawer-close-row { display: flex; justify-content: flex-end; margin-bottom: 8px; }
.drawer-actions { display: flex; flex-wrap: wrap; gap: 8px; margin: 12px 0; }
.export-name { margin: 0 0 12px; color: #667085; font-size: 13px; overflow-wrap: anywhere; }
.error-filters { display: flex; flex-wrap: wrap; gap: 4px 10px; }
.error-filters :deep(.el-form-item) { margin-bottom: 10px; }
</style>
