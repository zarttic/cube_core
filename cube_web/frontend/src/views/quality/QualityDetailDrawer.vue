<script setup>
import { computed } from 'vue';
import { Download, Refresh } from '@element-plus/icons-vue';

import AppTable from '@/components/AppTable.vue';
import DetailDrawer from '@/components/DetailDrawer.vue';
import StatusTag from '@/components/StatusTag.vue';
import { qualityErrorLabel, qualityRecoveryLabel, qualityRuleLabel } from '@/utils/qualityLabels';

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
const errorRuleOptions = computed(() => [...new Set(props.errors.map((item) => item.rule_code).filter(Boolean))]);
const treeProps = { children: 'children', label: 'label' };

function sceneLabel(row) {
  return row.scene_name || row.scene_id || row.source_asset_id || '数据集级问题';
}

function bandLabel(row) {
  if (!row.band_code && !row.band_name) return '景级问题';
  return row.band_name ? `${row.band_code || '未命名波段'} · ${row.band_name}` : row.band_code;
}

const locationTree = computed(() => {
  if (!props.errors.length) return [];
  const dataset = {
    key: `dataset:${props.detail?.dataset_id || 'current'}`,
    kind: 'dataset',
    label: props.detail?.dataset_code || props.detail?.dataset_id || '当前数据集',
    children: [],
  };
  const scenes = new Map();
  const bands = new Map();
  for (const row of props.errors) {
    const sceneKey = `${dataset.key}:scene:${row.scene_id || row.source_asset_id || 'dataset'}`;
    let scene = scenes.get(sceneKey);
    if (!scene) {
      scene = { key: sceneKey, kind: 'scene', label: sceneLabel(row), sourceAssetId: row.source_asset_id, children: [] };
      scenes.set(sceneKey, scene);
      dataset.children.push(scene);
    }
    const bandKey = `${sceneKey}:band:${row.band_code || 'scene'}`;
    let band = bands.get(bandKey);
    if (!band) {
      band = { key: bandKey, kind: 'band', label: bandLabel(row), children: [] };
      bands.set(bandKey, band);
      scene.children.push(band);
    }
    band.children.push({
      key: `error:${row.quality_error_id}`,
      kind: 'error',
      label: `${qualityErrorLabel(row.error_code)}：${row.message}`,
      row,
    });
  }
  return [dataset];
});

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
        <el-tab-pane name="errors"><template #label><span data-testid="quality-detail-tab-errors">问题定位</span></template>
          <section class="location-overview">
            <div><span>数据集</span><strong>{{ detail.dataset_code || detail.dataset_id }}</strong></div>
            <div><span>本页问题</span><strong>{{ errorTotal }}</strong></div>
            <div><span>定位粒度</span><strong>景 / 波段</strong></div>
          </section>
          <el-empty v-if="!errors.length" description="当前筛选条件下没有问题记录" />
          <el-tree v-else class="quality-location-tree" :data="locationTree" :props="treeProps" node-key="key" default-expand-all :expand-on-click-node="false">
            <template #default="{ data }">
              <div class="quality-tree-node" :class="`quality-tree-${data.kind}`">
                <template v-if="data.kind === 'error'">
                  <span class="tree-error-code">{{ qualityErrorLabel(data.row.error_code) }}</span>
                  <span class="tree-error-message">{{ data.row.message }}</span>
                </template>
                <template v-else>
                  <strong>{{ data.label }}</strong>
                  <small v-if="data.kind === 'scene' && data.sourceAssetId">{{ data.sourceAssetId }}</small>
                </template>
              </div>
            </template>
          </el-tree>
          <el-form class="error-filters" inline @submit.prevent="emit('load-errors')">
            <el-form-item label="规则">
              <el-select v-model="errorFilters.ruleCode" clearable placeholder="全部规则">
                <el-option v-for="ruleCode in errorRuleOptions" :key="ruleCode" :label="qualityRuleLabel(ruleCode)" :value="ruleCode" />
              </el-select>
            </el-form-item>
            <el-form-item label="错误码"><el-input v-model="errorFilters.errorCode" clearable /></el-form-item>
            <el-form-item label="字段"><el-input v-model="errorFilters.field" clearable /></el-form-item>
            <el-form-item><el-button type="primary" @click="emit('load-errors')">筛选</el-button></el-form-item>
          </el-form>
          <div class="drawer-actions">
            <el-button data-testid="quality-export-filtered" :icon="Download" :loading="exporting" @click="emit('export', { format: 'csv', filtered: true })">导出当前筛选结果 CSV</el-button>
            <el-button :icon="Download" :loading="exporting" @click="emit('export', { format: 'json', filtered: true })">导出当前筛选结果 JSON</el-button>
          </div>
          <AppTable :data="errors" :page="errorPage" :page-size="errorPageSize" :total="errorTotal" row-key="quality_error_id" @current-change="emit('error-page-change', $event)" @size-change="emit('error-page-size-change', $event)">
            <el-table-column label="景" min-width="180"><template #default="{ row }"><span :title="row.scene_id || row.source_asset_id || ''">{{ sceneLabel(row) }}</span></template></el-table-column>
            <el-table-column label="波段" min-width="150"><template #default="{ row }">{{ bandLabel(row) }}</template></el-table-column>
            <el-table-column label="规则" min-width="180"><template #default="{ row }"><span :title="row.rule_code">{{ qualityRuleLabel(row.rule_code) }}</span></template></el-table-column>
            <el-table-column label="错误码" min-width="180"><template #default="{ row }"><span :title="row.error_code">{{ qualityErrorLabel(row.error_code) }}</span></template></el-table-column>
            <el-table-column prop="field" label="字段" min-width="120" />
            <el-table-column prop="message" label="说明" min-width="230" show-overflow-tooltip />
            <el-table-column label="问题来源 / 建议处理" min-width="250"><template #default="{ row }">{{ qualityRecoveryLabel(row.rule_code, row.error_code) }}</template></el-table-column>
          </AppTable>
        </el-tab-pane>
        <el-tab-pane name="results"><template #label><span data-testid="quality-detail-tab-results">规则汇总</span></template>
          <AppTable :data="results" :page="resultsPage.page" :page-size="resultsPage.pageSize" :total="resultsPage.total" :pagination="false" row-key="rule_code">
            <el-table-column label="规则" min-width="170"><template #default="{ row }"><span :title="row.rule_code">{{ qualityRuleLabel(row.rule_code) }}</span></template></el-table-column>
            <el-table-column label="状态" width="100"><template #default="{ row }"><StatusTag domain="quality" :value="row.status" size="small" /></template></el-table-column>
            <el-table-column prop="error_count" label="错误" width="80" />
            <el-table-column prop="warning_count" label="告警" width="80" />
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
.location-overview { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 8px; margin: 0 0 12px; }
.location-overview > div { border: 1px solid #d7dde5; padding: 8px 10px; background: #f8fafc; }
.location-overview span, .location-overview strong { display: block; }
.location-overview span { color: #667085; font-size: 12px; margin-bottom: 3px; }
.quality-location-tree { border: 1px solid #d7dde5; padding: 8px; margin-bottom: 14px; max-height: 360px; overflow: auto; }
.quality-tree-node { display: flex; align-items: baseline; gap: 8px; min-width: 0; line-height: 1.65; }
.quality-tree-node small { color: #667085; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.quality-tree-band strong { color: #315a7a; }
.tree-error-code { color: #9b2c2c; flex: 0 0 auto; }
.tree-error-message { color: #344054; overflow-wrap: anywhere; }
@media (max-width: 680px) { .location-overview { grid-template-columns: 1fr; } }
</style>
