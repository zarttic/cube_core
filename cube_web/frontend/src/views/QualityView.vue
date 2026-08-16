<script setup>
import { onMounted, reactive, ref } from 'vue';
import { List, Refresh } from '@element-plus/icons-vue';
import { ElMessage } from 'element-plus';

import { requestGet, requestPost } from '@/api/client';
import { normalizePageResponse, pageQuery } from '@/api/pagination';
import AppTable from '@/components/AppTable.vue';
import StatusTag from '@/components/StatusTag.vue';
import { useQualityStore } from '@/stores/quality';
import { formatShanghaiTime } from '@/utils/time';
import PartitionQualityDrawer from '@/views/quality/PartitionQualityDrawer.vue';

const props = defineProps({ embedded: Boolean });
const store = useQualityStore();

const batches = ref([]);
const filters = reactive({ keyword: '', dataType: '', status: '' });
const pageState = reactive({ page: 1, pageSize: 20, total: 0 });
const loading = ref(false);
const error = ref('');
const selectedId = ref('');
const detail = ref(null);
const detailLoading = ref(false);
const detailVisible = ref(false);
const submitting = ref(false);

const ruleDrawerVisible = ref(false);
const ruleDetailVisible = ref(false);
const selectedRule = ref(null);
const dataTypeLabels = {
  optical: '光学遥感',
  radar: '雷达遥感',
  product: '信息产品',
  carbon: '碳卫星',
};
let batchRequestGeneration = 0;

function dataTypeLabel(value) {
  return dataTypeLabels[value] || value || '-';
}

function openRuleDetail(rule) {
  selectedRule.value = rule;
  ruleDetailVisible.value = true;
}

function ruleParameters(rule) {
  return JSON.stringify(rule?.parameters || {}, null, 2);
}

async function setRuleEnabled(rule, enabled) {
  if (rule.mandatory) return;
  try {
    await store.updateRuleSetting(rule.code, enabled);
    ElMessage.success('质检规则设置已保存');
  } catch (requestError) {
    ElMessage.error(requestError.message || '质检规则设置保存失败');
  }
}

async function openRuleCatalog() {
  ruleDrawerVisible.value = true;
  try {
    await store.loadRuleCatalog({ force: true });
  } catch (requestError) {
    ElMessage.error(requestError.message || '质检规则加载失败');
  }
}

async function loadBatches({ resetPage = false } = {}) {
  if (resetPage) pageState.page = 1;
  const generation = ++batchRequestGeneration;
  loading.value = true;
  error.value = '';
  try {
    const query = pageQuery({
      keyword: filters.keyword.trim(),
      data_type: filters.dataType,
      status: filters.status,
      page: pageState.page,
      page_size: pageState.pageSize,
    });
    const response = await requestGet(`/v1/partition/runs?${query}`);
    if (generation !== batchRequestGeneration) return;
    const page = normalizePageResponse(response, pageState.page, pageState.pageSize);
    batches.value = page.items;
    Object.assign(pageState, { page: page.page, pageSize: page.pageSize, total: page.total });
    if (!batches.value.length && pageState.total > 0 && pageState.page > 1) {
      pageState.page = Math.max(1, Math.ceil(pageState.total / pageState.pageSize));
      await loadBatches();
    }
  } catch (requestError) {
    if (generation === batchRequestGeneration) error.value = requestError.message || '剖分批次质检记录加载失败';
  } finally {
    if (generation === batchRequestGeneration) loading.value = false;
  }
}

function applyFilters() {
  return loadBatches({ resetPage: true });
}

function setPage(page) {
  pageState.page = page;
  return loadBatches();
}

function setPageSize(pageSize) {
  Object.assign(pageState, { page: 1, pageSize });
  return loadBatches();
}

async function openBatch(row) {
  selectedId.value = row.partition_run_id;
  detailVisible.value = true;
  detail.value = null;
  detailLoading.value = true;
  try {
    detail.value = await requestGet(`/v1/partition/runs/${encodeURIComponent(selectedId.value)}/quality`);
  } catch (requestError) {
    error.value = requestError.message || '剖分批次详情加载失败';
  } finally {
    detailLoading.value = false;
  }
}

async function requestQuality() {
  if (!selectedId.value) return;
  submitting.value = true;
  try {
    const response = await requestPost(`/v1/partition/runs/${encodeURIComponent(selectedId.value)}/quality`, {});
    ElMessage.success(`已提交 ${response.quality_runs?.length || 0} 个数据集质量任务`);
    await openBatch({ partition_run_id: selectedId.value });
    await loadBatches();
  } catch (requestError) {
    ElMessage.error(requestError.message || '批次质检提交失败');
  } finally {
    submitting.value = false;
  }
}

async function retryFailedPartition() {
  if (!selectedId.value) return;
  submitting.value = true;
  try {
    await requestPost(`/v1/partition/runs/${encodeURIComponent(selectedId.value)}/retry-failed`, {});
    ElMessage.success('已在原剖分批次中提交失败数据重试');
    await openBatch({ partition_run_id: selectedId.value });
    await loadBatches();
  } catch (requestError) {
    ElMessage.error(requestError.message || '失败剖分重试提交失败');
  } finally {
    submitting.value = false;
  }
}

async function exportQualityErrors(qualityRun) {
  try {
    await store.exportRunErrors(qualityRun, 'csv');
    ElMessage.success('质检错误明细已下载');
  } catch (requestError) {
    ElMessage.error(requestError.message || '质检错误明细下载失败');
  }
}

function closeDetail() {
  detailVisible.value = false;
  selectedId.value = '';
  detail.value = null;
}

onMounted(() => { loadBatches(); });
</script>

<template>
  <section class="quality-view" :class="{ embedded: props.embedded }">
    <header class="view-header">
      <div class="header-actions">
        <el-button :icon="List" @click="openRuleCatalog">质检规则</el-button>
        <el-button :icon="Refresh" :loading="loading" @click="loadBatches">刷新</el-button>
      </div>
    </header>
    <el-form class="filter-bar" inline @submit.prevent="applyFilters">
      <el-form-item>
        <el-input v-model="filters.keyword" clearable placeholder="剖分批次、载入批次或数据集" @keyup.enter="applyFilters" />
      </el-form-item>
      <el-form-item>
        <el-select v-model="filters.dataType" clearable placeholder="数据类型" style="width: 150px">
          <el-option v-for="(label, value) in dataTypeLabels" :key="value" :label="label" :value="value" />
        </el-select>
      </el-form-item>
      <el-form-item>
        <el-select v-model="filters.status" clearable placeholder="批次状态" style="width: 150px">
          <el-option label="待处理" value="pending" />
          <el-option label="排队中" value="queued" />
          <el-option label="运行中" value="running" />
          <el-option label="已完成" value="completed" />
          <el-option label="部分失败" value="partial_failure" />
          <el-option label="失败" value="failed" />
          <el-option label="已取消" value="cancelled" />
        </el-select>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" native-type="submit">查询</el-button>
      </el-form-item>
    </el-form>
    <el-alert v-if="error" :title="error" type="error" :closable="false" show-icon />
    <AppTable :data="batches" :loading="loading" row-key="partition_run_id" :page="pageState.page" :page-size="pageState.pageSize" :total="pageState.total" @current-change="setPage" @size-change="setPageSize" @row-click="openBatch">
      <el-table-column label="剖分批次" min-width="240"><template #default="{ row }"><div class="batch-cell"><strong>{{ row.partition_run_id }}</strong><span :title="(row.source_load_batch_ids || []).join('、')">来源 {{ (row.source_load_batch_names || row.source_load_batch_ids || []).join('、') || '-' }}</span></div></template></el-table-column>
      <el-table-column label="数据范围" min-width="150"><template #default="{ row }">{{ row.dataset_count }} 个数据集 · {{ row.scene_count }} 景 · {{ row.band_count }} 波段</template></el-table-column>
      <el-table-column label="剖分" width="105"><template #default="{ row }">{{ row.partitioned_count }}/{{ row.band_count }}</template></el-table-column>
      <el-table-column label="质检" min-width="130"><template #default="{ row }"><span class="pass-count">{{ row.quality_pass_count }} 通过</span><span v-if="row.quality_failed_count" class="failed-count"> · {{ row.quality_failed_count }} 失败</span></template></el-table-column>
      <el-table-column label="入库" width="105"><template #default="{ row }">{{ row.ingested_count }}/{{ row.band_count }}</template></el-table-column>
      <el-table-column label="批次状态" width="115"><template #default="{ row }"><StatusTag domain="partition" :value="row.status" size="small" /></template></el-table-column>
      <el-table-column label="创建时间" min-width="170"><template #default="{ row }">{{ formatShanghaiTime(row.created_at) }}</template></el-table-column>
      <el-table-column label="操作" width="80" fixed="right"><template #default="{ row }"><el-button link type="primary" @click.stop="openBatch(row)">查看</el-button></template></el-table-column>
    </AppTable>
    <PartitionQualityDrawer :visible="detailVisible" :detail="detail" :loading="detailLoading" :submitting="submitting" :exporting="store.exporting" @close="closeDetail" @request-quality="requestQuality" @retry-failed-partition="retryFailedPartition" @export-quality-errors="exportQualityErrors" />

    <el-drawer v-model="ruleDrawerVisible" title="质检规则" size="min(900px, 94vw)" destroy-on-close>
      <div class="rule-version">规则集版本 <strong>{{ store.ruleCatalog?.rule_set_version || '-' }}</strong></div>
      <AppTable :data="store.ruleCatalog?.items || []" :loading="store.ruleCatalogLoading" :pagination="false" row-key="code">
        <el-table-column prop="name" label="质检项" min-width="220">
          <template #default="{ row }">
            <button type="button" class="rule-link" @click="openRuleDetail(row)">
              <span class="rule-name"><strong>{{ row.name }}</strong><span>{{ row.code }}</span></span>
            </button>
          </template>
        </el-table-column>
        <el-table-column label="级别" width="90">
          <template #default="{ row }">
            <el-tag :type="row.mandatory ? 'danger' : 'info'" effect="plain" size="small">
              {{ row.mandatory ? '必选' : '可选' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="适用产品" min-width="230">
          <template #default="{ row }">
            <div class="applicability-tags">
              <el-tag v-for="type in row.applicability?.data_types || []" :key="type" effect="plain" size="small">
                {{ dataTypeLabel(type) }}
              </el-tag>
            </div>
          </template>
        </el-table-column>
        <el-table-column prop="implementation_version" label="实现版本" width="110" />
        <el-table-column label="启用" width="120">
          <template #default="{ row }">
            <el-checkbox
              :model-value="row.enabled"
              :disabled="row.mandatory || store.ruleCatalogSaving"
              @click.stop
              @change="(value) => setRuleEnabled(row, Boolean(value))"
            >{{ row.mandatory ? '固定启用' : '启用' }}</el-checkbox>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="100">
          <template #default="{ row }">
            <el-button link type="primary" @click.stop="openRuleDetail(row)">查看规则</el-button>
          </template>
        </el-table-column>
      </AppTable>
    </el-drawer>

    <el-dialog v-model="ruleDetailVisible" :title="selectedRule?.name || '规则详情'" width="min(680px, 92vw)" destroy-on-close>
      <template v-if="selectedRule">
        <div class="rule-detail-grid">
          <div><span>规则编码</span><strong>{{ selectedRule.code }}</strong></div>
          <div><span>检查级别</span><strong>{{ selectedRule.mandatory ? '必选' : '可选' }}</strong></div>
          <div><span>实现版本</span><strong>{{ selectedRule.implementation_version || '-' }}</strong></div>
          <div><span>规则集版本</span><strong>{{ store.ruleCatalog?.rule_set_version || '-' }}</strong></div>
        </div>
        <div class="rule-detail-section">
          <h4>适用产品</h4>
          <div class="applicability-tags">
            <el-tag v-for="type in selectedRule.applicability?.data_types || []" :key="type" effect="plain">
              {{ dataTypeLabel(type) }}
            </el-tag>
            <span v-if="!(selectedRule.applicability?.data_types || []).length">全部产品</span>
          </div>
        </div>
        <div v-if="selectedRule.applicability?.product_types?.length" class="rule-detail-section">
          <h4>适用产品类型</h4>
          <div class="applicability-tags">
            <el-tag v-for="type in selectedRule.applicability.product_types" :key="type" effect="plain">{{ type }}</el-tag>
          </div>
        </div>
        <div class="rule-detail-section">
          <h4>规则说明</h4>
          <p class="rule-description">{{ selectedRule.description || '暂无规则说明' }}</p>
        </div>
        <div class="rule-detail-section">
          <h4>检查参数</h4>
          <pre class="rule-parameters">{{ ruleParameters(selectedRule) }}</pre>
        </div>
      </template>
    </el-dialog>
  </section>
</template>

<style scoped>
.quality-view { padding: 24px; }
.quality-view.embedded { padding: 0; }
.view-header { display: flex; justify-content: flex-end; align-items: flex-start; gap: 16px; margin-bottom: 18px; }
.header-actions { display: flex; gap: 8px; }
.filter-bar { display: flex; align-items: flex-end; flex-wrap: wrap; gap: 0 10px; margin-bottom: 16px; }
.filter-bar :deep(.el-form-item) { margin-bottom: 8px; }
.filter-bar :deep(.el-input) { width: min(330px, 44vw); }
.batch-cell { display: flex; flex-direction: column; min-width: 0; gap: 3px; }
.batch-cell strong, .batch-cell span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.batch-cell strong { color: #263247; }
.batch-cell span { color: #748095; font-size: 12px; }
.pass-count { color: #277a52; }
.failed-count { color: #a53b32; }
.rule-version { margin-bottom: 14px; color: #667085; font-size: 13px; }
.applicability-tags { display: flex; flex-wrap: wrap; gap: 5px; }
.rule-name { display: flex; flex-direction: column; min-width: 0; gap: 3px; }
.rule-name strong { overflow: hidden; color: #263247; text-overflow: ellipsis; white-space: nowrap; }
.rule-name span { overflow: hidden; color: #8993a4; font-size: 12px; text-overflow: ellipsis; white-space: nowrap; }
.rule-link { display: block; width: 100%; padding: 0; border: 0; background: transparent; text-align: left; cursor: pointer; font: inherit; }
.rule-link:hover strong { color: #1769aa; }
.rule-detail-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; padding-bottom: 18px; border-bottom: 1px solid #e7ebf1; }
.rule-detail-grid div { display: flex; flex-direction: column; gap: 4px; min-width: 0; }
.rule-detail-grid span, .rule-detail-section h4 { color: #748095; font-size: 12px; font-weight: 500; }
.rule-detail-grid strong { color: #263247; font-size: 13px; overflow-wrap: anywhere; }
.rule-detail-section { padding-top: 16px; }
.rule-detail-section h4 { margin: 0 0 8px; }
.rule-detail-section > span { color: #667085; font-size: 13px; }
.rule-description { margin: 0; color: #475467; font-size: 13px; line-height: 1.7; }
.rule-parameters {
  max-height: 240px;
  margin: 0;
  padding: 12px;
  overflow: auto;
  border: 1px solid #e1e6ee;
  border-radius: 5px;
  background: #f7f9fc;
  color: #475467;
  font: 12px/1.6 ui-monospace, SFMono-Regular, Menlo, monospace;
  white-space: pre-wrap;
  word-break: break-word;
}
@media (max-width: 760px) {
  .quality-view { padding: 16px; }
  .header-actions { width: 100%; display: grid; grid-template-columns: 1fr 1fr; }
  .rule-detail-grid { grid-template-columns: 1fr; }
}
</style>
