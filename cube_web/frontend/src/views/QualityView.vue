<script setup>
import { onMounted, onUnmounted, reactive, ref } from 'vue';
import { List, Refresh } from '@element-plus/icons-vue';
import { ElMessage } from 'element-plus';

import { requestGet, requestPost } from '@/api/client';
import { normalizePageResponse, pageQuery } from '@/api/pagination';
import AppTable from '@/components/AppTable.vue';
import StatusTag from '@/components/StatusTag.vue';
import { useQualityStore } from '@/stores/quality';
import { qualityExecutionErrorLabel } from '@/utils/qualityLabels';
import { formatShanghaiTime } from '@/utils/time';
import PartitionQualityDrawer from '@/views/quality/PartitionQualityDrawer.vue';

const props = defineProps({ embedded: Boolean });
const store = useQualityStore();

const batches = ref([]);
const filters = reactive({ keyword: '', dataType: '', status: '', createdAtRange: [] });
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
let detailRequestGeneration = 0;
let qualityPollTimer = null;
let qualityPollGeneration = 0;

const activeQualityStatuses = new Set(['pending', 'running']);
const activePartitionStatuses = new Set(['pending', 'queued', 'running', 'retrying', 'cancel_requested']);
const MAX_QUALITY_POLL_FAILURES = 5;
const MAX_QUALITY_POLL_DURATION_MS = 15 * 60 * 1000;

function dataTypeLabel(value) {
  return dataTypeLabels[value] || value || '-';
}

function qualityRunsInBatch(batch) {
  return (batch?.datasets || []).flatMap((dataset) => (dataset.quality_runs || []).map((run) => ({
    ...run,
    dataset_id: run.dataset_id || dataset.dataset_id,
    dataset_code: run.dataset_code || dataset.dataset_code,
    dataset_title: run.dataset_title || dataset.dataset_title,
  })));
}

function hasQualityRuns(batch, qualityRunIds) {
  const available = new Set(qualityRunsInBatch(batch).map((run) => String(run.quality_run_id)));
  return qualityRunIds.every((qualityRunId) => available.has(String(qualityRunId)));
}

function syncBatchFromDetail(batch) {
  const partitionRunId = String(batch?.partition_run_id || '');
  if (!partitionRunId) return;
  const summary = batch.summary || {};
  const summaryFields = [
    'band_count', 'partitioned_count', 'partition_failed_count',
    'quality_pass_count', 'quality_failed_count', 'ingested_count', 'ingest_failed_count',
  ];
  batches.value = batches.value.map((item) => {
    if (String(item.partition_run_id) !== partitionRunId) return item;
    const next = { ...item };
    if (batch.status) next.status = batch.status;
    for (const field of summaryFields) {
      if (Object.prototype.hasOwnProperty.call(summary, field)) next[field] = summary[field];
    }
    return next;
  });
}

function clearQualityPoll() {
  qualityPollGeneration += 1;
  if (qualityPollTimer !== null) {
    clearTimeout(qualityPollTimer);
    qualityPollTimer = null;
  }
}

function scheduleQualityPoll(partitionRunId, expectedQualityRunIds = []) {
  clearQualityPoll();
  const pollGeneration = qualityPollGeneration;
  const detailGeneration = detailRequestGeneration;
  const expectedIds = [...new Set(expectedQualityRunIds.map((qualityRunId) => String(qualityRunId)))];
  let consecutiveFailures = 0;
  const pollingStartedAt = Date.now();

  const nextPollDelay = () => Math.min(5000, 1000 * (2 ** Math.min(consecutiveFailures, 2)));

  const poll = async () => {
    if (pollGeneration !== qualityPollGeneration || detailGeneration !== detailRequestGeneration || selectedId.value !== partitionRunId) return;
    if (Date.now() - pollingStartedAt >= MAX_QUALITY_POLL_DURATION_MS) {
      qualityPollTimer = null;
      error.value = '批次质检长时间未完成，请刷新页面或到任务列表确认状态';
      return;
    }
    try {
      const response = await requestGet(`/v1/partition/runs/${encodeURIComponent(partitionRunId)}/quality`);
      if (pollGeneration !== qualityPollGeneration || detailGeneration !== detailRequestGeneration || selectedId.value !== partitionRunId) return;
      consecutiveFailures = 0;
      error.value = '';
      syncBatchFromDetail(response);
      if (expectedIds.length && !hasQualityRuns(response, expectedIds)) {
        qualityPollTimer = setTimeout(poll, nextPollDelay());
        return;
      }
      detail.value = response;
      const runs = qualityRunsInBatch(response);
      const trackedIds = expectedIds.length ? expectedIds : runs.map((run) => String(run.quality_run_id));
      if (activePartitionStatuses.has(response.status) || trackedIds.some((qualityRunId) => {
        const run = runs.find((item) => String(item.quality_run_id) === qualityRunId);
        return run && activeQualityStatuses.has(run.status);
      })) {
        qualityPollTimer = setTimeout(poll, 1000);
      } else {
        qualityPollTimer = null;
      }
    } catch (requestError) {
      if (pollGeneration !== qualityPollGeneration || detailGeneration !== detailRequestGeneration || selectedId.value !== partitionRunId) return;
      error.value = qualityExecutionErrorLabel(requestError) || '质检任务状态刷新失败';
      consecutiveFailures += 1;
      const permanent = requestError?.retryable === false || [401, 403, 404].includes(requestError?.status);
      if (!permanent && consecutiveFailures < MAX_QUALITY_POLL_FAILURES) {
        qualityPollTimer = setTimeout(poll, nextPollDelay());
      } else {
        qualityPollTimer = null;
        error.value = `${error.value}；请刷新页面或重新打开批次详情`;
      }
    }
  };

  qualityPollTimer = setTimeout(poll, 1000);
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
    ElMessage.error(qualityExecutionErrorLabel(requestError) || '质检规则设置保存失败');
  }
}

async function openRuleCatalog() {
  ruleDrawerVisible.value = true;
  try {
    await store.loadRuleCatalog({ force: true });
  } catch (requestError) {
    ElMessage.error(qualityExecutionErrorLabel(requestError) || '质检规则加载失败');
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
      created_from: filters.createdAtRange?.[0],
      created_to: filters.createdAtRange?.[1],
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
    if (generation === batchRequestGeneration) error.value = qualityExecutionErrorLabel(requestError) || '剖分批次质检记录加载失败';
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
  return openBatchForRun(row.partition_run_id);
}

async function openBatchForRun(partitionRunId, { expectedQualityRunIds = [] } = {}) {
  const generation = ++detailRequestGeneration;
  clearQualityPoll();
  selectedId.value = partitionRunId;
  detailVisible.value = true;
  detail.value = null;
  detailLoading.value = true;
  try {
    const response = await requestGet(`/v1/partition/runs/${encodeURIComponent(selectedId.value)}/quality`);
    if (generation !== detailRequestGeneration || selectedId.value !== partitionRunId) return;
    syncBatchFromDetail(response);
    if (expectedQualityRunIds.length && !hasQualityRuns(response, expectedQualityRunIds)) {
      detail.value = null;
      scheduleQualityPoll(partitionRunId, expectedQualityRunIds);
      return;
    }
    detail.value = response;
    const qualityRunIds = expectedQualityRunIds.length
      ? expectedQualityRunIds
      : qualityRunsInBatch(response)
        .filter((run) => activeQualityStatuses.has(run.status))
        .map((run) => run.quality_run_id);
    if (activePartitionStatuses.has(response.status) || qualityRunIds.length) scheduleQualityPoll(partitionRunId, qualityRunIds);
  } catch (requestError) {
    if (generation === detailRequestGeneration && selectedId.value === partitionRunId) error.value = qualityExecutionErrorLabel(requestError) || '剖分批次详情加载失败';
  } finally {
    if (generation === detailRequestGeneration && selectedId.value === partitionRunId) detailLoading.value = false;
  }
}

async function retryQualityRun(run) {
  if (!run?.dataset_id || !run?.output_version || !selectedId.value) return;
  submitting.value = true;
  try {
    const response = await requestPost('/v1/quality/runs', {
      dataset_id: run.dataset_id,
      output_version: run.output_version,
    });
    if (!response?.quality_run_id) throw new Error('质检重试未返回新的 quality_run_id');
    ElMessage.success(`已提交质检重试（${response.quality_run_id}）`);
    await openBatchForRun(selectedId.value, { expectedQualityRunIds: [response.quality_run_id] });
    await loadBatches();
  } catch (requestError) {
    ElMessage.error(qualityExecutionErrorLabel(requestError) || '质检立即重试提交失败');
  } finally {
    submitting.value = false;
  }
}

async function retryFailedPartition() {
  if (!selectedId.value) return;
  submitting.value = true;
  try {
    await requestPost(`/v1/partition/runs/${encodeURIComponent(selectedId.value)}/retry-failed`, {});
    ElMessage.success('已在原剖分批次中提交失败/终止数据重剖');
    await openBatch({ partition_run_id: selectedId.value });
    await loadBatches();
  } catch (requestError) {
    ElMessage.error(qualityExecutionErrorLabel(requestError) || '剖分重试提交失败');
  } finally {
    submitting.value = false;
  }
}

async function cancelPartition() {
  if (!selectedId.value) return;
  submitting.value = true;
  try {
    await requestPost(`/v1/partition/runs/${encodeURIComponent(selectedId.value)}/cancel`, {});
    ElMessage.success('剖分任务已强制终止');
    await openBatchForRun(selectedId.value);
    await loadBatches();
  } catch (requestError) {
    ElMessage.error(qualityExecutionErrorLabel(requestError) || '剖分任务终止失败');
  } finally {
    submitting.value = false;
  }
}

async function exportQualityErrors(qualityRun) {
  try {
    await store.exportQualityWorkbook(qualityRun);
    ElMessage.success('质检结果已下载');
  } catch (requestError) {
    ElMessage.error(qualityExecutionErrorLabel(requestError) || '质检结果下载失败');
  }
}

function closeDetail() {
  detailRequestGeneration += 1;
  clearQualityPoll();
  detailVisible.value = false;
  selectedId.value = '';
  detail.value = null;
}

onMounted(() => { loadBatches(); });
onUnmounted(() => {
  detailRequestGeneration += 1;
  clearQualityPoll();
});
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
        <el-date-picker
          v-model="filters.createdAtRange"
          type="daterange"
          format="YYYY年M月D日"
          value-format="YYYY-MM-DD"
          range-separator="至"
          start-placeholder="开始日期"
          end-placeholder="结束日期"
          clearable
          style="width: 270px"
        />
      </el-form-item>
      <el-form-item>
        <el-button type="primary" native-type="submit">查询</el-button>
      </el-form-item>
    </el-form>
    <el-alert v-if="error" :title="error" type="error" :closable="false" show-icon />
    <div class="quality-batch-table">
      <AppTable :data="batches" :loading="loading" row-key="partition_run_id" :page="pageState.page" :page-size="pageState.pageSize" :total="pageState.total" @current-change="setPage" @size-change="setPageSize" @row-click="openBatch">
        <el-table-column label="序号" width="72" align="center"><template #default="{ $index }">{{ (pageState.page - 1) * pageState.pageSize + $index + 1 }}</template></el-table-column>
        <el-table-column label="剖分批次" min-width="180"><template #default="{ row }"><div class="batch-cell"><strong>{{ row.partition_run_id }}</strong></div></template></el-table-column>
        <el-table-column label="数据集" min-width="180"><template #default="{ row }"><div v-if="row.datasets?.length" class="dataset-list"><div v-for="dataset in row.datasets" :key="dataset.dataset_id" class="dataset-cell"><strong>{{ dataset.dataset_title || dataset.dataset_code || dataset.dataset_id }}</strong></div></div><span v-else>{{ row.dataset_count || 0 }} 个数据集</span></template></el-table-column>
        <el-table-column label="数据范围" min-width="150"><template #default="{ row }">{{ row.dataset_count }} 个数据集 · {{ row.scene_count }} 景 · {{ row.band_count }} 波段</template></el-table-column>
        <el-table-column label="剖分" min-width="120"><template #default="{ row }">{{ row.partitioned_count }}/{{ row.band_count }}</template></el-table-column>
        <el-table-column label="质检" min-width="130"><template #default="{ row }"><span class="pass-count">{{ row.quality_pass_count }} 通过</span><span v-if="row.quality_failed_count" class="failed-count"> · {{ row.quality_failed_count }} 失败</span></template></el-table-column>
        <el-table-column label="入库" min-width="110"><template #default="{ row }">{{ row.ingested_count }}/{{ row.band_count }}</template></el-table-column>
        <el-table-column label="批次状态" min-width="130"><template #default="{ row }"><StatusTag domain="partition" :value="row.status" size="small" /></template></el-table-column>
        <el-table-column label="创建时间" min-width="165"><template #default="{ row }">{{ formatShanghaiTime(row.created_at) }}</template></el-table-column>
        <el-table-column label="操作" width="105" fixed="right"><template #default="{ row }"><el-button :data-testid="`quality-task-detail-${row.partition_run_id}`" link type="primary" @click.stop="openBatch(row)">任务详情</el-button></template></el-table-column>
      </AppTable>
    </div>
    <PartitionQualityDrawer :visible="detailVisible" :detail="detail" :loading="detailLoading" :submitting="submitting" :exporting="store.exporting" @close="closeDetail" @cancel-partition="cancelPartition" @retry-failed-partition="retryFailedPartition" @retry-quality-run="retryQualityRun" @export-quality-errors="exportQualityErrors" />

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
.quality-batch-table :deep(.el-table .cell) { padding-right: 14px; padding-left: 14px; }
.quality-batch-table :deep(.el-table th .cell), .quality-batch-table :deep(.el-table td .cell) { text-align: center; }
.quality-batch-table { width: 100%; }
.quality-batch-table :deep(.el-table) { width: 100%; }
.batch-cell { display: flex; flex-direction: column; min-width: 0; gap: 3px; }
.batch-cell strong { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.batch-cell strong { color: #263247; }
.dataset-list { display: grid; gap: 4px; min-width: 0; }
.dataset-cell { display: flex; flex-direction: column; min-width: 0; gap: 2px; }
.dataset-cell strong { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.dataset-cell strong { color: #263247; }
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
