<script setup>
import { computed } from 'vue';
import { Download } from '@element-plus/icons-vue';

import DetailDrawer from '@/components/DetailDrawer.vue';
import StatusTag from '@/components/StatusTag.vue';
import {
  filterActiveQualityRules,
  qualityErrorLabel,
  qualityExecutionErrorLabel,
  qualityRuleLabel,
} from '@/utils/qualityLabels';

const props = defineProps({
  visible: Boolean,
  loading: Boolean,
  submitting: Boolean,
  exporting: Boolean,
  detail: { type: Object, default: null },
});
const emit = defineEmits(['close', 'cancel-partition', 'retry-failed-partition', 'retry-quality-run', 'export-quality-errors']);

const treeProps = { children: 'children', label: 'label' };
const title = computed(() => props.detail ? `剖分批次 ${props.detail.partition_run_id}` : '剖分批次质检');
const summary = computed(() => props.detail?.summary || {});
const partitionTiming = computed(() => props.detail?.partition_compute_timing
  || props.detail?.partition_execution_timing
  || props.detail?.partition_timing);
const hasPartitionTiming = computed(() => Boolean(partitionTiming.value));
const partitionPendingLabel = computed(() => partitionTiming.value?.scope === 'partition_to_ingest'
  ? '等待入库完成'
  : '等待剖分完成');
const hasFailedPartition = computed(() => Number(summary.value.partition_failed_count || 0) > 0);
const hasFailedQuality = computed(() => Number(summary.value.quality_failed_count || 0) > 0);
const activePartition = computed(() => ['pending', 'queued', 'running', 'retrying', 'cancel_requested'].includes(props.detail?.status));
const canRetryPartition = computed(() => hasFailedPartition.value || hasFailedQuality.value
  || props.detail?.status === 'cancelled'
  || (props.detail?.attempts || []).some((attempt) => ['failed', 'cancelled', 'manual_required'].includes(attempt?.status)));
const sourceLoadBatchLabels = computed(() => props.detail?.source_load_batch_names?.length
  ? props.detail.source_load_batch_names
  : (props.detail?.source_load_batch_ids || []));
const qualityRuns = computed(() => (props.detail?.datasets || []).flatMap((dataset) =>
  (dataset.quality_runs || []).map((run) => ({
    ...run,
    items: filterActiveQualityRules(run.items || []),
    dataset_id: run.dataset_id || dataset.dataset_id,
    dataset_code: run.dataset_code || dataset.dataset_code,
    dataset_title: run.dataset_title || dataset.dataset_title,
    datasetLabel: dataset.dataset_title || dataset.dataset_code || dataset.dataset_id,
  }))
));

function isActiveQualityRun(run) {
  return ['pending', 'running'].includes(run?.status);
}

function canExportQualityRun(run) {
  return Boolean(run?.quality_run_id) && ['pass', 'warn', 'fail', 'error', 'cancelled'].includes(run?.status);
}

function ruleExecutionErrors(run) {
  if (isActiveQualityRun(run)) return [];
  return (run?.items || []).filter((item) => item.execution_error);
}

function executionError(run) {
  return isActiveQualityRun(run) ? '' : (run?.execution_error || '');
}

function hasFailureLog(run) {
  return Boolean(executionError(run) || ruleExecutionErrors(run).length || (!isActiveQualityRun(run) && run?.error_logs?.length));
}

function errorLogLocation(item) {
  const labels = [];
  if (item?.source_asset_id) labels.push(`源数据：${item.source_asset_id}`);
  if (item?.band_code) labels.push(`波段：${item.band_code}`);
  if (item?.field) labels.push(`字段：${item.field}`);
  if (item?.row_number !== null && item?.row_number !== undefined) labels.push(`行号：${item.row_number}`);
  if (item?.output_id || item?.index_id || item?.tile_id) {
    labels.push(`定位：${item.output_id || item.index_id || item.tile_id}`);
  }
  return labels.join(' · ');
}

function errorLogContext(item) {
  const context = item?.context;
  if (!context || typeof context !== 'object' || !Object.keys(context).length) return '';
  const contextLabels = {
    reason: '异常类型', declared: '声明坐标系', actual: '实际坐标系', expected: '期望值', observed: '实际值',
  };
  return `具体原因：${Object.entries(context)
    .map(([key, value]) => `${contextLabels[key] || key}：${key === 'reason' ? qualityExecutionErrorLabel(String(value)) : String(value)}`)
    .join('；')}`;
}

function workflowAdvice(band) {
  if (band.partition_status === 'failed') return '剖分失败，重试该波段的原剖分批次';
  if (band.partition_status !== 'completed') return '等待剖分完成';
  if (band.quality_status === 'error') return '质检执行异常，可直接重新质检';
  if (band.quality_status === 'fail') return '质检未通过，修复产物后重剖该波段';
  if (band.quality_status !== 'pass' && band.quality_status !== 'warn') return '等待或提交质检';
  if (band.ingest_status === 'failed') return '入库失败，可重试入库';
  if (band.ingest_status !== 'completed') return '质检通过，等待手动入库';
  return '流程完成';
}

function attemptOperationLabel(operation) {
  return {
    auto_run: '自动执行',
    manual_retry: '手动重试',
  }[operation] || operation || '剖分执行';
}

function attemptErrorLabel(attempt) {
  const detail = partitionFailureDetail(attempt?.error_message);
  const failureReason = partitionFailureDetail(attempt?.failure_reason);
  if (detail && failureReason && detail !== failureReason) {
    return `${detail}；具体原因：${failureReason}`;
  }
  if (detail || failureReason) return detail || failureReason;
  const labels = {
    partition_execution_failed: '一个或多个数据集剖分执行失败',
    grid_limit: '格网覆盖范围过大，请降低格网层级或缩小数据范围',
    source_missing: '源数据不存在或不可访问',
    local_task_failed: '剖分任务执行失败',
  };
  const generic = labels[attempt?.error_type];
  const errorMessage = qualityExecutionErrorLabel(attempt?.error_message);
  const reasonMessage = qualityExecutionErrorLabel(attempt?.failure_reason);
  if (generic && reasonMessage && reasonMessage !== generic) {
    return `${generic}；具体原因：${reasonMessage}`;
  }
  if (generic) return generic;
  if (errorMessage && reasonMessage && errorMessage !== reasonMessage) {
    return `${errorMessage}；具体原因：${reasonMessage}`;
  }
  return errorMessage || reasonMessage || '-';
}

function numericMetric(value) {
  if (value === null || value === undefined || value === '') return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function formatElapsed(value, pendingLabel = '等待完成') {
  const number = numericMetric(value);
  return number === null ? pendingLabel : `${number.toFixed(3)} 秒`;
}

function formatQualityElapsed(value) {
  const number = numericMetric(value);
  return number === null ? '等待质检完成' : `${number.toFixed(3)} 秒`;
}

function formatCount(value) {
  const number = numericMetric(value);
  return number === null ? '-' : number.toLocaleString('zh-CN');
}

function formatThroughput(value) {
  const number = numericMetric(value);
  return number === null ? '等待质检完成' : `${number.toFixed(2)} 格网/秒`;
}

function partitionFailureDetail(message) {
  const text = String(message || '');
  if (/[\u4e00-\u9fff]/.test(text)) return qualityExecutionErrorLabel(text);
  const limit = text.match(/MAX_(?:CANDIDATE|OUTPUT)_CELLS:\s*limit=(\d+),\s*observed=(\d+)/i);
  if (limit) {
    return `格网覆盖范围过大：候选格网 ${Number(limit[2]).toLocaleString()} 个，超过上限 ${Number(limit[1]).toLocaleString()} 个。请降低格网层级或缩小数据范围。`;
  }
  if (/NoSuchKey|NoSuchObject|NoSuchBucket|source COG.*not found/i.test(text)) return '源数据不存在或不可访问，请等待数据载入后重试。';
  if (/Failed to deserialize exception/i.test(text)) return 'Ray 任务异常，未能读取底层错误；请查看任务日志。';
  return '';
}

const tree = computed(() => (props.detail?.datasets || []).map((dataset) => ({
  key: `dataset:${dataset.dataset_id}`,
  kind: 'dataset',
  label: `${dataset.dataset_code} · ${dataset.dataset_title}`,
  children: (dataset.scenes || []).map((scene) => ({
    key: `scene:${scene.scene_id}`,
    kind: 'scene',
    label: scene.scene_name || scene.scene_id,
    sourceLoadBatchId: scene.source_load_batch_name || scene.source_load_batch_id,
    children: (scene.bands || []).map((band) => ({
      key: `band:${band.band_unit_id}`,
      kind: 'band',
      label: band.band_name ? `${band.band_code} · ${band.band_name}` : band.band_code,
      band,
    })),
  })),
})));
</script>

<template>
  <DetailDrawer :visible="visible" :title="title" :loading="loading" @update:visible="(value) => !value && emit('close')" @closed="emit('close')">
    <div class="drawer-close-row"><el-button link type="primary" @click="emit('close')">关闭</el-button></div>
    <template v-if="detail">
      <section class="batch-overview">
        <div><span>来源载入批次</span><strong>{{ sourceLoadBatchLabels.join('、') || '-' }}</strong></div>
        <div><span>数据集 / 景 / 波段</span><strong>{{ detail.summary?.band_count || 0 }} 个波段</strong></div>
        <div><span>剖分状态</span><StatusTag domain="partition" :value="detail.status" size="small" /></div>
      </section>
      <section class="batch-stats">
        <div><span>剖分</span><strong>{{ summary.partitioned_count || 0 }}/{{ summary.band_count || 0 }}</strong></div>
        <div><span>质检通过</span><strong>{{ summary.quality_pass_count || 0 }}/{{ summary.band_count || 0 }}</strong></div>
        <div><span>质检失败</span><strong class="failure">{{ summary.quality_failed_count || 0 }}</strong></div>
        <div><span>已入库</span><strong>{{ summary.ingested_count || 0 }}/{{ summary.band_count || 0 }}</strong></div>
      </section>
      <section v-if="hasPartitionTiming" class="partition-timing" data-testid="partition-timing">
        <div v-if="partitionTiming"><span>剖分耗时</span><strong>{{ formatElapsed(partitionTiming.elapsed_sec, partitionPendingLabel) }}</strong></div>
      </section>
      <section class="dataset-associations" data-testid="quality-dataset-associations">
        <h3>关联数据集</h3>
        <div v-for="dataset in detail.datasets || []" :key="dataset.dataset_id" class="dataset-association">
          <strong>{{ dataset.dataset_title || dataset.dataset_code || dataset.dataset_id }}</strong>
        </div>
        <span v-if="!(detail.datasets || []).length" class="quality-pending">暂无关联数据集</span>
      </section>
      <div v-if="activePartition || canRetryPartition" class="drawer-actions">
        <el-button v-if="activePartition" data-testid="force-cancel-partition" type="danger" plain :loading="submitting" @click="emit('cancel-partition')">强制终止剖分</el-button>
        <el-button v-if="canRetryPartition" data-testid="retry-partition" :loading="submitting" @click="emit('retry-failed-partition')">重新提交失败数据剖分</el-button>
      </div>
      <section v-if="qualityRuns.length" class="quality-run-list" data-testid="partition-quality-items">
        <h3>自动质检项</h3>
        <article v-for="run in qualityRuns" :key="run.quality_run_id" class="quality-run-card">
          <header>
            <strong>{{ run.datasetLabel }}</strong><StatusTag domain="quality" :value="run.status" size="small" />
            <el-button v-if="['fail', 'error'].includes(run.status)" link type="primary" :loading="submitting" @click="emit('retry-quality-run', run)">立刻重试</el-button>
            <el-button v-if="canExportQualityRun(run)" :data-testid="`quality-export-content-${run.quality_run_id}`" link type="primary" :icon="Download" :loading="exporting" @click="emit('export-quality-errors', run)">导出质检结果</el-button>
          </header>
          <div v-if="run.items?.length" class="quality-item-list">
            <div v-for="item in run.items" :key="item.rule_code" class="quality-item-row">
              <span>{{ qualityRuleLabel(item.rule_code) }}</span>
              <StatusTag domain="quality" :value="item.status" size="small" />
              <small>{{ item.finding_count ? `${item.finding_count} 项发现` : '未发现问题' }}</small>
            </div>
          </div>
          <small v-else class="quality-pending">{{ run.results_complete ? '暂无规则结果' : '质检项执行中，结果生成后自动显示' }}</small>
          <section v-if="run.metrics" class="quality-throughput" :data-testid="`quality-throughput-${run.quality_run_id}`">
            <div><span>检查格网</span><strong>{{ formatCount(run.metrics.checked_grid_count) }}</strong></div>
            <div><span>质检耗时</span><strong>{{ formatQualityElapsed(run.metrics.quality_elapsed_sec) }}</strong></div>
            <div><span>吞吐</span><strong>{{ formatThroughput(run.metrics.grid_throughput_per_sec) }}</strong></div>
          </section>
          <section v-if="hasFailureLog(run)" class="quality-failure-log" :data-testid="`quality-failure-log-${run.quality_run_id}`">
            <strong>失败日志</strong>
            <p v-if="executionError(run)">{{ qualityExecutionErrorLabel(executionError(run)) }}</p>
            <p v-for="item in ruleExecutionErrors(run)" :key="`${item.rule_code}:${item.execution_error}`">{{ qualityRuleLabel(item.rule_code) }}：{{ qualityExecutionErrorLabel(item.execution_error) }}</p>
            <p v-for="item in run.error_logs || []" :key="`${item.quality_error_id || item.error_code}:${item.message}`"><span>{{ qualityErrorLabel(item.error_code) }}</span>：{{ qualityExecutionErrorLabel(item.message) }}<small v-if="errorLogLocation(item)">{{ errorLogLocation(item) }}</small><small v-if="errorLogContext(item)">{{ errorLogContext(item) }}</small></p>
          </section>
        </article>
      </section>
      <el-tree class="partition-quality-tree" :data="tree" :props="treeProps" node-key="key" :expand-on-click-node="false">
        <template #default="{ data }">
          <div class="quality-node" :class="`quality-node-${data.kind}`">
            <template v-if="data.kind !== 'band'">
              <strong>{{ data.label }}</strong>
              <small v-if="data.sourceLoadBatchId">来源 {{ data.sourceLoadBatchId }}</small>
            </template>
            <template v-else>
              <span class="band-name">{{ data.label }}</span>
              <StatusTag domain="partition" :value="data.band.partition_status" size="small" />
              <StatusTag domain="quality" :value="data.band.quality_status" size="small" />
              <StatusTag domain="ingest" :value="data.band.ingest_status" size="small" />
              <small>{{ workflowAdvice(data.band) }}</small>
            </template>
          </div>
        </template>
      </el-tree>
      <details class="attempt-history">
        <summary>剖分尝试历史 <span>{{ (detail.attempts || []).length }} 次</span></summary>
        <div v-for="attempt in detail.attempts || []" :key="attempt.task_id" class="attempt-row">
          <span>#{{ attempt.attempt_no }} · {{ attemptOperationLabel(attempt.operation) }}</span>
          <StatusTag domain="partition" :value="attempt.status" size="small" />
          <small>{{ attemptErrorLabel(attempt) }}</small>
        </div>
      </details>
    </template>
  </DetailDrawer>
</template>

<style scoped>
.drawer-close-row { display: flex; justify-content: flex-end; margin-bottom: 8px; }
.batch-overview, .batch-stats { display: grid; gap: 8px; margin-bottom: 12px; }
.batch-overview { grid-template-columns: 2fr 1fr 1fr; }
.batch-stats { grid-template-columns: repeat(4, minmax(0, 1fr)); }
.batch-overview > div, .batch-stats > div { border: 1px solid #dfe4ec; background: #fafbfd; padding: 9px 10px; min-width: 0; }
.batch-overview span, .batch-stats span { display: block; color: #667085; font-size: 12px; margin-bottom: 3px; }
.batch-overview strong { display: block; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.batch-stats strong { color: #1f5f8b; }
.batch-stats .failure { color: #a53b32; }
.partition-timing, .quality-throughput { display: grid; gap: 8px; margin: 0 0 14px; }
.partition-timing { grid-template-columns: repeat(2, minmax(0, 1fr)); }
.quality-throughput { grid-template-columns: repeat(3, minmax(0, 1fr)); }
.partition-timing > div, .quality-throughput > div { border: 1px solid #dfe4ec; background: #fafbfd; padding: 9px 10px; min-width: 0; }
.partition-timing span, .quality-throughput span { display: block; color: #667085; font-size: 12px; margin-bottom: 3px; }
.partition-timing strong, .quality-throughput strong { display: block; color: #1f5f8b; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.dataset-associations { margin-bottom: 14px; border: 1px solid #dfe4ec; background: #fafbfd; padding: 10px; }
.dataset-associations h3 { margin: 0 0 8px; color: #344054; font-size: 14px; }
.dataset-association { display: grid; grid-template-columns: minmax(0, 1fr); gap: 8px; padding: 6px 0; border-top: 1px solid #edf0f4; }
.dataset-association:first-of-type { border-top: 0; }
.dataset-association strong { color: #1f3b57; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.drawer-actions { display: flex; gap: 8px; margin: 12px 0; }
.quality-run-list { display: grid; gap: 8px; margin: 14px 0; }
.quality-run-list h3 { margin: 0; color: #344054; font-size: 14px; }
.quality-run-card { border: 1px solid #dfe4ec; background: #fafbfd; padding: 9px 10px; }
.quality-run-card header, .quality-item-row { display: flex; align-items: center; gap: 8px; min-width: 0; }
.quality-run-card header > strong { color: #1f3b57; }
.quality-run-card header > span, .quality-item-row small, .quality-pending { color: #667085; font-size: 12px; }
.quality-item-list { display: grid; gap: 5px; margin-top: 8px; }
.quality-item-row > span { flex: 1; min-width: 0; }
.quality-item-row small { min-width: 82px; text-align: right; }
.quality-failure-log { display: grid; gap: 4px; margin-top: 9px; border-left: 3px solid #c24d45; background: #fff7f6; padding: 8px 10px; color: #7f2d28; }
.quality-failure-log > strong { color: #9b2c2c; }
.quality-failure-log > small { color: #667085; overflow-wrap: anywhere; }
.quality-failure-log p { margin: 0; overflow-wrap: anywhere; }
.quality-failure-log p span { font-weight: 600; }
.partition-quality-tree { border: 1px solid #dfe4ec; padding: 8px; max-height: 560px; overflow: auto; }
.attempt-history { margin-top: 12px; border: 1px solid #dfe4ec; padding: 8px 10px; }
.attempt-history summary { color: #344054; cursor: pointer; font-weight: 600; }
.attempt-history summary span { color: #667085; font-size: 12px; font-weight: 400; }
.attempt-row { display: grid; grid-template-columns: minmax(120px, 1fr) auto minmax(160px, 2fr); gap: 8px; align-items: center; padding: 8px 0; border-top: 1px solid #edf0f4; }
.attempt-row:first-of-type { margin-top: 7px; }
.attempt-row small { color: #667085; overflow-wrap: anywhere; }
.quality-node { display: flex; align-items: center; gap: 7px; min-height: 28px; min-width: 0; }
.quality-node small { color: #667085; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.quality-node-dataset > strong { color: #1f3b57; }
.quality-node-scene > strong { color: #344054; }
.band-name { min-width: 150px; color: #315a7a; }
@media (max-width: 680px) { .batch-overview, .batch-stats, .quality-throughput { grid-template-columns: 1fr 1fr; } .partition-timing { grid-template-columns: 1fr; } .batch-overview > div:first-child { grid-column: 1 / -1; } .dataset-association { grid-template-columns: 1fr; gap: 3px; } }
</style>
