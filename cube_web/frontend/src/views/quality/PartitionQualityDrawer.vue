<script setup>
import { computed } from 'vue';
import { Refresh } from '@element-plus/icons-vue';

import DetailDrawer from '@/components/DetailDrawer.vue';
import StatusTag from '@/components/StatusTag.vue';

const props = defineProps({
  visible: Boolean,
  loading: Boolean,
  submitting: Boolean,
  detail: { type: Object, default: null },
});
const emit = defineEmits(['close', 'request-quality', 'retry-failed-partition']);

const treeProps = { children: 'children', label: 'label' };
const title = computed(() => props.detail ? `剖分批次 ${props.detail.partition_run_id}` : '剖分批次质检');
const summary = computed(() => props.detail?.summary || {});
const canRequestQuality = computed(() => Number(summary.value.partitioned_count || 0) > Number(summary.value.quality_pass_count || 0));
const hasFailedPartition = computed(() => Number(summary.value.partition_failed_count || 0) > 0);

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

const tree = computed(() => (props.detail?.datasets || []).map((dataset) => ({
  key: `dataset:${dataset.dataset_id}`,
  kind: 'dataset',
  label: `${dataset.dataset_code} · ${dataset.dataset_title}`,
  children: (dataset.scenes || []).map((scene) => ({
    key: `scene:${scene.scene_id}`,
    kind: 'scene',
    label: scene.scene_name || scene.scene_id,
    sourceLoadBatchId: scene.source_load_batch_id,
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
        <div><span>来源载入批次</span><strong>{{ (detail.source_load_batch_ids || []).join('、') || '-' }}</strong></div>
        <div><span>数据集 / 景 / 波段</span><strong>{{ detail.summary?.band_count || 0 }} 个波段</strong></div>
        <div><span>剖分状态</span><StatusTag domain="partition" :value="detail.status" size="small" /></div>
      </section>
      <section class="batch-stats">
        <div><span>剖分</span><strong>{{ summary.partitioned_count || 0 }}/{{ summary.band_count || 0 }}</strong></div>
        <div><span>质检通过</span><strong>{{ summary.quality_pass_count || 0 }}/{{ summary.band_count || 0 }}</strong></div>
        <div><span>质检失败</span><strong class="failure">{{ summary.quality_failed_count || 0 }}</strong></div>
        <div><span>已入库</span><strong>{{ summary.ingested_count || 0 }}/{{ summary.band_count || 0 }}</strong></div>
      </section>
      <div class="drawer-actions">
        <el-button v-if="hasFailedPartition" :loading="submitting" @click="emit('retry-failed-partition')">重试失败剖分</el-button>
        <el-button type="primary" :icon="Refresh" :loading="submitting" :disabled="!canRequestQuality" @click="emit('request-quality')">提交批次质检</el-button>
      </div>
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
          <span>#{{ attempt.attempt_no }} · {{ attempt.operation }}</span>
          <StatusTag domain="partition" :value="attempt.status" size="small" />
          <small>{{ attempt.error_message || attempt.failure_reason || '-' }}</small>
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
.drawer-actions { display: flex; gap: 8px; margin: 12px 0; }
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
@media (max-width: 680px) { .batch-overview, .batch-stats { grid-template-columns: 1fr 1fr; } .batch-overview > div:first-child { grid-column: 1 / -1; } }
</style>
