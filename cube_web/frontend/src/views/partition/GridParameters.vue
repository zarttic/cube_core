<script setup>
import { computed, ref, watch } from 'vue';

const props = defineProps({
  modelValue: { type: Object, required: true },
  loading: Boolean,
  submitDisabled: Boolean,
  dataTypeLabel: { type: String, default: '' },
  selectedCount: { type: Number, default: 0 },
  selectedDatasetCount: { type: Number, default: 0 },
  sourceBatchIds: { type: Array, default: () => [] },
});
const emit = defineEmits(['update:modelValue', 'reset', 'submit', 'open-datasets']);

// 容器限制默认不设置（模型值 0 = 按系统默认并发运行）。开启后只允许 >= 1 的整数；
// 编辑期只保留开头的数字串，避免负号/小数/科学计数法被带进来。
const blockedWorkerContainerKeys = new Set(['-', '+', 'e', 'E']);
const defaultEnabledWorkerContainerLimit = 4;

function blockInvalidWorkerContainerKeys(event) {
  if (blockedWorkerContainerKeys.has(event.key)) event.preventDefault();
}

function normalizeWorkerContainerLimit(value) {
  const limit = Number(value ?? 0);
  return Number.isInteger(limit) && limit >= 0 ? limit : 0;
}

function updateWorkerContainerLimit(value) {
  emit('update:modelValue', {
    ...props.modelValue,
    workerContainerLimit: normalizeWorkerContainerLimit(value),
  });
}

const workerContainerLimit = computed(() => normalizeWorkerContainerLimit(props.modelValue.workerContainerLimit));
const workerContainerLimitEnabled = computed(() => workerContainerLimit.value > 0);
// 未开启时不渲染步进器；编辑期展示原始草稿（允许暂时为空或 0），失焦后回写模型值。
const editingWorkerContainerLimit = ref(null);
// 重新开启时沿用上次填过的值，没填过就用默认值。
const lastPositiveWorkerContainerLimit = ref(0);
watch(workerContainerLimit, (value) => {
  if (value > 0) lastPositiveWorkerContainerLimit.value = value;
}, { immediate: true });
const workerContainerNote = computed(
  () => `本次任务同时最多占用 ${workerContainerLimit.value} 个计算容器`,
);

function toggleWorkerContainerLimit(enabled) {
  editingWorkerContainerLimit.value = null;
  updateWorkerContainerLimit(
    enabled ? (lastPositiveWorkerContainerLimit.value || defaultEnabledWorkerContainerLimit) : 0,
  );
}

function onWorkerContainerInput(event) {
  const raw = String(event.target.value ?? '').trim().match(/^\d*/)[0];
  if (event.target.value !== raw) event.target.value = raw;
  editingWorkerContainerLimit.value = raw;
  // 空草稿和 0 只是编辑中的临时文本，不写回模型：下限是 1，关限制用开关。
  const parsed = normalizeWorkerContainerLimit(raw);
  if (parsed > 0) updateWorkerContainerLimit(parsed);
}

function onWorkerContainerBlur(event) {
  editingWorkerContainerLimit.value = null;
  if (event?.target) event.target.value = String(workerContainerLimit.value);
}

function stepWorkerContainerLimit(delta) {
  updateWorkerContainerLimit(Math.max(1, workerContainerLimit.value + delta));
}
</script>

<template>
  <section class="config-panel">
    <h3>{{ dataTypeLabel === '光学遥感' ? '数据配置' : '参数配置' }}</h3>
    <div class="form-group">
      <div class="data-queue-panel">
        <button type="button" class="queue-header queue-drawer-toggle" @click="$emit('open-datasets')">
          <span class="queue-title">已载入{{ dataTypeLabel }}数据</span>
          <span class="queue-header-meta">
            <span class="queue-open-text">打开列表</span>
          </span>
        </button>
        <div class="queue-selected-summary">
          当前选择：{{ selectedDatasetCount }} 个数据集 · {{ selectedCount }} 个波段
        </div>
      </div>
    </div>
    <div class="form-group">
      <label>来源载入批次</label>
      <div class="source-batch-summary" data-testid="selected-load-batches">
        <div class="source-batch-summary-head"><strong>{{ sourceBatchIds.length ? sourceBatchIds.length + ' 个批次' : '未关联批次' }}</strong></div>
        <div v-if="sourceBatchIds.length" class="source-batch-tags">
          <el-tooltip v-for="batchId in sourceBatchIds" :key="batchId" :content="batchId" placement="top" :show-after="300">
            <el-tag class="source-batch-tag" data-overflow-title-off size="small" effect="plain" tabindex="0">
              <span class="source-batch-id">{{ batchId }}</span>
            </el-tag>
          </el-tooltip>
        </div>
        <span v-else>选择待剖分数据单元后自动关联</span>
      </div>
    </div>
    <div class="form-group worker-container-form-group">
      <div class="worker-limit-head">
        <label for="partition-worker-container-limit">容器限制</label>
        <el-tooltip
          data-testid="worker-container-limit-tooltip"
          content="默认不设置单任务容器限制，按系统默认并发运行；开启后可限制本次任务最多使用的容器数量。"
          placement="top"
          :show-after="200"
        >
          <span class="worker-limit-help" tabindex="0" aria-label="容器限制说明">?</span>
        </el-tooltip>
        <el-switch
          data-testid="worker-container-limit-toggle"
          aria-label="设置容器限制"
          :model-value="workerContainerLimitEnabled"
          @update:model-value="toggleWorkerContainerLimit"
        />
      </div>
      <div
        v-if="workerContainerLimitEnabled"
        class="worker-limit-stepper"
        @keydown.capture="blockInvalidWorkerContainerKeys"
      >
        <button
          type="button"
          class="step-button"
          data-testid="worker-container-decrease"
          aria-label="减少容器限制"
          :disabled="workerContainerLimit <= 1"
          @click="stepWorkerContainerLimit(-1)"
        ><span class="step-glyph step-glyph-minus" aria-hidden="true" /></button>
        <label class="step-field" for="partition-worker-container-limit">
          <input
            id="partition-worker-container-limit"
            data-testid="worker-container-limit"
            class="step-input"
            inputmode="numeric"
            autocomplete="off"
            :value="editingWorkerContainerLimit ?? String(workerContainerLimit)"
            @input="onWorkerContainerInput"
            @blur="onWorkerContainerBlur"
            @keydown.enter="$event.target.blur()"
          >
        </label>
        <button
          type="button"
          class="step-button"
          data-testid="worker-container-increase"
          aria-label="增加容器限制"
          @click="stepWorkerContainerLimit(1)"
        ><span class="step-glyph step-glyph-plus" aria-hidden="true" /></button>
      </div>
      <p v-if="workerContainerLimitEnabled" class="worker-limit-note" data-testid="worker-container-note">{{ workerContainerNote }}</p>
    </div>
    <div class="form-group action-buttons">
      <el-button @click="$emit('reset')">重置</el-button>
      <el-button type="primary" :loading="loading" :disabled="submitDisabled" @click="$emit('submit')">提交剖分</el-button>
    </div>
  </section>
</template>

<style scoped>
.source-batch-summary { display: grid; gap: 8px; padding: 10px 11px; border: 1px solid var(--el-border-color); border-radius: 4px; background: var(--el-fill-color-light); }
.source-batch-summary-head { display: flex; align-items: center; justify-content: space-between; color: var(--el-text-color-primary); font-size: 12px; }
.source-batch-summary > span { color: var(--el-text-color-secondary); font-size: 12px; }
.source-batch-tags { display: grid; grid-template-columns: minmax(0, 1fr); gap: 6px; min-width: 0; overflow: hidden; }
.source-batch-tags :deep(.source-batch-tag) { box-sizing: border-box; width: 100%; max-width: 100%; min-width: 0; overflow: hidden; }
.source-batch-tags :deep(.source-batch-tag .el-tag__content) { display: block; flex: 1 1 auto; max-width: 100%; min-width: 0; overflow: hidden; }
.source-batch-id { display: block; width: 100%; max-width: 100%; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.worker-container-form-group { display: grid; gap: 8px; }
.worker-limit-head { display: flex; align-items: center; gap: 6px; }
.worker-limit-head label { color: #263247; font-size: 13px; font-weight: 600; }
.worker-limit-head :deep(.el-switch) { margin-left: auto; }
.worker-limit-help { display: inline-flex; align-items: center; justify-content: center; width: 15px; height: 15px; border-radius: 50%; background: var(--el-fill-color); color: var(--el-text-color-secondary); font-size: 11px; line-height: 1; cursor: help; }
.worker-limit-stepper { display: grid; grid-template-columns: 34px minmax(0, 1fr) 34px; align-items: center; height: 34px; border: 1px solid var(--el-border-color); border-radius: 8px; background: var(--el-bg-color); transition: border-color 0.2s, box-shadow 0.2s; }
.worker-limit-stepper:focus-within { border-color: var(--primary); box-shadow: 0 0 0 3px rgba(26, 95, 180, 0.12); }
/* −／＋ 用 CSS 笔画而不是字体字符：字符的墨迹上下位置随字体变化，笔画是几何居中。 */
.step-button { display: flex; align-items: center; justify-content: center; height: 100%; border: 0; background: transparent; color: #43506b; cursor: pointer; transition: background 0.2s, color 0.2s; }
.step-glyph { position: relative; display: block; width: 12px; height: 12px; }
.step-glyph::before, .step-glyph::after { content: ''; position: absolute; background: currentColor; }
.step-glyph::before { top: 50%; left: 0; width: 100%; height: 1.5px; transform: translateY(-50%); }
.step-glyph-plus::after { left: 50%; top: 0; height: 100%; width: 1.5px; transform: translateX(-50%); }
.step-button:first-child { border-radius: 8px 0 0 8px; }
.step-button:last-child { border-radius: 0 8px 8px 0; }
.step-button:hover:not(:disabled) { background: var(--primary-bg); color: var(--primary); }
.step-button:disabled { color: #c7ccd6; cursor: not-allowed; }
/* 输入框铺满中间格并用 text-align:center，数字落在整个步进器的正中心。
   注意 .form-group label 全局带 10px 下边距，会把 32px 的格内内容顶高 5px，这里必须清掉。 */
.step-field { height: 100%; margin: 0; cursor: text; }
/* text-box 把行盒裁到“大写高/基线”，数字按墨迹高度上下居中（Chrome 133+）；
   不支持时退回按行盒居中，不会报错。 */
.step-input { display: block; width: 100%; height: 100%; border: 0; outline: none; background: transparent; color: #263247; font-family: inherit; font-size: 15px; font-weight: 600; text-align: center; text-box: trim-both cap alphabetic; }
.worker-limit-note { margin: 0; color: #8993a4; font-size: 12px; line-height: 1.5; }
.action-buttons { flex-wrap: nowrap; }
.action-buttons :deep(.el-button) { flex: 1 1 50%; min-width: 0; }
</style>
