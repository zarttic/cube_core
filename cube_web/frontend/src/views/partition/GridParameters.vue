<script setup>
import { computed, ref } from 'vue';

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

// 容器数量只允许非负整数；编辑期只保留开头的数字串，避免负号/小数/科学计数法被带进来。
const blockedWorkerContainerKeys = new Set(['-', '+', 'e', 'E']);
const workerContainerPresets = [
  { value: 0, label: '默认' },
  { value: 1, label: '1' },
  { value: 2, label: '2' },
  { value: 4, label: '4' },
  { value: 8, label: '8' },
];

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
// 编辑期展示原始输入（允许暂时为空），失焦后回写规范化后的值。
const editingWorkerContainerLimit = ref(null);
const workerContainerNote = computed(() => (
  workerContainerLimit.value === 0
    ? '不限制单任务并发，按系统默认值运行'
    : `本次任务同时最多占用 ${workerContainerLimit.value} 个计算容器`
));

function onWorkerContainerInput(event) {
  const raw = String(event.target.value ?? '').trim().match(/^\d*/)[0];
  if (event.target.value !== raw) event.target.value = raw;
  editingWorkerContainerLimit.value = raw;
  updateWorkerContainerLimit(raw === '' ? 0 : raw);
}

function onWorkerContainerBlur(event) {
  editingWorkerContainerLimit.value = null;
  if (event?.target) event.target.value = String(workerContainerLimit.value);
}

function stepWorkerContainerLimit(delta) {
  updateWorkerContainerLimit(workerContainerLimit.value + delta);
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
            <el-tag class="source-batch-tag" size="small" effect="plain" tabindex="0">
              <span class="source-batch-id">{{ batchId }}</span>
            </el-tag>
          </el-tooltip>
        </div>
        <span v-else>选择待剖分数据单元后自动关联</span>
      </div>
    </div>
    <div class="form-group worker-container-form-group">
      <div class="worker-limit-head">
        <label for="partition-worker-container-limit">容器数量</label>
        <el-tooltip
          data-testid="worker-container-limit-tooltip"
          content="限制本次任务最多使用的容器数量；0 表示按系统默认值运行。"
          placement="top"
          :show-after="200"
        >
          <span class="worker-limit-help" tabindex="0" aria-label="容器数量说明">?</span>
        </el-tooltip>
      </div>
      <div
        class="worker-limit-stepper"
        :class="{ 'is-default': workerContainerLimit === 0 }"
        @keydown.capture="blockInvalidWorkerContainerKeys"
      >
        <button
          type="button"
          class="step-button"
          data-testid="worker-container-decrease"
          aria-label="减少容器数量"
          :disabled="workerContainerLimit <= 0"
          @click="stepWorkerContainerLimit(-1)"
        >−</button>
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
          <span class="step-unit">容器</span>
        </label>
        <button
          type="button"
          class="step-button"
          data-testid="worker-container-increase"
          aria-label="增加容器数量"
          @click="stepWorkerContainerLimit(1)"
        >＋</button>
      </div>
      <div class="worker-limit-chips" role="group" aria-label="常用容器数量">
        <button
          v-for="preset in workerContainerPresets"
          :key="preset.value"
          type="button"
          class="limit-chip"
          :class="{ 'is-active': workerContainerLimit === preset.value }"
          :data-testid="`worker-container-preset-${preset.value}`"
          @click="updateWorkerContainerLimit(preset.value)"
        >{{ preset.label }}</button>
      </div>
      <p class="worker-limit-note" data-testid="worker-container-note">{{ workerContainerNote }}</p>
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
.worker-limit-help { display: inline-flex; align-items: center; justify-content: center; width: 15px; height: 15px; border-radius: 50%; background: var(--el-fill-color); color: var(--el-text-color-secondary); font-size: 11px; line-height: 1; cursor: help; }
.worker-limit-stepper { display: grid; grid-template-columns: 34px minmax(0, 1fr) 34px; align-items: center; height: 34px; border: 1px solid var(--el-border-color); border-radius: 8px; background: var(--el-bg-color); transition: border-color 0.2s, box-shadow 0.2s; }
.worker-limit-stepper.is-default { border-style: dashed; }
.worker-limit-stepper:focus-within { border-color: var(--primary); box-shadow: 0 0 0 3px rgba(26, 95, 180, 0.12); }
.step-button { height: 100%; border: 0; background: transparent; color: #43506b; font-size: 15px; line-height: 1; cursor: pointer; transition: background 0.2s, color 0.2s; }
.step-button:first-child { border-radius: 8px 0 0 8px; }
.step-button:last-child { border-radius: 0 8px 8px 0; }
.step-button:hover:not(:disabled) { background: var(--primary-bg); color: var(--primary); }
.step-button:disabled { color: #c7ccd6; cursor: not-allowed; }
.step-field { display: flex; align-items: center; justify-content: center; gap: 4px; height: 100%; cursor: text; }
.step-input { width: 3em; height: 100%; border: 0; outline: none; background: transparent; color: #263247; font-size: 15px; font-weight: 600; text-align: center; }
.step-unit { color: #8993a4; font-size: 12px; }
.worker-limit-chips { display: flex; flex-wrap: wrap; gap: 6px; }
.limit-chip { padding: 3px 12px; border: 1px solid var(--el-border-color); border-radius: 999px; background: var(--el-bg-color); color: #5e6b85; font-size: 12px; cursor: pointer; transition: all 0.2s; }
.limit-chip:hover { border-color: var(--primary); color: var(--primary); }
.limit-chip.is-active { border-color: var(--primary); background: var(--primary-bg); color: var(--primary); font-weight: 600; }
.worker-limit-note { margin: 0; color: #8993a4; font-size: 12px; line-height: 1.5; }
.action-buttons { flex-wrap: nowrap; }
.action-buttons :deep(.el-button) { flex: 1 1 50%; min-width: 0; }
</style>
