<script setup>
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

function updateWorkerContainerLimit(value) {
  emit('update:modelValue', {
    ...props.modelValue,
    workerContainerLimit: value == null ? 0 : value,
  });
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
      <label for="partition-worker-container-limit">最多的容器数量</label>
      <el-tooltip
        data-testid="worker-container-limit-tooltip"
        content="限制本次任务最多使用的容器数量；0 表示按系统默认值运行。"
        placement="top"
        :show-after="200"
      >
        <el-input-number
          id="partition-worker-container-limit"
          data-testid="worker-container-limit"
          :model-value="Number(modelValue.workerContainerLimit ?? 0)"
          :min="0"
          :step="1"
          :precision="0"
          controls-position="right"
          @update:model-value="updateWorkerContainerLimit"
        />
      </el-tooltip>
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
.worker-container-form-group :deep(.el-tooltip) { display: block; width: 100%; }
.worker-container-form-group :deep(.el-input-number) { width: 100%; }
.action-buttons { flex-wrap: nowrap; }
.action-buttons :deep(.el-button) { flex: 1 1 50%; min-width: 0; }
</style>
