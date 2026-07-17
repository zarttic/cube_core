<script setup>
defineProps({
  modelValue: { type: Object, required: true },
  loading: Boolean,
  submitDisabled: Boolean,
  dataTypeLabel: { type: String, default: '' },
  selectedCount: { type: Number, default: 0 },
  selectedDatasetCount: { type: Number, default: 0 },
  sourceBatchIds: { type: Array, default: () => [] },
});
defineEmits(['reset', 'submit', 'open-datasets']);
</script>

<template>
  <section class="config-panel">
    <h3>{{ dataTypeLabel === '光学遥感' ? '数据配置' : '参数配置' }}</h3>
    <div class="form-group">
      <label>待剖分数据队列</label>
      <div class="task-note">
        <span>任务备注：请在 ARD数据载入 子系统中完成数据接入与登记后，待剖分数据将自动出现在此队列中。</span>
      </div>
      <div class="data-queue-panel">
        <button type="button" class="queue-header queue-drawer-toggle" @click="$emit('open-datasets')">
          <span class="queue-title">已载入{{ dataTypeLabel }}数据</span>
          <span class="queue-header-meta">
            <span class="queue-open-text">打开列表</span>
          </span>
        </button>
        <div class="queue-selected-summary">
          当前选择：{{ selectedDatasetCount }} 个数据集 · {{ selectedCount }} 个数据单元
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
.action-buttons { flex-wrap: nowrap; }
.action-buttons :deep(.el-button) { flex: 1 1 50%; min-width: 0; }
</style>
