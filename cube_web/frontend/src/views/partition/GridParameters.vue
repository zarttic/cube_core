<script setup>
import { computed } from 'vue';

import { derivedPartitionMethod, gridDefinition, gridDefinitions, nativeLevelLabel } from '@/utils/grid';

const props = defineProps({ modelValue: { type: Object, required: true }, loading: Boolean });
const emit = defineEmits(['update:modelValue', 'reset', 'submit', 'load-map']);

const definition = computed(() => gridDefinition(props.modelValue.gridType));
const levels = computed(() => {
  if (!definition.value) return [];
  return Array.from({ length: definition.value.maxLevel - definition.value.minLevel + 1 }, (_, index) => definition.value.minLevel + index);
});
const partitionMethod = computed(() => derivedPartitionMethod(props.modelValue.gridType));

function update(key, value) {
  emit('update:modelValue', { ...props.modelValue, [key]: value });
}
</script>

<template>
  <section class="partition-section grid-parameters">
    <div class="section-heading"><h2>格网参数</h2><span>剖分方式由格网类型自动确定</span></div>
    <el-form label-position="top" class="partition-form-grid">
      <el-form-item label="格网类型">
        <el-select data-testid="partition-grid-type" :model-value="modelValue.gridType" @update:model-value="update('gridType', $event)">
          <el-option v-for="grid in gridDefinitions" :key="grid.value" :label="grid.label" :value="grid.value" />
        </el-select>
      </el-form-item>
      <el-form-item label="原生层级">
        <el-select :model-value="Number(modelValue.requestedGridLevel)" @update:model-value="update('requestedGridLevel', Number($event))">
          <el-option v-for="level in levels" :key="level" :label="nativeLevelLabel(modelValue.gridType, level)" :value="level" />
        </el-select>
      </el-form-item>
      <el-form-item label="剖分方式">
        <div class="readonly-method">{{ partitionMethod === 'entity' ? '实体剖分' : '逻辑剖分' }}</div>
      </el-form-item>
      <el-form-item label="覆盖方式">
        <el-select :model-value="modelValue.coverMode" @update:model-value="update('coverMode', $event)">
          <el-option label="相交" value="intersect" /><el-option label="包含" value="contain" /><el-option label="最小覆盖" value="minimal" />
        </el-select>
      </el-form-item>
      <el-form-item label="时间粒度">
        <el-select :model-value="modelValue.timeGranularity" @update:model-value="update('timeGranularity', $event)">
          <el-option label="秒" value="second" /><el-option label="分钟" value="minute" /><el-option label="小时" value="hour" /><el-option label="天" value="day" /><el-option label="月" value="month" />
        </el-select>
      </el-form-item>
      <el-form-item label="每资产最大格网单元数">
        <el-input-number :model-value="Number(modelValue.maxCellsPerAsset)" :min="0" :precision="0" @update:model-value="update('maxCellsPerAsset', Number($event || 0))" />
      </el-form-item>
    </el-form>
    <div class="partition-actions">
      <el-button @click="$emit('load-map')">加载格网预览</el-button>
      <el-button @click="$emit('reset')">重置</el-button>
      <el-button type="primary" :loading="loading" @click="$emit('submit')">提交剖分</el-button>
    </div>
  </section>
</template>

<style scoped>
.readonly-method { min-height: 32px; display: flex; align-items: center; padding: 0 11px; border: 1px solid var(--el-border-color); border-radius: 4px; background: var(--el-fill-color-light); color: var(--el-text-color-regular); }
</style>
