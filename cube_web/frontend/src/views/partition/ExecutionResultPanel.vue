<script setup>
const props = defineProps({ result: { type: Object, default: null }, error: { type: String, default: '' } });
function count(value, keys) {
  for (const key of keys) if (Array.isArray(value?.[key])) return value[key].length;
  return 0;
}
</script>

<template>
  <div v-if="result || error" class="legacy-execution-result">
    <el-alert v-if="error" :title="error" type="error" :closable="false" show-icon />
    <template v-if="result">
      <div class="quality-section-title">剖分结果</div>
      <div class="result-item"><div class="result-label">任务 ID</div><div class="result-value">{{ result.task_id || '-' }}</div></div>
      <div class="result-item"><div class="result-label">剖分状态</div><div class="result-value">{{ result.status || '-' }}</div></div>
      <div class="result-item"><div class="result-label">瓦片数据</div><div class="result-value">{{ count(result, ['tiles', 'tile_objects']) }}</div></div>
      <div class="result-item"><div class="result-label">格网单元</div><div class="result-value">{{ count(result, ['grid_cells']) }}</div></div>
      <div class="result-item"><div class="result-label">索引</div><div class="result-value">{{ count(result, ['indexes']) }}</div></div>
      <div v-if="result.error" class="result-item"><div class="result-label">服务错误</div><div class="result-value">{{ result.error }}</div></div>
    </template>
  </div>
</template>
