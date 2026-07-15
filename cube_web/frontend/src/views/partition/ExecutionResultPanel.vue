<script setup>
const props = defineProps({ result: { type: Object, default: null }, error: { type: String, default: '' } });
function count(value, keys) {
  for (const key of keys) if (Array.isArray(value?.[key])) return value[key].length;
  return 0;
}
</script>

<template>
  <section v-if="result || error" class="partition-section execution-result">
    <div class="section-heading"><h2>执行结果</h2></div>
    <el-alert v-if="error" :title="error" type="error" :closable="false" show-icon />
    <el-descriptions v-if="result" :column="2" border>
      <el-descriptions-item label="任务 ID">{{ result.task_id || '-' }}</el-descriptions-item>
      <el-descriptions-item label="剖分状态">{{ result.status || '-' }}</el-descriptions-item>
      <el-descriptions-item label="瓦片数据">{{ count(result, ['tiles', 'tile_objects']) }}</el-descriptions-item>
      <el-descriptions-item label="格网单元">{{ count(result, ['grid_cells']) }}</el-descriptions-item>
      <el-descriptions-item label="索引">{{ count(result, ['indexes']) }}</el-descriptions-item>
      <el-descriptions-item label="服务错误">{{ result.error || '-' }}</el-descriptions-item>
    </el-descriptions>
  </section>
</template>
