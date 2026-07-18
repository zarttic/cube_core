<script setup>
import AppTable from '@/components/AppTable.vue';
import StatusTag from '@/components/StatusTag.vue';
import { formatShanghaiTime } from '@/utils/time';

defineProps({ tasks: { type: Array, default: () => [] }, page: { type: Object, required: true }, loading: Boolean });
defineEmits(['page-change', 'page-size-change']);
</script>

<template>
  <section class="partition-task-table-panel">
    <AppTable :data="tasks" :loading="loading" row-key="task_id" :page="page.page" :page-size="page.pageSize" :total="page.total" @current-change="$emit('page-change', $event)" @size-change="$emit('page-size-change', $event)">
      <el-table-column prop="task_id" label="任务 ID" min-width="180" show-overflow-tooltip />
      <el-table-column prop="batch_id" label="剖分执行 ID" min-width="170" show-overflow-tooltip />
      <el-table-column prop="data_type" label="数据类型" width="100" />
      <el-table-column label="状态" width="120"><template #default="{ row }"><StatusTag domain="partition" :value="row.status" /></template></el-table-column>
      <el-table-column label="创建时间" min-width="170"><template #default="{ row }">{{ formatShanghaiTime(row.created_at) }}</template></el-table-column>
    </AppTable>
  </section>
</template>
