<script setup>
import { computed, onMounted } from 'vue';
import { ElMessage } from 'element-plus';

import { usePartitionStore } from '@/stores/partition';
import BatchAssetsPanel from '@/views/partition/BatchAssetsPanel.vue';
import ExecutionResultPanel from '@/views/partition/ExecutionResultPanel.vue';
import GridParameters from '@/views/partition/GridParameters.vue';
import TaskQueuePanel from '@/views/partition/TaskQueuePanel.vue';

const store = usePartitionStore();
const formModel = computed({
  get: () => store.form,
  set: (value) => Object.assign(store.form, value),
});

async function submit() {
  try {
    await store.submit();
    ElMessage.success('剖分任务已提交。');
  } catch (error) {
    ElMessage.error(error.message || '提交剖分失败。');
  }
}

function reset() {
  store.resetForm();
}

function loadMap() {
  ElMessage.info('格网预览将基于已选择的数据集和参数加载。');
}

function updateDatasets(datasets) {
  store.form.datasets = datasets;
}

onMounted(() => {
  store.loadBatches();
  store.loadTasks();
});
</script>

<template>
  <main class="partition-workspace">
    <header class="workspace-heading">
      <div><p class="workspace-kicker">剖分数据服务</p><h1>数据剖分</h1></div>
      <p>选择完整数据集、原生格网和覆盖参数后提交生产剖分任务。</p>
    </header>
    <GridParameters v-model="formModel" :loading="store.loading.submit" @reset="reset" @submit="submit" @load-map="loadMap" />
    <BatchAssetsPanel :model-value="store.form.datasets" @update:model-value="updateDatasets" />
    <ExecutionResultPanel :result="store.result" :error="store.error" />
    <TaskQueuePanel
      :tasks="store.tasks"
      :page="store.taskPage"
      :loading="store.loading.tasks"
      @page-change="store.loadTasks($event, store.taskPage.pageSize)"
      @page-size-change="store.loadTasks(1, $event)"
    />
  </main>
</template>
