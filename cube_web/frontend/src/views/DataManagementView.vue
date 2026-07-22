<script setup>
import { ref } from 'vue';
import { ElMessage } from 'element-plus';

import router from '@/router';
import { createDatasetReloadBatch } from '@/api/partitionDrafts';
import { queuePartitionSelection } from '@/stores/partitionTransfer';
import DatasetsView from '@/views/DatasetsView.vue';
import IngestView from '@/views/IngestView.vue';

const props = defineProps({ embedded: Boolean });
const emit = defineEmits(['queue-partition']);

const activeView = ref('datasets');

async function queuePartition(dataset) {
  try {
    const reloadBatch = await createDatasetReloadBatch(dataset);
    if (props.embedded) {
      emit('queue-partition', reloadBatch);
      return;
    }
    queuePartitionSelection(reloadBatch);
    router.push({ name: 'partition', query: { module: dataset.data_type, load_batch_id: reloadBatch.load_batch_id } });
  } catch (error) {
    ElMessage.error(error.message || '创建重新载入批次失败');
  }
}
</script>

<template>
  <section class="data-management-view" :class="{ embedded }">
    <el-tabs v-model="activeView" class="data-management-tabs">
      <el-tab-pane name="datasets" label="数据管理">
        <DatasetsView embedded title="" @queue-partition="queuePartition" />
      </el-tab-pane>
      <el-tab-pane name="ingest-runs" label="数据入库" lazy>
        <IngestView embedded title="数据入库" />
      </el-tab-pane>
    </el-tabs>
  </section>
</template>

<style scoped>
.data-management-view {
  width: min(1520px, calc(100% - 40px));
  margin: 0 auto;
  padding: 26px 0 40px;
}
.data-management-view.embedded {
  width: 100%;
  padding: 0;
}
.data-management-tabs :deep(.el-tabs__header) {
  margin: 0 0 20px;
  border-bottom: 1px solid #dfe4ec;
}
.data-management-tabs :deep(.el-tabs__nav-wrap::after) { display: none; }
.data-management-tabs :deep(.el-tabs__item) {
  height: 46px;
  padding: 0 22px;
  color: #5c667a;
  font-size: 15px;
  font-weight: 500;
}
.data-management-tabs :deep(.el-tabs__item.is-active) {
  color: #1769aa;
  font-weight: 650;
}
.data-management-tabs :deep(.el-tabs__active-bar) { height: 3px; background: #1769aa; }
@media (max-width: 720px) {
  .data-management-view { width: calc(100% - 24px); padding-top: 18px; }
  .data-management-tabs :deep(.el-tabs__item) { padding: 0 12px; font-size: 14px; }
}
</style>
