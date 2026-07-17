<script setup>
import { ref } from 'vue';

import DatasetsView from '@/views/DatasetsView.vue';
import IngestView from '@/views/IngestView.vue';
import LoadBatchesView from '@/views/data/LoadBatchesView.vue';

defineProps({ embedded: Boolean });

const activeView = ref('datasets');
</script>

<template>
  <section class="data-management-view" :class="{ embedded }">
    <header class="data-management-header">
      <h1>数据管理与入库</h1>
    </header>

    <el-tabs v-model="activeView" class="data-management-tabs">
      <el-tab-pane name="datasets" label="数据集">
        <DatasetsView embedded title="数据集" />
      </el-tab-pane>
      <el-tab-pane name="load-batches" label="载入批次" lazy>
        <LoadBatchesView />
      </el-tab-pane>
      <el-tab-pane name="ingest-runs" label="入库运行" lazy>
        <IngestView embedded title="入库运行" />
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
.data-management-header {
  display: flex;
  align-items: center;
  min-height: 42px;
  margin-bottom: 8px;
}
.data-management-header h1 {
  margin: 0;
  color: #172033;
  font-size: 24px;
  font-weight: 650;
  letter-spacing: 0;
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
  .data-management-header h1 { font-size: 21px; }
  .data-management-tabs :deep(.el-tabs__item) { padding: 0 12px; font-size: 14px; }
}
</style>
