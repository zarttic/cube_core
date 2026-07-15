<script setup>
import { computed } from 'vue';

import AppTable from '@/components/AppTable.vue';
import DetailDrawer from '@/components/DetailDrawer.vue';
import StatusTag from '@/components/StatusTag.vue';

const props = defineProps({
  testId: { type: String, default: '' },
  visible: Boolean,
  datasetId: { type: String, default: '' },
  detail: { type: Object, default: () => ({}) },
  loading: Boolean,
  activeTab: { type: String, default: 'overview' },
  tabPages: { type: Object, default: () => ({}) },
});
const emit = defineEmits(['close', 'tab-change', 'tab-page-change', 'tab-page-size-change']);

const tabs = [
  ['overview', '概览'], ['assets', '资产'], ['bands', '波段'], ['tiles', '瓦片'], ['indexes', '索引'], ['grid', '格网'], ['quality', '质检'], ['publications', '发布'],
];
const title = computed(() => props.detail?.overview?.dataset_code || props.datasetId || '数据集详情');

function collection(tab) {
  return props.detail?.[tab]?.items || [];
}

function page(tab) {
  return props.tabPages?.[tab] || { page: 1, pageSize: 20, total: 0 };
}
</script>

<template>
  <DetailDrawer :visible="visible" :test-id="testId" :title="title" :loading="loading" @update:visible="(value) => !value && emit('close')" @closed="emit('close')">
    <div class="drawer-close-row"><el-button data-testid="dataset-detail-close" link type="primary" @click="emit('close')">关闭</el-button></div>
    <el-tabs :model-value="activeTab" @tab-change="emit('tab-change', $event)">
      <el-tab-pane v-for="[key, label] in tabs" :key="key" :name="key">
        <template #label><span :data-testid="`dataset-detail-tab-${key}`">{{ label }}</span></template>
        <template v-if="key === 'overview'">
          <el-descriptions v-if="detail?.overview" :column="1" border>
            <el-descriptions-item label="数据集 ID">{{ detail.overview.dataset_id }}</el-descriptions-item>
            <el-descriptions-item label="名称">{{ detail.overview.dataset_title || '-' }}</el-descriptions-item>
            <el-descriptions-item label="批次">{{ detail.overview.batch_id }}</el-descriptions-item>
            <el-descriptions-item label="输出版本">{{ detail.overview.current_output_version || '-' }}</el-descriptions-item>
            <el-descriptions-item label="格网">{{ detail.overview.grid_type || '-' }}</el-descriptions-item>
            <el-descriptions-item label="请求层级">{{ detail.overview.requested_grid_level ?? '-' }}</el-descriptions-item>
            <el-descriptions-item label="格网层级显示名">{{ detail.overview.grid_level_display_name || '-' }}</el-descriptions-item>
            <el-descriptions-item label="剖分状态"><StatusTag domain="partition" :value="detail.overview.partition_status" size="small" /></el-descriptions-item>
            <el-descriptions-item label="质检状态"><StatusTag domain="quality" :value="detail.overview.quality_status" size="small" /></el-descriptions-item>
            <el-descriptions-item label="发布状态"><StatusTag domain="publication" :value="detail.overview.publish_status" size="small" /></el-descriptions-item>
          </el-descriptions>
          <el-empty v-else description="正在加载数据集概览" />
        </template>
        <template v-else>
          <AppTable
            :data="collection(key)"
            :page="page(key).page"
            :page-size="page(key).pageSize"
            :total="page(key).total"
            row-key="output_id"
            @current-change="(value) => emit('tab-page-change', { tab: key, page: value })"
            @size-change="(value) => emit('tab-page-size-change', { tab: key, pageSize: value })"
          >
            <el-table-column label="标识" min-width="180"><template #default="{ row }">{{ row.output_id || row.source_asset_id || row.band_code || row.publication_id || row.quality_run_id || '-' }}</template></el-table-column>
            <el-table-column label="状态" min-width="110"><template #default="{ row }"><StatusTag v-if="row.status" :domain="key === 'publications' ? 'publication' : 'quality'" :value="row.status" size="small" /><span v-else>-</span></template></el-table-column>
            <el-table-column label="格网编码" prop="space_code" min-width="150" show-overflow-tooltip />
            <el-table-column label="创建时间" prop="created_at" min-width="170" show-overflow-tooltip />
          </AppTable>
        </template>
      </el-tab-pane>
    </el-tabs>
  </DetailDrawer>
</template>

<style scoped>
.drawer-close-row { display: flex; justify-content: flex-end; margin-bottom: 8px; }
</style>
