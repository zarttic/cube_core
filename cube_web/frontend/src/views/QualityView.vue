<script setup>
import { onMounted, ref } from 'vue';
import { Refresh } from '@element-plus/icons-vue';
import { ElMessage } from 'element-plus';

import { requestGet, requestPost } from '@/api/client';
import AppTable from '@/components/AppTable.vue';
import StatusTag from '@/components/StatusTag.vue';
import { formatShanghaiTime } from '@/utils/time';
import PartitionQualityDrawer from '@/views/quality/PartitionQualityDrawer.vue';

defineProps({ embedded: Boolean });

const batches = ref([]);
const loading = ref(false);
const error = ref('');
const selectedId = ref('');
const detail = ref(null);
const detailLoading = ref(false);
const detailVisible = ref(false);
const submitting = ref(false);

async function loadBatches() {
  loading.value = true;
  error.value = '';
  try {
    const response = await requestGet('/v1/partition/runs?limit=100');
    batches.value = response.items || [];
  } catch (requestError) {
    error.value = requestError.message || '剖分批次质检记录加载失败';
  } finally {
    loading.value = false;
  }
}

async function openBatch(row) {
  selectedId.value = row.partition_run_id;
  detailVisible.value = true;
  detail.value = null;
  detailLoading.value = true;
  try {
    detail.value = await requestGet(`/v1/partition/runs/${encodeURIComponent(selectedId.value)}/quality`);
  } catch (requestError) {
    error.value = requestError.message || '剖分批次详情加载失败';
  } finally {
    detailLoading.value = false;
  }
}

async function requestQuality() {
  if (!selectedId.value) return;
  submitting.value = true;
  try {
    const response = await requestPost(`/v1/partition/runs/${encodeURIComponent(selectedId.value)}/quality`, {});
    ElMessage.success(`已提交 ${response.quality_runs?.length || 0} 个数据集质量任务`);
    await openBatch({ partition_run_id: selectedId.value });
    await loadBatches();
  } catch (requestError) {
    ElMessage.error(requestError.message || '批次质检提交失败');
  } finally {
    submitting.value = false;
  }
}

async function retryFailedPartition() {
  if (!selectedId.value) return;
  submitting.value = true;
  try {
    await requestPost(`/v1/partition/runs/${encodeURIComponent(selectedId.value)}/retry-failed`, {});
    ElMessage.success('已在原剖分批次中提交失败数据重试');
    await openBatch({ partition_run_id: selectedId.value });
    await loadBatches();
  } catch (requestError) {
    ElMessage.error(requestError.message || '失败剖分重试提交失败');
  } finally {
    submitting.value = false;
  }
}

function closeDetail() {
  detailVisible.value = false;
  selectedId.value = '';
  detail.value = null;
}

onMounted(() => { loadBatches(); });
</script>

<template>
  <section class="quality-view">
    <header class="view-header">
      <el-button :icon="Refresh" :loading="loading" @click="loadBatches">刷新</el-button>
    </header>
    <el-alert v-if="error" :title="error" type="error" :closable="false" show-icon />
    <AppTable :data="batches" :loading="loading" :pagination="false" row-key="partition_run_id" @row-click="openBatch">
      <el-table-column label="剖分批次" min-width="240"><template #default="{ row }"><div class="batch-cell"><strong>{{ row.partition_run_id }}</strong><span :title="(row.source_load_batch_ids || []).join('、')">来源 {{ (row.source_load_batch_names || row.source_load_batch_ids || []).join('、') || '-' }}</span></div></template></el-table-column>
      <el-table-column label="数据范围" min-width="150"><template #default="{ row }">{{ row.dataset_count }} 个数据集 · {{ row.scene_count }} 景 · {{ row.band_count }} 波段</template></el-table-column>
      <el-table-column label="剖分" width="105"><template #default="{ row }">{{ row.partitioned_count }}/{{ row.band_count }}</template></el-table-column>
      <el-table-column label="质检" min-width="130"><template #default="{ row }"><span class="pass-count">{{ row.quality_pass_count }} 通过</span><span v-if="row.quality_failed_count" class="failed-count"> · {{ row.quality_failed_count }} 失败</span></template></el-table-column>
      <el-table-column label="入库" width="105"><template #default="{ row }">{{ row.ingested_count }}/{{ row.band_count }}</template></el-table-column>
      <el-table-column label="批次状态" width="115"><template #default="{ row }"><StatusTag domain="partition" :value="row.status" size="small" /></template></el-table-column>
      <el-table-column label="创建时间" min-width="170"><template #default="{ row }">{{ formatShanghaiTime(row.created_at) }}</template></el-table-column>
      <el-table-column label="操作" width="80" fixed="right"><template #default="{ row }"><el-button link type="primary" @click.stop="openBatch(row)">查看</el-button></template></el-table-column>
    </AppTable>
    <PartitionQualityDrawer :visible="detailVisible" :detail="detail" :loading="detailLoading" :submitting="submitting" @close="closeDetail" @request-quality="requestQuality" @retry-failed-partition="retryFailedPartition" />
  </section>
</template>

<style scoped>
.quality-view { padding: 24px; }
.view-header { display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; margin-bottom: 18px; }
.batch-cell { display: flex; flex-direction: column; min-width: 0; gap: 3px; }
.batch-cell strong, .batch-cell span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.batch-cell strong { color: #263247; }
.batch-cell span { color: #748095; font-size: 12px; }
.pass-count { color: #277a52; }.failed-count { color: #a53b32; }
@media (max-width: 760px) { .quality-view { padding: 16px; } }
</style>
