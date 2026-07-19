<script setup>
import { computed, onMounted, onUnmounted, reactive, ref } from 'vue';
import { ArrowDown, ArrowRight, Refresh, Search } from '@element-plus/icons-vue';

import { requestGet } from '@/api/client';
import { createRequestScope } from '@/api/requestScope';

const batches = ref([]);
const keyword = ref('');
const loading = ref(false);
const error = ref('');
const expanded = ref([]);
const details = reactive({});
const detailLoading = reactive({});
const listScope = createRequestScope();
const detailScopes = new Map();

const visibleBatches = computed(() => {
  const query = keyword.value.trim().toLowerCase();
  if (!query) return batches.value;
  return batches.value.filter((batch) => [batch.load_batch_id, batch.batch_name]
    .some((value) => String(value || '').toLowerCase().includes(query)));
});
const sceneCount = computed(() => batches.value.reduce((total, batch) => total + Number(batch.scene_count || 0), 0));
const datasetCount = computed(() => batches.value.reduce((total, batch) => total + Number(batch.dataset_count || 0), 0));

function isExpanded(batchId) {
  return expanded.value.includes(batchId);
}

function detailScope(batchId) {
  if (!detailScopes.has(batchId)) detailScopes.set(batchId, createRequestScope());
  return detailScopes.get(batchId);
}

async function loadBatches() {
  const request = listScope.begin();
  loading.value = true;
  error.value = '';
  try {
    const response = await requestGet('/v1/partition/load-batches?limit=100&status=succeeded', { signal: request.signal });
    if (listScope.isCurrent(request.token)) batches.value = Array.isArray(response?.load_batches) ? response.load_batches : [];
  } catch (caught) {
    if (!request.signal.aborted && listScope.isCurrent(request.token)) error.value = caught.message || '载入批次加载失败';
  } finally {
    if (listScope.isCurrent(request.token)) loading.value = false;
  }
}

async function toggleBatch(batch) {
  const batchId = batch.load_batch_id;
  if (isExpanded(batchId)) {
    expanded.value = expanded.value.filter((id) => id !== batchId);
    return;
  }
  expanded.value = [...expanded.value, batchId];
  if (details[batchId]) return;
  const scope = detailScope(batchId);
  const request = scope.begin();
  detailLoading[batchId] = true;
  try {
    const response = await requestGet(`/v1/partition/load-batches/${encodeURIComponent(batchId)}/scenes?limit=10000`, { signal: request.signal });
    if (scope.isCurrent(request.token)) details[batchId] = response;
  } catch (caught) {
    if (!request.signal.aborted && scope.isCurrent(request.token)) error.value = caught.message || '批次明细加载失败';
  } finally {
    if (scope.isCurrent(request.token)) detailLoading[batchId] = false;
  }
}

onMounted(loadBatches);
onUnmounted(() => {
  listScope.dispose();
  detailScopes.forEach((scope) => scope.dispose());
  detailScopes.clear();
});
</script>

<template>
  <section class="load-batches-view">
    <header class="section-toolbar">
      <div>
        <h2>载入批次</h2>
      </div>
      <div class="toolbar-actions">
        <el-input v-model="keyword" :prefix-icon="Search" clearable placeholder="搜索批次名称或 ID" />
        <el-button :icon="Refresh" :loading="loading" @click="loadBatches">刷新</el-button>
      </div>
    </header>

    <el-alert v-if="error" :title="error" type="error" :closable="false" show-icon />

    <div class="batch-summary" aria-label="载入批次统计">
      <div><span>载入批次</span><strong>{{ batches.length }}</strong></div>
      <div><span>批次数据集</span><strong>{{ datasetCount }}</strong></div>
      <div><span>已载入景</span><strong>{{ sceneCount }}</strong></div>
    </div>

    <div v-loading="loading" class="batch-browser">
      <div class="batch-browser-head" aria-hidden="true">
        <span>批次</span><span>数据集</span><span>景</span><span>状态</span><span></span>
      </div>
      <article v-for="batch in visibleBatches" :key="batch.load_batch_id" class="batch-row">
        <button
          type="button"
          class="batch-row-main"
          :data-testid="`load-batch-${batch.load_batch_id}`"
          :aria-expanded="isExpanded(batch.load_batch_id)"
          @click="toggleBatch(batch)"
        >
          <span class="batch-identity">
            <el-icon><ArrowDown v-if="isExpanded(batch.load_batch_id)" /><ArrowRight v-else /></el-icon>
            <span><strong>{{ batch.batch_name || batch.load_batch_id }}</strong><small>{{ batch.load_batch_id }}</small></span>
          </span>
          <strong class="numeric-cell">{{ batch.dataset_count || 0 }}</strong>
          <strong class="numeric-cell">{{ batch.scene_count || 0 }}</strong>
          <span class="loaded-status">已载入</span>
          <span class="open-label">{{ isExpanded(batch.load_batch_id) ? '收起' : '查看' }}</span>
        </button>

        <div v-if="isExpanded(batch.load_batch_id)" class="batch-detail">
          <div v-if="detailLoading[batch.load_batch_id]" class="detail-state">正在加载批次明细</div>
          <template v-else-if="details[batch.load_batch_id]?.datasets?.length">
            <section v-for="dataset in details[batch.load_batch_id].datasets" :key="dataset.dataset_id" class="batch-dataset">
              <header class="dataset-heading">
                <div>
                  <strong>{{ dataset.dataset_title || dataset.dataset_code || dataset.dataset_id }}</strong>
                  <span>{{ dataset.dataset_code || dataset.dataset_id }}</span>
                </div>
                <span>{{ dataset.scenes?.length || 0 }} 景</span>
              </header>
              <div class="scene-table">
                <div class="scene-table-head"><span>景</span><span>源数据</span><span>坐标系</span><span>状态</span></div>
                <div v-for="scene in dataset.scenes || []" :key="scene.scene_id" class="scene-row">
                  <span><strong>{{ scene.scene_key || scene.scene_code || scene.scene_id }}</strong><small>{{ scene.scene_id }}</small></span>
                  <span class="truncate">{{ scene.source_asset_id || scene.source_uri || '-' }}</span>
                  <span>{{ scene.crs || '-' }}</span>
                  <span class="loaded-status">已载入</span>
                </div>
              </div>
            </section>
          </template>
          <el-empty v-else description="该批次暂无可用景" :image-size="52" />
        </div>
      </article>
      <el-empty v-if="!loading && !visibleBatches.length" description="暂无已载入批次" />
    </div>
  </section>
</template>

<style scoped>
.load-batches-view { min-width: 0; }
.section-toolbar { display: flex; align-items: flex-end; justify-content: space-between; gap: 20px; margin-bottom: 16px; }
.section-toolbar h2 { margin: 0 0 4px; color: #172033; font-size: 18px; letter-spacing: 0; }
.section-toolbar span { color: #748095; font-size: 13px; }
.toolbar-actions { display: flex; align-items: center; gap: 10px; }
.toolbar-actions .el-input { width: 280px; }
.batch-summary { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); border: 1px solid #dfe4ec; border-radius: 6px; margin: 14px 0 18px; background: #fff; }
.batch-summary div { display: flex; align-items: center; justify-content: space-between; min-height: 64px; padding: 0 20px; border-right: 1px solid #e7ebf1; }
.batch-summary div:last-child { border-right: 0; }
.batch-summary span { color: #667085; font-size: 13px; }
.batch-summary strong { color: #1769aa; font-size: 24px; font-weight: 650; }
.batch-browser { min-height: 180px; border: 1px solid #dfe4ec; border-radius: 6px; overflow: hidden; background: #fff; }
.batch-browser-head, .batch-row-main { display: grid; grid-template-columns: minmax(280px, 1fr) 100px 80px 110px 64px; align-items: center; }
.batch-browser-head { min-height: 42px; padding: 0 18px; background: #f5f7fa; color: #657086; font-size: 13px; font-weight: 600; }
.batch-row { border-top: 1px solid #e6eaf0; }
.batch-row:first-of-type { border-top: 0; }
.batch-row-main { width: 100%; min-height: 68px; padding: 8px 18px; border: 0; background: #fff; color: #263247; text-align: left; cursor: pointer; }
.batch-row-main:hover { background: #f7faff; }
.batch-identity { display: flex; align-items: center; gap: 10px; min-width: 0; }
.batch-identity > span { display: flex; flex-direction: column; min-width: 0; }
.batch-identity strong, .scene-row strong { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.batch-identity small, .scene-row small { margin-top: 3px; color: #8993a4; font-size: 12px; }
.numeric-cell { color: #364155; font-size: 15px; }
.loaded-status { justify-self: start; padding: 3px 9px; border: 1px solid #a9d7c2; border-radius: 4px; background: #eff9f4; color: #187452; font-size: 12px; white-space: nowrap; }
.open-label { color: #1769aa; font-size: 13px; text-align: right; }
.batch-detail { padding: 4px 18px 18px 48px; background: #fafbfd; }
.detail-state { padding: 26px; color: #748095; text-align: center; }
.batch-dataset { margin-top: 12px; border-left: 3px solid #5797c7; background: #fff; }
.dataset-heading { display: flex; align-items: center; justify-content: space-between; min-height: 56px; padding: 8px 16px; border: 1px solid #e1e6ed; border-left: 0; }
.dataset-heading div { display: flex; flex-direction: column; gap: 3px; }
.dataset-heading strong { color: #263247; font-size: 14px; }
.dataset-heading span { color: #748095; font-size: 12px; }
.scene-table { border: 1px solid #e1e6ed; border-top: 0; border-left: 0; }
.scene-table-head, .scene-row { display: grid; grid-template-columns: minmax(220px, 1.2fr) minmax(180px, 1fr) 130px 90px; align-items: center; gap: 12px; padding: 0 16px; }
.scene-table-head { min-height: 36px; background: #f7f8fa; color: #7a8496; font-size: 12px; }
.scene-row { min-height: 54px; border-top: 1px solid #edf0f4; color: #475369; font-size: 13px; }
.scene-row > span:first-child { display: flex; flex-direction: column; min-width: 0; }
.truncate { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
@media (max-width: 900px) {
  .section-toolbar { align-items: stretch; flex-direction: column; }
  .toolbar-actions .el-input { width: 100%; }
  .batch-browser { overflow-x: auto; }
  .batch-browser-head, .batch-row-main { min-width: 720px; }
  .batch-detail { min-width: 720px; padding-left: 30px; }
}
@media (max-width: 560px) {
  .toolbar-actions { align-items: stretch; flex-direction: column; }
  .batch-summary { grid-template-columns: 1fr; }
  .batch-summary div { border-right: 0; border-bottom: 1px solid #e7ebf1; }
  .batch-summary div:last-child { border-bottom: 0; }
}
</style>
