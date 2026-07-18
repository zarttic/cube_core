<script setup>
import { computed, reactive, ref, watch } from 'vue';

import AppTable from '@/components/AppTable.vue';
import DetailDrawer from '@/components/DetailDrawer.vue';
import StatusTag from '@/components/StatusTag.vue';
import { bandDisplayLabel, dataUnitTypeLabel } from '@/utils/bands';
import { formatShanghaiRange, formatShanghaiTime } from '@/utils/time';

const props = defineProps({
  testId: { type: String, default: '' }, visible: Boolean, datasetId: { type: String, default: '' },
  detail: { type: Object, default: () => ({}) }, loading: Boolean, actionLoading: Boolean,
  writeEnabled: { type: Boolean, default: true },
  activeTab: { type: String, default: 'overview' }, tabPages: { type: Object, default: () => ({}) },
});
const emit = defineEmits([
  'close', 'tab-change', 'tab-page-change', 'tab-page-size-change', 'update-metadata',
  'reassign-scene', 'rerun-quality', 'retry-scene-ingest', 'archive',
]);

const tabs = [
  ['overview', '概览'], ['scenes', '数据'],
  ['outputs', '剖分版本'], ['tiles', '瓦片'],
  ['ingest-records', '入库记录'], ['quality', '质检'], ['provenance', '来源追踪'],
];
const title = computed(() => props.detail?.overview?.dataset_code || props.datasetId || '数据集详情');
const canRequestQuality = computed(() => Boolean(props.detail?.overview?.current_output_version));
function bandWorkflowSteps(scene, band) {
  const overview = props.detail?.overview || {};
  const sceneStatus = String(scene?.status || '').toLowerCase();
  const partitionDone = ['partitioned', 'quality_pending', 'quality_passed', 'quality_failed', 'ingesting', 'available'].includes(sceneStatus);
  const qualityDone = partitionDone && (sceneStatus === 'quality_passed' || (sceneStatus !== 'quality_failed' && overview.quality_status === 'pass'));
  const qualityFailed = partitionDone && (sceneStatus === 'quality_failed' || overview.quality_status === 'fail');
  // Scene `available` may belong to an older output version; only the
  // current-output dataset status authorizes publication.
  const ingestDone = qualityDone && overview.ingest_status === 'completed';
  const ingestFailed = qualityDone && (['failed'].includes(sceneStatus) || ['failed', 'partial_failure'].includes(overview.ingest_status));
  return [
    { key: 'partition', label: '剖分', status: partitionDone ? '已完成' : '等待中', state: partitionDone ? 'done' : 'pending' },
    { key: 'quality', label: '质检', status: qualityDone ? '已通过' : qualityFailed ? '未通过' : '等待中', state: qualityDone ? 'done' : qualityFailed ? 'error' : 'pending' },
    { key: 'ingest', label: '入库', status: ingestDone ? '已完成' : ingestFailed ? '未完成' : '等待中', state: ingestDone ? 'done' : ingestFailed ? 'error' : 'pending' },
  ];
}
const editing = ref(false);
const metadataForm = reactive({ dataset_title: '', description: '', keywords: '' });
const reassignDialog = ref(false);
const reassignForm = reactive({ scene_id: '', target_dataset_id: '', reason: '' });
const archiveDialog = ref(false);
const archiveReason = ref('');
const collapsedScenes = ref(new Set());

function resetLocalState() {
  editing.value = false;
  reassignDialog.value = false;
  archiveDialog.value = false;
  Object.assign(reassignForm, { scene_id: '', target_dataset_id: '', reason: '' });
  archiveReason.value = '';
  collapsedScenes.value = new Set((props.detail?.scenes?.items || []).map((scene) => String(scene.scene_id)));
  const overview = props.detail?.overview || {};
  Object.assign(metadataForm, {
    dataset_title: overview.dataset_title || '',
    description: overview.description || '',
    keywords: Array.isArray(overview.keywords) ? overview.keywords.join(', ') : overview.keywords || '',
  });
}

watch(() => [props.datasetId, props.detail?.overview, props.detail?.scenes], resetLocalState, { immediate: true });

function collection(tab) { return props.detail?.[tab]?.items || []; }
function page(tab) { return props.tabPages?.[tab] || { page: 1, pageSize: 20, total: 0 }; }
function rowId(row) {
  return row.scene_id || row.source_asset_id || row.band_code || row.output_version || row.output_id
    || row.index_id || row.ingest_run_id || row.quality_run_id || row.publication_id || row.load_batch_id || '-';
}

function sourceOrError(row) {
  if (row?.error_message) return row.error_message;
  if (row?.source_uri) return row.source_uri;
  if (row?.tile_uri) return row.tile_uri;
  if (row?.value_ref_uri) return row.value_ref_uri;
  if (row?.source_kind) return row.source_kind;
  if (row?.details) {
    const details = typeof row.details === 'string' ? row.details : JSON.stringify(row.details);
    return details === '{}' ? '-' : details;
  }
  return row?.source_load_batch_id || row?.load_batch_id || row?.provenance || '-';
}

function publicationTargets(row) {
  const targets = Array.isArray(row?.targets) ? row.targets : [];
  if (!targets.length) return '全部数据单元';
  const labels = targets.map((target) => `${target.source_asset_id || '-'} / ${target.band_code || '-'}`);
  return `${targets.length} 个波段：${labels.slice(0, 3).join('、')}${labels.length > 3 ? ` 等 ${labels.length} 个` : ''}`;
}

function saveMetadata() {
  emit('update-metadata', {
    ...metadataForm,
    keywords: metadataForm.keywords.split(',').map((item) => item.trim()).filter(Boolean),
  });
  editing.value = false;
}

function openReassign(row) {
  Object.assign(reassignForm, { scene_id: row.scene_id, target_dataset_id: '', reason: '' });
  reassignDialog.value = true;
}

function confirmReassign() {
  if (!reassignForm.scene_id || !reassignForm.target_dataset_id || !reassignForm.reason.trim()) return;
  emit('reassign-scene', { ...reassignForm });
  reassignDialog.value = false;
}

function confirmArchive() {
  if (!archiveReason.value.trim()) return;
  emit('archive', archiveReason.value.trim());
  archiveDialog.value = false;
}

function toggleScene(sceneId) {
  const next = new Set(collapsedScenes.value);
  const key = String(sceneId);
  if (next.has(key)) next.delete(key); else next.add(key);
  collapsedScenes.value = next;
}

function sceneCollapsed(sceneId) {
  return collapsedScenes.value.has(String(sceneId));
}

const workflowSummary = computed(() => {
  const bands = collection('scenes').flatMap((scene) => (scene.bands || []).map((band) => bandWorkflowSteps(scene, band)));
  const total = bands.length;
  const count = (key, state = 'done') => bands.filter((steps) => steps.find((step) => step.key === key)?.state === state).length;
  return {
    total,
    partition: count('partition'), quality: count('quality'), ingest: count('ingest'),
  };
});

function sceneWorkflowSummary(scene) {
  const workflows = (scene.bands || []).map((band) => bandWorkflowSteps(scene, band));
  const total = workflows.length;
  const count = (key) => workflows.filter((steps) => steps.find((step) => step.key === key)?.state === 'done').length;
  return { total, partition: count('partition'), quality: count('quality'), ingest: count('ingest') };
}
</script>

<template>
  <DetailDrawer :visible="visible" :test-id="testId" :title="title" :loading="loading" size="860px" @update:visible="(value) => !value && emit('close')" @closed="emit('close')">
    <div class="drawer-actions">
      <el-button v-if="writeEnabled" :disabled="!canRequestQuality" :loading="actionLoading" @click="emit('rerun-quality')">重新质检</el-button>
      <el-button v-if="writeEnabled" :loading="actionLoading" type="danger" plain @click="archiveDialog = true">归档</el-button>
      <el-button data-testid="dataset-detail-close" link type="primary" @click="emit('close')">关闭</el-button>
    </div>
    <el-tabs :model-value="activeTab" @tab-change="emit('tab-change', $event)">
      <el-tab-pane v-for="[key, label] in tabs" :key="key" :name="key">
        <template #label><span :data-testid="`dataset-detail-tab-${key}`">{{ label }}</span></template>
        <template v-if="key === 'overview'">
          <template v-if="detail?.overview">
            <div v-if="writeEnabled" class="section-actions"><el-button link type="primary" @click="editing = !editing">{{ editing ? '取消编辑' : '编辑元数据' }}</el-button></div>
            <el-form v-if="editing" label-width="92px" class="metadata-form">
              <el-form-item label="名称"><el-input v-model="metadataForm.dataset_title" /></el-form-item>
              <el-form-item label="描述"><el-input v-model="metadataForm.description" type="textarea" :rows="3" /></el-form-item>
              <el-form-item label="关键词"><el-input v-model="metadataForm.keywords" placeholder="多个关键词使用逗号分隔" /></el-form-item>
              <el-form-item><el-button type="primary" :loading="actionLoading" @click="saveMetadata">保存</el-button></el-form-item>
            </el-form>
            <el-descriptions v-else :column="2" border>
              <el-descriptions-item label="数据集 ID">{{ detail.overview.dataset_id }}</el-descriptions-item>
              <el-descriptions-item label="名称">{{ detail.overview.dataset_title || '-' }}</el-descriptions-item>
              <el-descriptions-item label="景数量">{{ detail.overview.scene_count ?? 0 }}</el-descriptions-item>
              <el-descriptions-item label="时间范围">{{ formatShanghaiRange(detail.overview.time_start, detail.overview.time_end) }}</el-descriptions-item>
              <el-descriptions-item label="空间范围" :span="2">{{ detail.overview.bbox || '-' }}</el-descriptions-item>
              <el-descriptions-item label="当前版本">{{ detail.overview.current_output_version || '-' }}</el-descriptions-item>
              <el-descriptions-item label="入库状态"><StatusTag domain="ingest" :value="detail.overview.ingest_status" size="small" /></el-descriptions-item>
              <el-descriptions-item label="质检状态"><StatusTag domain="quality" :value="detail.overview.quality_status" size="small" /></el-descriptions-item>
              <el-descriptions-item label="描述" :span="2">{{ detail.overview.description || '-' }}</el-descriptions-item>
            </el-descriptions>
          </template>
          <el-empty v-else description="正在加载数据集概览" />
        </template>
        <template v-else-if="key === 'scenes'">
          <div class="dataset-workflow-summary" data-testid="dataset-workflow-summary">
            <span>剖分 <b>{{ workflowSummary.partition }}/{{ workflowSummary.total }}</b></span>
            <span>质检 <b>{{ workflowSummary.quality }}/{{ workflowSummary.total }}</b></span>
            <span>入库 <b>{{ workflowSummary.ingest }}/{{ workflowSummary.total }}</b></span>
          </div>
          <div class="dataset-scene-hierarchy" data-testid="dataset-scene-hierarchy">
            <section v-for="scene in collection('scenes')" :key="scene.scene_id" class="dataset-scene-group" :data-testid="`managed-scene-${scene.scene_id}`">
              <header class="scene-toggle" :aria-expanded="!sceneCollapsed(scene.scene_id)" @click="toggleScene(scene.scene_id)">
                <div><span class="data-level-label">数据</span><strong>{{ scene.scene_key || scene.scene_id }}</strong><span>{{ scene.scene_id }} · {{ formatShanghaiTime(scene.acquisition_time) }}</span></div>
                <div class="scene-header-meta"><StatusTag domain="scene" :value="scene.status" size="small" /><span>{{ scene.bands?.length || 0 }} 个数据单元</span><div class="scene-workflow-summary"><span>剖分 {{ sceneWorkflowSummary(scene).partition }}/{{ sceneWorkflowSummary(scene).total }}</span><span>质检 {{ sceneWorkflowSummary(scene).quality }}/{{ sceneWorkflowSummary(scene).total }}</span><span>入库 {{ sceneWorkflowSummary(scene).ingest }}/{{ sceneWorkflowSummary(scene).total }}</span></div><el-button v-if="writeEnabled" link type="primary" @click.stop="openReassign(scene)">修正归属</el-button><span class="scene-toggle-mark">{{ sceneCollapsed(scene.scene_id) ? '展开' : '收起' }}</span></div>
              </header>
              <div v-if="!sceneCollapsed(scene.scene_id) && scene.bands?.length" class="managed-band-list">
                <div v-for="band in scene.bands" :key="band.band_unit_id || `${band.asset_id}-${band.band_code}`" class="managed-band-row" :data-testid="band.band_unit_id ? `managed-band-${band.band_unit_id}` : undefined">
                  <div class="managed-band-identity"><strong>{{ bandDisplayLabel(band) }}</strong><span>{{ band.band_unit_id || '-' }}</span></div>
                  <span>{{ dataUnitTypeLabel(detail?.overview?.data_type) }}</span>
                  <span>{{ band.band_type || '-' }}</span>
                  <span>{{ band.asset_id || '-' }}</span>
                  <div class="band-workflow" :aria-label="`${bandDisplayLabel(band)}处理流程`">
                    <span v-for="(step, index) in bandWorkflowSteps(scene, band)" :key="step.key" class="band-workflow-step" :class="step.state" :title="`${index + 1}. ${step.label}: ${step.status}`"><b>{{ index + 1 }}</b>{{ step.label }}<em>{{ step.status }}</em></span>
                  </div>
                </div>
              </div>
              <div v-else-if="!sceneCollapsed(scene.scene_id)" class="managed-band-empty">该数据未登记可用波段</div>
            </section>
            <el-empty v-if="!collection('scenes').length" description="暂无景数据" />
          </div>
          <div v-if="page('scenes').total > page('scenes').pageSize" class="hierarchy-pagination">
            <el-pagination
              :current-page="page('scenes').page"
              :page-size="page('scenes').pageSize"
              :total="page('scenes').total"
              layout="total, prev, pager, next"
              @current-change="(value) => emit('tab-page-change', { tab: 'scenes', page: value })"
            />
          </div>
        </template>
        <template v-else>
          <AppTable :data="collection(key)" :page="page(key).page" :page-size="page(key).pageSize" :total="page(key).total" row-key="scene_id" @current-change="(value) => emit('tab-page-change', { tab: key, page: value })" @size-change="(value) => emit('tab-page-size-change', { tab: key, pageSize: value })">
            <el-table-column label="标识" min-width="185"><template #default="{ row }">{{ rowId(row) }}</template></el-table-column>
            <el-table-column v-if="key === 'scenes'" prop="scene_code" label="景编码" min-width="155" show-overflow-tooltip />
            <el-table-column v-if="key === 'scenes'" label="采集时间" min-width="170"><template #default="{ row }">{{ formatShanghaiTime(row.acquisition_time) }}</template></el-table-column>
            <el-table-column v-if="key === 'provenance'" prop="relation_type" label="关系" width="130" />
            <el-table-column label="状态" min-width="110"><template #default="{ row }"><StatusTag v-if="row.status" :domain="key === 'quality' ? 'quality' : key === 'scenes' ? 'scene' : 'ingest'" :value="row.status" size="small" /><span v-else>-</span></template></el-table-column>
            <el-table-column label="来源或错误" min-width="200" show-overflow-tooltip><template #default="{ row }">{{ sourceOrError(row) }}</template></el-table-column>
            <el-table-column label="创建时间" min-width="170"><template #default="{ row }">{{ formatShanghaiTime(row.created_at) }}</template></el-table-column>
            <el-table-column v-if="writeEnabled && ['scenes', 'ingest-records'].includes(key)" label="操作" width="130" fixed="right">
              <template #default="{ row }">
                <el-button v-if="key === 'scenes'" link type="primary" @click="openReassign(row)">修正归属</el-button>
                <el-button v-else-if="writeEnabled && key === 'ingest-records' && row.status === 'failed'" link type="primary" @click="emit('retry-scene-ingest', row.scene_id)">重试</el-button>
              </template>
            </el-table-column>
          </AppTable>
        </template>
      </el-tab-pane>
    </el-tabs>

    <el-dialog v-model="reassignDialog" title="修正景归属" width="480px" append-to-body>
      <el-form label-width="100px">
        <el-form-item label="景">{{ reassignForm.scene_id }}</el-form-item>
        <el-form-item label="目标数据集"><el-input v-model="reassignForm.target_dataset_id" /></el-form-item>
        <el-form-item label="修正原因"><el-input v-model="reassignForm.reason" type="textarea" :rows="3" /></el-form-item>
      </el-form>
      <template #footer><el-button @click="reassignDialog = false">取消</el-button><el-button type="primary" :disabled="!reassignForm.target_dataset_id || !reassignForm.reason.trim()" @click="confirmReassign">确认修正</el-button></template>
    </el-dialog>
    <el-dialog v-model="archiveDialog" title="归档数据集" width="480px" append-to-body>
      <el-input v-model="archiveReason" type="textarea" :rows="3" placeholder="请输入归档原因" />
      <template #footer><el-button @click="archiveDialog = false">取消</el-button><el-button type="danger" :disabled="!archiveReason.trim()" @click="confirmArchive">确认归档</el-button></template>
    </el-dialog>
  </DetailDrawer>
</template>

<style scoped>
.drawer-actions, .section-actions { display: flex; justify-content: flex-end; gap: 8px; margin-bottom: 10px; }
.metadata-form { max-width: 680px; padding-top: 8px; }
.dataset-workflow-summary { display: grid; grid-template-columns: repeat(4, minmax(120px, 1fr)); gap: 8px; margin-bottom: 10px; }
.dataset-workflow-summary span { padding: 9px 11px; border: 1px solid var(--el-border-color-lighter); border-radius: 4px; color: var(--el-text-color-secondary); font-size: 12px; }
.dataset-workflow-summary b { margin-left: 5px; color: var(--el-text-color-primary); font-size: 14px; }
.dataset-scene-hierarchy { display: flex; flex-direction: column; }
.dataset-scene-group { padding: 12px 0 14px; border-bottom: 1px solid var(--el-border-color-lighter); }
.scene-toggle { cursor: pointer; }
.scene-toggle:hover { background: #f7fafc; }
.scene-toggle-mark { color: var(--el-color-primary) !important; font-size: 11px !important; }
.dataset-scene-group > header { display: flex; align-items: center; justify-content: space-between; gap: 16px; margin-bottom: 9px; }
.dataset-scene-group > header > div:first-child { display: flex; min-width: 0; flex-direction: column; gap: 2px; }
.data-level-label { color: var(--el-color-primary); font-size: 10px; font-weight: 600; letter-spacing: .04em; }
.dataset-scene-group > header strong { color: var(--el-text-color-primary); font-size: 13px; overflow-wrap: anywhere; }
.dataset-scene-group > header span { color: var(--el-text-color-secondary); font-size: 11px; overflow-wrap: anywhere; }
.scene-header-meta { display: flex; flex: none; align-items: center; gap: 10px; }
.scene-workflow-summary { display: grid; grid-template-columns: repeat(2, minmax(72px, 1fr)); gap: 4px; }
.scene-workflow-summary span { padding: 3px 6px; border: 1px solid var(--el-border-color-lighter); border-radius: 3px; background: #f8fafc; color: var(--el-text-color-regular); font-size: 10px; white-space: nowrap; }
.managed-band-list { display: flex; flex-direction: column; margin-left: 20px; border-left: 2px solid #c7d9e9; }
.managed-band-row { display: grid; grid-template-columns: minmax(200px, 1.35fr) minmax(72px, .5fr) minmax(72px, .5fr) minmax(130px, .8fr) minmax(230px, 1.25fr); gap: 9px; align-items: center; min-height: 48px; padding: 7px 10px; border-bottom: 1px solid var(--el-border-color-extra-light); }
.managed-band-row > div { display: flex; min-width: 0; flex-direction: column; gap: 2px; }
.managed-band-row strong { color: #2d628d; font-size: 12px; overflow-wrap: anywhere; }
.managed-band-row span { color: var(--el-text-color-secondary); font-size: 11px; overflow-wrap: anywhere; }
.managed-band-row > .band-workflow { display: grid; grid-template-columns: repeat(2, minmax(82px, 1fr)); gap: 4px; min-width: 0; }
.band-workflow-step { display: grid; grid-template-columns: 15px 1fr; align-items: center; column-gap: 3px; padding: 3px 4px; border: 1px solid var(--el-border-color-lighter); border-radius: 4px; color: var(--el-text-color-secondary); font-size: 10px; line-height: 1.15; }
.band-workflow-step b { grid-row: span 2; display: grid; place-items: center; width: 16px; height: 16px; border-radius: 50%; background: #eef2f6; color: #697586; font-size: 9px; }
.band-workflow-step em { overflow: hidden; color: inherit; font-size: 9px; font-style: normal; text-overflow: ellipsis; white-space: nowrap; }
.band-workflow-step.done { border-color: #b7dec9; background: #f1fbf5; color: #237848; }
.band-workflow-step.done b { background: #3eae70; color: white; }
.band-workflow-step.error { border-color: #f1c0c0; background: #fff5f5; color: #c0392b; }
.band-workflow-step.error b { background: #d9534f; color: white; }
.managed-band-empty { margin-left: 20px; padding: 10px; border-left: 2px solid var(--el-border-color); color: var(--el-text-color-secondary); font-size: 12px; }
.hierarchy-pagination { display: flex; justify-content: flex-end; padding-top: 14px; }
.dataset-workflow { display: grid; grid-template-columns: repeat(4, 1fr); margin: 2px 0 18px; border-bottom: 1px solid #e1e6ed; }
.workflow-step { position: relative; display: flex; align-items: center; gap: 9px; min-width: 0; padding: 10px 8px 16px; color: #7b8495; }
.workflow-step:not(:last-child)::after { position: absolute; top: 24px; right: -12%; width: 24%; height: 2px; background: #d9dee7; content: ''; }
.workflow-marker { display: grid; flex: 0 0 28px; height: 28px; place-items: center; border: 1px solid #cdd4df; border-radius: 50%; background: #fff; font-size: 12px; font-weight: 650; }
.workflow-step > span:last-child { display: flex; flex-direction: column; min-width: 0; }
.workflow-step strong { color: #495468; font-size: 13px; }
.workflow-step small { margin-top: 2px; font-size: 12px; }
.workflow-step.done .workflow-marker { border-color: #38a47a; background: #38a47a; color: #fff; }
.workflow-step.done strong { color: #17694d; }
.workflow-step.error .workflow-marker { border-color: #d95050; background: #fff2f2; color: #b83232; }
.workflow-step.error strong { color: #a93434; }
@media (max-width: 640px) {
  .dataset-workflow-summary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .dataset-scene-group > header { align-items: flex-start; flex-direction: column; }
  .scene-header-meta { flex-wrap: wrap; }
  .managed-band-list, .managed-band-empty { margin-left: 8px; }
  .managed-band-row { grid-template-columns: 1fr 1fr; }
  .band-workflow { grid-column: 1 / -1; }
  .dataset-workflow { grid-template-columns: repeat(2, 1fr); }
  .workflow-step::after { display: none; }
}
</style>
