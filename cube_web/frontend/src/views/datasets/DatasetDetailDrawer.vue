<script setup>
import { computed, reactive, ref, watch } from 'vue';

import AppTable from '@/components/AppTable.vue';
import DetailDrawer from '@/components/DetailDrawer.vue';
import StatusTag from '@/components/StatusTag.vue';

const props = defineProps({
  testId: { type: String, default: '' }, visible: Boolean, datasetId: { type: String, default: '' },
  detail: { type: Object, default: () => ({}) }, loading: Boolean, actionLoading: Boolean,
  writeEnabled: { type: Boolean, default: true },
  activeTab: { type: String, default: 'overview' }, tabPages: { type: Object, default: () => ({}) },
});
const emit = defineEmits([
  'close', 'tab-change', 'tab-page-change', 'tab-page-size-change', 'update-metadata',
  'reassign-scene', 'rerun-quality', 'retry-scene-ingest', 'publish', 'withdraw', 'archive',
]);

const tabs = [
  ['overview', '概览'], ['scenes', '景'], ['bands', '波段'],
  ['outputs', '剖分版本'], ['grid', '格网'], ['tiles', '瓦片'], ['indexes', '索引'],
  ['ingest-records', '入库记录'], ['quality', '质检'], ['publications', '发布'], ['provenance', '来源追踪'],
];
const title = computed(() => props.detail?.overview?.dataset_code || props.datasetId || '数据集详情');
const canRequestQuality = computed(() => Boolean(props.detail?.overview?.current_output_version));
const canPublish = computed(() => (
  props.detail?.overview?.quality_status === 'pass'
  && props.detail?.overview?.ingest_status === 'completed'
));
const workflowSteps = computed(() => {
  const overview = props.detail?.overview || {};
  return [
    { key: 'partition', label: '剖分', status: overview.current_output_version ? '已完成' : '等待中', state: overview.current_output_version ? 'done' : 'pending' },
    { key: 'quality', label: '质检', status: overview.quality_status === 'pass' ? '已通过' : overview.quality_status === 'fail' ? '未通过' : '等待中', state: overview.quality_status === 'pass' ? 'done' : ['fail', 'error'].includes(overview.quality_status) ? 'error' : 'pending' },
    { key: 'ingest', label: '入库', status: overview.ingest_status === 'completed' ? '已完成' : ['failed', 'partial_failure'].includes(overview.ingest_status) ? '未完成' : '等待中', state: overview.ingest_status === 'completed' ? 'done' : ['failed', 'partial_failure'].includes(overview.ingest_status) ? 'error' : 'pending' },
    { key: 'publish', label: '发布', status: overview.publish_status === 'active' ? '已发布' : '未发布', state: overview.publish_status === 'active' ? 'done' : 'pending' },
  ];
});
const editing = ref(false);
const metadataForm = reactive({ dataset_title: '', description: '', keywords: '' });
const reassignDialog = ref(false);
const reassignForm = reactive({ scene_id: '', target_dataset_id: '', reason: '' });
const archiveDialog = ref(false);
const archiveReason = ref('');

function resetLocalState() {
  editing.value = false;
  reassignDialog.value = false;
  archiveDialog.value = false;
  Object.assign(reassignForm, { scene_id: '', target_dataset_id: '', reason: '' });
  archiveReason.value = '';
  const overview = props.detail?.overview || {};
  Object.assign(metadataForm, {
    dataset_title: overview.dataset_title || '',
    description: overview.description || '',
    keywords: Array.isArray(overview.keywords) ? overview.keywords.join(', ') : overview.keywords || '',
  });
}

watch(() => [props.datasetId, props.detail?.overview], resetLocalState, { immediate: true });

function collection(tab) { return props.detail?.[tab]?.items || []; }
function page(tab) { return props.tabPages?.[tab] || { page: 1, pageSize: 20, total: 0 }; }
function rowId(row) {
  return row.scene_id || row.source_asset_id || row.band_code || row.output_version || row.output_id
    || row.index_id || row.ingest_run_id || row.quality_run_id || row.publication_id || row.load_batch_id || '-';
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
</script>

<template>
  <DetailDrawer :visible="visible" :test-id="testId" :title="title" :loading="loading" size="860px" @update:visible="(value) => !value && emit('close')" @closed="emit('close')">
    <div class="drawer-actions">
      <el-button v-if="writeEnabled" :disabled="!canRequestQuality" :loading="actionLoading" @click="emit('rerun-quality')">重新质检</el-button>
      <el-tooltip v-if="writeEnabled" content="质检通过且入库完成后可发布" placement="bottom">
        <span><el-button :disabled="!canPublish" :loading="actionLoading" @click="emit('publish')">发布</el-button></span>
      </el-tooltip>
      <el-button v-if="writeEnabled" :loading="actionLoading" type="danger" plain @click="archiveDialog = true">归档</el-button>
      <el-button data-testid="dataset-detail-close" link type="primary" @click="emit('close')">关闭</el-button>
    </div>
    <el-tabs :model-value="activeTab" @tab-change="emit('tab-change', $event)">
      <el-tab-pane v-for="[key, label] in tabs" :key="key" :name="key">
        <template #label><span :data-testid="`dataset-detail-tab-${key}`">{{ label }}</span></template>
        <template v-if="key === 'overview'">
          <template v-if="detail?.overview">
            <div class="dataset-workflow" aria-label="数据处理流程">
              <div v-for="(step, index) in workflowSteps" :key="step.key" class="workflow-step" :class="step.state">
                <span class="workflow-marker">{{ index + 1 }}</span>
                <span><strong>{{ step.label }}</strong><small>{{ step.status }}</small></span>
              </div>
            </div>
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
              <el-descriptions-item label="时间范围">{{ detail.overview.time_start || '-' }} 至 {{ detail.overview.time_end || '-' }}</el-descriptions-item>
              <el-descriptions-item label="空间范围" :span="2">{{ detail.overview.bbox || '-' }}</el-descriptions-item>
              <el-descriptions-item label="当前版本">{{ detail.overview.current_output_version || '-' }}</el-descriptions-item>
              <el-descriptions-item label="入库状态"><StatusTag domain="ingest" :value="detail.overview.ingest_status" size="small" /></el-descriptions-item>
              <el-descriptions-item label="质检状态"><StatusTag domain="quality" :value="detail.overview.quality_status" size="small" /></el-descriptions-item>
              <el-descriptions-item label="发布状态"><StatusTag domain="publication" :value="detail.overview.publish_status" size="small" /></el-descriptions-item>
              <el-descriptions-item label="描述" :span="2">{{ detail.overview.description || '-' }}</el-descriptions-item>
            </el-descriptions>
          </template>
          <el-empty v-else description="正在加载数据集概览" />
        </template>
        <template v-else>
          <AppTable :data="collection(key)" :page="page(key).page" :page-size="page(key).pageSize" :total="page(key).total" row-key="scene_id" @current-change="(value) => emit('tab-page-change', { tab: key, page: value })" @size-change="(value) => emit('tab-page-size-change', { tab: key, pageSize: value })">
            <el-table-column label="标识" min-width="185"><template #default="{ row }">{{ rowId(row) }}</template></el-table-column>
            <el-table-column v-if="key === 'scenes'" prop="scene_code" label="景编码" min-width="155" show-overflow-tooltip />
            <el-table-column v-if="key === 'scenes'" prop="acquisition_time" label="采集时间" min-width="170" />
            <el-table-column v-if="key === 'provenance'" prop="relation_type" label="关系" width="130" />
            <el-table-column label="状态" min-width="110"><template #default="{ row }"><StatusTag v-if="row.status" :domain="key === 'publications' ? 'publication' : key === 'quality' ? 'quality' : key === 'scenes' ? 'scene' : 'ingest'" :value="row.status" size="small" /><span v-else>-</span></template></el-table-column>
            <el-table-column label="来源/错误" min-width="200" show-overflow-tooltip><template #default="{ row }">{{ row.source_uri || row.error_message || row.source_load_batch_id || row.load_batch_id || '-' }}</template></el-table-column>
            <el-table-column prop="created_at" label="创建时间" min-width="170" />
            <el-table-column v-if="writeEnabled && ['scenes', 'ingest-records', 'publications'].includes(key)" label="操作" width="130" fixed="right">
              <template #default="{ row }">
                <el-button v-if="key === 'scenes'" link type="primary" @click="openReassign(row)">修正归属</el-button>
                <el-button v-else-if="writeEnabled && key === 'ingest-records' && row.status === 'failed'" link type="primary" @click="emit('retry-scene-ingest', row.scene_id)">重试</el-button>
                <el-button v-else-if="writeEnabled && key === 'publications' && row.status === 'active'" link type="warning" @click="emit('withdraw', row.publication_id)">撤回</el-button>
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
  .dataset-workflow { grid-template-columns: repeat(2, 1fr); }
  .workflow-step::after { display: none; }
}
</style>
