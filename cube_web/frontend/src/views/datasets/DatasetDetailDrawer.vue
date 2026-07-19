<script setup>
import { computed, reactive, ref, watch } from 'vue';
import { Lock, Unlock } from '@element-plus/icons-vue';

import AppTable from '@/components/AppTable.vue';
import DetailDrawer from '@/components/DetailDrawer.vue';
import StatusTag from '@/components/StatusTag.vue';
import { bandDisplayLabel, dataUnitTypeLabel } from '@/utils/bands';
import { derivedPartitionMethod, gridDefinition, nativeLevelLabel, recommendedGridLevel } from '@/utils/grid';
import { formatShanghaiRange, formatShanghaiTime } from '@/utils/time';

const props = defineProps({
  testId: { type: String, default: '' }, visible: Boolean, datasetId: { type: String, default: '' },
  detail: { type: Object, default: () => ({}) }, loading: Boolean, actionLoading: Boolean,
  writeEnabled: { type: Boolean, default: true },
  activeTab: { type: String, default: 'overview' }, tabPages: { type: Object, default: () => ({}) },
});
const emit = defineEmits([
  'close', 'tab-change', 'tab-page-change', 'tab-page-size-change', 'update-metadata',
  'reassign-scene', 'rerun-quality', 'retry-scene-ingest', 'queue-partition', 'archive',
]);

const tabs = [
  ['overview', '概览'], ['scenes', '数据'],
  ['outputs', '剖分版本'], ['tiles', '瓦片'],
  ['ingest-records', '入库记录'], ['quality', '质检'], ['provenance', '来源追踪'],
];
const title = computed(() => props.detail?.overview?.dataset_code || props.datasetId || '数据集详情');
const canRequestQuality = computed(() => Boolean(props.detail?.overview?.current_output_version));
const gridTypes = [
  { value: 'geohash', label: '经纬度格网' },
  { value: 'mgrs', label: '平面格网' },
  { value: 'isea4h', label: '六边形格网' },
];
function latestGridStatus(band, gridType) {
  return (band?.grid_statuses || []).filter((item) => item.grid_type === gridType)
    .sort((left, right) => Number(right.grid_level) - Number(left.grid_level))[0] || null;
}
function gridStatusLabel(status) {
  if (!status) return '未剖分';
  if (status.ingest_status === 'completed') return '已入库';
  if (status.quality_status === 'pass' || status.quality_status === 'warn') return '质检通过';
  if (status.quality_status === 'fail' || status.quality_status === 'error') return '质检未通过';
  if (status.partition_status === 'completed') return '已剖分';
  if (status.partition_status === 'failed') return '剖分失败';
  return status.partition_status === 'running' ? '剖分中' : '未剖分';
}
function gridStatusClass(status) {
  if (status?.ingest_status === 'completed') return 'is-ingested';
  if (!status || ['pending', 'queued', 'cancelled'].includes(status.partition_status)) return 'is-empty';
  if (status.partition_status === 'failed' || ['fail', 'error'].includes(status.quality_status)) return 'is-error';
  return 'is-progress';
}
function gridWorkflowSummary(bands, gridType) {
  const list = bands || [];
  const statuses = list.map((band) => latestGridStatus(band, gridType)).filter(Boolean);
  return {
    total: list.length,
    partition: statuses.filter((status) => status.partition_status === 'completed').length,
    quality: statuses.filter((status) => ['pass', 'warn'].includes(status.quality_status)).length,
    ingest: statuses.filter((status) => status.ingest_status === 'completed').length,
  };
}
function datasetGridSummary(gridType) {
  const summary = props.detail?.overview?.grid_summary?.[gridType];
  if (!summary) return gridWorkflowSummary(datasetBands(), gridType);
  return {
    total: Number(summary.total || 0),
    partition: Number(summary.partition || 0),
    quality: Number(summary.quality || 0),
    ingest: Number(summary.ingest || 0),
  };
}
function datasetBands() { return collection('scenes').flatMap((scene) => scene.bands || []); }
function recommendedLevel(gridType = repartitionGridType.value) {
  return recommendedGridLevel(props.detail?.overview?.resolution_m, gridType);
}
function levelOptions(gridType) {
  const definition = gridDefinition(gridType);
  return definition ? Array.from({ length: definition.maxLevel - definition.minLevel + 1 }, (_, index) => definition.minLevel + index) : [];
}
function gridCompleted(band, gridType = repartitionGridType.value) {
  return (band?.grid_statuses || []).some((status) => status.grid_type === gridType && status.partition_status === 'completed');
}
function selectableBands(scene = null) {
  const bands = scene ? (scene.bands || []) : datasetBands();
  return bands.filter((band) => band.band_unit_id && !gridCompleted(band));
}
function selectionState(bands) {
  const ids = bands.map((band) => band.band_unit_id);
  const selected = ids.filter((id) => selectedPartitionBandIds.value.includes(id)).length;
  return { checked: ids.length > 0 && selected === ids.length, indeterminate: selected > 0 && selected < ids.length };
}
function togglePartitionBands(bands, checked) {
  const next = new Set(selectedPartitionBandIds.value);
  bands.forEach((band) => checked ? next.add(band.band_unit_id) : next.delete(band.band_unit_id));
  selectedPartitionBandIds.value = [...next];
}
function openQueueConfirm() {
  if (!selectedPartitionBandIds.value.length) return;
  if (!draftName.value) draftName.value = defaultDraftName();
  queueConfirmDialog.value = true;
}
function defaultDraftName() {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23',
  }).formatToParts(new Date()).reduce((result, part) => ({ ...result, [part.type]: part.value }), {});
  const dataset = props.detail?.overview?.dataset_code || props.detail?.overview?.dataset_id || '数据集';
  return `${dataset}-剖分批次-${parts.year}${parts.month}${parts.day}${parts.hour}${parts.minute}${parts.second}`;
}
function queuePartition() {
  const selected = new Set(selectedPartitionBandIds.value);
  const scenes = collection('scenes').filter((scene) => (scene.bands || []).some((band) => selected.has(band.band_unit_id)));
  emit('queue-partition', {
    dataset_id: props.detail?.overview?.dataset_id,
    dataset_code: props.detail?.overview?.dataset_code,
    dataset_title: props.detail?.overview?.dataset_title,
    data_type: props.detail?.overview?.data_type,
    product_type: props.detail?.overview?.product_type,
    selection_source: 'dataset',
    draft_name: draftName.value.trim(),
    scenes,
    band_unit_ids: [...selected],
    partition: {
      grid_type: repartitionGridType.value,
      requested_grid_level: Number(repartitionGridLevel.value),
      partition_method: derivedPartitionMethod(repartitionGridType.value),
      cover_mode: 'intersect', time_granularity: 'day', max_cells_per_asset: 0,
    },
  });
  queueConfirmDialog.value = false;
}
function changeRepartitionGrid(gridType) {
  repartitionGridType.value = gridType;
  repartitionGridLevel.value = recommendedLevel(gridType);
  repartitionGridLevelLocked.value = true;
  const selectable = new Set(selectableBands().map((band) => band.band_unit_id));
  selectedPartitionBandIds.value = selectedPartitionBandIds.value.filter((bandUnitId) => selectable.has(bandUnitId));
}
function lockRecommendedLevel() {
  repartitionGridLevel.value = recommendedLevel();
  repartitionGridLevelLocked.value = true;
}
const editing = ref(false);
const metadataForm = reactive({ dataset_title: '', description: '', keywords: '' });
const reassignDialog = ref(false);
const reassignForm = reactive({ scene_id: '', target_dataset_id: '', reason: '' });
const archiveDialog = ref(false);
const archiveReason = ref('');
const queueConfirmDialog = ref(false);
const draftName = ref('');
const collapsedScenes = ref(new Set());
const selectedPartitionBandIds = ref([]);
const repartitionGridType = ref('geohash');
const repartitionGridLevel = ref(5);
const repartitionGridLevelLocked = ref(true);

function resetLocalState() {
  editing.value = false;
  reassignDialog.value = false;
  archiveDialog.value = false;
  queueConfirmDialog.value = false;
  draftName.value = '';
  Object.assign(reassignForm, { scene_id: '', target_dataset_id: '', reason: '' });
  archiveReason.value = '';
  collapsedScenes.value = new Set((props.detail?.scenes?.items || []).map((scene) => String(scene.scene_id)));
  selectedPartitionBandIds.value = [];
  repartitionGridLevel.value = recommendedLevel();
  repartitionGridLevelLocked.value = true;
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

</script>

<template>
  <DetailDrawer :visible="visible" :test-id="testId" :title="title" :loading="loading" size="min(860px, 100vw)" @update:visible="(value) => !value && emit('close')" @closed="emit('close')">
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
          <div v-if="writeEnabled" class="repartition-toolbar">
            <div class="repartition-heading"><strong>重新剖分</strong><span>选择尚未完成当前格网的数据单元，加入新的剖分批次</span></div>
            <div class="repartition-controls" aria-label="剖分格网设置">
              <label><span>格网</span><el-select :model-value="repartitionGridType" size="small" @update:model-value="changeRepartitionGrid"><el-option v-for="grid in gridTypes" :key="grid.value" :label="grid.label" :value="grid.value" /></el-select></label>
              <label><span>层级</span><div class="level-control"><el-select v-model="repartitionGridLevel" size="small" :disabled="repartitionGridLevelLocked"><el-option v-for="level in levelOptions(repartitionGridType)" :key="level" :label="nativeLevelLabel(repartitionGridType, level)" :value="level" /></el-select><el-tooltip v-if="repartitionGridLevelLocked" content="解锁格网层级" placement="top"><el-button :icon="Unlock" aria-label="解锁格网层级" @click="repartitionGridLevelLocked = false" /></el-tooltip><el-tooltip v-else content="恢复推荐层级" placement="top"><el-button :icon="Lock" aria-label="恢复推荐层级" @click="lockRecommendedLevel" /></el-tooltip></div></label>
            </div>
            <div class="repartition-actions">
              <div class="selection-action"><el-checkbox :model-value="selectionState(selectableBands()).checked" :indeterminate="selectionState(selectableBands()).indeterminate" :disabled="!selectableBands().length" @change="(value) => togglePartitionBands(selectableBands(), value)">全选当前页</el-checkbox></div>
              <el-button class="batch-action" type="primary" size="small" :disabled="!selectedPartitionBandIds.length" @click="openQueueConfirm">加入剖分批次</el-button>
            </div>
          </div>
          <div class="dataset-grid-summary" aria-label="数据集格网处理状态" data-testid="dataset-workflow-summary">
            <section v-for="grid in gridTypes" :key="grid.value" class="grid-summary-row" :class="`grid-${grid.value}`" :data-testid="`dataset-grid-summary-${grid.value}`">
              <strong><i />{{ grid.label }}</strong>
              <div><span>剖分 <b>{{ datasetGridSummary(grid.value).partition }}/{{ datasetGridSummary(grid.value).total }}</b></span><span>质检 <b>{{ datasetGridSummary(grid.value).quality }}/{{ datasetGridSummary(grid.value).total }}</b></span><span>入库 <b>{{ datasetGridSummary(grid.value).ingest }}/{{ datasetGridSummary(grid.value).total }}</b></span></div>
            </section>
          </div>
          <div class="dataset-scene-hierarchy" data-testid="dataset-scene-hierarchy">
            <section v-for="scene in collection('scenes')" :key="scene.scene_id" class="dataset-scene-group" :data-testid="`managed-scene-${scene.scene_id}`">
              <header class="scene-toggle" :aria-expanded="!sceneCollapsed(scene.scene_id)" @click="toggleScene(scene.scene_id)">
                <div><span class="data-level-label">数据</span><strong>{{ scene.scene_key || scene.scene_id }}</strong><span>{{ scene.scene_id }} · {{ formatShanghaiTime(scene.acquisition_time) }}</span></div>
                <div class="scene-header-meta"><StatusTag domain="scene" :value="scene.status" size="small" /><span>{{ scene.bands?.length || 0 }} 个数据单元</span><div class="scene-grid-summary"><span v-for="grid in gridTypes" :key="grid.value" :class="`grid-${grid.value}`"><i />{{ grid.label }} · 剖 {{ gridWorkflowSummary(scene.bands, grid.value).partition }}/{{ gridWorkflowSummary(scene.bands, grid.value).total }} · 质 {{ gridWorkflowSummary(scene.bands, grid.value).quality }}/{{ gridWorkflowSummary(scene.bands, grid.value).total }} · 入 {{ gridWorkflowSummary(scene.bands, grid.value).ingest }}/{{ gridWorkflowSummary(scene.bands, grid.value).total }}</span></div><el-checkbox v-if="writeEnabled" :model-value="selectionState(selectableBands(scene)).checked" :indeterminate="selectionState(selectableBands(scene)).indeterminate" :disabled="!selectableBands(scene).length" @click.stop @change="(value) => togglePartitionBands(selectableBands(scene), value)">全选该景</el-checkbox><el-button v-if="writeEnabled" link type="primary" @click.stop="openReassign(scene)">修正归属</el-button><span class="scene-toggle-mark">{{ sceneCollapsed(scene.scene_id) ? '展开' : '收起' }}</span></div>
              </header>
              <div v-if="!sceneCollapsed(scene.scene_id) && scene.bands?.length" class="managed-band-list">
                <div v-for="band in scene.bands" :key="band.band_unit_id || `${band.asset_id}-${band.band_code}`" class="managed-band-row" :data-testid="band.band_unit_id ? `managed-band-${band.band_unit_id}` : undefined">
                  <div class="managed-band-identity"><el-checkbox v-if="writeEnabled" :model-value="selectedPartitionBandIds.includes(band.band_unit_id)" :disabled="!band.band_unit_id || gridCompleted(band)" @click.stop @change="(value) => togglePartitionBands([band], value)" /><strong>{{ bandDisplayLabel(band) }}</strong><span>{{ band.band_unit_id || '-' }}</span><div class="band-grid-tags"><span v-for="grid in gridTypes" :key="grid.value" class="band-grid-status" :class="[`grid-${grid.value}`, gridStatusClass(latestGridStatus(band, grid.value))]">{{ grid.label }} · {{ gridStatusLabel(latestGridStatus(band, grid.value)) }}</span></div></div>
                  <span>{{ dataUnitTypeLabel(detail?.overview?.data_type) }}</span>
                  <span>{{ band.band_type || '-' }}</span>
                  <span>{{ band.asset_id || '-' }}</span>
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
    <el-dialog v-model="queueConfirmDialog" title="确认加入剖分批次" width="440px" append-to-body>
      <el-form label-width="76px"><el-form-item label="批次名称"><el-input v-model="draftName" maxlength="160" show-word-limit /></el-form-item></el-form>
      <el-descriptions :column="1" border>
        <el-descriptions-item label="数据集">{{ detail?.overview?.dataset_title || detail?.overview?.dataset_code || datasetId }}</el-descriptions-item>
        <el-descriptions-item label="已选波段">{{ selectedPartitionBandIds.length }} 个</el-descriptions-item>
        <el-descriptions-item label="格网">{{ gridTypes.find((grid) => grid.value === repartitionGridType)?.label }}</el-descriptions-item>
        <el-descriptions-item label="层级">{{ nativeLevelLabel(repartitionGridType, repartitionGridLevel) }}</el-descriptions-item>
      </el-descriptions>
      <template #footer><el-button @click="queueConfirmDialog = false">取消</el-button><el-button type="primary" :disabled="!draftName.trim()" @click="queuePartition">确认加入</el-button></template>
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
.repartition-toolbar { display: grid; grid-template-columns: minmax(180px, 1fr) max-content; gap: 10px 16px; margin-bottom: 14px; padding: 11px 13px; border: 1px solid #dfe5ec; border-radius: 6px; background: #fbfcfd; }
.repartition-heading { display: flex; min-width: 0; flex-direction: column; gap: 2px; }
.repartition-heading strong { color: #263247; font-size: 13px; }
.repartition-heading span { color: #7b8493; font-size: 11px; }
.repartition-controls { display: grid; grid-template-columns: max-content max-content; align-items: end; gap: 10px; }
.repartition-controls label { display: grid; grid-template-columns: 1fr; gap: 3px; color: #667085; font-size: 10px; }
.repartition-controls label:first-child :deep(.el-select) { width: 132px; }
.repartition-controls label:last-child :deep(.el-select) { width: 142px; }
.level-control { display: flex; align-items: center; gap: 4px; }
.level-control :deep(.el-button) { width: 28px; padding: 0; }
.repartition-actions { display: grid; grid-column: 1 / -1; grid-template-columns: minmax(124px, 1fr) minmax(136px, 1fr) minmax(136px, 1fr); align-items: center; gap: 12px; padding-top: 9px; border-top: 1px solid #e7ebf0; }
.selection-action { display: flex; min-height: 32px; align-items: center; padding: 0 10px; border: 1px solid #d8dee7; border-radius: 4px; background: #fff; }
.batch-action { width: 100%; margin: 0 !important; }
.dataset-grid-summary { display: flex; flex-direction: column; gap: 6px; margin-bottom: 12px; }
.grid-summary-row { --grid-accent: #52748a; --grid-soft: #eef3f6; display: grid; grid-template-columns: 150px minmax(0, 1fr); align-items: center; min-height: 42px; padding: 7px 11px; border: 1px solid color-mix(in srgb, var(--grid-accent) 26%, #fff); border-left: 3px solid var(--grid-accent); border-radius: 4px; background: var(--grid-soft); }
.grid-summary-row.grid-mgrs, .grid-mgrs { --grid-accent: #5d7c70; --grid-soft: #eef4f1; }
.grid-summary-row.grid-isea4h, .grid-isea4h { --grid-accent: #85735b; --grid-soft: #f4f1ec; }
.grid-summary-row > strong { display: flex; align-items: center; gap: 7px; color: #344054; font-size: 12px; }
.grid-summary-row i, .scene-grid-summary i { width: 7px; height: 7px; border-radius: 50%; background: var(--grid-accent); }
.grid-summary-row > div { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); }
.grid-summary-row span { color: #667085; font-size: 11px; }
.grid-summary-row b { margin-left: 4px; color: var(--grid-accent); font-size: 13px; }
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
.scene-grid-summary { display: flex; flex-direction: column; gap: 3px; }
.scene-grid-summary span { --grid-accent: #52748a; display: flex; align-items: center; gap: 4px; color: var(--el-text-color-secondary); font-size: 10px; white-space: nowrap; }
.managed-band-list { display: flex; flex-direction: column; margin-left: 20px; border-left: 2px solid #c7d9e9; }
.managed-band-row { display: grid; grid-template-columns: minmax(300px, 1.8fr) minmax(72px, .5fr) minmax(72px, .5fr) minmax(130px, .8fr); gap: 9px; align-items: center; min-height: 48px; padding: 7px 10px; border-bottom: 1px solid var(--el-border-color-extra-light); }
.managed-band-row > div { display: flex; min-width: 0; flex-direction: column; gap: 2px; }
.managed-band-row strong { color: #2d628d; font-size: 12px; overflow-wrap: anywhere; }
.managed-band-row span { color: var(--el-text-color-secondary); font-size: 11px; overflow-wrap: anywhere; }
.managed-band-identity { align-items: flex-start !important; }
.band-grid-tags { display: grid; width: min(100%, 220px); grid-template-columns: 1fr; gap: 4px; margin-top: 4px; }
.band-grid-status { --grid-accent: #52748a; --grid-soft: #eef3f6; padding: 3px 7px; border: 1px solid var(--grid-accent); border-radius: 3px; font-size: 10px !important; line-height: 1.35; }
.band-grid-status.is-empty { background: transparent; color: var(--grid-accent); }
.band-grid-status.is-progress { background: var(--grid-soft); color: var(--grid-accent); }
.band-grid-status.is-ingested { background: var(--grid-accent); color: #fff; }
.band-grid-status.is-error { border-style: dashed; background: #fff7f6; color: #a64b45; }
.scene-grid-summary .grid-mgrs, .band-grid-status.grid-mgrs { --grid-accent: #5d7c70; --grid-soft: #eef4f1; }
.scene-grid-summary .grid-isea4h, .band-grid-status.grid-isea4h { --grid-accent: #85735b; --grid-soft: #f4f1ec; }
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
  .repartition-toolbar { grid-template-columns: 1fr; }
  .repartition-controls { grid-template-columns: 1fr 1fr; }
  .repartition-controls label:first-child :deep(.el-select), .repartition-controls label:last-child :deep(.el-select) { width: 100%; }
  .repartition-actions { grid-template-columns: 1fr; }
  .grid-summary-row { grid-template-columns: 1fr; gap: 6px; }
  .dataset-scene-group > header { align-items: flex-start; flex-direction: column; }
  .scene-header-meta { flex-wrap: wrap; }
  .managed-band-list, .managed-band-empty { margin-left: 8px; }
  .managed-band-row { grid-template-columns: 1fr 1fr; }
  .dataset-workflow { grid-template-columns: repeat(2, 1fr); }
  .workflow-step::after { display: none; }
}
</style>
