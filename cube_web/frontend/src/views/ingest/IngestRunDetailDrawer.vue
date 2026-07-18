<script setup>
import { computed, ref, watch } from 'vue';

import AppTable from '@/components/AppTable.vue';
import DetailDrawer from '@/components/DetailDrawer.vue';
import StatusTag from '@/components/StatusTag.vue';
import { formatShanghaiTime } from '@/utils/time';

const props = defineProps({
  visible: Boolean, runId: { type: String, default: '' }, detail: { type: Object, default: null },
  loading: Boolean, actionLoading: Boolean, writeEnabled: { type: Boolean, default: true },
});
const emit = defineEmits(['close', 'retry-scenes', 'cancel']);
const cancelDialog = ref(false);
const cancelReason = ref('');
const title = computed(() => props.detail?.ingest_run_id || props.runId || '数据入库详情');
const scenes = computed(() => props.detail?.scenes || []);
const failedSceneIds = computed(() => scenes.value.filter((scene) => scene.status === 'failed').map((scene) => scene.scene_id));
const cancellable = computed(() => ['pending', 'queued', 'running'].includes(props.detail?.status));

watch(() => props.runId, () => {
  cancelDialog.value = false;
  cancelReason.value = '';
});

function confirmCancel() {
  emit('cancel', cancelReason.value.trim());
  cancelDialog.value = false;
}
</script>

<template>
  <DetailDrawer :visible="visible" :title="title" :loading="loading" test-id="ingest-run-detail-drawer" size="820px" @update:visible="(value) => !value && emit('close')" @closed="emit('close')">
    <div class="drawer-actions">
      <el-button v-if="writeEnabled && failedSceneIds.length" type="primary" plain :loading="actionLoading" @click="emit('retry-scenes', failedSceneIds)">重试全部失败景</el-button>
      <el-button v-if="writeEnabled && cancellable" type="danger" plain :loading="actionLoading" @click="cancelDialog = true">取消运行</el-button>
      <el-button data-testid="ingest-detail-close" link type="primary" @click="emit('close')">关闭</el-button>
    </div>
    <template v-if="detail">
      <el-descriptions :column="2" border>
        <el-descriptions-item label="数据入库">{{ detail.ingest_run_id }}</el-descriptions-item>
        <el-descriptions-item label="状态"><StatusTag domain="ingest" :value="detail.status" size="small" /></el-descriptions-item>
        <el-descriptions-item label="数据集">{{ detail.dataset_code || detail.dataset_id }}</el-descriptions-item>
        <el-descriptions-item label="剖分运行">{{ detail.partition_run_id }}</el-descriptions-item>
        <el-descriptions-item label="创建时间">{{ formatShanghaiTime(detail.created_at) }}</el-descriptions-item>
        <el-descriptions-item label="完成时间">{{ formatShanghaiTime(detail.completed_at) }}</el-descriptions-item>
        <el-descriptions-item label="错误" :span="2">{{ detail.error_message || '-' }}</el-descriptions-item>
      </el-descriptions>
      <h3>景入库明细</h3>
      <AppTable :data="scenes" :pagination="false" row-key="scene_id">
        <el-table-column prop="scene_id" label="景 ID" min-width="170" show-overflow-tooltip />
        <el-table-column prop="output_version" label="输出版本" min-width="130" show-overflow-tooltip />
        <el-table-column label="状态" width="110"><template #default="{ row }"><StatusTag domain="ingest" :value="row.status" size="small" /></template></el-table-column>
        <el-table-column prop="attempt_count" label="尝试" width="75" />
        <el-table-column prop="error_message" label="错误" min-width="180" show-overflow-tooltip />
        <el-table-column label="来源批次" min-width="180"><template #default="{ row }">{{ (row.source_load_batch_ids || []).join(', ') || '-' }}</template></el-table-column>
        <el-table-column v-if="writeEnabled" label="操作" width="80" fixed="right"><template #default="{ row }"><el-button v-if="row.status === 'failed'" link type="primary" @click="emit('retry-scenes', [row.scene_id])">重试</el-button></template></el-table-column>
      </AppTable>
    </template>
    <el-empty v-else description="正在加载数据入库" />
    <el-dialog v-model="cancelDialog" title="取消数据入库" width="480px" append-to-body>
      <el-input v-model="cancelReason" type="textarea" :rows="3" placeholder="可填写取消原因" />
      <template #footer><el-button @click="cancelDialog = false">返回</el-button><el-button type="danger" @click="confirmCancel">确认取消</el-button></template>
    </el-dialog>
  </DetailDrawer>
</template>

<style scoped>
.drawer-actions { display: flex; justify-content: flex-end; gap: 8px; margin-bottom: 12px; }
h3 { margin: 20px 0 10px; font-size: 16px; letter-spacing: 0; }
</style>
