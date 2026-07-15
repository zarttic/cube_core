<script setup>
import { computed, onMounted, ref, watch } from 'vue';
import { Download, Refresh, Search } from '@element-plus/icons-vue';
import { ElMessage } from 'element-plus';

import { apiPrefixes, authHeaders, requestGet } from '@/api/client';

const records = ref([]);
const total = ref(0);
const page = ref(1);
const pageSize = ref(20);
const keyword = ref('');
const status = ref('');
const dataType = ref('');
const loading = ref(false);
const error = ref('');
const selectedRun = ref(null);
const results = ref([]);
const errors = ref([]);
const errorTotal = ref(0);
const detailsLoading = ref(false);
const exportLoading = ref(false);

function statusText(value) {
  return { pass: '通过', warn: '告警', fail: '失败', error: '异常', pending: '等待中', running: '运行中' }[String(value || '').toLowerCase()] || value || '-';
}

function statusType(value) {
  return { pass: 'success', warn: 'warning', fail: 'danger', error: 'danger', running: 'primary' }[String(value || '').toLowerCase()] || 'info';
}

function formatTime(value) {
  if (!value) return '-';
  const time = new Date(value);
  return Number.isNaN(time.getTime()) ? value : time.toLocaleString('zh-CN', { hour12: false });
}

function queryString(values) {
  const params = new URLSearchParams();
  Object.entries(values).forEach(([key, value]) => {
    if (value !== '' && value !== undefined && value !== null) params.set(key, String(value));
  });
  return params.toString();
}

async function loadRecords() {
  loading.value = true;
  error.value = '';
  try {
    const { qualityPrefix } = apiPrefixes();
    const query = queryString({ page: page.value, page_size: pageSize.value, keyword: keyword.value.trim(), status: status.value, data_type: dataType.value });
    const response = await requestGet(`${qualityPrefix}/records?${query}`);
    records.value = response.items || [];
    total.value = Number(response.total || 0);
    if (!records.value.length && total.value && page.value > 1) {
      page.value = Math.ceil(total.value / pageSize.value);
      await loadRecords();
    }
  } catch (requestError) {
    error.value = requestError.message;
    ElMessage.error(`质检记录加载失败：${requestError.message}`);
  } finally {
    loading.value = false;
  }
}

async function selectRun(row) {
  if (!row?.quality_run_id) return;
  selectedRun.value = null;
  results.value = [];
  errors.value = [];
  errorTotal.value = 0;
  detailsLoading.value = true;
  try {
    const { qualityPrefix } = apiPrefixes();
    const id = encodeURIComponent(row.quality_run_id);
    const [run, resultPage, errorPage] = await Promise.all([
      requestGet(`${qualityPrefix}/records/${id}`),
      requestGet(`${qualityPrefix}/records/${id}/results?page=1&page_size=200`),
      requestGet(`${qualityPrefix}/records/${id}/errors?page=1&page_size=100`),
    ]);
    selectedRun.value = run;
    results.value = resultPage.items || [];
    errors.value = errorPage.items || [];
    errorTotal.value = Number(errorPage.total || 0);
  } catch (requestError) {
    ElMessage.error(`质检详情加载失败：${requestError.message}`);
  } finally {
    detailsLoading.value = false;
  }
}

async function exportErrors(format) {
  if (!selectedRun.value?.quality_run_id) return;
  exportLoading.value = true;
  try {
    const { qualityPrefix } = apiPrefixes();
    const id = encodeURIComponent(selectedRun.value.quality_run_id);
    const response = await fetch(`${qualityPrefix}/records/${id}/errors/export?format=${format}`, { headers: authHeaders() });
    if (!response.ok) throw new Error(`导出失败: ${response.status}`);
    const link = document.createElement('a');
    link.href = URL.createObjectURL(await response.blob());
    link.download = response.headers.get('Content-Disposition')?.match(/filename="?([^";]+)"?/)?.[1] || `quality-errors.${format}`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(link.href);
  } catch (requestError) {
    ElMessage.error(requestError.message);
  } finally {
    exportLoading.value = false;
  }
}

const selectedTitle = computed(() => selectedRun.value ? `${selectedRun.value.dataset_code} / ${selectedRun.value.quality_run_id}` : '选择一条质检记录查看详情');

watch([keyword, status, dataType], () => { page.value = 1; });
onMounted(loadRecords);
</script>

<template>
  <section class="quality-records-page">
    <div class="container">
      <div class="quality-records-header">
        <div><h1>质量运行记录</h1><p>查看数据集剖分后的质量结果与完整错误明细。</p></div>
        <el-button :icon="Refresh" :loading="loading" @click="loadRecords">刷新</el-button>
      </div>
      <div class="quality-records-filters">
        <el-input v-model="keyword" :prefix-icon="Search" placeholder="数据集、批次或运行 ID" clearable @keyup.enter="loadRecords" />
        <el-select v-model="dataType" placeholder="产品类型" clearable><el-option label="光学" value="optical" /><el-option label="雷达" value="radar" /><el-option label="信息产品" value="product" /><el-option label="碳卫星" value="carbon" /></el-select>
        <el-select v-model="status" placeholder="质检状态" clearable><el-option label="通过" value="pass" /><el-option label="告警" value="warn" /><el-option label="失败" value="fail" /><el-option label="异常" value="error" /></el-select>
        <el-button type="primary" @click="loadRecords">查询</el-button>
      </div>
      <el-alert v-if="error" type="error" :title="error" :closable="false" show-icon />
      <el-table v-loading="loading" :data="records" row-key="quality_run_id" class="quality-records-table" highlight-current-row @row-click="selectRun">
        <el-table-column label="数据集" prop="dataset_code" min-width="150" />
        <el-table-column label="批次" prop="batch_id" min-width="150" show-overflow-tooltip />
        <el-table-column label="产品类型" min-width="110"><template #default="{ row }">{{ row.product_type || row.data_type }}</template></el-table-column>
        <el-table-column label="剖分状态" prop="partition_status" width="100" />
        <el-table-column label="质检状态" width="100"><template #default="{ row }"><el-tag :type="statusType(row.status)">{{ statusText(row.status) }}</el-tag></template></el-table-column>
        <el-table-column label="错误数" prop="error_count" width="90" />
        <el-table-column label="质检时间" min-width="180"><template #default="{ row }">{{ formatTime(row.completed_at || row.started_at || row.created_at) }}</template></el-table-column>
        <el-table-column label="操作" width="90"><template #default="{ row }"><el-button link type="primary" @click.stop="selectRun(row)">查看</el-button></template></el-table-column>
      </el-table>
      <el-pagination v-model:current-page="page" v-model:page-size="pageSize" :total="total" :page-sizes="[20, 50, 100, 200]" layout="total, sizes, prev, pager, next" background @current-change="loadRecords" @size-change="loadRecords" />
      <section v-loading="detailsLoading" class="quality-records-detail">
        <div class="quality-detail-heading"><h2>{{ selectedTitle }}</h2><span v-if="selectedRun">{{ statusText(selectedRun.status) }} · {{ selectedRun.error_count }} 个错误</span><el-button v-if="selectedRun" :icon="Download" :loading="exportLoading" @click="exportErrors('csv')">导出 CSV</el-button><el-button v-if="selectedRun" :icon="Download" :loading="exportLoading" @click="exportErrors('json')">导出 JSON</el-button></div>
        <template v-if="selectedRun">
          <el-descriptions :column="4" border><el-descriptions-item label="数据集">{{ selectedRun.dataset_code }}</el-descriptions-item><el-descriptions-item label="批次">{{ selectedRun.batch_id }}</el-descriptions-item><el-descriptions-item label="质量运行 ID" :span="2">{{ selectedRun.quality_run_id }}</el-descriptions-item></el-descriptions>
          <h3>规则结果</h3>
          <el-table :data="results" size="small"><el-table-column label="规则" prop="rule_code" min-width="180" /><el-table-column label="状态" width="100"><template #default="{ row }"><el-tag :type="statusType(row.status)" size="small">{{ statusText(row.status) }}</el-tag></template></el-table-column><el-table-column label="错误" prop="error_count" width="80" /><el-table-column label="告警" prop="warning_count" width="80" /><el-table-column label="发现数" prop="finding_count" width="90" /></el-table>
          <h3>错误明细 <small>共 {{ errorTotal }} 条，以下展示前 {{ errors.length }} 条</small></h3>
          <el-table :data="errors" size="small"><el-table-column label="规则" prop="rule_code" min-width="140" /><el-table-column label="错误码" prop="error_code" min-width="140" /><el-table-column label="字段" prop="field" width="120" /><el-table-column label="说明" prop="message" min-width="280" show-overflow-tooltip /></el-table>
        </template>
      </section>
    </div>
  </section>
</template>

<style scoped>
.quality-records-page { padding: 28px 0 48px; background: #f5f7fa; min-height: calc(100vh - 76px); }
.quality-records-header, .quality-detail-heading { display: flex; align-items: center; justify-content: space-between; gap: 16px; }
.quality-records-header h1 { margin: 0; font-size: 24px; letter-spacing: 0; color: #202d3d; }
.quality-records-header p { margin: 8px 0 0; color: #607085; }
.quality-records-filters { display: grid; grid-template-columns: minmax(250px, 1fr) 160px 160px auto; gap: 12px; margin: 22px 0 14px; }
.quality-records-table { background: #fff; }
.quality-records-detail { margin-top: 22px; padding: 20px; background: #fff; border: 1px solid #dfe5ec; border-radius: 6px; }
.quality-detail-heading { flex-wrap: wrap; margin-bottom: 18px; }
.quality-detail-heading h2, .quality-records-detail h3 { margin: 0; font-size: 17px; color: #253447; }
.quality-records-detail h3 { margin-top: 22px; margin-bottom: 10px; }
.quality-records-detail small { font-size: 12px; color: #7a8796; font-weight: normal; }
@media (max-width: 760px) { .quality-records-filters { grid-template-columns: 1fr; } .quality-records-header { align-items: flex-start; } }
</style>
