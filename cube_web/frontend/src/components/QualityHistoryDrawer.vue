<script setup>
import { Search } from '@element-plus/icons-vue';

const visible = defineModel('visible', { type: Boolean, default: false });
const search = defineModel('search', { type: String, default: '' });
const status = defineModel('status', { type: String, default: '' });
const page = defineModel('page', { type: Number, default: 1 });
const pageSize = defineModel('pageSize', { type: Number, default: 30 });

const props = defineProps({
  rows: { type: Array, default: () => [] },
  total: { type: Number, default: 0 },
  loading: { type: Boolean, default: false },
  dataType: { type: String, default: 'optical' },
  selectedReportId: { type: String, default: '' },
});

const emit = defineEmits(['select', 'page-change', 'page-size-change']);

function statusType(value) {
  if (value === 'PASS') return 'success';
  if (value === 'WARN') return 'warning';
  if (value === 'FAIL') return 'danger';
  return 'info';
}

function statusText(value) {
  if (value === 'PASS') return '通过';
  if (value === 'WARN') return '告警';
  if (value === 'FAIL') return '失败';
  return '未知';
}

function rowClass({ row }) {
  return row.report_id === props.selectedReportId ? 'selected-quality-history-row' : '';
}

function productYearsText(row) {
  return row.summary?.product_years?.join(', ') || '-';
}

function qualityFlagsText(row) {
  return Object.entries(row.summary?.quality_counts || {}).map(([flag, count]) => `Q${flag}: ${count}`).join(', ') || '-';
}

function formatTime(value) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString('zh-CN', { hour12: false });
}
</script>

<template>
  <el-drawer v-model="visible" title="历史质检记录" size="760px" direction="rtl">
    <div class="quality-history-filterbar">
      <el-input v-model="search" :prefix-icon="Search" placeholder="按数据集、批次或路径筛选" clearable />
      <el-select v-model="status" placeholder="状态" clearable>
        <el-option label="通过" value="PASS" />
        <el-option label="告警" value="WARN" />
        <el-option label="失败" value="FAIL" />
      </el-select>
    </div>
    <el-table
      v-loading="loading"
      :data="rows"
      class="drawer-table quality-history-table"
      highlight-current-row
      :row-class-name="rowClass"
      @row-click="emit('select', $event)"
    >
      <el-table-column label="数据集" prop="dataset" min-width="130" />
      <el-table-column label="批次" prop="run_name" min-width="170" />
      <el-table-column v-if="dataType === 'product'" label="年份" min-width="150">
        <template #default="{ row }">
          <div class="table-text-clamp" :title="productYearsText(row)">{{ productYearsText(row) }}</div>
        </template>
      </el-table-column>
      <el-table-column v-if="dataType === 'carbon'" label="质量标记" min-width="150">
        <template #default="{ row }">
          <div class="table-text-clamp" :title="qualityFlagsText(row)">{{ qualityFlagsText(row) }}</div>
        </template>
      </el-table-column>
      <el-table-column label="状态" width="86">
        <template #default="{ row }">
          <el-tag :type="statusType(row.status)" size="small">{{ statusText(row.status) }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="索引行" width="82">
        <template #default="{ row }">{{ row.summary?.index_rows ?? 0 }}</template>
      </el-table-column>
      <el-table-column label="告警/失败" width="98">
        <template #default="{ row }">{{ row.summary?.warning_checks ?? 0 }}/{{ row.summary?.failed_checks ?? 0 }}</template>
      </el-table-column>
      <el-table-column label="质检时间" min-width="160">
        <template #default="{ row }">{{ formatTime(row.generated_at || row.modified_at) }}</template>
      </el-table-column>
    </el-table>
    <div class="quality-history-pagination">
      <el-pagination
        v-model:current-page="page"
        v-model:page-size="pageSize"
        :total="total"
        :page-sizes="[10, 20, 30, 50, 100]"
        layout="total, sizes, prev, pager, next, jumper"
        background
        small
        @current-change="emit('page-change', $event)"
        @size-change="emit('page-size-change', $event)"
      />
    </div>
  </el-drawer>
</template>
