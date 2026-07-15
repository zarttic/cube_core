<script setup>
defineProps({
  data: { type: Array, default: () => [] },
  loading: Boolean,
  rowKey: { type: String, default: '' },
  page: { type: Number, default: 1 },
  pageSize: { type: Number, default: 20 },
  pageSizes: { type: Array, default: () => [20, 50, 100] },
  total: { type: Number, default: 0 },
  pagination: { type: Boolean, default: true },
});

defineEmits(['current-change', 'size-change', 'row-click']);
</script>

<template>
  <el-table
    v-loading="loading"
    :data="data"
    :row-key="rowKey || undefined"
    empty-text="暂无数据"
    @row-click="$emit('row-click', $event)"
  >
    <slot />
  </el-table>
  <el-pagination
    v-if="pagination"
    :current-page="page"
    :page-size="pageSize"
    :page-sizes="pageSizes"
    :total="total"
    background
    layout="total, sizes, prev, pager, next"
    @current-change="$emit('current-change', $event)"
    @size-change="$emit('size-change', $event)"
  />
</template>
