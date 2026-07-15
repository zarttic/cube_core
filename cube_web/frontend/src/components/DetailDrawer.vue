<script setup>
import { computed } from 'vue';

const props = defineProps({
  visible: Boolean,
  title: { type: String, default: '' },
  loading: Boolean,
  size: { type: [String, Number], default: '720px' },
  testId: { type: String, default: '' },
});
const emit = defineEmits(['update:visible', 'closed']);
const open = computed({
  get: () => props.visible,
  set: (value) => emit('update:visible', value),
});
</script>

<template>
  <el-drawer v-model="open" :data-testid="testId || undefined" :title="title" :size="size" :modal="false" modal-class="detail-drawer-overlay" @closed="$emit('closed')">
    <div v-loading="loading">
      <slot />
    </div>
  </el-drawer>
</template>

<style>
.detail-drawer-overlay { pointer-events: none; }
.detail-drawer-overlay .el-drawer { pointer-events: auto; }
</style>
