<script setup>
import { computed } from 'vue';

import { statusPresentation } from '@/utils/status';

const props = defineProps({
  domain: { type: String, required: true },
  value: { type: String, default: '' },
  size: { type: String, default: 'default' },
});

const presentation = computed(() => statusPresentation(props.domain, props.value));
</script>

<template>
  <el-tag :type="presentation.type" :size="size">{{ presentation.label }}</el-tag>
</template>
