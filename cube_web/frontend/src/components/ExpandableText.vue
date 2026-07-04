<script setup>
import { computed, ref, watch } from 'vue';

const props = defineProps({
  text: {
    type: [String, Number],
    default: '',
  },
  lines: {
    type: Number,
    default: 2,
  },
  threshold: {
    type: Number,
    default: 120,
  },
  emptyText: {
    type: String,
    default: '-',
  },
  buttonClass: {
    type: String,
    default: '',
  },
});

const expanded = ref(false);

const normalizedText = computed(() => {
  const value = props.text == null ? '' : String(props.text);
  return value.trim();
});

const displayText = computed(() => normalizedText.value || props.emptyText);
const expandable = computed(() => (
  normalizedText.value.length > props.threshold || normalizedText.value.includes('\n')
));

watch(() => props.text, () => {
  expanded.value = false;
});
</script>

<template>
  <div class="expandable-text">
    <div
      class="expandable-text-body"
      :class="{ expanded }"
      :style="expanded ? undefined : { '-webkit-line-clamp': String(lines) }"
      :title="displayText"
    >
      {{ displayText }}
    </div>
    <button
      v-if="expandable"
      type="button"
      class="expandable-text-toggle"
      :class="buttonClass"
      @click="expanded = !expanded"
    >
      {{ expanded ? '收起' : '展开' }}
    </button>
  </div>
</template>

<style scoped>
.expandable-text {
  min-width: 0;
}

.expandable-text-body {
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  word-break: break-word;
}

.expandable-text-body:not(.expanded) {
  display: -webkit-box;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.expandable-text-toggle {
  margin-top: 6px;
  padding: 0;
  border: 0;
  background: transparent;
  color: var(--primary);
  font-size: 12px;
  line-height: 1;
  cursor: pointer;
}

.expandable-text-toggle:hover {
  text-decoration: underline;
}
</style>
