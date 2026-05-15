<script setup>
import { computed, onBeforeUnmount, onMounted, ref } from 'vue';
import { navItems, normalizePath } from '@/data/navigation';
import HomeView from '@/views/HomeView.vue';
import PartitionView from '@/views/PartitionView.vue';
import EncodingView from '@/views/EncodingView.vue';

const currentPath = ref(normalizePath(window.location.pathname));

const pageMap = {
  '/': HomeView,
  '/partition': PartitionView,
  '/quality': PartitionView,
  '/encoding': EncodingView,
};

const currentView = computed(() => pageMap[currentPath.value] || HomeView);

function goInternal(path) {
  if (path === currentPath.value) return;
  window.history.pushState({}, '', path);
  currentPath.value = path;
}

function handlePopState() {
  currentPath.value = normalizePath(window.location.pathname);
}

onMounted(() => window.addEventListener('popstate', handlePopState));
onBeforeUnmount(() => window.removeEventListener('popstate', handlePopState));
</script>

<template>
  <div class="app-shell">
    <header class="portal-header">
      <div class="portal-header-inner">
        <div class="portal-brand">
          <div class="portal-logo" aria-hidden="true">
            <svg viewBox="0 0 24 24">
              <circle cx="12" cy="12" r="10"></circle>
              <path d="M2 12h20"></path>
              <path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10"></path>
            </svg>
          </div>
          <div class="portal-brand-text">
            <div class="portal-brand-title">分析就绪数据剖分管理系统</div>
            <div class="portal-brand-subtitle">Analysis-Ready Data Partitioning Management System</div>
          </div>
        </div>
        <nav class="portal-nav" aria-label="主导航">
          <template v-for="item in navItems" :key="item.label">
            <a
              v-if="item.kind === 'internal'"
              href="#"
              :class="{ active: currentPath === item.path }"
              @click.prevent="goInternal(item.path)"
            >
              {{ item.label }}
            </a>
            <a v-else :href="item.url">{{ item.label }}</a>
          </template>
        </nav>
        <div class="portal-header-side">
          <span class="user-chip">系统总览</span>
        </div>
      </div>
    </header>

    <main>
      <component :is="currentView" />
    </main>
  </div>
</template>
