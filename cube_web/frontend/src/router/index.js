import { createRouter, createWebHistory } from 'vue-router';

import { authRequired } from '@/config';
import { portalHomeUrl } from '@/data/navigation';
import { useSubUserStore } from '@/stores/subUser';

const viewModules = import.meta.glob('../views/*.vue');

function view(name) {
  return viewModules[`../views/${name}.vue`];
}

let resolveApplicationAuth;
const applicationAuthReady = new Promise((resolve) => {
  resolveApplicationAuth = resolve;
});

export function resolveApplicationAuthReady() {
  resolveApplicationAuth();
}

export function safeLocalTarget(value) {
  const target = String(value || '');
  if (!target.startsWith('/') || target.startsWith('//') || target.includes('\\')) return '';
  return target;
}

export function installGuards(routerInstance, dependencies) {
  const {
    ready,
    authenticated,
    admin,
    redirectToAuth,
  } = dependencies;

  routerInstance.beforeEach(async (to) => {
    await ready();
    if (to.meta.requiresAuth && !authenticated()) {
      redirectToAuth(safeLocalTarget(to.fullPath) || '/');
      return false;
    }
    if (to.meta.requiresAdmin && !admin()) return { name: 'encoding' };
    return true;
  });
}

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/partition', name: 'partition', component: view('PartitionView'), meta: { requiresAuth: true, requiresAdmin: true } },
    { path: '/data-management', name: 'data-management', component: view('DataManagementView'), meta: { requiresAuth: true, requiresAdmin: true } },
    { path: '/quality', name: 'quality', component: view('QualityView'), meta: { requiresAuth: true, requiresAdmin: true } },
    { path: '/encoding', name: 'encoding', component: view('EncodingView'), meta: { requiresAuth: true } },
    { path: '/config', name: 'config', component: view('ConfigView'), meta: { requiresAuth: true, requiresAdmin: true } },
    { path: '/callback', name: 'callback', component: view('PartitionView') },
    { path: '/', redirect: '/partition' },
    { path: '/:pathMatch(.*)*', redirect: '/partition' },
  ],
});

installGuards(router, {
  ready: () => applicationAuthReady,
  authenticated: () => !authRequired() || useSubUserStore().isAuthenticated.value,
  admin: () => !authRequired() || useSubUserStore().role.value === '管理员',
  redirectToAuth: (target) => useSubUserStore().redirectToAuth(target),
  portalHomeUrl,
});

export default router;
