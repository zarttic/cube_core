import { createMemoryHistory, createRouter } from 'vue-router';
import { describe, expect, it, vi } from 'vitest';

import router, { installGuards, safeLocalTarget } from '@/router';
import { navItems } from '@/data/navigation';

describe('router guards', () => {
  it('exposes only the unified data management route', () => {
    expect(router.hasRoute('data-management')).toBe(true);
    expect(router.hasRoute('datasets')).toBe(false);
  });

  it('hides management, quality and config entries from the global navigation', () => {
    const labels = navItems(true).map((item) => item.label);

    expect(labels).not.toContain('数据管理与入库');
    expect(labels).not.toContain('自动化质检');
    expect(labels).not.toContain('系统配置');
    expect(labels).toContain('分析就绪数据剖分');
    expect(labels).toContain('全球离散格网模型与编码');
  });

  it('sends an unauthenticated protected route to the auth redirect with its local target', async () => {
    const redirectToAuth = vi.fn();
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [{ path: '/data-management', component: { template: '<div />' }, meta: { requiresAuth: true, requiresAdmin: true } }],
    });
    installGuards(router, {
      ready: () => true,
      authenticated: () => false,
      admin: () => false,
      redirectToAuth,
    });

    await router.push('/data-management?status=completed');

    expect(redirectToAuth).toHaveBeenCalledWith('/data-management?status=completed');
  });

  it('redirects a non-admin away from an administrator route', async () => {
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [
        { path: '/partition', component: { template: '<div />' }, meta: { requiresAuth: true, requiresAdmin: true } },
        { path: '/encoding', name: 'encoding', component: { template: '<div />' } },
      ],
    });
    installGuards(router, {
      ready: () => true,
      authenticated: () => true,
      admin: () => false,
      redirectToAuth: vi.fn(),
    });

    await router.push('/partition');

    expect(router.currentRoute.value.fullPath).toBe('/encoding');
  });

  it('only accepts a same-origin local callback target', () => {
    expect(safeLocalTarget('/data-management?status=completed')).toBe('/data-management?status=completed');
    expect(safeLocalTarget('//portal.example')).toBe('');
    expect(safeLocalTarget('/\\portal.example')).toBe('');
    expect(safeLocalTarget('https://portal.example')).toBe('');
  });
});
