import { createMemoryHistory, createRouter } from 'vue-router';
import { describe, expect, it, vi } from 'vitest';

import { installGuards, safeLocalTarget } from '@/router';

describe('router guards', () => {
  it('sends an unauthenticated protected route to the auth redirect with its local target', async () => {
    const redirectToAuth = vi.fn();
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [{ path: '/datasets', component: { template: '<div />' }, meta: { requiresAuth: true, requiresAdmin: true } }],
    });
    installGuards(router, {
      ready: () => true,
      authenticated: () => false,
      admin: () => false,
      redirectToAuth,
    });

    await router.push('/datasets?status=completed');

    expect(redirectToAuth).toHaveBeenCalledWith('/datasets?status=completed');
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
    expect(safeLocalTarget('/datasets?status=completed')).toBe('/datasets?status=completed');
    expect(safeLocalTarget('//portal.example')).toBe('');
    expect(safeLocalTarget('/\\portal.example')).toBe('');
    expect(safeLocalTarget('https://portal.example')).toBe('');
  });
});
