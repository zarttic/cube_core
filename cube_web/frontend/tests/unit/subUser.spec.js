import { beforeEach, describe, expect, it } from 'vitest';

import { useSubUserStore } from '@/stores/subUser';

describe('sub-user permissions', () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it('matches effective permissions and gives super administrators full access', () => {
    const store = useSubUserStore();
    store.persistUserInfo({ role: '操作员', permissions: ['data_import:view'] });

    expect(store.can('data_import:view')).toBe(true);
    expect(store.can('data_import:operate')).toBe(false);
    expect(store.canAny(['system_config:view', 'data_import:view'])).toBe(true);
    expect(store.isSuperAdmin.value).toBe(false);

    store.persistUserInfo({ role: '管理员', permissions: [] });
    expect(store.can('system_config:view')).toBe(true);
    expect(store.isSuperAdmin.value).toBe(true);
  });
});
