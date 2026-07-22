import { createPinia, setActivePinia } from 'pinia';
import { describe, expect, it, vi } from 'vitest';

const deferred = [];
vi.mock('@/api/client', () => ({
  request: vi.fn(),
  requestGet: vi.fn(() => new Promise((resolve) => deferred.push(resolve))),
  requestPost: vi.fn(),
  download: vi.fn(),
}));

import { useQualityStore } from '@/stores/quality';

describe('quality store', () => {
  it('keeps the second quality detail when the first deferred response resolves last', async () => {
    setActivePinia(createPinia());
    const store = useQualityStore();
    const first = store.openDetail('quality-run-a');
    const second = store.openDetail('quality-run-b');
    deferred[1]({ quality_run_id: 'quality-run-b', status: 'pass', results_complete: true });
    deferred[0]({ quality_run_id: 'quality-run-a', status: 'fail', results_complete: true });
    await Promise.all([first, second]);
    expect(store.selectedQualityRunId).toBe('quality-run-b');
    expect(store.detail.quality_run_id).toBe('quality-run-b');
    expect(store.detail.status).toBe('pass');
  });

  it('exports all errors without filters and filtered errors without visible-page parameters', async () => {
    setActivePinia(createPinia());
    const store = useQualityStore();
    store.selectedQualityRunId = 'quality-run-a';
    store.errorFilters.ruleCode = 'asset_readability';
    store.errorPage = 3;
    store.errorPageSize = 50;
    await store.exportErrors('csv', false);
    await store.exportErrors('json', true);
    const { download } = await import('@/api/client');
    expect(download.mock.calls[0][0]).toContain('format=csv');
    expect(download.mock.calls[0][0]).not.toMatch(/rule_code=|page=|page_size=/);
    expect(download.mock.calls[1][0]).toContain('format=json');
    expect(download.mock.calls[1][0]).toContain('rule_code=asset_readability');
    expect(download.mock.calls[1][0]).not.toMatch(/(?:\?|&)page=|(?:\?|&)page_size=/);
  });

  it('loads the rule catalog once, drops retired rules, and exports a record without page filters', async () => {
    setActivePinia(createPinia());
    const store = useQualityStore();
    const pending = store.loadRuleCatalog();
    deferred.at(-1)({
      rule_set_version: 'rules-v1',
      items: [
        { code: 'asset_readability', mandatory: true },
        { code: 'product_band_contract', mandatory: true },
        { code: 'carbon_observation_duplicates', mandatory: true },
        { code: 'carbon_footprints', mandatory: true },
      ],
    });
    await pending;
    await store.loadRuleCatalog();
    expect(store.ruleCatalog.items.map((item) => item.code)).toEqual(['asset_readability']);
    await store.exportRunErrors({ quality_run_id: 'quality-run-a', dataset_code: 'DS-A' }, 'csv');

    const { download, requestGet } = await import('@/api/client');
    expect(requestGet.mock.calls.filter(([url]) => url === '/v1/quality/rules')).toHaveLength(1);
    expect(download).toHaveBeenLastCalledWith('/v1/quality/records/quality-run-a/errors/export?format=csv');
  });

  it('persists one optional rule toggle and refreshes the catalog', async () => {
    setActivePinia(createPinia());
    const store = useQualityStore();
    const { request } = await import('@/api/client');
    request.mockResolvedValue({
      rule_set_version: 'rules-v2',
      enabled_optional_rules: ['carbon_schema'],
      items: [
        { code: 'asset_readability', mandatory: true, enabled: true },
        { code: 'carbon_schema', mandatory: false, enabled: true },
        { code: 'carbon_footprints', mandatory: false, enabled: false },
      ],
    });

    await store.updateRuleSetting('carbon_schema', true);

    expect(request).toHaveBeenCalledWith('/v1/quality/rules/carbon_schema/enabled', {
      method: 'PUT', body: { enabled: true },
    });
    expect(store.ruleCatalog.items.map((item) => item.code)).toEqual(['asset_readability', 'carbon_schema']);
    expect(store.ruleCatalog.items[1].enabled).toBe(true);
  });
});
