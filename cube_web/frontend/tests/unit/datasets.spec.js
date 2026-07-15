import { createPinia, setActivePinia } from 'pinia';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { mount } from '@vue/test-utils';

const deferred = [];
vi.mock('@/api/client', () => ({
  requestGet: vi.fn(() => new Promise((resolve) => deferred.push(resolve))),
}));

import { useDatasetsStore } from '@/stores/datasets';
import DatasetDetailDrawer from '@/views/datasets/DatasetDetailDrawer.vue';

const wrappers = [];
afterEach(() => wrappers.splice(0).forEach((wrapper) => wrapper.unmount()));

describe('datasets store', () => {
  it('keeps only the most recently opened dataset detail', async () => {
    setActivePinia(createPinia());
    const store = useDatasetsStore();
    const first = store.openDetail('dataset-a');
    const second = store.openDetail('dataset-b');
    deferred[1]({ dataset_id: 'dataset-b', dataset_title: 'B' });
    deferred[0]({ dataset_id: 'dataset-a', dataset_title: 'A' });
    await Promise.all([first, second]);
    expect(store.selectedDatasetId).toBe('dataset-b');
    expect(store.detail.overview.dataset_title).toBe('B');
  });

  it('uses only normalized M3 list query names', async () => {
    setActivePinia(createPinia());
    const store = useDatasetsStore();
    store.filters.dataType = 'optical';
    store.filters.batchId = 'batch-a';
    const pending = store.loadList();
    deferred.at(-1)({ items: [], total: 0, page: 1, page_size: 20 });
    await pending;
    const { requestGet } = await import('@/api/client');
    expect(requestGet.mock.calls.at(-1)[0]).toContain('data_type=optical');
    expect(requestGet.mock.calls.at(-1)[0]).toContain('batch_id=batch-a');
    expect(requestGet.mock.calls.at(-1)[0]).not.toContain('datasetIds');
  });
});

describe('DatasetDetailDrawer', () => {
  it('renders all eight required detail tabs', () => {
    const wrapper = mount(DatasetDetailDrawer, {
      props: { visible: true, datasetId: 'dataset-1', detail: {} },
      global: {
        stubs: {
          DetailDrawer: { template: '<div><slot /></div>' },
          AppTable: { template: '<div><slot /></div>' },
          'el-tabs': { template: '<div><slot /></div>' },
          'el-tab-pane': { template: '<section><slot name="label" /><slot /></section>' },
          'el-table-column': { template: '<div><slot :row="{}" /></div>' },
          'el-button': { template: '<button><slot /></button>' },
          'el-descriptions': { template: '<div><slot /></div>' },
          'el-descriptions-item': { template: '<div><slot /></div>' },
          'el-empty': { template: '<div />' },
        },
      },
    });
    wrappers.push(wrapper);
    expect(wrapper.text()).toContain('概览');
    expect(wrapper.text()).toContain('资产');
    expect(wrapper.text()).toContain('波段');
    expect(wrapper.text()).toContain('瓦片');
    expect(wrapper.text()).toContain('索引');
    expect(wrapper.text()).toContain('格网');
    expect(wrapper.text()).toContain('质检');
    expect(wrapper.text()).toContain('发布');
  });
});
