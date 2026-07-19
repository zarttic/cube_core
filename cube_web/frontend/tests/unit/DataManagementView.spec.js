import { flushPromises, mount } from '@vue/test-utils';
import { afterEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/api/client', () => ({ requestGet: vi.fn(), requestPost: vi.fn() }));

import { requestGet } from '@/api/client';
import DataManagementView from '@/views/DataManagementView.vue';
import LoadBatchesView from '@/views/data/LoadBatchesView.vue';

const wrappers = [];
afterEach(() => {
  wrappers.splice(0).forEach((wrapper) => wrapper.unmount());
  vi.clearAllMocks();
});

const commonStubs = {
  'el-alert': { template: '<div />' },
  'el-button': { template: '<button><slot /></button>' },
  'el-empty': { props: ['description'], template: '<div>{{ description }}</div>' },
  'el-icon': { template: '<i><slot /></i>' },
  'el-input': { template: '<input />' },
};

describe('DataManagementView', () => {
  it('contains dataset, load batch and ingest run views in one module', () => {
    const wrapper = mount(DataManagementView, {
      global: {
        stubs: {
          DatasetsView: { template: '<div data-testid="datasets-view" />' },
          LoadBatchesView: { template: '<div data-testid="load-batches-view" />' },
          IngestView: { template: '<div data-testid="ingest-runs-view" />' },
          'el-tabs': { template: '<div><slot /></div>' },
          'el-tab-pane': { props: ['label'], template: '<section :data-label="label"><slot /></section>' },
        },
      },
    });
    wrappers.push(wrapper);

    expect(wrapper.find('h1').exists()).toBe(false);
    expect(wrapper.findAll('[data-label]').map((item) => item.attributes('data-label'))).toEqual(['数据管理', '载入批次', '数据入库']);
  });
});

describe('LoadBatchesView', () => {
  it('expands a batch into dataset groups and scenes', async () => {
    requestGet
      .mockResolvedValueOnce({
        load_batches: [{ load_batch_id: 'batch-a', batch_name: '山东光学批次', status: 'succeeded', dataset_count: 2, scene_count: 3 }],
      })
      .mockResolvedValueOnce({
        datasets: [{
          dataset_id: 'dataset-a', dataset_code: 'DS-A', dataset_title: '山东光学数据集',
          scenes: [{ scene_id: 'scene-a', scene_key: '山东光学景 A', source_asset_id: 'asset-a', crs: 'EPSG:4326' }],
        }],
      });
    const wrapper = mount(LoadBatchesView, {
      global: {
        stubs: commonStubs,
        directives: { loading: () => {} },
      },
    });
    wrappers.push(wrapper);
    await flushPromises();

    expect(requestGet).toHaveBeenNthCalledWith(1, '/v1/partition/load-batches?limit=100&status=succeeded', expect.any(Object));
    expect(wrapper.text()).toContain('山东光学批次');
    await wrapper.get('[data-testid="load-batch-batch-a"]').trigger('click');
    await flushPromises();

    expect(requestGet).toHaveBeenNthCalledWith(2, '/v1/partition/load-batches/batch-a/scenes?limit=10000', expect.any(Object));
    expect(wrapper.text()).toContain('山东光学数据集');
    expect(wrapper.text()).toContain('山东光学景 A');
  });
});
