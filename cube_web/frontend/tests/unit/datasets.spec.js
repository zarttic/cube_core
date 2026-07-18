import { createPinia, setActivePinia } from 'pinia';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { mount } from '@vue/test-utils';

const deferred = [];
vi.mock('@/api/client', () => ({
  requestGet: vi.fn(() => new Promise((resolve) => deferred.push(resolve))),
  requestJson: vi.fn(),
  requestPost: vi.fn(),
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

  it('uses scene-level dataset filters without a load-batch ownership filter', async () => {
    setActivePinia(createPinia());
    const store = useDatasetsStore();
    store.filters.dataType = 'optical';
    store.filters.ingestStatus = 'completed';
    const pending = store.loadList();
    deferred.at(-1)({ items: [], total: 0, page: 1, page_size: 20 });
    await pending;
    const { requestGet } = await import('@/api/client');
    expect(requestGet.mock.calls.at(-1)[0]).toContain('data_type=optical');
    expect(requestGet.mock.calls.at(-1)[0]).toContain('ingest_status=completed');
    expect(requestGet.mock.calls.at(-1)[0]).toContain('/v1/datasets?');
    expect(requestGet.mock.calls.at(-1)[0]).not.toContain('batch_id');
    expect(requestGet.mock.calls.at(-1)[0]).not.toContain('datasetIds');
  });
});

describe('DatasetDetailDrawer', () => {
  it('renders scene, ingest and provenance alongside the existing detail tabs', () => {
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
          'el-tooltip': { template: '<span><slot /></span>' },
          'el-form': { template: '<form><slot /></form>' },
          'el-form-item': { template: '<div><slot /></div>' },
          'el-input': { template: '<input />' },
          'el-dialog': { template: '<div><slot /><slot name="footer" /></div>' },
          'el-descriptions': { template: '<div><slot /></div>' },
          'el-descriptions-item': { template: '<div><slot /></div>' },
          'el-empty': { template: '<div />' },
        },
      },
    });
    wrappers.push(wrapper);
    expect(wrapper.text()).toContain('概览');
    expect(wrapper.text()).toContain('景');
    expect(wrapper.text()).not.toContain('资产');
    expect(wrapper.text()).toContain('波段');
    expect(wrapper.text()).toContain('剖分版本');
    expect(wrapper.text()).toContain('瓦片');
    expect(wrapper.text()).toContain('索引');
    expect(wrapper.text()).toContain('格网');
    expect(wrapper.text()).toContain('质检');
    expect(wrapper.text()).toContain('发布');
    expect(wrapper.text()).toContain('入库记录');
    expect(wrapper.text()).toContain('来源追踪');
  });

  it('enables publication only after quality passes and ingest completes', async () => {
    const wrapper = mount(DatasetDetailDrawer, {
      props: {
        visible: true,
        datasetId: 'dataset-ready',
        detail: {
          overview: {
            dataset_id: 'dataset-ready', current_output_version: 'v1',
            quality_status: 'pass', ingest_status: 'completed', publish_status: 'unpublished',
          },
        },
      },
      global: {
        stubs: {
          DetailDrawer: { template: '<div><slot /></div>' },
          AppTable: { template: '<div><slot /></div>' },
          StatusTag: { template: '<span />' },
          'el-tabs': { template: '<div><slot /></div>' },
          'el-tab-pane': { template: '<section><slot name="label" /><slot /></section>' },
          'el-table-column': { template: '<div />' },
          'el-button': { props: ['disabled'], template: '<button :disabled="disabled"><slot /></button>' },
          'el-tooltip': { template: '<span><slot /></span>' },
          'el-form': { template: '<form><slot /></form>' },
          'el-form-item': { template: '<div><slot /></div>' },
          'el-input': { template: '<input />' },
          'el-dialog': { template: '<div />' },
          'el-descriptions': { template: '<div><slot /></div>' },
          'el-descriptions-item': { template: '<div><slot /></div>' },
          'el-empty': { template: '<div />' },
        },
      },
    });
    wrappers.push(wrapper);

    const publish = wrapper.findAll('button').find((button) => button.text() === '发布');
    expect(publish.attributes('disabled')).toBeUndefined();
    await publish.trigger('click');
    expect(wrapper.emitted('publish')).toHaveLength(1);

    await wrapper.setProps({
      detail: {
        overview: {
          dataset_id: 'dataset-ready', current_output_version: 'v1',
          quality_status: 'pass', ingest_status: 'partial_failure', publish_status: 'unpublished',
        },
      },
    });
    expect(wrapper.findAll('button').find((button) => button.text() === '发布').attributes('disabled')).toBeDefined();
  });
});
