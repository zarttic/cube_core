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
  it('groups band units below their scene in the dataset detail', async () => {
    const wrapper = mount(DatasetDetailDrawer, {
      props: {
        visible: true, datasetId: 'dataset-1', activeTab: 'scenes',
        detail: {
          overview: {
            dataset_id: 'dataset-1', data_type: 'optical',
            grid_summary: {
              geohash: { partition: 0, quality: 0, ingest: 0, total: 1 },
              mgrs: { partition: 1, quality: 1, ingest: 1, total: 1 },
              isea4h: { partition: 0, quality: 0, ingest: 0, total: 1 },
            },
          },
          scenes: { items: [{
            scene_id: 'scene-1', scene_key: '景一', status: 'available',
            acquisition_time: '2026-07-18T00:00:00+08:00',
            bands: [{
              band_unit_id: 'band-1', asset_id: 'asset-1', band_code: 'B04',
              band_name: '红光', band_type: 'spectral', unit: 'reflectance',
              grid_statuses: [{
                grid_type: 'mgrs', grid_level: 1, partition_status: 'completed',
                quality_status: 'pass', ingest_status: 'completed',
              }],
            }],
          }] },
        },
      },
      global: {
        stubs: {
          DetailDrawer: { template: '<div><slot /></div>' },
          StatusTag: { template: '<span />' },
          AppTable: { template: '<div />' },
          'el-tabs': { template: '<div><slot /></div>' },
          'el-tab-pane': { template: '<section><slot name="label" /><slot /></section>' },
          'el-button': { template: '<button><slot /></button>' },
          'el-tooltip': { template: '<span><slot /></span>' },
          'el-form': { template: '<form><slot /></form>' },
          'el-form-item': { template: '<div><slot /></div>' },
          'el-input': { template: '<input />' },
          'el-dialog': { template: '<div />' },
          'el-descriptions': { template: '<div><slot /></div>' },
          'el-descriptions-item': { template: '<div><slot /></div>' },
          'el-empty': { template: '<div />' },
          'el-pagination': { template: '<div />' },
          'el-table-column': { template: '<div />' },
        },
      },
    });
    wrappers.push(wrapper);

    expect(wrapper.get('[data-testid="managed-scene-scene-1"]').text()).toContain('景一');
    await wrapper.get('[data-testid="managed-scene-scene-1"] .scene-toggle').trigger('click');
    expect(wrapper.get('[data-testid="managed-band-band-1"]').text()).toContain('B04 · 红光 [reflectance]');
    expect(wrapper.get('[data-testid="managed-band-band-1"]').text()).toContain('band-1');
    expect(wrapper.get('[data-testid="dataset-grid-summary-geohash"]').text()).toContain('剖分 0/1');
    expect(wrapper.get('[data-testid="dataset-grid-summary-geohash"]').text()).toContain('质检 0/1');
    expect(wrapper.get('[data-testid="dataset-grid-summary-geohash"]').text()).toContain('入库 0/1');
    expect(wrapper.get('[data-testid="dataset-grid-summary-mgrs"]').text()).toContain('质检 1/1');
    expect(wrapper.findAll('.grid-summary-row')).toHaveLength(3);
    const gridTags = wrapper.findAll('.band-grid-status');
    expect(gridTags.find((tag) => tag.text().includes('经纬度格网')).classes()).toContain('is-empty');
    expect(gridTags.find((tag) => tag.text().includes('平面格网')).classes()).toContain('is-ingested');
    expect(wrapper.find('.band-workflow').exists()).toBe(false);
  });

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
          'el-pagination': { template: '<div />' },
        },
      },
    });
    wrappers.push(wrapper);
    expect(wrapper.text()).toContain('概览');
    expect(wrapper.text()).toContain('数据');
    expect(wrapper.text()).not.toContain('资产');
    expect(wrapper.text()).toContain('数据');
    expect(wrapper.text()).toContain('剖分版本');
    expect(wrapper.text()).toContain('瓦片');
    expect(wrapper.text()).not.toContain('索引');
    expect(wrapper.text()).toContain('经纬度格网');
    expect(wrapper.text()).toContain('质检');
    expect(wrapper.text()).not.toContain('发布');
    expect(wrapper.text()).toContain('入库记录');
    expect(wrapper.text()).toContain('来源追踪');
  });

  it('does not expose manual ingest in the data-management detail', async () => {
    const wrapper = mount(DatasetDetailDrawer, {
      props: {
        visible: true,
        datasetId: 'dataset-ready',
        detail: {
          overview: {
            dataset_id: 'dataset-ready', current_output_version: 'v1',
            quality_status: 'pass', ingest_status: 'pending',
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
          'el-checkbox': { props: ['modelValue', 'label'], template: '<span />' },
          'el-tooltip': { template: '<span><slot /></span>' },
          'el-form': { template: '<form><slot /></form>' },
          'el-form-item': { template: '<div><slot /></div>' },
          'el-input': { template: '<input />' },
          'el-dialog': { template: '<div />' },
          'el-descriptions': { template: '<div><slot /></div>' },
          'el-descriptions-item': { template: '<div><slot /></div>' },
          'el-empty': { template: '<div />' },
          'el-pagination': { template: '<div />' },
        },
      },
    });
    wrappers.push(wrapper);

    expect(wrapper.text()).not.toContain('手动入库');

    await wrapper.setProps({
      detail: {
        overview: {
          dataset_id: 'dataset-ready', current_output_version: 'v1',
            quality_status: 'fail', ingest_status: 'pending',
        },
      },
    });
    expect(wrapper.text()).not.toContain('手动入库');
  });
});
