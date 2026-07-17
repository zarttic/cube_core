import { flushPromises, mount } from '@vue/test-utils';
import { createPinia, setActivePinia } from 'pinia';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import PartitionView from '@/views/PartitionView.vue';
import { usePartitionStore } from '@/stores/partition';

const { requestJson } = vi.hoisted(() => ({
  requestJson: vi.fn(async () => ({
    cells: [{ space_code: 'grid-1', grid_level: 6, geometry: null, bbox: [100, 20, 101, 21] }],
  })),
}));

vi.mock('@/api/client', () => ({
  requestGet: vi.fn(async () => ({ items: [], total: 0, page: 1, page_size: 20 })),
  requestJson,
}));

const GlobeMapStub = {
  name: 'GlobeMap',
  props: ['geometries'],
  template: '<div data-testid="partition-map-stub" :data-geometry-count="geometries.length" />',
};
const legacyLayoutStubs = {
  'el-button': { template: '<button v-bind="$attrs" @click="$emit(\'click\')"><slot /></button>' },
  'el-tag': { template: '<span><slot /></span>' },
};

describe('PartitionView map workspace', () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    requestJson.mockReset().mockResolvedValue({
      cells: [{ space_code: 'grid-1', grid_level: 6, geometry: null, bbox: [100, 20, 101, 21] }],
    });
  });

  it('renders the map and previews selected asset bounds', async () => {
    const store = usePartitionStore();
    store.form.datasets = [{
      dataset_id: 'dataset-a',
      dataset_title: 'Dataset A',
      data_type: 'optical',
      scenes: [{ scene_id: 'scene-a', source_batch_ids: ['loader-batch-a'] }],
      assets: [{ source_asset_id: 'asset-a', bbox: [100, 20, 101, 21] }],
      bands: [],
    }];
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          GlobeMap: GlobeMapStub,
          ...legacyLayoutStubs,
          GridParameters: true,
          BatchAssetsPanel: true,
          ExecutionResultPanel: true,
          TaskQueuePanel: true,
          QualityView: true,
          DatasetsView: true,
          'el-drawer': { template: '<div><slot /></div>' },
        },
      },
    });

    expect(wrapper.get('[data-testid="partition-module-optical"]').classes()).toContain('active');
    expect(store.form).not.toHaveProperty('batchId');
    expect(wrapper.find('.workspace').exists()).toBe(true);
    expect(wrapper.find('.workspace-sidebar').exists()).toBe(true);
    expect(wrapper.find('.map-panel').exists()).toBe(true);
    expect(wrapper.find('.result-panel').exists()).toBe(true);
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('1');
    await wrapper.get('[data-testid="load-map"]').trigger('click');
    await flushPromises();
    expect(requestJson).toHaveBeenCalledWith('/v1/grid/cover', expect.objectContaining({
      grid_type: 'geohash',
      requested_grid_level: 4,
      bbox: [100, 20, 101, 21],
    }));
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('2');
    await wrapper.get('[data-testid="reset-grid"]').trigger('click');
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('1');

    await wrapper.get('[data-testid="partition-module-carbon"]').trigger('click');
    expect(wrapper.text()).toContain('碳卫星空间预览');
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('0');
  });

  it('loads independent Geohash, MGRS and ISEA4H layers together at their exact selected levels', async () => {
    requestJson.mockImplementation(async (_path, payload) => {
      return {
        cells: [{
          space_code: `${payload.grid_type}-${payload.requested_grid_level}`,
          grid_level: payload.requested_grid_level,
          geometry: null,
          bbox: payload.bbox,
        }],
      };
    });
    const store = usePartitionStore();
    store.form.datasets = [
      {
        dataset_id: 'higlass', dataset_title: 'HiGLASS', data_type: 'optical',
        assets: [{ source_asset_id: 'higlass-a', bbox: [121.5, 30, 122.7, 31.2] }],
        scenes: [{ scene_id: 'scene-higlass' }],
        partition: { grid_type: 'geohash', requested_grid_level: 6, partition_method: 'logical' },
      },
      {
        dataset_id: 'history', dataset_title: 'History', data_type: 'optical',
        assets: [{ source_asset_id: 'history-a', bbox: [114.7, 33.8, 122.7, 38.5] }],
        scenes: [{ scene_id: 'scene-history' }],
        partition: { grid_type: 'mgrs', requested_grid_level: 2, partition_method: 'logical' },
      },
      {
        dataset_id: 'recent', dataset_title: 'Recent', data_type: 'optical',
        assets: [{ source_asset_id: 'recent-a', bbox: [114.7, 33.8, 122.7, 38.5] }],
        scenes: [{ scene_id: 'scene-recent' }],
        partition: { grid_type: 'isea4h', requested_grid_level: 6, partition_method: 'entity' },
      },
    ];
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          GlobeMap: GlobeMapStub,
          ...legacyLayoutStubs,
          GridParameters: true,
          BatchAssetsPanel: true,
          ExecutionResultPanel: true,
          TaskQueuePanel: true,
          QualityView: true,
          DataManagementView: true,
          'el-drawer': { template: '<div><slot /></div>' },
        },
      },
    });

    await wrapper.get('[data-testid="load-map"]').trigger('click');
    await flushPromises();

    const requests = requestJson.mock.calls.map(([, payload]) => [payload.grid_type, payload.requested_grid_level]);
    expect(requests).toEqual(expect.arrayContaining([
      ['geohash', 6], ['mgrs', 2], ['isea4h', 6],
    ]));
    expect(requests).not.toContainEqual(['mgrs', 1]);
    expect(wrapper.vm.gridGeometries.map((item) => item.color)).toEqual([
      '#2f73d9', '#16836f', '#d97706',
    ]);
    expect(wrapper.text()).toContain('经纬度格网 · 第 6 级');
    expect(wrapper.text()).toContain('平面格网 · 第 2 级');
    expect(wrapper.text()).toContain('六边形格网 · 第 6 级');
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('6');
  });

  it('exposes product, quality, ingest and task pages as peer modules', async () => {
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          ...legacyLayoutStubs,
          GlobeMap: GlobeMapStub,
          GridParameters: true,
          BatchAssetsPanel: true,
          ExecutionResultPanel: true,
          TaskQueuePanel: true,
          QualityView: { template: '<div data-testid="quality-view-stub">quality</div>' },
          DataManagementView: { template: '<div data-testid="data-management-view-stub">data</div>' },
          'el-drawer': { template: '<div><slot /></div>' },
        },
      },
    });

    expect(wrapper.findAll('.module-tab')).toHaveLength(7);
    expect(wrapper.find('.partition-module-nav').exists()).toBe(false);
    await wrapper.get('[data-testid="partition-module-quality"]').trigger('click');
    expect(wrapper.get('[data-testid="quality-view-stub"]').exists()).toBe(true);
    await wrapper.get('[data-testid="partition-module-ingest"]').trigger('click');
    expect(wrapper.get('[data-testid="data-management-view-stub"]').exists()).toBe(true);
  });

  it('keeps partition parameters independent between product pages', async () => {
    const store = usePartitionStore();
    store.form.datasets = [{
      dataset_id: 'dataset-a',
      data_type: 'optical',
      assets: [],
      bands: [],
      partition: { grid_type: 'geohash', requested_grid_level: 6, partition_method: 'logical' },
      grid_level_unlocked: true,
    }];
    const GridParametersStub = {
      props: ['modelValue'],
      emits: ['update:modelValue'],
      template: '<div data-testid="parameters" :data-grid-type="modelValue.gridType"><button data-testid="set-mgrs" @click="$emit(\'update:modelValue\', { ...modelValue, gridType: \'mgrs\', requestedGridLevel: 2 })">set</button></div>',
    };
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          ...legacyLayoutStubs,
          GlobeMap: GlobeMapStub,
          GridParameters: GridParametersStub,
          BatchAssetsPanel: true,
          ExecutionResultPanel: true,
          TaskQueuePanel: true,
          QualityView: true,
          DatasetsView: true,
          'el-drawer': { template: '<div><slot /></div>' },
        },
      },
    });

    await wrapper.get('[data-testid="set-mgrs"]').trigger('click');
    expect(wrapper.get('[data-testid="parameters"]').attributes('data-grid-type')).toBe('mgrs');
    expect(store.form.datasets[0].partition).toMatchObject({
      grid_type: 'mgrs',
      requested_grid_level: 1,
      partition_method: 'logical',
      cover_mode: 'intersect',
      time_granularity: 'day',
      max_cells_per_asset: 0,
    });
    await wrapper.get('[data-testid="partition-module-carbon"]').trigger('click');
    expect(wrapper.get('[data-testid="parameters"]').attributes('data-grid-type')).toBe('isea4h');
    await wrapper.get('[data-testid="partition-module-optical"]').trigger('click');
    expect(wrapper.get('[data-testid="parameters"]').attributes('data-grid-type')).toBe('mgrs');
  });

  it('keeps source load batch summaries scoped to the active product page', async () => {
    const store = usePartitionStore();
    store.form.datasets = [
      {
        dataset_id: 'dataset-optical', data_type: 'optical', assets: [],
        scenes: [{ scene_id: 'scene-optical', source_batch_ids: ['load-optical'] }],
        partition: { grid_type: 'geohash', requested_grid_level: 6, partition_method: 'logical' },
      },
      {
        dataset_id: 'dataset-carbon', data_type: 'carbon', assets: [],
        scenes: [{ scene_id: 'scene-carbon', source_batch_ids: ['load-carbon'] }],
        partition: { grid_type: 'isea4h', requested_grid_level: 5, partition_method: 'entity' },
      },
    ];
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          ...legacyLayoutStubs,
          GlobeMap: GlobeMapStub,
          GridParameters: {
            props: ['sourceBatchIds'],
            template: '<div data-testid="source-batches">{{ sourceBatchIds.join(",") }}</div>',
          },
          BatchAssetsPanel: true,
          ExecutionResultPanel: true,
          TaskQueuePanel: true,
          QualityView: true,
          DataManagementView: true,
          'el-drawer': { template: '<div><slot /></div>' },
        },
      },
    });

    expect(wrapper.get('[data-testid="source-batches"]').text()).toBe('load-optical');
    await wrapper.get('[data-testid="partition-module-carbon"]').trigger('click');
    expect(wrapper.get('[data-testid="source-batches"]').text()).toBe('load-carbon');
    await wrapper.get('[data-testid="partition-module-radar"]').trigger('click');
    expect(wrapper.get('[data-testid="source-batches"]').text()).toBe('');
  });

  it('does not show a completed product result on another product page', async () => {
    const store = usePartitionStore();
    vi.spyOn(store, 'submit').mockResolvedValue({ task_id: 'optical-task', status: 'queued' });
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          ...legacyLayoutStubs,
          GlobeMap: GlobeMapStub,
          GridParameters: { template: '<button data-testid="submit" @click="$emit(\'submit\')">submit</button>' },
          BatchAssetsPanel: true,
          ExecutionResultPanel: { props: ['result'], template: '<div data-testid="result">{{ result?.task_id || "none" }}</div>' },
          TaskQueuePanel: true,
          QualityView: true,
          DatasetsView: true,
          'el-drawer': { template: '<div><slot /></div>' },
        },
      },
    });

    await wrapper.get('[data-testid="submit"]').trigger('click');
    await flushPromises();
    expect(wrapper.get('[data-testid="result"]').text()).toBe('optical-task');
    await wrapper.get('[data-testid="partition-module-carbon"]').trigger('click');
    expect(wrapper.get('[data-testid="result"]').text()).toBe('none');
  });

  it('discards a stale map preview after switching products', async () => {
    let resolvePreview;
    requestJson.mockImplementationOnce(() => new Promise((resolve) => { resolvePreview = resolve; }));
    const store = usePartitionStore();
    store.form.datasets = [{
      dataset_id: 'dataset-a',
      dataset_title: 'Dataset A',
      data_type: 'optical',
      assets: [{ source_asset_id: 'asset-a', bbox: [100, 20, 101, 21] }],
      bands: [],
      partition: { grid_type: 'geohash', requested_grid_level: 6, partition_method: 'logical' },
    }];
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          ...legacyLayoutStubs,
          GlobeMap: GlobeMapStub,
          GridParameters: true,
          BatchAssetsPanel: true,
          ExecutionResultPanel: true,
          TaskQueuePanel: true,
          QualityView: true,
          DatasetsView: true,
          'el-drawer': { template: '<div><slot /></div>' },
        },
      },
    });

    await wrapper.get('[data-testid="load-map"]').trigger('click');
    await wrapper.get('[data-testid="partition-module-carbon"]').trigger('click');
    resolvePreview({ cells: [{ space_code: 'old-grid', grid_level: 6, bbox: [100, 20, 101, 21] }] });
    await flushPromises();
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('0');
  });

  it('allows selections accumulated from different loader batches', async () => {
    const store = usePartitionStore();
    const submit = vi.spyOn(store, 'submit');
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          ...legacyLayoutStubs,
          GlobeMap: GlobeMapStub,
          GridParameters: { template: '<button data-testid="submit" @click="$emit(\'submit\')">submit</button>' },
          BatchAssetsPanel: true,
          ExecutionResultPanel: true,
          TaskQueuePanel: true,
          QualityView: true,
          DatasetsView: true,
          'el-drawer': { template: '<div><slot /></div>' },
        },
      },
    });
    wrapper.vm.updateDatasets([
      { dataset_id: 'a', data_type: 'optical', scenes: [{ scene_id: 'scene-a', source_batch_ids: ['loader-a'] }], assets: [] },
      { dataset_id: 'b', data_type: 'radar', scenes: [{ scene_id: 'scene-b', source_batch_ids: ['loader-b'] }], assets: [] },
    ]);

    expect(store.form).not.toHaveProperty('batchId');
    await wrapper.get('[data-testid="submit"]').trigger('click');
    expect(submit).toHaveBeenCalledOnce();
  });
});
