import { flushPromises, mount } from '@vue/test-utils';
import { createPinia, setActivePinia } from 'pinia';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ElMessage } from 'element-plus';

import PartitionView from '@/views/PartitionView.vue';
import { usePartitionStore } from '@/stores/partition';

const { requestGet, requestJson, requestPost } = vi.hoisted(() => ({
  requestGet: vi.fn(async () => ({ items: [], total: 0, page: 1, page_size: 20 })),
  requestJson: vi.fn(async () => ({
    cells: [{ space_code: 'grid-1', grid_level: 6, geometry: null, bbox: [100, 20, 101, 21] }],
  })),
  requestPost: vi.fn(),
}));

vi.mock('@/api/client', () => ({
  requestGet,
  requestPost,
  requestJson,
}));

const GlobeMapStub = {
  name: 'GlobeMap',
  props: ['geometries'],
  template: '<div data-testid="partition-map-stub" :data-geometry-count="geometries.length" />',
};
const layoutStubs = {
  'el-button': { template: '<button v-bind="$attrs" @click="$emit(\'click\')"><slot /></button>' },
  'el-tag': { template: '<span><slot /></span>' },
};

describe('PartitionView map workspace', () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    requestGet.mockReset().mockResolvedValue({ items: [], total: 0, page: 1, page_size: 20 });
    requestPost.mockReset();
    requestJson.mockReset().mockResolvedValue({
      cells: [{ space_code: 'grid-1', grid_level: 6, geometry: null, bbox: [100, 20, 101, 21] }],
    });
  });

  it('loads a confirmed reload batch as a formal selection', async () => {
    const reloadBatch = {
      load_batch_id: 'dataset-reload-a',
      batch_name: '山东光学重新载入',
      data_type: 'optical',
      selection: {
        datasets: [{
          dataset_id: 'dataset-a',
          data_type: 'optical',
          source_batch_id: 'dataset-reload-a',
          band_unit_ids: ['band-a'],
          scenes: [{ scene_id: 'scene-a', source_batch_ids: ['dataset-reload-a'] }],
          partition: { grid_type: 'geohash', requested_grid_level: 4, partition_method: 'logical' },
        }],
      },
    };
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          GlobeMap: GlobeMapStub, ...layoutStubs, GridParameters: true,
          BatchAssetsPanel: true, TaskQueuePanel: true, QualityView: true, DataManagementView: true,
          'el-drawer': { template: '<div><slot /></div>' },
        },
      },
    });

    wrapper.vm.queueManagedPartition(reloadBatch);
    expect(wrapper.vm.datasetDrawerVisible).toBe(true);
    expect(usePartitionStore().datasetsFor('optical')).toEqual(reloadBatch.selection.datasets);
  });

  it('merges a confirmed reload batch into the existing product context', async () => {
    const store = usePartitionStore();
    const existing = {
      dataset_id: 'dataset-existing', data_type: 'optical', source_batch_id: 'load-existing',
      selection_id: 'load-existing:dataset-existing', scenes: [{ scene_id: 'scene-existing' }],
      partition: { grid_type: 'geohash', requested_grid_level: 4, partition_method: 'logical' },
    };
    store.setDatasets('optical', [existing]);
    const reloadBatch = {
      load_batch_id: 'dataset-reload-a', data_type: 'optical',
      selection: { datasets: [{
        dataset_id: 'dataset-reload', data_type: 'optical', source_batch_id: 'dataset-reload-a',
        selection_id: 'dataset-reload-a:dataset-reload', scenes: [{ scene_id: 'scene-reload' }],
        partition: { grid_type: 'mgrs', requested_grid_level: 1, partition_method: 'logical' },
      }] },
    };
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          GlobeMap: GlobeMapStub, ...layoutStubs, GridParameters: true,
          BatchAssetsPanel: true, TaskQueuePanel: true, QualityView: true, DataManagementView: true,
          'el-drawer': { template: '<div><slot /></div>' },
        },
      },
    });

    wrapper.vm.queueManagedPartition(reloadBatch);

    expect(store.datasetsFor('optical').map((dataset) => dataset.selection_id)).toEqual([
      'load-existing:dataset-existing', 'dataset-reload-a:dataset-reload',
    ]);
  });

  it('splits a slightly out-of-bounds global product extent for Cesium', () => {
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          GlobeMap: GlobeMapStub,
          ...layoutStubs,
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

    const geometry = wrapper.vm.bboxGeometry([-180.0044, -90.0022, 180.0044, 90.0022]);
    expect(wrapper.vm.normalizeBbox([-180.0044, -90.0022, 180.0044, 90.0022]))
      .toEqual([-180, -90, 180, 90]);
    expect(geometry.type).toBe('MultiPolygon');
    expect(geometry.coordinates).toHaveLength(8);
    expect(geometry.coordinates.every((polygon) => polygon[0].length === 5)).toBe(true);
    expect(geometry.coordinates.flat(2).every(([longitude, latitude]) => (
      longitude >= -180 && longitude <= 180 && latitude >= -90 && latitude <= 90
    ))).toBe(true);
    expect(geometry.coordinates.flat(2).every(([, latitude]) => Math.abs(latitude) < 90)).toBe(true);
    wrapper.unmount();
  });

  it('draws grid cells after overlapping selected data bounds', () => {
    const store = usePartitionStore();
    store.setDatasets('optical', [{
      dataset_id: 'dataset-a',
      data_type: 'optical',
      assets: [
        { source_asset_id: 'asset-a', bbox: [100, 20, 101, 21] },
        { source_asset_id: 'asset-b', bbox: [100.2, 20.2, 100.8, 20.8] },
      ],
      scenes: [{ scene_id: 'scene-a' }],
      partition: { grid_type: 'geohash', requested_grid_level: 4, partition_method: 'logical' },
    }]);
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          GlobeMap: GlobeMapStub, ...layoutStubs, GridParameters: true,
          BatchAssetsPanel: true, ExecutionResultPanel: true, TaskQueuePanel: true,
          QualityView: true, DatasetsView: true,
          'el-drawer': { template: '<div><slot /></div>' },
        },
      },
    });
    expect(wrapper.vm.mapGeometries).toEqual(wrapper.vm.selectedGeometries);
    wrapper.vm.gridGeometriesByModule = { optical: [{ geometry: { type: 'Polygon', coordinates: [] } }] };
    expect(wrapper.vm.mapGeometries).toHaveLength(3);
    expect(wrapper.vm.selectedGeometries.map((item) => item.color)).toEqual(['#e53935', '#e53935']);
    expect(wrapper.vm.mapGeometries).toEqual([
      ...wrapper.vm.selectedGeometries,
      ...wrapper.vm.gridGeometries,
    ]);
    wrapper.unmount();
  });

  it('renders the map and previews selected asset bounds', async () => {
    const store = usePartitionStore();
    store.setDatasets('optical', [{
      dataset_id: 'dataset-a',
      dataset_title: 'Dataset A',
      data_type: 'optical',
      scenes: [{ scene_id: 'scene-a', source_batch_ids: ['loader-batch-a'] }],
      assets: [{ source_asset_id: 'asset-a', bbox: [100, 20, 101, 21] }],
      bands: [],
    }]);
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          GlobeMap: GlobeMapStub,
          ...layoutStubs,
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
    expect(wrapper.find('.result-panel').exists()).toBe(false);
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('1');
    await wrapper.get('[data-testid="load-map"]').trigger('click');
    await flushPromises();
    expect(requestJson).toHaveBeenCalledWith('/v1/grid/cover', expect.objectContaining({
      grid_type: 'geohash',
      requested_grid_level: 4,
      bbox: [100, 20, 101, 21],
    }), expect.objectContaining({ signal: expect.any(AbortSignal) }));
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('2');
    await wrapper.get('[data-testid="reset-grid"]').trigger('click');
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('1');

    await wrapper.get('[data-testid="partition-module-carbon"]').trigger('click');
    expect(wrapper.text()).toContain('地图');
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('0');
  });

  it('automatically loads the selected dataset grid on the partition page', async () => {
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          GlobeMap: GlobeMapStub,
          ...layoutStubs,
          GridParameters: true,
          BatchAssetsPanel: true,
          TaskQueuePanel: true,
          QualityView: true,
          DataManagementView: true,
          'el-drawer': { template: '<div><slot /></div>' },
        },
      },
    });
    requestJson.mockClear();

    wrapper.vm.updateDatasets([{
      dataset_id: 'dataset-selected',
      data_type: 'optical',
      scenes: [{ scene_id: 'scene-selected' }],
      assets: [{ source_asset_id: 'asset-selected', bbox: [100, 20, 101, 21] }],
      partition: { grid_type: 'mgrs', requested_grid_level: 2, partition_method: 'logical' },
    }]);
    await flushPromises();

    expect(requestJson).toHaveBeenCalledWith('/v1/grid/cover', expect.objectContaining({
      grid_type: 'mgrs',
      requested_grid_level: 2,
      bbox: [100, 20, 101, 21],
    }), expect.objectContaining({ signal: expect.any(AbortSignal) }));
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('2');
  });

  it('uses the web facade continuous geometry for MGRS preview only', async () => {
    requestJson.mockResolvedValue({
      cells: [
        { space_code: '47RRG', grid_level: 0, geometry: null, bbox: [101.9, 24, 102, 24.4] },
        { space_code: '48RSM', grid_level: 0, geometry: null, bbox: [102, 24, 102.1, 24.4] },
      ],
      preview_cells: [{
        space_code: 'preview-48-0-2400000',
        grid_level: 0,
        geometry: {
          type: 'Polygon',
          coordinates: [[[101.9, 24], [102.1, 24], [102.1, 24.4], [101.9, 24.4], [101.9, 24]]],
        },
      }],
      statistics: { cell_count: 2 },
    });
    const store = usePartitionStore();
    store.setDatasets('product', [{
      dataset_id: 'product-mgrs',
      data_type: 'product',
      scenes: [{ scene_id: 'scene-product' }],
      assets: [{ source_asset_id: 'asset-product', bbox: [100, 20, 104, 27], crs: 'EPSG:32648' }],
      partition: { grid_type: 'mgrs', requested_grid_level: 0, partition_method: 'logical' },
    }]);
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          GlobeMap: GlobeMapStub, ...layoutStubs, GridParameters: true,
          BatchAssetsPanel: true, QualityView: true, DataManagementView: true,
          'el-drawer': { template: '<div><slot /></div>' },
        },
      },
    });

    await wrapper.get('[data-testid="partition-module-product"]').trigger('click');
    await wrapper.get('[data-testid="load-map"]').trigger('click');
    await flushPromises();

    const coverCall = requestJson.mock.calls.find(([path]) => path === '/v1/grid/cover');
    expect(coverCall[1]).toEqual(expect.objectContaining({
      preview_mode: 'continuous', preview_crs: 'EPSG:32648',
    }));
    expect(wrapper.vm.activeGridGeometries).toHaveLength(1);
    expect(wrapper.vm.activeGridGeometries[0].label).toBe('preview-48-0-2400000');
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('2');
  });

  it('loads selected carbon source footprints onto the map', async () => {
    const store = usePartitionStore();
    store.setDatasets('carbon', [{
      dataset_id: 'carbon-a', dataset_title: 'TanSat A', data_type: 'carbon', product_type: 'tansat',
      scenes: [{ scene_id: 'scene-carbon', source_batch_ids: ['load-carbon'] }],
      assets: [{ source_asset_id: 'asset-carbon', bbox: [100, 20, 101, 21] }],
      partition: { grid_type: 'isea4h', requested_grid_level: 6, partition_method: 'entity' },
    }]);
    requestJson.mockImplementation(async (path) => (
      path === '/v1/partition/carbon/footprints'
        ? { items: [{ observation_id: 'obs-1', geometry: { type: 'Polygon', coordinates: [[[100, 20], [101, 20], [101, 21], [100, 20]]] } }] }
        : { cells: [] }
    ));
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          GlobeMap: GlobeMapStub, ...layoutStubs, GridParameters: true, BatchAssetsPanel: true,
          QualityView: true, DataManagementView: true, 'el-drawer': { template: '<div><slot /></div>' },
        },
      },
    });

    await wrapper.get('[data-testid="partition-module-carbon"]').trigger('click');
    await wrapper.get('[data-testid="load-carbon-footprints"]').trigger('click');
    await flushPromises();

    expect(requestJson).toHaveBeenCalledWith('/v1/partition/carbon/footprints', expect.objectContaining({
      source_batch_ids: ['load-carbon'], scene_ids: ['scene-carbon'],
    }), expect.objectContaining({ signal: expect.any(AbortSignal) }));
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('2');
  });

  it('loads carbon footprint coverage cells when loading the map', async () => {
    const store = usePartitionStore();
    store.setDatasets('carbon', [{
      dataset_id: 'carbon-a', dataset_title: 'TanSat A', data_type: 'carbon', product_type: 'tansat',
      scenes: [{ scene_id: 'scene-carbon', source_batch_ids: ['load-carbon'] }],
      assets: [], partition: { grid_type: 'isea4h', requested_grid_level: 6, partition_method: 'entity' },
    }]);
    requestJson.mockResolvedValue({
      items: [{ observation_id: 'obs-1', geometry: { type: 'Polygon', coordinates: [[[100, 20], [101, 20], [100.5, 21], [100, 20]]] } }],
      cells: [{ space_code: 'H5', grid_level: 5, geometry: { type: 'Polygon', coordinates: [[[100, 20], [101, 20], [100.5, 21], [100, 20]]] } }],
      cell_limit_reached: false,
      unavailable_sources: [],
    });
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          GlobeMap: GlobeMapStub, ...layoutStubs, GridParameters: true, BatchAssetsPanel: true,
          QualityView: true, DataManagementView: true, 'el-drawer': { template: '<div><slot /></div>' },
        },
      },
    });

    await wrapper.get('[data-testid="partition-module-carbon"]').trigger('click');
    await wrapper.get('[data-testid="load-map"]').trigger('click');
    await flushPromises();

    expect(requestJson).toHaveBeenCalledWith('/v1/partition/carbon/grid-preview', expect.objectContaining({
      source_batch_ids: ['load-carbon'], scene_ids: ['scene-carbon'],
      grid_type: 'isea4h', requested_grid_level: 6,
    }), expect.objectContaining({ signal: expect.any(AbortSignal) }));
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('2');
  });

  it('discards a carbon footprint response after the selected scenes change', async () => {
    const store = usePartitionStore();
    store.setDatasets('carbon', [{
      dataset_id: 'carbon-a', dataset_title: 'TanSat A', data_type: 'carbon',
      scenes: [{ scene_id: 'scene-a', source_batch_ids: ['load-a'] }],
      assets: [{ source_asset_id: 'asset-a', bbox: [100, 20, 101, 21] }],
      partition: { grid_type: 'isea4h', requested_grid_level: 6, partition_method: 'entity' },
    }]);
    let resolveFootprints;
    let resolveGridPreview;
    requestJson.mockImplementation((path) => new Promise((resolve) => {
      if (path === '/v1/partition/carbon/footprints') resolveFootprints = resolve;
      else resolveGridPreview = resolve;
    }));
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          GlobeMap: GlobeMapStub, ...layoutStubs, GridParameters: true, BatchAssetsPanel: true,
          QualityView: true, DataManagementView: true, 'el-drawer': { template: '<div><slot /></div>' },
        },
      },
    });

    await wrapper.get('[data-testid="partition-module-carbon"]').trigger('click');
    await wrapper.get('[data-testid="load-carbon-footprints"]').trigger('click');
    wrapper.vm.updateDatasets([{
      dataset_id: 'carbon-b', dataset_title: 'TanSat B', data_type: 'carbon',
      scenes: [{ scene_id: 'scene-b', source_batch_ids: ['load-b'] }],
      assets: [{ source_asset_id: 'asset-b', bbox: [110, 30, 111, 31] }],
      partition: { grid_type: 'isea4h', requested_grid_level: 6, partition_method: 'entity' },
    }]);
    resolveFootprints({ items: [{ observation_id: 'stale', geometry: { type: 'Point', coordinates: [100.5, 20.5] } }] });
    resolveGridPreview({ cells: [] });
    await flushPromises();

    expect(wrapper.vm.activeCarbonFootprints).toEqual([]);
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('1');
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
    store.setDatasets('optical', [
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
    ]);
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          GlobeMap: GlobeMapStub,
          ...layoutStubs,
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
    expect(wrapper.text()).toContain('经纬度格网 · 层级 6');
    expect(wrapper.text()).toContain('平面格网 · 层级 2');
    expect(wrapper.text()).toContain('六边形格网 · 层级 6');
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('6');
  });

  it('preserves each product preview while switching between product pages', async () => {
    requestJson.mockImplementation(async (_path, payload) => ({
      cells: [{
        space_code: `${payload.grid_type}-${payload.requested_grid_level}`,
        grid_level: payload.requested_grid_level,
        bbox: payload.bbox,
      }],
    }));
    const store = usePartitionStore();
    store.setDatasets('optical', [{
        dataset_id: 'optical-a', data_type: 'optical', scenes: [{ scene_id: 'scene-o' }],
        assets: [{ source_asset_id: 'asset-o', bbox: [100, 20, 101, 21] }],
        partition: { grid_type: 'geohash', requested_grid_level: 6, partition_method: 'logical' },
      }]);
    store.setDatasets('radar', [{
        dataset_id: 'radar-a', data_type: 'radar', scenes: [{ scene_id: 'scene-r' }],
        assets: [{ source_asset_id: 'asset-r', bbox: [110, 30, 111, 31] }],
        partition: { grid_type: 'mgrs', requested_grid_level: 1, partition_method: 'logical' },
      }]);
    store.setDatasets('product', [{
        dataset_id: 'product-a', data_type: 'product', scenes: [{ scene_id: 'scene-p' }],
        assets: [{ source_asset_id: 'asset-p', bbox: [120, 40, 121, 41] }],
        partition: { grid_type: 'isea4h', requested_grid_level: 6, partition_method: 'entity' },
      }]);
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          GlobeMap: GlobeMapStub,
          ...layoutStubs,
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
    await wrapper.get('[data-testid="partition-module-radar"]').trigger('click');
    await wrapper.get('[data-testid="load-map"]').trigger('click');
    await flushPromises();
    await wrapper.get('[data-testid="partition-module-product"]').trigger('click');
    await wrapper.get('[data-testid="load-map"]').trigger('click');
    await flushPromises();

    expect(wrapper.vm.activeGridGeometries.map((item) => item.color)).toEqual(['#d97706']);
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('2');
    await wrapper.get('[data-testid="partition-module-optical"]').trigger('click');
    expect(wrapper.vm.activeGridGeometries.map((item) => item.color)).toEqual(['#2f73d9']);
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('2');
  });

  it('renders every cell returned for a large recommended-level preview', async () => {
    requestJson.mockResolvedValue({
      cells: Array.from({ length: 5001 }, (_, index) => ({
        space_code: `cell-${index}`,
        grid_level: 6,
        bbox: [100 + index / 10000, 20, 100.01 + index / 10000, 20.01],
      })),
    });
    const store = usePartitionStore();
    store.setDatasets('optical', [{
      dataset_id: 'dataset-large', data_type: 'optical', scenes: [{ scene_id: 'scene-large' }],
      assets: [{ source_asset_id: 'asset-large', bbox: [100, 20, 101, 21] }],
      partition: { grid_type: 'geohash', requested_grid_level: 6, partition_method: 'logical' },
    }]);
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          GlobeMap: GlobeMapStub,
          ...layoutStubs,
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

    expect(wrapper.vm.gridGeometries).toHaveLength(5001);
    expect(wrapper.text()).not.toContain('已加载 5001 个格网单元');
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('5002');
  });

  it('exposes product, quality and ingest pages as peer modules', async () => {
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          ...layoutStubs,
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

    expect(wrapper.findAll('.module-tab')).toHaveLength(6);
    expect(wrapper.find('[data-testid="partition-module-tasks"]').exists()).toBe(false);
    expect(wrapper.find('.partition-module-nav').exists()).toBe(false);
    await wrapper.get('[data-testid="partition-module-quality"]').trigger('click');
    expect(wrapper.get('[data-testid="quality-view-stub"]').exists()).toBe(true);
    await wrapper.get('[data-testid="partition-module-ingest"]').trigger('click');
    expect(wrapper.get('[data-testid="data-management-view-stub"]').exists()).toBe(true);
  });

  it('keeps partition parameters independent between product pages', async () => {
    const store = usePartitionStore();
    store.setDatasets('optical', [{
      dataset_id: 'dataset-a',
      data_type: 'optical',
      assets: [],
      bands: [],
      partition: { grid_type: 'geohash', requested_grid_level: 6, partition_method: 'logical' },
      grid_level_unlocked: true,
    }]);
    const GridParametersStub = {
      props: ['modelValue'],
      emits: ['update:modelValue'],
      template: '<div data-testid="parameters" :data-grid-type="modelValue.gridType"><button data-testid="set-mgrs" @click="$emit(\'update:modelValue\', { ...modelValue, gridType: \'mgrs\', requestedGridLevel: 2 })">set</button></div>',
    };
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          ...layoutStubs,
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
    expect(store.datasetsFor('optical')[0].partition).toMatchObject({
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
    store.setDatasets('optical', [{
        dataset_id: 'dataset-optical', data_type: 'optical', assets: [],
        scenes: [{ scene_id: 'scene-optical', source_batch_ids: ['load-optical'] }],
        partition: { grid_type: 'geohash', requested_grid_level: 6, partition_method: 'logical' },
      }]);
    store.setDatasets('carbon', [{
        dataset_id: 'dataset-carbon', data_type: 'carbon', assets: [],
        scenes: [{ scene_id: 'scene-carbon', source_batch_ids: ['load-carbon'] }],
        partition: { grid_type: 'isea4h', requested_grid_level: 6, partition_method: 'entity' },
      }]);
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          ...layoutStubs,
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

  it('does not render an execution result panel', async () => {
    const store = usePartitionStore();
    vi.spyOn(store, 'submit').mockResolvedValue({ task_id: 'optical-task', status: 'queued' });
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          ...layoutStubs,
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
    expect(wrapper.find('[data-testid="result"]').exists()).toBe(false);
  });

  it('clears only the submitted product selection after a successful submission', async () => {
    const store = usePartitionStore();
    store.setDatasets('optical', [{
        dataset_id: 'dataset-optical',
        data_type: 'optical',
        scenes: [{ scene_id: 'scene-optical', source_batch_ids: ['load-optical'] }],
        assets: [{ source_asset_id: 'asset-optical', bbox: [100, 20, 101, 21] }],
        partition: { grid_type: 'geohash', requested_grid_level: 4, partition_method: 'logical' },
      }]);
    store.setDatasets('carbon', [{
        dataset_id: 'dataset-carbon',
        data_type: 'carbon',
        scenes: [{ scene_id: 'scene-carbon', source_batch_ids: ['load-carbon'] }],
        assets: [{ source_asset_id: 'asset-carbon', bbox: [110, 30, 111, 31] }],
        partition: { grid_type: 'isea4h', requested_grid_level: 6, partition_method: 'entity' },
      }]);
    vi.spyOn(store, 'submit').mockResolvedValue({ task_id: 'optical-task', status: 'queued' });
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          ...layoutStubs,
          GlobeMap: GlobeMapStub,
          GridParameters: { template: '<button data-testid="submit" @click="$emit(\'submit\')">submit</button>' },
          BatchAssetsPanel: true,
          TaskQueuePanel: true,
          QualityView: true,
          DataManagementView: true,
          'el-drawer': { template: '<div><slot /></div>' },
        },
      },
    });

    wrapper.vm.datasetDrawerVisible = true;
    await wrapper.get('[data-testid="load-map"]').trigger('click');
    await flushPromises();
    const mapBeforeSubmit = wrapper.vm.mapGeometries;
    const gridBeforeSubmit = wrapper.vm.gridGeometries;
    await wrapper.get('[data-testid="submit"]').trigger('click');
    await flushPromises();

    expect(store.datasetsFor('optical')).toEqual([]);
    expect(store.datasetsFor('carbon')).toEqual([expect.objectContaining({ dataset_id: 'dataset-carbon' })]);
    expect(wrapper.vm.datasetDrawerVisible).toBe(false);
    expect(wrapper.vm.gridGeometries).toEqual(gridBeforeSubmit);
    expect(wrapper.vm.mapGeometries).toEqual(mapBeforeSubmit);
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('2');
  });

  it('shows the returned partition run id after submission', async () => {
    const store = usePartitionStore();
    const successMessage = vi.spyOn(ElMessage, 'success').mockImplementation(() => undefined);
    vi.spyOn(store, 'submit').mockResolvedValue({
      partition_run_id: 'partition-run-real-batch',
      task_id: 'partition-task-real',
      status: 'queued',
    });
    store.setDatasets('optical', [{
      dataset_id: 'dataset-optical',
      data_type: 'optical',
      scenes: [{ scene_id: 'scene-optical', source_batch_ids: ['load-optical'] }],
      assets: [],
      partition: { grid_type: 'geohash', requested_grid_level: 4, partition_method: 'logical' },
    }]);
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          ...layoutStubs,
          GlobeMap: GlobeMapStub,
          GridParameters: true,
          BatchAssetsPanel: true,
          TaskQueuePanel: true,
          QualityView: true,
          DataManagementView: true,
          'el-drawer': { template: '<div><slot /></div>' },
        },
      },
    });

    await wrapper.vm.submit();

    expect(successMessage).toHaveBeenCalledWith('剖分任务已提交，剖分批次：partition-run-real-batch');
    successMessage.mockRestore();
  });

  it('preserves product selections changed while submission is in flight', async () => {
    let resolveSubmit;
    const store = usePartitionStore();
    const submitted = {
      dataset_id: 'dataset-optical', data_type: 'optical',
      scenes: [{ scene_id: 'scene-optical', source_batch_ids: ['load-optical'] }],
      assets: [], partition: { grid_type: 'geohash', requested_grid_level: 4, partition_method: 'logical' },
    };
    const added = {
      dataset_id: 'dataset-added', data_type: 'optical',
      scenes: [{ scene_id: 'scene-added', source_batch_ids: ['load-added'] }],
      assets: [], partition: { grid_type: 'mgrs', requested_grid_level: 1, partition_method: 'logical' },
    };
    store.setDatasets('optical', [submitted]);
    vi.spyOn(store, 'submit').mockReturnValue(new Promise((resolve) => { resolveSubmit = resolve; }));
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          ...layoutStubs, GlobeMap: GlobeMapStub,
          GridParameters: { template: '<button data-testid="submit" @click="$emit(\'submit\')">submit</button>' },
          BatchAssetsPanel: true, TaskQueuePanel: true, QualityView: true, DataManagementView: true,
          'el-drawer': { template: '<div><slot /></div>' },
        },
      },
    });

    await wrapper.get('[data-testid="submit"]').trigger('click');
    store.setDatasets('optical', [submitted, added]);
    resolveSubmit({ task_id: 'optical-task', status: 'queued' });
    await flushPromises();

    expect(store.datasetsFor('optical')).toEqual([submitted, added]);
  });

  it('keeps an in-flight preview scoped to its originating product', async () => {
    let resolvePreview;
    requestJson.mockImplementationOnce(() => new Promise((resolve) => { resolvePreview = resolve; }));
    const store = usePartitionStore();
    store.setDatasets('optical', [{
      dataset_id: 'dataset-a',
      dataset_title: 'Dataset A',
      data_type: 'optical',
      assets: [{ source_asset_id: 'asset-a', bbox: [100, 20, 101, 21] }],
      bands: [],
      partition: { grid_type: 'geohash', requested_grid_level: 6, partition_method: 'logical' },
    }]);
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          ...layoutStubs,
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
    expect(wrapper.vm.gridGeometries).toHaveLength(1);
    expect(wrapper.vm.activeGridGeometries).toEqual([]);
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('0');
    await wrapper.get('[data-testid="partition-module-optical"]').trigger('click');
    expect(wrapper.vm.activeGridGeometries).toHaveLength(1);
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('2');
  });

  it('invalidates an in-flight map preview while retaining rendered layers on submit', async () => {
    let previewSignal;
    let resolvePreview;
    requestJson.mockImplementation((path, _payload, options = {}) => {
      if (path === '/v1/grid/cover') {
        previewSignal = options.signal;
        return new Promise((resolve) => {
          resolvePreview = resolve;
        });
      }
      return Promise.resolve({ cells: [] });
    });
    const store = usePartitionStore();
    const submit = vi.spyOn(store, 'submit').mockImplementation(() => {
      expect(previewSignal.aborted).toBe(false);
      return Promise.resolve({ task_id: 'optical-task', status: 'queued' });
    });
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          ...layoutStubs,
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

    wrapper.vm.updateDatasets([{
      dataset_id: 'dataset-a',
      data_type: 'optical',
      scenes: [{ scene_id: 'scene-a', source_batch_ids: ['load-a'] }],
      assets: [{ source_asset_id: 'asset-a', bbox: [100, 20, 101, 21] }],
      partition: { grid_type: 'geohash', requested_grid_level: 4, partition_method: 'logical' },
    }]);
    const existingGrid = {
      geometry: { type: 'Polygon', coordinates: [] },
      label: '已显示格网', color: '#2f73d9', fillColor: '#2f73d9', fillOpacity: 0.07, weight: 1.5,
    };
    wrapper.vm.gridGeometriesByModule = { optical: [existingGrid] };
    expect(previewSignal).toBeInstanceOf(AbortSignal);
    expect(wrapper.vm.gridPreviewLoading).toBe(true);

    await wrapper.get('[data-testid="submit"]').trigger('click');
    await flushPromises();

    expect(submit).toHaveBeenCalledWith('optical');
    expect(previewSignal.aborted).toBe(true);
    expect(wrapper.vm.gridPreviewLoading).toBe(false);
    expect(wrapper.vm.gridGeometries).toEqual([existingGrid]);

    resolvePreview({
      cells: [{ space_code: 'stale-grid', grid_level: 4, bbox: [100, 20, 101, 21] }],
    });
    await flushPromises();
    expect(wrapper.vm.gridGeometries).toEqual([existingGrid]);
    expect(wrapper.vm.gridPreviewLoading).toBe(false);
  });

  it('allows multiple loader batches in the same product submission', async () => {
    const store = usePartitionStore();
    const submit = vi.spyOn(store, 'submit').mockResolvedValue({ task_id: 'optical-task', status: 'queued' });
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          ...layoutStubs,
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
      { dataset_id: 'b', data_type: 'optical', scenes: [{ scene_id: 'scene-b', source_batch_ids: ['loader-b'] }], assets: [] },
    ]);

    expect(store.datasetsFor('optical')).toHaveLength(2);
    expect(store.form).not.toHaveProperty('batchId');
    await wrapper.get('[data-testid="submit"]').trigger('click');
    expect(submit).toHaveBeenCalledWith('optical');
  });
});
