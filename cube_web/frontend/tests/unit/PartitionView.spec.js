import { flushPromises, mount } from '@vue/test-utils';
import { createPinia, setActivePinia } from 'pinia';
import { beforeEach, describe, expect, it, vi } from 'vitest';

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

  it('lists a pending partition draft and loads its saved band selection', async () => {
    const draft = {
      draft_id: 'partition-draft-a',
      draft_name: '山东光学剖分批次',
      data_type: 'optical',
      source_load_batch_ids: ['load-batch-a'],
      selection: {
        datasets: [{
          dataset_id: 'dataset-a',
          data_type: 'optical',
          band_unit_ids: ['band-a'],
          scenes: [{ scene_id: 'scene-a', source_batch_ids: ['load-batch-a'] }],
          partition: { grid_type: 'geohash', requested_grid_level: 4, partition_method: 'logical' },
        }],
      },
    };
    requestGet.mockImplementation(async (path) => (
      path === '/v1/partition/drafts?limit=100'
        ? { items: [draft], total: 1 }
        : { items: [], total: 0, page: 1, page_size: 20 }
    ));
    const BatchAssetsPanelStub = {
      props: ['partitionDrafts'],
      emits: ['activate-partition-draft'],
      template: '<button v-for="draft in partitionDrafts" :key="draft.draft_id" data-testid="draft" @click="$emit(\'activate-partition-draft\', draft.draft_id)">{{ draft.draft_name }}</button>',
    };
    const wrapper = mount(PartitionView, {
      global: {
        stubs: {
          GlobeMap: GlobeMapStub, ...layoutStubs, GridParameters: true,
          BatchAssetsPanel: BatchAssetsPanelStub, TaskQueuePanel: true, QualityView: true, DataManagementView: true,
          'el-drawer': { template: '<div><slot /></div>' },
        },
      },
    });

    await flushPromises();
    expect(wrapper.get('[data-testid="draft"]').text()).toBe('山东光学剖分批次');
    await wrapper.get('[data-testid="draft"]').trigger('click');
    expect(wrapper.vm.activeDraftId).toBe(draft.draft_id);
    expect(wrapper.get('[data-testid="draft"]').exists()).toBe(true);
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

  it('uses grid cells alone after a grid preview is loaded', () => {
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
    expect(wrapper.vm.mapGeometries).toEqual(wrapper.vm.gridGeometries);
    wrapper.unmount();
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
    }));
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('1');
    await wrapper.get('[data-testid="reset-grid"]').trigger('click');
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('1');

    await wrapper.get('[data-testid="partition-module-carbon"]').trigger('click');
    expect(wrapper.text()).toContain('地图');
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('1');
  });

  it('loads selected carbon source footprints onto the map', async () => {
    const store = usePartitionStore();
    store.form.datasets = [{
      dataset_id: 'carbon-a', dataset_title: 'TanSat A', data_type: 'carbon', product_type: 'tansat',
      scenes: [{ scene_id: 'scene-carbon', source_batch_ids: ['load-carbon'] }],
      assets: [{ source_asset_id: 'asset-carbon', bbox: [100, 20, 101, 21] }],
      partition: { grid_type: 'isea4h', requested_grid_level: 5, partition_method: 'entity' },
    }];
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
      source_batch_ids: ['load-carbon'], scene_ids: ['scene-carbon'], limit: 2000,
    }));
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('1');
  });

  it('discards a carbon footprint response after the selected scenes change', async () => {
    const store = usePartitionStore();
    store.form.datasets = [{
      dataset_id: 'carbon-a', dataset_title: 'TanSat A', data_type: 'carbon',
      scenes: [{ scene_id: 'scene-a', source_batch_ids: ['load-a'] }],
      assets: [{ source_asset_id: 'asset-a', bbox: [100, 20, 101, 21] }],
      partition: { grid_type: 'isea4h', requested_grid_level: 5, partition_method: 'entity' },
    }];
    let resolvePreview;
    requestJson.mockReturnValue(new Promise((resolve) => { resolvePreview = resolve; }));
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
      partition: { grid_type: 'isea4h', requested_grid_level: 5, partition_method: 'entity' },
    }]);
    resolvePreview({ items: [{ observation_id: 'stale', geometry: { type: 'Point', coordinates: [100.5, 20.5] } }] });
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
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('3');
  });

  it('retains grid layers while switching between product pages', async () => {
    requestJson.mockImplementation(async (_path, payload) => ({
      cells: [{
        space_code: `${payload.grid_type}-${payload.requested_grid_level}`,
        grid_level: payload.requested_grid_level,
        bbox: payload.bbox,
      }],
    }));
    const store = usePartitionStore();
    store.form.datasets = [
      {
        dataset_id: 'optical-a', data_type: 'optical', scenes: [{ scene_id: 'scene-o' }],
        assets: [{ source_asset_id: 'asset-o', bbox: [100, 20, 101, 21] }],
        partition: { grid_type: 'geohash', requested_grid_level: 6, partition_method: 'logical' },
      },
      {
        dataset_id: 'radar-a', data_type: 'radar', scenes: [{ scene_id: 'scene-r' }],
        assets: [{ source_asset_id: 'asset-r', bbox: [110, 30, 111, 31] }],
        partition: { grid_type: 'mgrs', requested_grid_level: 1, partition_method: 'logical' },
      },
      {
        dataset_id: 'product-a', data_type: 'product', scenes: [{ scene_id: 'scene-p' }],
        assets: [{ source_asset_id: 'asset-p', bbox: [120, 40, 121, 41] }],
        partition: { grid_type: 'isea4h', requested_grid_level: 6, partition_method: 'entity' },
      },
    ];
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

    expect(wrapper.vm.gridGeometries.map((item) => item.color)).toEqual(['#2f73d9', '#16836f', '#d97706']);
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('3');
    await wrapper.get('[data-testid="partition-module-optical"]').trigger('click');
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('3');
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
    store.form.datasets = [{
      dataset_id: 'dataset-large', data_type: 'optical', scenes: [{ scene_id: 'scene-large' }],
      assets: [{ source_asset_id: 'asset-large', bbox: [100, 20, 101, 21] }],
      partition: { grid_type: 'geohash', requested_grid_level: 6, partition_method: 'logical' },
    }];
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
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('5001');
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

  it('keeps an in-flight preview scoped to its originating product', async () => {
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
    expect(wrapper.get('[data-testid="partition-map-stub"]').attributes('data-geometry-count')).toBe('1');
  });

  it('allows selections accumulated from different loader batches', async () => {
    const store = usePartitionStore();
    const submit = vi.spyOn(store, 'submit');
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
      { dataset_id: 'b', data_type: 'radar', scenes: [{ scene_id: 'scene-b', source_batch_ids: ['loader-b'] }], assets: [] },
    ]);

    expect(store.form).not.toHaveProperty('batchId');
    await wrapper.get('[data-testid="submit"]').trigger('click');
    expect(submit).toHaveBeenCalledOnce();
  });
});
