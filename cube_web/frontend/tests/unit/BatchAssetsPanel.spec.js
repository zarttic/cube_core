import { flushPromises, mount } from '@vue/test-utils';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/api/client', () => ({ requestGet: vi.fn() }));

import { requestGet } from '@/api/client';
import BatchAssetsPanel from '@/views/partition/BatchAssetsPanel.vue';

const wrappers = [];
const batches = {
  load_batches: [
    { load_batch_id: 'load-a', batch_name: 'Batch A', scene_count: 3, status: 'succeeded' },
    { load_batch_id: 'load-b', batch_name: 'Batch B', scene_count: 2, status: 'succeeded' },
    { load_batch_id: 'load-pending', batch_name: 'Pending', scene_count: 1, status: 'pending' },
  ],
};

function group(datasetId, scenes, dataType = 'optical', resolution = 10) {
  return {
    dataset_id: datasetId,
    dataset_code: datasetId.toUpperCase(),
    dataset_title: datasetId,
    data_type: dataType,
    product_type: 'L2A',
    resolution_m: resolution,
    suggested_grid_levels: dataType === 'carbon' ? {} : { geohash: 5, mgrs: 0, isea4h: 11 },
    scenes,
  };
}

function scene(sceneId, batchId, status = 'succeeded') {
  const bandCode = sceneId === 'scene-a' ? 'B04' : sceneId === 'b-scene' ? 'VV' : 'B08';
  return {
    scene_id: sceneId,
    scene_key: sceneId,
    dataset_id: sceneId.startsWith('b-') ? 'dataset-b' : 'dataset-a',
    load_batch_id: batchId,
    load_status: status,
    source_asset_id: `asset-${sceneId}`,
    bbox: [100, 20, 101, 21],
    crs: 'EPSG:4326',
    bands: [{
      band_unit_id: `band-${sceneId}-${bandCode}`,
      asset_id: `asset-${sceneId}`,
      band_code: bandCode,
      band_name: bandCode === 'B04' ? '红光' : bandCode,
      band_type: bandCode === 'VV' ? 'polarization' : 'spectral',
    }],
  };
}

function responseFor(batchId) {
  if (batchId === 'load-a') {
    return {
      datasets: [
        group('dataset-a', [scene('scene-shared', 'load-a'), scene('scene-a', 'load-a')]),
        group('dataset-b', [scene('b-scene', 'load-a')]),
      ],
    };
  }
  return { datasets: [group('dataset-a', [scene('scene-shared', 'load-b', 'duplicate'), scene('scene-b', 'load-b')])] };
}

const stubs = {
  'el-alert': true,
  'el-input': { props: ['modelValue'], template: '<input :value="modelValue" />' },
  'el-checkbox-group': { props: ['modelValue'], template: '<div><slot /></div>' },
  'el-checkbox': { props: ['value', 'disabled'], template: '<label><slot /></label>' },
  'el-icon': { template: '<i><slot /></i>' },
  'el-select': { props: ['disabled'], template: '<select :disabled="disabled"><slot /></select>' },
  'el-option': { template: '<option><slot /></option>' },
  'el-button': { template: '<button v-bind="$attrs"><slot /></button>' },
  'el-tooltip': { template: '<span><slot /></span>' },
};

function mountPanel(props = {}) {
  const wrapper = mount(BatchAssetsPanel, {
    props: { modelValue: [], dataTypeFilter: 'optical', ...props },
    global: { directives: { loading: {} }, stubs },
  });
  wrappers.push(wrapper);
  return wrapper;
}

beforeEach(() => {
  requestGet.mockReset().mockImplementation((url) => {
    if (url === '/v1/partition/load-batches?limit=100&status=succeeded&data_type=optical') return Promise.resolve(batches);
    const batchId = url.includes('/load-a/') ? 'load-a' : 'load-b';
    return Promise.resolve(responseFor(batchId));
  });
});
afterEach(() => wrappers.splice(0).forEach((wrapper) => wrapper.unmount()));

describe('BatchAssetsPanel dataset, scene and band selection', () => {
  it('shows created partition batches inside the pending-data queue', async () => {
    const draft = {
      draft_id: 'partition-draft-a',
      draft_name: '山东光学剖分批次',
      source_load_batch_ids: ['load-a'],
      selection: {
        datasets: [{
          ...group('dataset-a', [scene('scene-a', 'load-a')]),
          band_unit_ids: ['band-scene-a-B04'],
          partition: { grid_type: 'geohash', requested_grid_level: 4, partition_method: 'logical' },
        }],
      },
    };
    const wrapper = mountPanel({ partitionDrafts: [draft] });
    await flushPromises();

    expect(wrapper.text()).toContain('待剖分批次');
    expect(wrapper.get('[data-testid="load-batch-partition-draft-a"]').text()).toContain('山东光学剖分批次');
    await wrapper.vm.loadSelectedBatches(['partition-draft-a']);
    expect(wrapper.emitted('activate-partition-draft')).toEqual([['partition-draft-a']]);
    expect(wrapper.vm.availableBatchGroups.map((batch) => batch.load_batch_id)).toEqual(['partition-draft-a']);
    expect(wrapper.emitted('update:modelValue').at(-1)[0][0]).toMatchObject({
      dataset_id: 'dataset-a',
      band_unit_ids: ['band-scene-a-B04'],
      partition: { grid_type: 'geohash', requested_grid_level: 4 },
    });
  });

  it('locks an ingested load-batch band and displays its completed grid', async () => {
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.vm.loadSelectedBatches(['load-a']);
    const dataset = wrapper.vm.availableDatasets.find((item) => item.dataset_id === 'dataset-a');
    const target = dataset.scenes.find((item) => item.scene_id === 'scene-a');
    target.bands[0].grid_statuses = [{
      grid_type: 'mgrs', grid_level: 1, partition_status: 'completed', ingest_status: 'completed',
    }];

    expect(wrapper.vm.bandConsumedByLoadBatch(target.bands[0])).toBe(true);
    expect(wrapper.vm.ingestedGridLabel(target.bands[0])).toBe('平面格网 · 层级 1');
    expect(wrapper.vm.bandStatusLabel(target.bands[0])).toBe('已入库 · 平面格网 · 层级 1');
    expect(wrapper.vm.selectableBandIdsForScene(target, 'optical', dataset)).toEqual([]);
  });

  it('shows pending and running states for bands that remain in the queue', async () => {
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.vm.loadSelectedBatches(['load-a']);
    const dataset = wrapper.vm.availableDatasets.find((item) => item.dataset_id === 'dataset-a');
    const pending = dataset.scenes.find((item) => item.scene_id === 'scene-a').bands[0];
    pending.grid_statuses = [{ grid_type: 'geohash', grid_level: 4, partition_status: 'completed', quality_status: 'pass', ingest_status: 'pending' }];
    expect(wrapper.vm.bandStatusLabel(pending)).toBe('待入库 · 经纬度格网 · 层级 4');
    pending.grid_statuses[0].ingest_status = 'running';
    expect(wrapper.vm.bandStatusLabel(pending)).toBe('入库中 · 经纬度格网 · 层级 4');
    expect(wrapper.vm.bandConsumedByLoadBatch(pending)).toBe(false);
  });

  it('defaults geographic datasets to Geohash and projected datasets to MGRS', async () => {
    const wrapper = mountPanel();
    await flushPromises();
    const geographic = {
      suggested_grid_type: 'geohash', suggested_grid_levels: { geohash: 4 },
      resolution_native: 0.00030906354339487385, resolution_unit: 'degree',
    };
    const projected = {
      suggested_grid_type: 'mgrs', suggested_grid_levels: { mgrs: 0 },
      resolution_native: 10, resolution_unit: 'm',
    };

    expect(wrapper.vm.defaultPartition(geographic)).toMatchObject({ grid_type: 'geohash', requested_grid_level: 4 });
    expect(wrapper.vm.resolutionLabel(geographic)).toBe('0.00030906354339487385°');
    expect(wrapper.vm.defaultPartition(projected)).toMatchObject({ grid_type: 'mgrs', requested_grid_level: 0 });
    expect(wrapper.vm.resolutionLabel(projected)).toBe('10 m');
  });

  it('loads multiple batches and groups a single batch containing multiple datasets', async () => {
    const wrapper = mountPanel();
    await flushPromises();
    expect(wrapper.vm.availableBatches.map((item) => item.load_batch_id)).toEqual(['load-a', 'load-b']);
    await wrapper.vm.loadSelectedBatches(['load-a']);

    expect(requestGet).toHaveBeenCalledWith(
      '/v1/partition/load-batches/load-a/scenes?data_type=optical',
      expect.any(Object),
    );
    expect(wrapper.vm.availableDatasets.map((item) => item.dataset_id)).toEqual(['dataset-a', 'dataset-b']);
    expect(wrapper.get('[data-testid="batch-tree-load-a"]').exists()).toBe(true);
    expect(wrapper.get('[data-testid="dataset-tree-load-a-dataset-a"]').exists()).toBe(true);
    expect(wrapper.get('[data-testid="dataset-tree-load-a-dataset-b"]').exists()).toBe(true);

    wrapper.vm.updateBandSelection(['band-scene-a-B04', 'band-b-scene-VV']);
    const emitted = wrapper.emitted('update:modelValue').at(-1)[0];
    expect(emitted).toEqual(expect.arrayContaining([
      expect.objectContaining({ dataset_id: 'dataset-a', band_unit_ids: ['band-scene-a-B04'], scenes: [expect.objectContaining({ scene_id: 'scene-a' })] }),
      expect.objectContaining({ dataset_id: 'dataset-b', band_unit_ids: ['band-b-scene-VV'], scenes: [expect.objectContaining({ scene_id: 'b-scene' })] }),
    ]));
  });

  it('merges a dataset across batches and deduplicates the same scene while retaining provenance', async () => {
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.vm.loadSelectedBatches(['load-a', 'load-b']);

    const datasetA = wrapper.vm.availableDatasets.find((item) => item.dataset_id === 'dataset-a');
    expect(datasetA.scenes.map((item) => item.scene_id)).toEqual(['scene-shared', 'scene-a', 'scene-b']);
    expect(datasetA.scenes.find((item) => item.scene_id === 'scene-shared').source_batch_ids).toEqual(['load-a', 'load-b']);
    expect(wrapper.vm.availableBatchGroups.map((item) => item.load_batch_id)).toEqual(['load-a', 'load-b']);
    expect(wrapper.vm.availableBatchGroups[0].datasets[0].scenes[0].load_status).toBe('succeeded');
    expect(wrapper.vm.availableBatchGroups[1].datasets[0].scenes[0].load_status).toBe('duplicate');

    wrapper.vm.updateBandSelection(['band-scene-shared-B08', 'band-scene-b-B08']);
    const emitted = wrapper.emitted('update:modelValue').at(-1)[0];
    expect(emitted).toHaveLength(1);
    expect(emitted[0].dataset_id).toBe('dataset-a');
    expect(emitted[0].scenes).toHaveLength(2);
  });

  it('refreshes available batches without discarding selected bands or expanded nodes', async () => {
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.vm.loadSelectedBatches(['load-a']);
    wrapper.vm.updateBandSelection(['band-scene-a-B04']);
    wrapper.vm.toggleBatchGroup(wrapper.vm.availableBatchGroups[0]);

    await wrapper.vm.loadAvailable({ preserveSelection: true, preserveExpansion: true });

    expect(wrapper.vm.selectedBatchIds).toEqual(['load-a']);
    expect(wrapper.vm.selectedBandUnitIds).toEqual(['band-scene-a-B04']);
    expect(wrapper.vm.collapsedBatches.has('load-a')).toBe(false);
  });

  it('selects and clears all bands by scene or by dataset', async () => {
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.vm.loadSelectedBatches(['load-a']);

    const sceneA = wrapper.vm.availableDatasets[0].scenes.find((item) => item.scene_id === 'scene-a');
    sceneA.bands.push({
      band_unit_id: 'band-scene-a-B03', asset_id: 'asset-scene-a',
      band_code: 'B03', band_name: '绿光', band_type: 'spectral', display_order: 1,
    });
    wrapper.vm.toggleSceneSelection(sceneA, 'optical', true);
    let emitted = wrapper.emitted('update:modelValue').at(-1)[0];
    expect(emitted[0].band_unit_ids).toEqual(['band-scene-a-B04', 'band-scene-a-B03']);
    expect(wrapper.vm.groupSelectionState(wrapper.vm.selectableBandIdsForScene(sceneA, 'optical'))).toEqual({
      checked: true, indeterminate: false,
    });

    wrapper.vm.toggleSceneSelection(sceneA, 'optical', false);
    expect(wrapper.emitted('update:modelValue').at(-1)[0]).toEqual([]);

    wrapper.vm.toggleDatasetSelection('dataset-a', true);
    emitted = wrapper.emitted('update:modelValue').at(-1)[0];
    expect(emitted).toHaveLength(1);
    expect(emitted[0].scenes.map((item) => item.scene_id)).toEqual(['scene-shared', 'scene-a']);
    expect(emitted[0].band_unit_ids).toEqual([
      'band-scene-shared-B08', 'band-scene-a-B04', 'band-scene-a-B03',
    ]);
    expect(wrapper.get('[data-testid="select-dataset-dataset-a"]').exists()).toBe(true);
    expect(wrapper.get('[data-testid="select-scene-scene-a"]').exists()).toBe(true);
  });

  it('supports partial scene selection and keeps each dataset grid independent', async () => {
    const wrapper = mountPanel({ defaultGridType: 'isea4h', defaultRequestedGridLevel: 4 });
    await flushPromises();
    await wrapper.vm.loadSelectedBatches(['load-a']);
    wrapper.vm.updateBandSelection(['band-scene-a-B04']);
    let emitted = wrapper.emitted('update:modelValue').at(-1)[0];
    expect(emitted[0].partition).toMatchObject({
      grid_type: 'isea4h', requested_grid_level: 11, partition_method: 'entity',
      cover_mode: 'intersect', time_granularity: 'day', max_cells_per_asset: 0,
    });

    await wrapper.setProps({ modelValue: emitted });
    expect(wrapper.get('[data-testid="dataset-grid-dataset-a"]').attributes('disabled')).toBeUndefined();
    expect(wrapper.get('[data-testid="dataset-grid-level-dataset-a"]').attributes('disabled')).toBeDefined();
    wrapper.vm.updatePartition('dataset-a', { grid_type: 'mgrs' });
    emitted = wrapper.emitted('update:modelValue').at(-1)[0];
    expect(emitted[0].partition).toMatchObject({ grid_type: 'mgrs', requested_grid_level: 0, partition_method: 'logical' });
    expect(emitted[0].grid_level_unlocked).toBe(false);

    await wrapper.setProps({ modelValue: emitted });
    wrapper.vm.updatePartition('dataset-a', { requested_grid_level: 3 });
    expect(wrapper.emitted('update:modelValue').at(-1)[0][0].partition.requested_grid_level).toBe(0);
    wrapper.vm.unlockGridLevel('dataset-a');
    emitted = wrapper.emitted('update:modelValue').at(-1)[0];
    expect(emitted[0].grid_level_unlocked).toBe(true);
    await wrapper.setProps({ modelValue: emitted });
    wrapper.vm.updatePartition('dataset-a', { requested_grid_level: 3 });
    expect(wrapper.emitted('update:modelValue').at(-1)[0][0].partition.requested_grid_level).toBe(3);
  });

  it('uses the dataset resolution recommendation for initial MGRS selection', async () => {
    const wrapper = mountPanel({ defaultGridType: 'mgrs', defaultRequestedGridLevel: 5 });
    await flushPromises();
    await wrapper.vm.loadSelectedBatches(['load-a']);
    wrapper.vm.updateBandSelection(['band-scene-a-B04']);

    const emitted = wrapper.emitted('update:modelValue').at(-1)[0];
    expect(emitted[0]).toMatchObject({
      resolution_m: 10,
      partition: { grid_type: 'mgrs', requested_grid_level: 0, partition_method: 'logical' },
    });
  });

  it('falls back to 10 km MGRS when source data has no resolution', async () => {
    requestGet.mockImplementation((url) => {
      if (url === '/v1/partition/load-batches?limit=100&status=succeeded&data_type=optical') return Promise.resolve(batches);
      const response = responseFor('load-a');
      response.datasets.forEach((dataset) => {
        delete dataset.resolution_m;
        delete dataset.suggested_grid_levels;
      });
      return Promise.resolve(response);
    });
    const wrapper = mountPanel({ defaultGridType: 'mgrs', defaultRequestedGridLevel: 6 });
    await flushPromises();
    await wrapper.vm.loadSelectedBatches(['load-a']);
    wrapper.vm.updateBandSelection(['band-scene-a-B04']);

    const emitted = wrapper.emitted('update:modelValue').at(-1)[0];
    expect(emitted[0].partition.requested_grid_level).toBe(1);
    await wrapper.setProps({ modelValue: emitted });
    wrapper.vm.updatePartition('dataset-a', { grid_type: 'geohash' });
    expect(wrapper.emitted('update:modelValue').at(-1)[0][0].partition.requested_grid_level).toBe(4);
  });

  it('shows normalized band names and filters data units by band fields', async () => {
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.vm.loadSelectedBatches(['load-a']);

    expect(wrapper.text()).toContain('B04 · 红光');
    wrapper.vm.bandKeyword = 'B04';
    await wrapper.vm.$nextTick();

    expect(wrapper.vm.visibleBatchGroups.flatMap((batch) => batch.datasets.flatMap((dataset) => dataset.scenes.map((item) => item.scene_id)))).toEqual(['scene-a']);
    expect(wrapper.text()).not.toContain('b-scene');
  });

  it('preserves selections accumulated on another product page', async () => {
    const carbon = {
      dataset_id: 'dataset-carbon', data_type: 'carbon', scenes: [{ scene_id: 'carbon-scene', source_batch_ids: ['load-b'] }],
      partition: { grid_type: 'isea4h', requested_grid_level: 4, partition_method: 'entity' },
    };
    const wrapper = mountPanel({ modelValue: [carbon] });
    await flushPromises();
    await wrapper.vm.loadSelectedBatches(['load-a']);
    wrapper.vm.updateBandSelection(['band-scene-a-B04']);
    const emitted = wrapper.emitted('update:modelValue').at(-1)[0];
    expect(emitted).toEqual(expect.arrayContaining([
      carbon,
      expect.objectContaining({ dataset_id: 'dataset-a', data_type: 'optical' }),
    ]));
  });

  it('discards a stale batch detail response after the batch selection changes', async () => {
    let resolveOld;
    requestGet.mockImplementation((url) => {
      if (url === '/v1/partition/load-batches?limit=100&status=succeeded&data_type=optical') return Promise.resolve(batches);
      if (url.includes('/load-a/')) return new Promise((resolve) => { resolveOld = resolve; });
      return Promise.resolve(responseFor('load-b'));
    });
    const wrapper = mountPanel();
    await flushPromises();
    const oldRequest = wrapper.vm.loadSelectedBatches(['load-a']);
    await flushPromises();
    await wrapper.vm.loadSelectedBatches(['load-b']);
    resolveOld(responseFor('load-a'));
    await oldRequest;

    expect(wrapper.vm.selectedBatchIds).toEqual(['load-b']);
    expect(wrapper.vm.availableDatasets[0].scenes.map((item) => item.scene_id)).toEqual(['scene-shared', 'scene-b']);
  });

  it('restores the last successful hierarchy when the current batch request fails', async () => {
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.vm.loadSelectedBatches(['load-a']);
    wrapper.vm.updateBandSelection(['band-scene-a-B04']);
    requestGet.mockImplementation((url) => {
      if (url.includes('/load-b/')) return Promise.reject(new Error('batch detail unavailable'));
      return Promise.resolve(batches);
    });

    await wrapper.vm.loadSelectedBatches(['load-b']);

    expect(wrapper.vm.selectedBatchIds).toEqual(['load-a']);
    expect(wrapper.vm.selectedSceneIds).toEqual(['scene-a']);
    expect(wrapper.vm.selectedBandUnitIds).toEqual(['band-scene-a-B04']);
    expect(wrapper.vm.availableBatchGroups.map((item) => item.load_batch_id)).toEqual(['load-a']);
    expect(wrapper.vm.availableDatasets.map((item) => item.dataset_id)).toEqual(['dataset-a', 'dataset-b']);
  });

  it('rolls back to the last committed hierarchy across chained batch requests', async () => {
    let resolveBatchB;
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.vm.loadSelectedBatches(['load-a']);
    wrapper.vm.updateBandSelection(['band-scene-a-B04']);
    requestGet.mockImplementation((url) => {
      if (url.includes('/load-b/')) return new Promise((resolve) => { resolveBatchB = resolve; });
      if (url.includes('/load-c/')) return Promise.reject(new Error('batch C unavailable'));
      return Promise.resolve(batches);
    });

    const pendingBatchB = wrapper.vm.loadSelectedBatches(['load-b']);
    await flushPromises();
    await wrapper.vm.loadSelectedBatches(['load-c']);
    resolveBatchB(responseFor('load-b'));
    await pendingBatchB;

    expect(wrapper.vm.selectedBatchIds).toEqual(['load-a']);
    expect(wrapper.vm.selectedSceneIds).toEqual(['scene-a']);
    expect(wrapper.vm.selectedBandUnitIds).toEqual(['band-scene-a-B04']);
    expect(wrapper.vm.availableBatchGroups.map((item) => item.load_batch_id)).toEqual(['load-a']);
    expect(wrapper.vm.availableDatasets.map((item) => item.dataset_id)).toEqual(['dataset-a', 'dataset-b']);
  });

  it('does not render a manually editable batch ID input', async () => {
    const wrapper = mountPanel();
    await flushPromises();
    expect(wrapper.find('input[type="text"]').exists()).toBe(false);
    expect(wrapper.get('[data-testid="load-batch-selector"]').exists()).toBe(true);
  });
});
