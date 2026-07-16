import { createPinia, setActivePinia } from 'pinia';
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/api/client', () => ({ requestGet: vi.fn(), requestPost: vi.fn() }));

import { requestPost } from '@/api/client';
import { usePartitionStore } from '@/stores/partition';

function dataset(dataType = 'optical') {
  const carbon = dataType === 'carbon';
  return {
    dataset_id: 'dataset-1', dataset_code: 'DS-1', dataset_title: 'Dataset 1', data_type: dataType, product_type: 'L2A',
    assets: [{
      source_asset_id: 'asset-1',
      ...(carbon
        ? { source_uri: 's3://cube/cube/source/carbon/observation.nc4', source_kind: 'raw', source_format: 'netcdf' }
        : { cog_uri: 's3://cube/loader/dataset-1/asset-1.tif', source_kind: 'cog', source_format: 'cog' }),
      checksum: 'a'.repeat(64), bbox: [100, 20, 101, 21], crs: 'EPSG:4326', time_start: '2026-07-01T00:00:00Z', time_end: '2026-07-01T00:05:00Z', attributes: {},
    }],
    bands: [{ source_asset_id: 'asset-1', band_code: 'B04', band_name: 'Red', band_type: 'spectral', unit: null, display_order: 4, attributes: {} }], attributes: {},
    partition: { grid_type: 'geohash', requested_grid_level: 6, partition_method: 'logical' },
  };
}

describe('partition store', () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    requestPost.mockReset();
    requestPost.mockResolvedValue({ task_id: 'task-1', status: 'queued' });
  });

  it('submits a mixed batch to the generic M2 route with dataset partition overrides', async () => {
    const store = usePartitionStore();
    store.form.batchId = 'batch-frontend-001';
    store.form.gridType = 'isea4h';
    store.form.requestedGridLevel = 6;
    store.form.datasets = [
      { ...dataset('optical'), partition: { grid_type: 'geohash', requested_grid_level: 6, partition_method: 'logical' } },
      { ...dataset('carbon'), dataset_id: 'dataset-2', dataset_code: 'DS-2', partition: { grid_type: 'isea4h', requested_grid_level: 4, partition_method: 'entity' } },
    ];
    await store.submit();
    expect(requestPost).toHaveBeenCalledWith('/v1/partition/tasks/run', expect.objectContaining({
      batch_id: 'batch-frontend-001', datasets: store.form.datasets, grid_type: 'isea4h', requested_grid_level: 6,
      partition_method: 'entity', cover_mode: 'intersect', time_granularity: 'day', max_cells_per_asset: 0,
    }), expect.any(Object));
    const body = requestPost.mock.calls[0][1];
    expect(Object.keys(body).sort()).toEqual(['batch_id', 'cover_mode', 'datasets', 'grid_type', 'max_cells_per_asset', 'partition_method', 'requested_grid_level', 'time_granularity']);
    expect(body.datasets.map((item) => item.partition)).toEqual([
      { grid_type: 'geohash', requested_grid_level: 6, partition_method: 'logical' },
      { grid_type: 'isea4h', requested_grid_level: 4, partition_method: 'entity' },
    ]);
    expect(body.datasets[1].assets[0]).toMatchObject({ source_uri: 's3://cube/cube/source/carbon/observation.nc4', source_kind: 'raw', source_format: 'netcdf' });
    expect(body.datasets[1].assets[0]).not.toHaveProperty('cog_uri');
    expect(body).not.toHaveProperty('dataset_ids');
    expect(body).not.toHaveProperty('grid_level');
    expect(body).not.toHaveProperty('grid_level_mode');
  });

  it('keeps homogeneous batches on the typed strict endpoint', async () => {
    const store = usePartitionStore();
    store.form.batchId = 'batch-optical-001';
    store.form.datasets = [dataset('optical')];

    await store.submit();

    expect(requestPost).toHaveBeenCalledWith(
      '/v1/partition/optical/tasks/run',
      expect.objectContaining({ batch_id: 'batch-optical-001' }),
      expect.any(Object),
    );
  });

  it.each([
    ['empty batch', (form) => { form.batchId = ' '; }],
    ['empty assets', (form) => { form.datasets = [{ ...dataset(), assets: [] }]; }],
    ['empty bands', (form) => { form.datasets = [{ ...dataset(), bands: [] }]; }],
    ['invalid grid level', (form) => { form.requestedGridLevel = 99; }],
    ['invalid dataset partition', (form) => { form.datasets = [{ ...dataset(), partition: { grid_type: 'isea4h', requested_grid_level: 4, partition_method: 'logical' } }]; }],
    ['carbon COG asset', (form) => { form.datasets = [dataset('carbon'), { ...dataset('carbon'), assets: [{ ...dataset('carbon').assets[0], source_uri: undefined, cog_uri: 's3://cube/source/carbon/converted.tif' }] }]; }],
    ['retired request field', (form) => { form.grid_level_mode = 'legacy'; }],
  ])('rejects %s before the request', (_name, mutate) => {
    const store = usePartitionStore();
    store.form.batchId = 'batch-frontend-001';
    store.form.datasets = [dataset()];
    mutate(store.form);
    expect(() => store.buildRequest()).toThrow(/批次|资产|波段|层级|剖分方式|已退役/);
    expect(requestPost).not.toHaveBeenCalled();
  });
});
