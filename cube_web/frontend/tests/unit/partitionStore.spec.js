import { createPinia, setActivePinia } from 'pinia';
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/api/client', () => ({ requestGet: vi.fn(), requestPost: vi.fn() }));

import { requestPost } from '@/api/client';
import { usePartitionStore } from '@/stores/partition';

function dataset(dataType = 'optical') {
  return {
    dataset_id: 'dataset-1', dataset_code: 'DS-1', dataset_title: 'Dataset 1', data_type: dataType, product_type: 'L2A',
    assets: [{ source_asset_id: 'asset-1', cog_uri: 's3://cube/loader/dataset-1/asset-1.tif', checksum: 'a'.repeat(64), bbox: [100, 20, 101, 21], crs: 'EPSG:4326', time_start: '2026-07-01T00:00:00Z', time_end: '2026-07-01T00:05:00Z', attributes: {} }],
    bands: [{ source_asset_id: 'asset-1', band_code: 'B04', band_name: 'Red', band_type: 'spectral', unit: null, display_order: 4, attributes: {} }], attributes: {},
  };
}

describe('partition store', () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    requestPost.mockReset();
    requestPost.mockResolvedValue({ task_id: 'task-1', status: 'queued' });
  });

  it('submits the exact M2 StrictPartitionRequest with normalized datasets and a derived method', async () => {
    const store = usePartitionStore();
    store.form.batchId = 'batch-frontend-001';
    store.form.gridType = 'isea4h';
    store.form.requestedGridLevel = 6;
    store.form.datasets = [dataset()];
    await store.submit();
    expect(requestPost).toHaveBeenCalledWith('/v1/partition/optical/tasks/run', expect.objectContaining({
      batch_id: 'batch-frontend-001', datasets: store.form.datasets, grid_type: 'isea4h', requested_grid_level: 6,
      partition_method: 'entity', cover_mode: 'intersect', time_granularity: 'day', max_cells_per_asset: 0,
    }), expect.any(Object));
    const body = requestPost.mock.calls[0][1];
    expect(Object.keys(body).sort()).toEqual(['batch_id', 'cover_mode', 'datasets', 'grid_type', 'max_cells_per_asset', 'partition_method', 'requested_grid_level', 'time_granularity']);
    expect(body).not.toHaveProperty('dataset_ids');
    expect(body).not.toHaveProperty('grid_level');
    expect(body).not.toHaveProperty('grid_level_mode');
  });

  it.each([
    ['empty batch', (form) => { form.batchId = ' '; }],
    ['mixed data types', (form) => { form.datasets = [dataset('optical'), { ...dataset('radar'), dataset_id: 'dataset-2' }]; }],
    ['empty assets', (form) => { form.datasets = [{ ...dataset(), assets: [] }]; }],
    ['empty bands', (form) => { form.datasets = [{ ...dataset(), bands: [] }]; }],
    ['invalid grid level', (form) => { form.requestedGridLevel = 99; }],
    ['retired request field', (form) => { form.grid_level_mode = 'legacy'; }],
  ])('rejects %s before the request', (_name, mutate) => {
    const store = usePartitionStore();
    store.form.batchId = 'batch-frontend-001';
    store.form.datasets = [dataset()];
    mutate(store.form);
    expect(() => store.buildRequest()).toThrow(/批次|一种数据类型|资产|波段|层级|已退役/);
    expect(requestPost).not.toHaveBeenCalled();
  });
});
