import { createPinia, setActivePinia } from 'pinia';
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/api/client', () => ({ requestGet: vi.fn(), requestPost: vi.fn() }));

import { requestGet, requestPost } from '@/api/client';
import { usePartitionStore } from '@/stores/partition';

function scene(sceneId, batchIds) {
  return { scene_id: sceneId, source_batch_ids: batchIds, bbox: [100, 20, 101, 21] };
}

function dataset(datasetId, scenes, gridType = 'geohash', level = 4) {
  return {
    dataset_id: datasetId,
    dataset_code: datasetId.toUpperCase(),
    dataset_title: datasetId,
    data_type: gridType === 'isea4h' ? 'carbon' : 'optical',
    scenes,
    partition: {
      grid_type: gridType,
      requested_grid_level: level,
      partition_method: gridType === 'isea4h' ? 'entity' : 'logical',
      cover_mode: 'intersect',
      time_granularity: 'day',
      max_cells_per_asset: 0,
    },
  };
}

describe('partition store scene request', () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    requestGet.mockReset().mockResolvedValue({ tasks: [], total: 0, page: 1, page_size: 20, load_batches: [] });
    requestPost.mockReset().mockResolvedValue({ partition_run_id: 'server-run', task_id: 'task-1', status: 'queued' });
  });

  it('submits one run for multiple datasets and source load batches', async () => {
    const store = usePartitionStore();
    store.form.datasets = [
      dataset('dataset-optical', [scene('scene-shared', ['load-a', 'load-b']), scene('scene-a', ['load-a'])]),
      dataset('dataset-carbon', [scene('scene-carbon', ['load-b'])], 'geohash', 4),
    ];

    await store.submit();

    expect(requestPost).toHaveBeenCalledOnce();
    const [path, body] = requestPost.mock.calls[0];
    expect(path).toBe('/v1/partition/runs');
    expect(body.partition_run_id).toMatch(/^partition-run-/);
    expect(body.source_batch_ids).toEqual(['load-a', 'load-b']);
    expect(body.selection_source).toBe('load_batch');
    expect(body.source_batch_ids).not.toContain(body.partition_run_id);
    expect(body.datasets).toEqual(expect.arrayContaining([
      expect.objectContaining({
        dataset_id: 'dataset-optical',
        source_batch_id: 'load-a',
        selection_id: 'load-a:dataset-optical',
        scene_ids: ['scene-shared', 'scene-a'],
        partition: expect.objectContaining({ grid_type: 'geohash', requested_grid_level: 4, partition_method: 'logical' }),
      }),
      expect.objectContaining({
        dataset_id: 'dataset-carbon',
        source_batch_id: 'load-b',
        selection_id: 'load-b:dataset-carbon',
        scene_ids: ['scene-carbon'],
        partition: expect.objectContaining({ grid_type: 'geohash', requested_grid_level: 4, partition_method: 'logical' }),
      }),
    ]));
    expect(JSON.stringify(body)).not.toContain('/tasks/run');
    expect(body).not.toHaveProperty('batch_id');
    expect(body.datasets[0]).not.toHaveProperty('assets');
  });

  it('keeps execution identity separate from a load batch identity', () => {
    const store = usePartitionStore();
    store.form.datasets = [dataset('dataset-a', [scene('scene-a', ['load-a'])])];

    expect(() => store.buildRequest('load-a')).toThrow(/不能复用载入批次/);
    expect(store.buildRequest('partition-run-explicit')).toMatchObject({
      partition_run_id: 'partition-run-explicit',
      source_batch_ids: ['load-a'],
    });
  });

  it('submits the selected band units under their dataset and scene hierarchy', () => {
    const store = usePartitionStore();
    const selected = dataset('dataset-a', [scene('scene-a', ['load-a'])]);
    selected.band_unit_ids = ['band-scene-a-b04', 'band-scene-a-b08'];
    store.form.datasets = [selected];

    expect(store.buildRequest('partition-run-bands').datasets[0]).toMatchObject({
      dataset_id: 'dataset-a',
      scene_ids: ['scene-a'],
      band_unit_ids: ['band-scene-a-b04', 'band-scene-a-b08'],
    });
  });

  it('deduplicates source batches retained by the same scene', () => {
    const store = usePartitionStore();
    store.form.datasets = [dataset('dataset-a', [scene('scene-a', ['load-a', 'load-b', 'load-a'])])];

    expect(store.buildRequest('partition-run-a')).toMatchObject({
      source_batch_ids: ['load-a', 'load-b'],
      datasets: [{ dataset_id: 'dataset-a', scene_ids: ['scene-a'] }],
    });
  });

  it('marks a data-management selection so other grid types remain available there', () => {
    const store = usePartitionStore();
    const selected = dataset('dataset-a', [scene('scene-a', ['load-a'])]);
    selected.selection_source = 'dataset';
    store.form.datasets = [selected];

    expect(store.buildRequest('partition-run-dataset')).toMatchObject({
      selection_source: 'dataset',
      source_batch_ids: ['load-a'],
    });
  });

  it('normalizes per-dataset cover, time and cell-limit values', () => {
    const store = usePartitionStore();
    const selected = dataset('dataset-a', [scene('scene-a', ['load-a'])]);
    selected.partition = {
      ...selected.partition,
      cover_mode: 'minimal',
      time_granularity: 'month',
      max_cells_per_asset: 99,
    };
    store.form.datasets = [selected];

    expect(store.buildRequest('partition-run-fixed').datasets[0].partition).toMatchObject({
      cover_mode: 'intersect',
      time_granularity: 'day',
      max_cells_per_asset: 0,
    });
  });

  it.each([
    ['no scenes', [dataset('dataset-a', [])], /至少选择一个景/],
    ['no source batch', [dataset('dataset-a', [scene('scene-a', [])])], /来源载入批次/],
    ['invalid grid level', [dataset('dataset-a', [scene('scene-a', ['load-a'])], 'geohash', 99)], /层级/],
    ['invalid partition method', [{ ...dataset('dataset-a', [scene('scene-a', ['load-a'])]), partition: { grid_type: 'isea4h', requested_grid_level: 4, partition_method: 'logical' } }], /剖分方式/],
  ])('rejects %s before submission', (_name, datasets, message) => {
    const store = usePartitionStore();
    store.form.datasets = datasets;
    expect(() => store.buildRequest('partition-run-invalid')).toThrow(message);
    expect(requestPost).not.toHaveBeenCalled();
  });

  it('rejects one scene selected under two datasets', () => {
    const store = usePartitionStore();
    store.form.datasets = [
      dataset('dataset-a', [scene('scene-shared', ['load-a'])]),
      dataset('dataset-b', [scene('scene-shared', ['load-a'])]),
    ];
    expect(() => store.buildRequest('partition-run-invalid')).toThrow(/不能重复归入/);
  });

  it('loads server-generated batch identities from the formal endpoint', async () => {
    requestGet.mockResolvedValueOnce({
      load_batches: [
        { load_batch_id: 'load-a', status: 'succeeded' },
        { load_batch_id: 'load-pending', status: 'pending' },
      ],
    });
    const store = usePartitionStore();
    await store.loadBatches();
    expect(requestGet).toHaveBeenCalledWith('/v1/partition/load-batches?limit=100&status=succeeded', expect.any(Object));
    expect(store.batches).toEqual([{ load_batch_id: 'load-a', status: 'succeeded' }]);
  });

  it('cancels and retries a managed task through the formal task endpoints', async () => {
    requestPost.mockResolvedValue({ task_id: 'task-a', status: 'cancel_requested' });
    const store = usePartitionStore();

    await store.cancelTask('task-a');
    await store.retryTask('task-a');

    expect(requestPost).toHaveBeenNthCalledWith(1, '/v1/partition/tasks/task-a/cancel', {});
    expect(requestPost).toHaveBeenNthCalledWith(2, '/v1/partition/tasks/task-a/retry', {});
    expect(requestGet).toHaveBeenCalledTimes(2);
  });
});
