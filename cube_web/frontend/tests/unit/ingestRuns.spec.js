import { createPinia, setActivePinia } from 'pinia';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { mount } from '@vue/test-utils';

const pendingGets = [];
vi.mock('@/api/client', () => ({
  requestGet: vi.fn(() => new Promise((resolve) => pendingGets.push(resolve))),
  requestPost: vi.fn(),
}));

import { requestPost } from '@/api/client';
import { useIngestRunsStore } from '@/stores/ingestRuns';
import IngestRunDetailDrawer from '@/views/ingest/IngestRunDetailDrawer.vue';

beforeEach(() => {
  setActivePinia(createPinia());
  pendingGets.splice(0);
  vi.clearAllMocks();
});

describe('ingest run detail drawer', () => {
  it('treats partial failure as retryable but not cancellable and emits explicit failed band ids', async () => {
    const wrapper = mount(IngestRunDetailDrawer, {
      props: {
        visible: true,
        runId: 'ingest-a',
        detail: {
          ingest_run_id: 'ingest-a',
          status: 'partial_failure',
          scenes: [
            { scene_id: 'scene-ok', status: 'completed', band_unit_ids: ['band-ok'] },
            { scene_id: 'scene-failed-a', status: 'failed', band_unit_ids: ['band-failed-a'] },
            { scene_id: 'scene-failed-b', status: 'failed', band_unit_ids: ['band-failed-b'] },
          ],
        },
      },
      global: {
        stubs: {
          DetailDrawer: { template: '<div><slot /></div>' },
          AppTable: { template: '<div><slot /></div>' },
          StatusTag: { template: '<span />' },
          'el-button': { template: '<button><slot /></button>' },
          'el-input': { template: '<input />' },
          'el-descriptions': { template: '<div><slot /></div>' },
          'el-descriptions-item': { template: '<div><slot /></div>' },
          'el-table-column': { template: '<div />' },
          'el-empty': { template: '<div />' },
          'el-dialog': { template: '<div />' },
        },
      },
    });

    expect(wrapper.text()).toContain('重试全部失败波段');
    expect(wrapper.text()).not.toContain('取消运行');
    await wrapper.findAll('button').find((button) => button.text() === '重试全部失败波段').trigger('click');
    expect(wrapper.emitted('retry-band-units')).toEqual([[['band-failed-a', 'band-failed-b']]]);
  });
});

describe('ingest runs store', () => {
  it('keeps only the most recently opened run detail', async () => {
    const store = useIngestRunsStore();
    const first = store.openDetail('ingest-a');
    const second = store.openDetail('ingest-b');
    pendingGets[1]({ ingest_run_id: 'ingest-b', dataset_id: 'dataset-b', scenes: [] });
    pendingGets[0]({ ingest_run_id: 'ingest-a', dataset_id: 'dataset-a', scenes: [] });
    await Promise.all([first, second]);
    expect(store.selectedRunId).toBe('ingest-b');
    expect(store.detail.ingest_run_id).toBe('ingest-b');
  });

  it('retries only the explicit failed band ids and refreshes detail and list', async () => {
    const store = useIngestRunsStore();
    requestPost.mockResolvedValue({ status: 'queued' });
    const opened = store.openDetail('ingest-a');
    pendingGets[0]({ ingest_run_id: 'ingest-a', dataset_id: 'dataset-a', scenes: [] });
    await opened;

    const action = store.retryFailedBandUnits(['band-failed']);
    await vi.waitFor(() => expect(pendingGets[1]).toBeTypeOf('function'));
    pendingGets[1]({ ingest_run_id: 'ingest-a', dataset_id: 'dataset-a', scenes: [] });
    await vi.waitFor(() => expect(pendingGets[2]).toBeTypeOf('function'));
    pendingGets[2]({ items: [], total: 0, page: 1, page_size: 20 });
    await action;

    expect(requestPost).toHaveBeenCalledWith('/v1/ingest-runs/ingest-a/retry', { band_unit_ids: ['band-failed'] });
  });

  it('cancels an ingest run independently from its partition and load batch ids', async () => {
    const store = useIngestRunsStore();
    requestPost.mockResolvedValue({ status: 'cancelled' });
    const opened = store.openDetail('ingest-a');
    pendingGets[0]({ ingest_run_id: 'ingest-a', partition_run_id: 'partition-a', scenes: [] });
    await opened;

    const action = store.cancelRun('operator request');
    await vi.waitFor(() => expect(pendingGets[1]).toBeTypeOf('function'));
    pendingGets[1]({ ingest_run_id: 'ingest-a', partition_run_id: 'partition-a', scenes: [] });
    await vi.waitFor(() => expect(pendingGets[2]).toBeTypeOf('function'));
    pendingGets[2]({ items: [], total: 0, page: 1, page_size: 20 });
    await action;

    expect(requestPost).toHaveBeenCalledWith('/v1/ingest-runs/ingest-a/cancel', { reason: 'operator request' });
  });
});
