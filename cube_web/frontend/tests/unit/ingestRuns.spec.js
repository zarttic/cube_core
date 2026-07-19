import { createPinia, setActivePinia } from 'pinia';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { mount } from '@vue/test-utils';

const pendingGets = [];
vi.mock('@/api/client', () => ({
  requestGet: vi.fn(() => new Promise((resolve) => pendingGets.push(resolve))),
  requestPost: vi.fn(),
}));

import { requestGet, requestPost } from '@/api/client';
import { useIngestRunsStore } from '@/stores/ingestRuns';
import IngestRunDetailDrawer from '@/views/ingest/IngestRunDetailDrawer.vue';
import IngestView from '@/views/IngestView.vue';

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

describe('manual ingest selection', () => {
  it('shows selectable data as dataset, scene, and readable band hierarchy', async () => {
    const collection = {
      partition_run_id: 'partition-run-a', dataset_count: 1, scene_count: 1, quality_pass_count: 2, ingested_count: 0,
      units: [
        { dataset_id: 'dataset-a', dataset_code: 'ARD-OPTICAL', dataset_title: '光学遥感样例', data_type: 'optical', scene_id: 'scene-internal', scene_key: 'LC08_120029_20240622', band_unit_id: 'band-internal-a', band_code: 'B04', band_name: '红光', band_type: 'spectral', display_order: 1, quality_status: 'pass', ingest_status: 'pending' },
        { dataset_id: 'dataset-a', dataset_code: 'ARD-OPTICAL', dataset_title: '光学遥感样例', data_type: 'optical', scene_id: 'scene-internal', scene_key: 'LC08_120029_20240622', band_unit_id: 'band-internal-b', band_code: 'B08', band_name: '近红外', band_type: 'spectral', display_order: 2, quality_status: 'warn', ingest_status: 'pending' },
      ],
    };
    requestGet.mockImplementation((url) => Promise.resolve(url.startsWith('/v1/ingest-runs?')
      ? { items: [], total: 0, page: 1, page_size: 20 }
      : { items: [collection] }));
    const wrapper = mount(IngestView, {
      global: {
        stubs: {
          AppTable: { template: '<div><slot /></div>' },
          StatusTag: { template: '<span />' },
          IngestRunDetailDrawer: { template: '<div />' },
          'el-icon': { template: '<span><slot /></span>' },
          'el-button': { template: '<button @click="$emit(\'click\')"><slot /></button>' },
          'el-form': { template: '<form><slot /></form>' },
          'el-form-item': { template: '<div><slot /></div>' },
          'el-select': { template: '<div><slot /></div>' },
          'el-option': { template: '<div><slot /></div>' },
          'el-checkbox-group': { template: '<div><slot /></div>' },
          'el-checkbox': { template: '<label><slot /></label>' },
          'el-dialog': { template: '<div><slot /><slot name="footer" /></div>' },
          'el-alert': { template: '<div />' },
          'el-progress': { template: '<div />' },
          'el-table-column': { template: '<div />' },
          'el-input': { template: '<input />' },
        },
      },
    });

    await vi.waitFor(() => expect(wrapper.findAll('button').some((button) => button.text() === '选择数据入库')).toBe(true));
    await wrapper.findAll('button').find((button) => button.text() === '选择数据入库').trigger('click');
    await vi.waitFor(() => expect(wrapper.get('[data-testid="manual-ingest-tree"]')).toBeTruthy());

    expect(wrapper.text()).toContain('光学遥感样例');
    expect(wrapper.text()).toContain('LC08_120029_20240622');
    expect(wrapper.text()).toContain('B04 · 红光');
    expect(wrapper.text()).toContain('B08 · 近红外');
    expect(wrapper.findAll('.manual-select-all')).toHaveLength(2);
    expect(wrapper.text()).toContain('全选数据集');
    expect(wrapper.text()).toContain('全选该景');
    expect(wrapper.text()).not.toContain('band-internal-a');
    expect(wrapper.text()).not.toContain('scene-internal');
  });
});
