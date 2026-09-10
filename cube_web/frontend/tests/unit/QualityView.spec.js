import { flushPromises, mount } from '@vue/test-utils';
import { createPinia, setActivePinia } from 'pinia';
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/api/client', () => ({ requestGet: vi.fn(), requestPost: vi.fn() }));

import { requestGet, requestPost } from '@/api/client';
import QualityView from '@/views/QualityView.vue';

const stubs = {
  AppTable: { template: '<div><slot /></div>' },
  PartitionQualityDrawer: { template: '<div />' },
  StatusTag: { template: '<span />' },
  'el-alert': { template: '<div />' },
  'el-button': { template: '<button><slot /></button>' },
  'el-dialog': { template: '<div><slot /></div>' },
  'el-date-picker': { props: ['modelValue'], template: '<input :value="modelValue" />' },
  'el-drawer': { template: '<div><slot /></div>' },
  'el-form': { template: '<form><slot /></form>' },
  'el-form-item': { template: '<div><slot /></div>' },
  'el-input': { props: ['modelValue'], template: '<input :value="modelValue" />' },
  'el-option': { template: '<option><slot /></option>' },
  'el-select': { template: '<select><slot /></select>' },
  'el-pagination': { template: '<div />' },
  'el-table-column': {
    props: ['label'],
    data: () => ({
      slotProps: {
        row: {
          partition_run_id: 'partition-run-1',
          source_load_batch_names: ['ARD-OPTICAL-c93f070e3a08-剖分批次-20260821110658'],
          datasets: [{ dataset_id: 'dataset-id-1', dataset_code: 'ARD-OPTICAL-c93f070e3a08', dataset_title: '2021年中国黄土高原GF-1 ARD地表反射率数据-01' }],
        },
        $index: 0,
      },
    }),
    template: '<div class="table-column"><span class="column-label">{{ label }}</span><slot v-bind="slotProps" /></div>',
  },
  'el-checkbox': { template: '<label><slot /></label>' },
  'el-tag': { template: '<span><slot /></span>' },
};

beforeEach(() => {
  setActivePinia(createPinia());
  requestGet.mockReset().mockResolvedValue({ items: [], total: 0, page: 1, page_size: 20 });
});

describe('QualityView partition batch list', () => {
  it('uses server-side search and pagination for automated quality batches', async () => {
    requestGet.mockResolvedValue({
      items: [{
        partition_run_id: 'partition-run-1',
        status: 'completed',
        source_load_batch_names: ['ARD-OPTICAL-c93f070e3a08-剖分批次-20260821110658'],
        datasets: [{ dataset_id: 'dataset-id-1', dataset_code: 'ARD-OPTICAL-c93f070e3a08', dataset_title: '2021年中国黄土高原GF-1 ARD地表反射率数据-01' }],
      }],
      total: 41,
      page: 1,
      page_size: 20,
    });
    const wrapper = mount(QualityView, { global: { stubs } });
    await flushPromises();

    expect(requestGet).toHaveBeenNthCalledWith(1, '/v1/partition/runs?page=1&page_size=20');
    expect(wrapper.vm.pageState.total).toBe(41);
    expect(wrapper.text()).toContain('2021年中国黄土高原GF-1 ARD地表反射率数据-01');
    expect(wrapper.text()).not.toContain('来源');
    expect(wrapper.text()).not.toContain('ARD-OPTICAL-c93f070e3a08-剖分批次-20260821110658');
    expect(wrapper.text()).not.toContain('dataset-id-1');

    wrapper.vm.filters.keyword = 'landsat';
    wrapper.vm.filters.dataType = 'optical';
    wrapper.vm.filters.createdAtRange = ['2026-08-01', '2026-08-24'];
    await wrapper.vm.applyFilters();

    expect(requestGet).toHaveBeenLastCalledWith(
      '/v1/partition/runs?keyword=landsat&data_type=optical&created_from=2026-08-01&created_to=2026-08-24&page=1&page_size=20',
    );
  });

  it('keeps task detail data bound to the selected partition run when responses resolve out of order', async () => {
    const detailResolvers = [];
    requestGet.mockImplementation((url) => {
      if (url.startsWith('/v1/partition/runs?')) {
        return Promise.resolve({ items: [], total: 0, page: 1, page_size: 20 });
      }
      return new Promise((resolve) => detailResolvers.push(resolve));
    });
    const wrapper = mount(QualityView, { global: { stubs } });
    await flushPromises();

    const first = wrapper.vm.openBatch({ partition_run_id: 'partition-run-a' });
    const second = wrapper.vm.openBatch({ partition_run_id: 'partition-run-b' });
    detailResolvers[1]({ partition_run_id: 'partition-run-b', datasets: [] });
    detailResolvers[0]({ partition_run_id: 'partition-run-a', datasets: [] });
    await Promise.all([first, second]);

    expect(wrapper.vm.selectedId).toBe('partition-run-b');
    expect(wrapper.vm.detail.partition_run_id).toBe('partition-run-b');
    wrapper.vm.closeDetail();
  });

  it('synchronizes the batch status while the opened partition run is active', async () => {
    vi.useFakeTimers();
    try {
      let detailRequestCount = 0;
      requestGet.mockImplementation((url) => {
        if (url.startsWith('/v1/partition/runs?')) {
          return Promise.resolve({
            items: [{ partition_run_id: 'partition-run-1', status: 'queued', band_count: 2 }],
            total: 1,
            page: 1,
            page_size: 20,
          });
        }
        detailRequestCount += 1;
        return Promise.resolve({
          partition_run_id: 'partition-run-1',
          status: detailRequestCount === 1 ? 'running' : 'completed',
          summary: { band_count: detailRequestCount === 1 ? 1 : 2 },
          datasets: [],
        });
      });
      const wrapper = mount(QualityView, { global: { stubs } });
      await flushPromises();

      await wrapper.vm.openBatch({ partition_run_id: 'partition-run-1' });
      await flushPromises();
      expect(wrapper.vm.batches[0].status).toBe('running');
      expect(wrapper.vm.batches[0].band_count).toBe(1);

      await vi.advanceTimersByTimeAsync(1000);
      await flushPromises();
      expect(wrapper.vm.batches[0].status).toBe('completed');
      expect(wrapper.vm.batches[0].band_count).toBe(2);

      const requestCountAfterTerminal = requestGet.mock.calls.length;
      await vi.advanceTimersByTimeAsync(2000);
      await flushPromises();
      expect(requestGet.mock.calls.length).toBe(requestCountAfterTerminal);
      wrapper.vm.closeDetail();
    } finally {
      vi.useRealTimers();
    }
  });

  it('submits an immediate quality retry with the dataset identity and adopts only the new quality_run_id', async () => {
    requestGet.mockImplementation((url) => {
      if (url.startsWith('/v1/partition/runs?')) {
        return Promise.resolve({ items: [], total: 0, page: 1, page_size: 20 });
      }
      return Promise.resolve({
        partition_run_id: 'partition-run-1',
        datasets: [{
          dataset_id: 'dataset-1', dataset_code: 'DATASET-1', dataset_title: '数据集一',
          quality_runs: [{ quality_run_id: 'quality-run-new', output_version: 'output-1', status: 'running', execution_error: null, items: [] }],
        }],
      });
    });
    requestPost.mockResolvedValue({ quality_run_id: 'quality-run-new', status: 'pending' });
    const wrapper = mount(QualityView, { global: { stubs } });
    await flushPromises();
    wrapper.vm.selectedId = 'partition-run-1';
    wrapper.vm.detail = {
      partition_run_id: 'partition-run-1',
      datasets: [{
        dataset_id: 'dataset-1', dataset_code: 'DATASET-1', dataset_title: '数据集一',
        quality_runs: [{ quality_run_id: 'quality-run-old', output_version: 'output-1', status: 'error', execution_error: 'old failure' }],
      }],
    };

    await wrapper.vm.retryQualityRun({ dataset_id: 'dataset-1', output_version: 'output-1', quality_run_id: 'quality-run-old' });

    expect(requestPost).toHaveBeenCalledWith('/v1/quality/runs', { dataset_id: 'dataset-1', output_version: 'output-1' });
    expect(wrapper.vm.detail.datasets[0].dataset_id).toBe('dataset-1');
    expect(wrapper.vm.detail.datasets[0].quality_runs[0].quality_run_id).toBe('quality-run-new');
    expect(wrapper.vm.detail.datasets[0].quality_runs[0].execution_error).toBeNull();
    expect(wrapper.vm.detail.datasets[0].quality_runs[0].execution_error).not.toBe('old failure');
    wrapper.vm.closeDetail();
  });

  it('force stops the selected partition run through the run-level endpoint', async () => {
    requestGet.mockResolvedValue({ items: [], total: 0, page: 1, page_size: 20, datasets: [] });
    requestPost.mockResolvedValue({ partition_run_id: 'partition-run-running', status: 'cancelled' });
    const wrapper = mount(QualityView, { global: { stubs } });
    await flushPromises();
    wrapper.vm.selectedId = 'partition-run-running';

    await wrapper.vm.cancelPartition();

    expect(requestPost).toHaveBeenCalledWith('/v1/partition/runs/partition-run-running/cancel', {});
  });
});
