import { describe, expect, it } from 'vitest';
import { mount } from '@vue/test-utils';

import PartitionQualityDrawer from '@/views/quality/PartitionQualityDrawer.vue';

describe('PartitionQualityDrawer', () => {
  it('shows automatic quality rule results for the partition batch', () => {
    const wrapper = mount(PartitionQualityDrawer, {
      props: {
        visible: true,
        detail: {
          partition_run_id: 'partition-run-1',
          summary: { band_count: 1, partitioned_count: 1, quality_pass_count: 1 },
          partition_compute_timing: { elapsed_sec: 2.883303 },
          partition_write_timing: { elapsed_sec: 16.739349 },
          partition_execution_timing: { elapsed_sec: 5.90695 },
          partition_timing: { elapsed_sec: 61.980407, worker_count: 2 },
          datasets: [{
            dataset_id: 'dataset-1', dataset_code: 'CARBON-1', scenes: [],
            quality_runs: [{
              quality_run_id: 'quality-run-1', output_version: 'output-1', status: 'pass', results_complete: true,
              metrics: { checked_grid_count: 120, quality_elapsed_sec: 2, grid_throughput_per_sec: 60 },
              items: [{ rule_code: 'carbon_schema', status: 'pass', finding_count: 0 }],
            }],
          }],
        },
      },
      global: {
        stubs: {
          DetailDrawer: { template: '<div><slot /></div>' },
          StatusTag: { props: ['value'], template: '<span>{{ value }}</span>' },
          'el-button': { template: '<button><slot /></button>' },
          'el-tree': { template: '<div />' },
        },
      },
    });

    expect(wrapper.get('[data-testid="partition-quality-items"]').text()).toContain('自动质检项');
    expect(wrapper.get('[data-testid="partition-timing"]').text()).toContain('剖分耗时');
    expect(wrapper.get('[data-testid="partition-timing"]').text()).toContain('2.883 秒');
    expect(wrapper.get('[data-testid="partition-timing"]').text()).not.toContain('写库耗时');
    expect(wrapper.get('[data-testid="partition-timing"]').text()).not.toContain('16.739 秒');
    expect(wrapper.get('[data-testid="partition-timing"]').text()).not.toContain('5.907 秒');
    expect(wrapper.get('[data-testid="partition-timing"]').text()).not.toContain('计时起点');
    expect(wrapper.get('[data-testid="partition-timing"]').text()).not.toContain('Worker 计时记录');
    expect(wrapper.get('[data-testid="quality-throughput-quality-run-1"]').text()).toContain('60.00 格网/秒');
    expect(wrapper.get('[data-testid="quality-throughput-quality-run-1"]').text()).toContain('120');
    expect(wrapper.text()).toContain('CARBON-1');
    expect(wrapper.text()).toContain('碳卫星数据结构');
    expect(wrapper.text()).toContain('未发现问题');
    expect(wrapper.text()).not.toContain('提交批次质检');
    expect(wrapper.text()).not.toContain('输出版本');
    expect(wrapper.text()).not.toContain('运行 ID');
    expect(wrapper.text()).not.toContain('output-1');
    expect(wrapper.text()).not.toContain('quality-run-1');
  });

  it('allows failed quality units to be repartitioned and exported', async () => {
    const wrapper = mount(PartitionQualityDrawer, {
      props: {
        visible: true,
        detail: {
          partition_run_id: 'partition-run-1',
          summary: { band_count: 1, partitioned_count: 1, quality_failed_count: 1 },
          datasets: [{
            dataset_id: 'dataset-1', dataset_code: 'RADAR-1', scenes: [],
            quality_runs: [{
            quality_run_id: 'quality-run-1', output_version: 'output-1', status: 'fail', results_complete: true,
              execution_error: 'quality rule execution failed (RuntimeError: source object does not exist or cannot be opened)',
              error_logs: [{
                error_code: 'source_object_unreadable', message: 'source object does not exist or cannot be opened',
                source_asset_id: 'asset-1', field: 'source_uri', context: { reason: 'RasterioIOError' },
              }],
              items: [{ rule_code: 'asset_readability', status: 'fail', finding_count: 1 }],
            }],
          }],
        },
      },
      global: {
        stubs: {
          DetailDrawer: { template: '<div><slot /></div>' },
          StatusTag: { props: ['value'], template: '<span>{{ value }}</span>' },
          'el-button': { template: '<button @click="$emit(\'click\')"><slot /></button>' },
          'el-tree': { template: '<div />' },
        },
      },
    });

    await wrapper.findAll('button').find((button) => button.text() === '重新提交失败数据剖分').trigger('click');
    expect(wrapper.emitted('retry-failed-partition').length).toBeGreaterThan(0);
    expect(wrapper.text()).toContain('质检规则执行失败');
    expect(wrapper.text()).toContain('源数据不存在或无法打开');
    expect(wrapper.text()).toContain('源数据无法读取');
    expect(wrapper.text()).toContain('源数据：asset-1');
    expect(wrapper.text()).toContain('具体原因');
    expect(wrapper.text()).toContain('栅格文件读取异常');
    expect(wrapper.text()).not.toContain('source object does not exist or cannot be opened');
    await wrapper.findAll('button').find((button) => button.text() === '导出质检结果').trigger('click');
    expect(wrapper.emitted('export-quality-errors')[0][0]).toMatchObject({ quality_run_id: 'quality-run-1' });
  });

  it('restores quality export for a passed run and exposes force stop for an active batch', () => {
    const wrapper = mount(PartitionQualityDrawer, {
      props: {
        visible: true,
        detail: {
          partition_run_id: 'partition-run-active', status: 'running',
          summary: { band_count: 1, partitioned_count: 1, quality_pass_count: 1 },
          datasets: [{
            dataset_id: 'dataset-1', dataset_code: 'OPTICAL-1', scenes: [],
            quality_runs: [{ quality_run_id: 'quality-run-pass', status: 'pass', results_complete: true, items: [] }],
          }],
        },
      },
      global: {
        stubs: {
          DetailDrawer: { template: '<div><slot /></div>' },
          StatusTag: { props: ['value'], template: '<span>{{ value }}</span>' },
          'el-button': { template: '<button @click="$emit(\'click\')"><slot /></button>' },
          'el-tree': { template: '<div />' },
        },
      },
    });

    expect(wrapper.get('[data-testid="force-cancel-partition"]').text()).toContain('强制终止剖分');
    expect(wrapper.get('[data-testid="quality-export-content-quality-run-pass"]').text()).toContain('导出质检结果');
  });

  it('keeps the existing repartition action available after cancellation', () => {
    const wrapper = mount(PartitionQualityDrawer, {
      props: {
        visible: true,
        detail: { partition_run_id: 'partition-run-cancelled', status: 'cancelled', summary: {}, datasets: [], attempts: [{ status: 'cancelled' }] },
      },
      global: {
        stubs: {
          DetailDrawer: { template: '<div><slot /></div>' },
          StatusTag: { props: ['value'], template: '<span>{{ value }}</span>' },
          'el-button': { template: '<button><slot /></button>' },
          'el-tree': { template: '<div />' },
        },
      },
    });

    expect(wrapper.get('[data-testid="retry-partition"]').text()).toContain('重新提交失败数据剖分');
  });

  it('shows the associated dataset and hides stale failure details while a new run is active', async () => {
    const wrapper = mount(PartitionQualityDrawer, {
      props: {
        visible: true,
        detail: {
          partition_run_id: 'partition-run-2',
          partition_timing: { scope: 'partition_to_ingest', elapsed_sec: null, worker_count: 2 },
          datasets: [{
            dataset_id: 'dataset-2', dataset_code: 'OPTICAL-2', dataset_title: '光学数据集二', scenes: [],
            quality_runs: [{
              quality_run_id: 'quality-run-2', output_version: 'output-2', status: 'running',
              metrics: { checked_grid_count: 0, quality_elapsed_sec: null, grid_throughput_per_sec: null },
              execution_error: 'old failure should not be shown',
              items: [{ rule_code: 'asset_readability', status: 'error', execution_error: 'old rule error' }],
            }],
          }],
        },
      },
      global: {
        stubs: {
          DetailDrawer: { template: '<div><slot /></div>' },
          StatusTag: { props: ['value'], template: '<span>{{ value }}</span>' },
          'el-button': { template: '<button @click="$emit(\'click\')"><slot /></button>' },
          'el-tree': { template: '<div />' },
        },
      },
    });

    expect(wrapper.get('[data-testid="quality-dataset-associations"]').text()).toContain('光学数据集二');
    expect(wrapper.get('[data-testid="partition-timing"]').text()).toContain('等待入库完成');
    expect(wrapper.get('[data-testid="quality-throughput-quality-run-2"]').text()).toContain('等待质检完成');
    expect(wrapper.text()).not.toContain('编码：');
    expect(wrapper.text()).not.toContain('ID：');
    expect(wrapper.text()).not.toContain('OPTICAL-2');
    expect(wrapper.text()).not.toContain('dataset-2');
    expect(wrapper.text()).not.toContain('old failure should not be shown');
    expect(wrapper.text()).not.toContain('old rule error');
    expect(wrapper.find('[data-testid="quality-failure-log-quality-run-2"]').exists()).toBe(false);
  });
});
