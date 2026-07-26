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
          datasets: [{
            dataset_id: 'dataset-1', dataset_code: 'CARBON-1', scenes: [],
            quality_runs: [{
              quality_run_id: 'quality-run-1', output_version: 'output-1', status: 'pass', results_complete: true,
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
    expect(wrapper.text()).toContain('CARBON-1');
    expect(wrapper.text()).toContain('碳卫星数据结构');
    expect(wrapper.text()).toContain('未发现问题');
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
    await wrapper.findAll('button').find((button) => button.text() === '下载错误明细').trigger('click');
    expect(wrapper.emitted('export-quality-errors')[0][0]).toMatchObject({ quality_run_id: 'quality-run-1' });
  });
});
