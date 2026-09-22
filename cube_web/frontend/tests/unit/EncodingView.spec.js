import { flushPromises, mount } from '@vue/test-utils';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { nextTick } from 'vue';

const { requestJson } = vi.hoisted(() => ({ requestJson: vi.fn() }));

vi.mock('@/api/client', () => ({
  apiPrefixes: () => ({ gridPrefix: '/v1/grid', codePrefix: '/v1/code', topologyPrefix: '/v1/topology' }),
  requestJson,
}));

import EncodingView from '@/views/EncodingView.vue';

const GlobeMapStub = {
  name: 'GlobeMap',
  template: '<div data-testid="encoding-map-stub" />',
};

function mountView() {
  return mount(EncodingView, {
    global: { stubs: { GlobeMap: GlobeMapStub } },
  });
}

beforeEach(() => {
  requestJson.mockReset().mockImplementation(async (path, payload) => {
    if (path === '/v1/grid/locate') {
      return {
        cell: {
          grid_type: payload.grid_type,
          grid_level: payload.requested_grid_level,
          space_code: 'wx4g0b',
          center: [116.4074, 39.9042],
          bbox: [116.3, 39.8, 116.5, 40.0],
        },
      };
    }
    if (path === '/v1/topology/geometry' && payload.boundary_type === 'bbox') {
      return { geometry: { bbox: [116.3, 39.8, 116.5, 40.0] } };
    }
    if (path === '/v1/topology/geometry') {
      return { geometry: { type: 'Polygon', coordinates: [] } };
    }
    if (path === '/v1/topology/children') {
      return { addresses: [] };
    }
    if (path === '/v1/topology/neighbors') {
      return { addresses: [{ ...payload.address, space_code: 'wx4g0c' }] };
    }
    if (path === '/v1/code/GNse') {
      return { grid_type: 'geohash', grid_level: 6, space_code: 'wx4g0b', time_code: '202603091530' };
    }
    if (path === '/v1/code/st') return { st_code: 'gh:6:wx4g0b:202603091530' };
    return {};
  });
});

describe('EncodingView', () => {
  it('labels every execute button the same way and still switches decode result title', async () => {
    const wrapper = mountView();

    expect(wrapper.get('button.btn-primary').text()).toBe('执行');

    wrapper.vm.activeModule = 'encoding';
    await nextTick();
    expect(wrapper.get('button.btn-primary').text()).toBe('执行');

    await wrapper.get('input[value="decode"]').setValue();
    expect(wrapper.get('button.btn-primary').text()).toBe('执行');
    expect(wrapper.get('.result-panel h3').text()).toBe('解码结果');
  });

  it('runs the selected encoding mode instead of always encoding', async () => {
    const wrapper = mountView();
    wrapper.vm.activeModule = 'encoding';
    await nextTick();
    await wrapper.get('input[value="decode"]').setValue();
    wrapper.vm.encoding.decodeInput = 'gh:6:wx4g0b:202603091530';

    await wrapper.get('button.btn-primary').trigger('click');
    await flushPromises();

    expect(requestJson).toHaveBeenCalledWith('/v1/code/GNse', { st_code: 'gh:6:wx4g0b:202603091530' });
    expect(requestJson).not.toHaveBeenCalledWith('/v1/code/st', expect.anything());
    expect(wrapper.text()).toContain('解码');
  });

  it('runs the encoding request chain when encoding is selected', async () => {
    const wrapper = mountView();
    wrapper.vm.activeModule = 'encoding';
    await nextTick();

    await wrapper.get('button.btn-primary').trigger('click');
    await flushPromises();

    expect(requestJson).toHaveBeenCalledWith('/v1/grid/locate', expect.objectContaining({
      grid_type: 'geohash', requested_grid_level: 6, point: [116.4074, 39.9042],
    }));
    expect(requestJson).toHaveBeenCalledWith('/v1/code/st', expect.objectContaining({
      time_granularity: 'minute',
    }));
  });

  it('switches topology and coordinate conversion inputs and results together', async () => {
    const wrapper = mountView();
    wrapper.vm.activeModule = 'operations';
    await nextTick();

    expect(wrapper.get('[role="tabpanel"]').attributes('aria-label')).toBe('拓扑运算');
    expect(wrapper.get('[data-testid="topology-result"]').exists()).toBe(true);
    expect(wrapper.find('[data-testid="conversion-result"]').exists()).toBe(false);

    wrapper.vm.gridGeometries = [{ geometry: { type: 'Polygon', coordinates: [] } }];
    await wrapper.get('[data-testid="operation-tab-conversion"]').trigger('click');

    expect(wrapper.vm.activeOperation).toBe('conversion');
    expect(wrapper.vm.gridGeometries).toEqual([]);
    expect(wrapper.get('[role="tabpanel"]').attributes('aria-label')).toBe('坐标转换');
    expect(wrapper.get('[data-testid="conversion-result"]').exists()).toBe(true);
    expect(wrapper.find('[data-testid="topology-result"]').exists()).toBe(false);
  });

  it('defaults the child target level to one level above the base level', async () => {
    const wrapper = mountView();
    wrapper.vm.activeModule = 'operations';
    await nextTick();

    expect(wrapper.vm.topology.targetLevel).toBe(7);

    wrapper.vm.topology.level = 9;
    await nextTick();
    expect(wrapper.vm.topology.targetLevel).toBe(10);

    wrapper.vm.topology.level = 12;
    await nextTick();
    expect(wrapper.vm.topology.targetLevel).toBe(12);
  });

  it('falls back to one level deeper for children when the target level is not deeper', async () => {
    const wrapper = mountView();
    wrapper.vm.activeModule = 'operations';
    await nextTick();
    wrapper.vm.topology.operation = 'children';
    wrapper.vm.topology.targetLevel = 6;

    await wrapper.vm.runTopologyOperation();
    await flushPromises();

    expect(wrapper.vm.topology.targetLevel).toBe(7);
    expect(requestJson).toHaveBeenCalledWith('/v1/topology/children', expect.objectContaining({ target_grid_level: 7 }));
  });

  it('keeps topology context separate from the result rows', async () => {
    const wrapper = mountView();
    wrapper.vm.activeModule = 'operations';
    await nextTick();

    await wrapper.vm.runTopologyOperation();
    await flushPromises();

    const result = wrapper.get('[data-testid="topology-result"]');
    const context = wrapper.get('[data-testid="topology-context"]');
    expect(context.text()).toContain('格网类型经纬度格网');
    expect(context.text()).toContain('运算类型邻接单元计算');
    expect(context.text()).toContain('输入编码');
    // 已去掉的两行不再出现
    expect(context.text()).not.toContain('基准编码数');
    expect(context.text()).not.toContain('目标层级');
    // 上下文（转换前）不是卡片样式，结果（转换后）才用 result-item 卡片。
    expect(context.findAll('.result-item')).toHaveLength(0);
    expect(context.find('.operation-context-item').exists()).toBe(true);
    expect(result.findAll('.result-item')).toHaveLength(2);
    expect(result.text()).toContain('结果数量1');
    expect(result.text()).toContain('wx4g0c');
  });

  it('keeps coordinate conversion context separate from the result rows', async () => {
    const wrapper = mountView();
    wrapper.vm.activeModule = 'operations';
    wrapper.vm.activeOperation = 'conversion';
    await nextTick();

    await wrapper.vm.runCoordinateConversion();
    await flushPromises();

    const context = wrapper.get('[data-testid="conversion-context"]');
    expect(context.text()).toContain('格网类型经纬度格网');
    expect(context.text()).toContain('点选坐标39.904200°N, 116.407400°E');
    expect(context.text()).toContain('基准编码wx4g0b');
    expect(context.findAll('.result-item')).toHaveLength(0);
    expect(wrapper.get('[data-testid="conversion-result"]').text()).toContain('转换结果');
  });
});
