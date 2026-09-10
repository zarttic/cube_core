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
    if (path === '/v1/code/parse') {
      return { grid_type: 'geohash', grid_level: 6, space_code: 'wx4g0b', time_code: '202603091530' };
    }
    if (path === '/v1/code/st') return { st_code: 'gh:6:wx4g0b:202603091530' };
    return {};
  });
});

describe('EncodingView', () => {
  it('uses the requested action label for grid division and encoding or decoding', async () => {
    const wrapper = mountView();

    expect(wrapper.get('button.btn-primary').text()).toContain('查看结果');

    wrapper.vm.activeModule = 'encoding';
    await nextTick();
    expect(wrapper.get('button.btn-primary').text()).toContain('执行编码');

    await wrapper.get('input[value="decode"]').setValue();
    expect(wrapper.get('button.btn-primary').text()).toContain('执行解码');
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

    expect(requestJson).toHaveBeenCalledWith('/v1/code/parse', { st_code: 'gh:6:wx4g0b:202603091530' });
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

  it('keeps coordinate conversion context separate from the result rows', async () => {
    const wrapper = mountView();
    wrapper.vm.activeModule = 'operations';
    wrapper.vm.activeOperation = 'conversion';
    await nextTick();

    await wrapper.vm.runCoordinateConversion();
    await flushPromises();

    const context = wrapper.get('[data-testid="conversion-context"]');
    expect(context.text()).toContain('格网类型经纬度格网');
    expect(context.text()).toContain('点选坐标39.904200, 116.407400');
    expect(context.text()).toContain('基准编码wx4g0b');
    expect(context.findAll('.result-item')).toHaveLength(0);
    expect(wrapper.get('[data-testid="conversion-result"]').text()).toContain('转换结果');
  });
});
