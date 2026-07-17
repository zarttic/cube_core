import { mount } from '@vue/test-utils';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import AppLayout from '@/layouts/AppLayout.vue';

const wrappers = [];

beforeEach(() => {
  localStorage.clear();
});

afterEach(() => wrappers.splice(0).forEach((wrapper) => wrapper.unmount()));

describe('AppLayout navigation', () => {
  it('shows the remaining portal navigation when authentication is disabled', () => {
    const wrapper = mount(AppLayout, {
      global: {
        stubs: {
          RouterLink: { props: ['to'], template: '<a :href="to"><slot /></a>' },
        },
      },
    });
    wrappers.push(wrapper);

    expect(wrapper.findAll('.portal-nav a').map((item) => item.text())).toEqual([
      '首页',
      'ARD数据载入',
      '分析就绪数据剖分',
      '剖分数据服务',
      '资源调度',
      '后台管理',
      '全球离散格网模型与编码',
    ]);
  });
});
