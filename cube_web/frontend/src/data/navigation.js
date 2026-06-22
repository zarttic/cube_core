import { runtimeNavigation } from '@/config';

const defaultNavItems = [
  { label: '首页', kind: 'internal', path: '/' },
  { label: 'ARD数据载入', kind: 'external', url: '/ard' },
  { label: '分析就绪数据剖分', kind: 'internal', path: '/partition' },
  { label: '剖分数据服务', kind: 'internal', path: '/partition' },
  { label: '资源调度', kind: 'external', url: '/dispatch' },
  { label: '后台管理', kind: 'external', url: '/admin' },
  { label: '全球离散格网模型与编码', kind: 'internal', path: '/encoding' },
];

const headerLabelOrder = [
  '首页',
  'ARD数据载入',
  '分析就绪数据剖分',
  '剖分数据服务',
  '资源调度',
  '后台管理',
  '全球离散格网模型与编码',
];

export function navItems() {
  const itemsByLabel = new Map(defaultNavItems.map((item) => [item.label, item]));
  runtimeNavigation().forEach((item) => {
    if (item?.label) itemsByLabel.set(item.label, item);
  });
  const items = [...itemsByLabel.values()];
  return [
    ...headerLabelOrder.flatMap((label) => items.filter((item) => item?.label === label)),
    ...items.filter((item) => !headerLabelOrder.includes(item?.label)),
  ];
}

export function normalizePath(pathname) {
  if (pathname === '/index.html') return '/';
  if (pathname === '/partition.html') return '/partition';
  if (pathname === '/quality.html') return '/quality';
  if (pathname === '/encoding.html') return '/encoding';
  if (pathname === '/config' || pathname === '/config.html') return '/partition';
  if (pathname === '/门户首页.html') return '/';
  return pathname || '/';
}
