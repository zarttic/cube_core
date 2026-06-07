import { runtimeNavigation } from '@/config';

const defaultNavItems = [
  { label: '首页', kind: 'internal', path: '/' },
  { label: '分析就绪数据剖分', kind: 'internal', path: '/partition' },
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
  const runtimeItems = runtimeNavigation();
  const includeLocalHome = !runtimeItems.some((item) => item?.label === '首页');
  const items = [
    ...(includeLocalHome ? defaultNavItems.slice(0, 1) : []),
    ...runtimeItems,
    ...defaultNavItems.slice(1),
  ];
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
