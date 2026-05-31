import { runtimeNavigation } from '@/config';

const defaultNavItems = [
  { label: '首页', kind: 'internal', path: '/' },
  { label: '分析就绪数据剖分', kind: 'internal', path: '/partition' },
  { label: '全球离散格网模型与编码', kind: 'internal', path: '/encoding' },
];

export function navItems() {
  return [
    ...defaultNavItems.slice(0, 1),
    ...runtimeNavigation(),
    ...defaultNavItems.slice(1),
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
