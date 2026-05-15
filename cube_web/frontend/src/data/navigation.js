export const navItems = [
  { label: '首页', kind: 'internal', path: '/' },
  { label: 'ARD数据载入', kind: 'external', url: 'http://10.136.1.14:5177/ard' },
  { label: '分析就绪数据剖分', kind: 'internal', path: '/partition' },
  { label: '剖分数据服务', kind: 'external', url: 'http://10.136.1.14:5176/#/partition' },
  { label: '资源调度', kind: 'external', url: 'http://10.136.1.14:5176/#/dispatch' },
  { label: '后台管理', kind: 'external', url: 'http://10.136.1.14:5177/admin' },
  { label: '全球离散格网模型与编码', kind: 'internal', path: '/encoding' },
];

export function normalizePath(pathname) {
  if (pathname === '/index.html') return '/';
  if (pathname === '/partition.html') return '/partition';
  if (pathname === '/quality.html') return '/quality';
  if (pathname === '/encoding.html') return '/encoding';
  if (pathname === '/门户首页.html') return '/';
  return pathname || '/';
}
