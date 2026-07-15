export const defaultPageSize = 20;

export function pageQuery(params = {}) {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') query.set(key, String(value));
  });
  return query.toString();
}

export function normalizePageResponse(body, fallbackPage = 1, fallbackPageSize = defaultPageSize) {
  return {
    items: Array.isArray(body?.items) ? body.items : [],
    total: Number(body?.total || 0),
    page: Number(body?.page || fallbackPage),
    pageSize: Number(body?.page_size || fallbackPageSize),
  };
}
