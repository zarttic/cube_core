# Milestone 4 Frontend Architecture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the ad-hoc frontend shell and monolithic partition workspace with a routed, Pinia-backed application that manages partition submissions, versioned datasets, and normalized quality records through the Milestone 3 APIs.

**Architecture:** Vue Router owns the five application URLs and applies authentication and administrator guards before rendering a shared `AppLayout`; it preserves existing external portal and OAuth return behavior instead of reimplementing it in pages. Pinia stores own partition, dataset, and quality domain state, while a shared API layer supplies method-aware requests, pagination normalization, download handling, status presentation, drawers, and cancellable request scopes. Feature views render tables and drawers from those stores, with narrow components replacing the current `PartitionView.vue` state machine.

**Tech Stack:** Vue 3.5, Vue Router 4, Pinia 2, Element Plus 2, Vite 6, Vitest, Vue Test Utils, jsdom, Playwright.

## Global Constraints

- Every worker runs every command from that worker's isolated worktree root, never from `/home/lyajun/projects/cube_project` or another worktree. File paths in this plan are repository-relative; commands below assume the current directory is the assigned isolated worktree root.
- M4 file ownership is exclusive: Opus owns shared contracts/router integration and final synthesis; Sonnet owns one bounded frontend slice at a time; Haiku owns only mechanical deletion, focused source searches, and narrow test execution. No two workers edit a shared file concurrently. The assigned branch and owned files are recorded before work starts; each owner posts its narrow-test evidence at its review checkpoint.
- Integration is no-commit: workers submit reviewed diffs and test evidence to the Opus integration worktree; the integrator applies them without worker commits, resolves any shared-file conflict, and reruns each affected owner's narrow test. No worker pushes, creates a remote branch or PR, or commits an integration slice; only the final M4 integrator may create the one local milestone commit after all gates pass.
- Milestone 3 is the only dataset/quality/publication backend contract. M4 uses M2—not M3—for partition submission: `POST /v1/partition/{data_type}/tasks/run` with the exact `StrictPartitionRequest` body. Do not add a compatibility adapter for `history`, `latest`, `report`, or data-type-specific quality APIs.
- Production grid options are exactly `geohash`, `mgrs`, and `isea4h`; their native levels are respectively `1–12`, `0–5`, and `0–15`.
- Partition submission sends `batch_id`, selected complete normalized `DatasetInput` objects (including non-empty `assets` and dataset-level `bands`), `grid_type`, `requested_grid_level`, the validated derived `partition_method`, `cover_mode`, `time_granularity`, and `max_cells_per_asset`. Never send `dataset_ids`/`datasetIds`, `grid_level`, `grid_level_mode`, `selected_assets`, or any loader/runtime credential field. The UI derives the method from `grid_type`, displays it read-only, and never offers a manual logical/entity selector.
- Preserve the portal-home redirect and OAuth callback target semantics: only same-origin absolute paths beginning with one `/` are accepted as return targets.
- The router is the source of page selection. Do not retain `window.history.pushState`, a `pageMap`, or path-switching logic in `App.vue`.
- Use Pinia only for the `partition`, `datasets`, and `quality` business domains. Keep authentication state in the existing `subUser` module until a dedicated auth milestone changes that contract.
- All collection API responses use `{ items, total, page, page_size }`; requests send `page`, `page_size`, `sort_by`, and `sort_order` only when values are defined.
- Dataset filters are exactly `keyword`, `data_type`, `product_type`, `batch_id`, `grid_type`, `partition_status`, `quality_status`, `publish_status`, `time_start`, and `time_end`. Quality-record filters are exactly `keyword`, `dataset_id`, `output_version`, `data_type`, `status`, `trigger`, `requested_by`, `current_only`, `started_from`, and `started_to`.
- Publication record status is exactly `publishing | active | withdrawing | failed | withdrawn`; dataset-derived `publish_status` additionally permits only `unpublished` to mean no publication record. Never emit, filter, fixture, or display `published`.
- Implement cancellation with a compatibility-safe `combineAbortSignals(signals)` helper. Do not use `AbortSignal.any`.
- A view/store that can replace its selected record or query must both abort the previous request and reject stale results with a monotonically increasing request token before updating reactive state.
- Detail drawers reset their selected identifier, child records, selected table rows, child pagination, active tab, and request token before beginning a different-record request.
- Downloads use the authenticated `fetch` API and `Content-Disposition` filename when supplied; they must not navigate to an unauthenticated URL.
- Remove the old quality workspace, `QualityHistoryDrawer.vue`, and every frontend use of old quality endpoints in the same milestone. Do not retain hidden routes or UI aliases.
- Remove COG conversion/progress wording from the partition UI. Use `瓦片数据` for output tile objects and `格网单元` for grid-cell objects.
- Add dependencies and explicitly refresh the lock file with `npm --prefix cube_web/frontend install --package-lock-only`; every installation used for validation is `npm --prefix cube_web/frontend ci`.
- Unit tests must not import `cleanup` from `@vue/test-utils`; every mounted wrapper calls `wrapper.unmount()` in `afterEach`.
- Playwright tests must open Element Plus selects and assert visible teleported `.el-select-dropdown__item` entries; do not use native `<select>` APIs or `selectOption` for Element Plus controls.
- Each implementation task runs only its narrow unit or browser test. The cross-package Python, backend, lint, type, build, and browser suite occurs once in **M4 Completion Gate**.
- Technical acceptance gates are distinct from personnel review: **L1** required narrow tests pass for each owned slice; **L2** the combined unit suite, production/current-doc static scans, build, and browser suite pass; **L3** explicit negative/rejection and stale-race tests pass; **L4** final cross-package Python/lint/mypy plus live M4 browser evidence pass. Any failed, skipped, unavailable required dependency, or unclosed high-severity finding blocks M4.
- Review roles are separate from L1–L4: implementer self-review checks owned diffs; an independent Sonnet/Opus reviewer checks contracts and regressions; a separate adversarial validator executes the rejection/race matrix; Opus synthesizes findings and personally runs the applicable L4 commands. No reviewer may approve their own implementation as independent review.

---

## File Structure

| File | Responsibility |
|---|---|
| `cube_web/frontend/package.json` | Declares Vue Router, Pinia, Vitest, Vue Test Utils, and jsdom scripts/dependencies. |
| `cube_web/frontend/package-lock.json` | Exact dependency graph regenerated with npm. |
| `cube_web/frontend/vite.config.js` | Adds Vitest aliases and jsdom test configuration while preserving Vite application setup. |
| `cube_web/frontend/src/main.js` | Installs Pinia and the router before mounting the app. |
| `cube_web/frontend/src/router/index.js` | Defines routes, route metadata, callback normalization, and authentication/administrator guards. |
| `cube_web/frontend/src/layouts/AppLayout.vue` | Renders the shared portal header, navigation, account menu, and `<RouterView>`. |
| `cube_web/frontend/src/App.vue` | Initializes runtime auth configuration and renders the router once ready. |
| `cube_web/frontend/src/api/client.js` | Implements method-aware authenticated request, JSON parsing, timeout/cancellation composition, binary download, and normalized errors. |
| `cube_web/frontend/src/api/pagination.js` | Defines page defaults, query encoding, and response normalization. |
| `cube_web/frontend/src/api/requestScope.js` | Owns per-consumer abort controllers and request-token checks. |
| `cube_web/frontend/src/utils/grid.js` | Owns native grid metadata, level labels, and derived read-only partition method. |
| `cube_web/frontend/src/utils/status.js` | Maps partition, quality, and publication statuses to Element Plus tag labels/types. |
| `cube_web/frontend/src/components/AppTable.vue` | Standardizes empty/loading table presentation and pagination event forwarding. |
| `cube_web/frontend/src/components/DetailDrawer.vue` | Standardizes reset-safe right-side detail drawers and slot layout. |
| `cube_web/frontend/src/components/StatusTag.vue` | Renders shared status labels from `utils/status.js`. |
| `cube_web/frontend/src/stores/partition.js` | Holds submission form state, batch/task list state, and partition request lifecycle. |
| `cube_web/frontend/src/stores/datasets.js` | Holds dataset table filters/pagination and one reset-safe dataset detail request scope. |
| `cube_web/frontend/src/stores/quality.js` | Holds normalized quality record filters/pagination and one reset-safe quality detail/error request scope. |
| `cube_web/frontend/src/views/PartitionView.vue` | Composes focused partition components; it no longer contains the batch/data/quality workspace. |
| `cube_web/frontend/src/views/partition/GridParameters.vue` | Selects the three grids, native level, map action, derived method, reset, and submit. |
| `cube_web/frontend/src/views/partition/BatchAssetsPanel.vue` | Selects compliant loaded datasets/assets for a partition submission. |
| `cube_web/frontend/src/views/partition/TaskQueuePanel.vue` | Displays paginated partition batches/tasks and opens task details. |
| `cube_web/frontend/src/views/partition/ExecutionResultPanel.vue` | Displays server task status and result counts without COG conversion stages. |
| `cube_web/frontend/src/views/DatasetsView.vue` | Renders the dataset list and opens `DatasetDetailDrawer`. |
| `cube_web/frontend/src/views/datasets/DatasetDetailDrawer.vue` | Renders overview, assets, bands, tiles, indexes, grid, quality, and publications tabs. |
| `cube_web/frontend/src/views/QualityView.vue` | Renders the normalized all-records quality table and opens `QualityDetailDrawer`. |
| `cube_web/frontend/src/views/quality/QualityDetailDrawer.vue` | Renders run metadata, results, paginated/filterable errors, exports, and rerun actions. |
| `cube_web/frontend/src/views/EncodingView.vue` | Removes obsolete grids and uses shared grid metadata and modern API methods. |
| `cube_web/frontend/src/views/ConfigView.vue` | Removes old grid and COG-conversion controls and labels. |
| `cube_web/frontend/tests/unit/**/*.spec.js` | Focused router, API, store, component, and view tests. |
| `cube_web/frontend/tests/e2e/app.spec.js` | Browser regression for routes, guard behavior, Element Plus menus, drawers, filters, downloads, and responsive layout. |

### Task 1: Establish the frontend test and dependency foundation

**Files:**
- Modify: `cube_web/frontend/package.json`
- Modify: `cube_web/frontend/package-lock.json`
- Modify: `cube_web/frontend/vite.config.js`
- Create: `cube_web/frontend/tests/unit/setup.js`
- Create: `cube_web/frontend/tests/unit/grid.spec.js`

**Interfaces:**
- Produces: `npm run test:unit`, `npm run test:unit:watch`, and a jsdom Vitest environment.
- Produces: `gridDefinitions`, `gridDefinition(gridType)`, `nativeLevelLabel(gridType, level)`, and `derivedPartitionMethod(gridType)` for all later grid controls.

- [ ] **Step 1: Add the failing grid-contract unit test.**

```js
import { describe, expect, it } from 'vitest';
import { derivedPartitionMethod, gridDefinitions, nativeLevelLabel } from '@/utils/grid';

describe('grid contract', () => {
  it('exposes only production grids and derives their methods', () => {
    expect(gridDefinitions.map((item) => item.value)).toEqual(['geohash', 'mgrs', 'isea4h']);
    expect(derivedPartitionMethod('geohash')).toBe('logical');
    expect(derivedPartitionMethod('mgrs')).toBe('logical');
    expect(derivedPartitionMethod('isea4h')).toBe('entity');
    expect(nativeLevelLabel('mgrs', 3)).toBe('100 m');
  });
});
```

- [ ] **Step 2: Run the narrow test to verify the missing module failure.**

Run from the repository root:

```bash
npm --prefix cube_web/frontend ci
npm --prefix cube_web/frontend run test:unit -- tests/unit/grid.spec.js
```

Expected: FAIL because `@/utils/grid` does not exist.

- [ ] **Step 3: Add the required dependencies, scripts, lock file, Vitest configuration, and grid utility.**

Add these exact scripts to `cube_web/frontend/package.json`:

```json
"test:unit": "vitest run",
"test:unit:watch": "vitest"
```

Install the required packages and explicitly regenerate the lock file:

```bash
npm --prefix cube_web/frontend install vue-router@^4 pinia@^2
npm --prefix cube_web/frontend install --save-dev vitest@^3 @vue/test-utils@^2 jsdom@^26
npm --prefix cube_web/frontend install --package-lock-only
```

Configure `cube_web/frontend/vite.config.js` with the existing `@` alias plus this test block:

```js
test: {
  environment: 'jsdom',
  setupFiles: ['./tests/unit/setup.js'],
  globals: true,
},
```

Create `cube_web/frontend/tests/unit/setup.js`:

```js
import { afterEach } from 'vitest';

afterEach(() => {
  document.body.innerHTML = '';
  localStorage.clear();
});
```

Create `cube_web/frontend/src/utils/grid.js`:

```js
export const gridDefinitions = Object.freeze([
  { value: 'geohash', label: 'Geohash', minLevel: 1, maxLevel: 12 },
  { value: 'mgrs', label: '扩展 MGRS', minLevel: 0, maxLevel: 5 },
  { value: 'isea4h', label: 'ISEA4H', minLevel: 0, maxLevel: 15 },
]);

const mgrsLabels = ['100 km', '10 km', '1 km', '100 m', '10 m', '1 m'];

export function gridDefinition(gridType) {
  return gridDefinitions.find((item) => item.value === gridType) || null;
}

export function derivedPartitionMethod(gridType) {
  if (gridType === 'geohash' || gridType === 'mgrs') return 'logical';
  if (gridType === 'isea4h') return 'entity';
  return '';
}

export function nativeLevelLabel(gridType, level) {
  const numericLevel = Number(level);
  if (gridType === 'geohash') return `Geohash 精度 ${numericLevel}`;
  if (gridType === 'mgrs') return mgrsLabels[numericLevel] || '';
  if (gridType === 'isea4h') return `ISEA4H 分辨率 ${numericLevel}`;
  return '';
}
```

- [ ] **Step 4: Run the narrow test with the locked install.**

Run:

```bash
npm --prefix cube_web/frontend ci
npm --prefix cube_web/frontend run test:unit -- tests/unit/grid.spec.js
```

Expected: PASS with one passing grid-contract test.

### Task 2: Build shared API, request-scope, pagination, status, table, and drawer infrastructure

**Files:**
- Modify: `cube_web/frontend/src/api/client.js`
- Create: `cube_web/frontend/src/api/pagination.js`
- Create: `cube_web/frontend/src/api/requestScope.js`
- Create: `cube_web/frontend/src/utils/status.js`
- Create: `cube_web/frontend/src/components/AppTable.vue`
- Create: `cube_web/frontend/src/components/DetailDrawer.vue`
- Create: `cube_web/frontend/src/components/StatusTag.vue`
- Create: `cube_web/frontend/tests/unit/client.spec.js`
- Create: `cube_web/frontend/tests/unit/requestScope.spec.js`

**Interfaces:**
- Produces: `request(path, { method, body, headers, signal, responseType })`, `requestJson(path, payload, options)`, `requestGet(path, options)`, `requestPost(path, payload, options)`, and `download(path, options)`.
- Produces: `combineAbortSignals(signals) -> { signal: AbortSignal | undefined, dispose: () => void }` and `createRequestScope() -> { begin, isCurrent, cancel, dispose }`.
- Produces: `normalizePageResponse(body, fallbackPage, fallbackPageSize)` and `pageQuery(params)`.

- [ ] **Step 1: Write failing unit tests for cancellation composition, stale tokens, method-aware requests, and download filenames.**

```js
import { describe, expect, it, vi } from 'vitest';
import { combineAbortSignals, download, request } from '@/api/client';
import { createRequestScope } from '@/api/requestScope';

describe('api client', () => {
  it('uses the supplied method and merges cancellation signals without AbortSignal.any', async () => {
    const caller = new AbortController();
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('{"ok":true}', { status: 200 }));
    const combined = combineAbortSignals([caller.signal]);
    await request('/v1/example', { method: 'PATCH', body: { enabled: true }, signal: combined.signal });
    expect(fetchMock).toHaveBeenCalledWith('/v1/example', expect.objectContaining({ method: 'PATCH' }));
    caller.abort();
    expect(combined.signal.aborted).toBe(true);
    combined.dispose();
    fetchMock.mockRestore();
  });

  it('downloads an authenticated response using its Content-Disposition filename', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('id,message\n1,test\n', {
      status: 200,
      headers: {
        'Content-Disposition': 'attachment; filename="quality.csv"',
        'Content-Type': 'text/csv',
      },
    }));
    const result = await download('/v1/quality/records/run-1/errors/export?format=csv');
    expect(result.filename).toBe('quality.csv');
    expect(result.blob.type).toBe('text/csv');
  });
});

describe('request scope', () => {
  it('rejects a stale response after a newer request begins', () => {
    const scope = createRequestScope();
    const first = scope.begin();
    const second = scope.begin();
    expect(first.signal.aborted).toBe(true);
    expect(scope.isCurrent(first.token)).toBe(false);
    expect(scope.isCurrent(second.token)).toBe(true);
    scope.dispose();
  });
});
```

- [ ] **Step 2: Run the narrow tests to verify the initial failures.**

Run:

```bash
npm --prefix cube_web/frontend ci
npm --prefix cube_web/frontend run test:unit -- tests/unit/client.spec.js tests/unit/requestScope.spec.js
```

Expected: FAIL because the named shared APIs do not exist.

- [ ] **Step 3: Replace the POST-only client with the method-aware implementation and add the shared modules.**

Replace `cube_web/frontend/src/api/client.js` request implementation with these exported primitives; retain `accessToken`, `authHeaders`, and `apiPrefixes`:

```js
export function combineAbortSignals(signals) {
  const activeSignals = signals.filter(Boolean);
  if (!activeSignals.length) return { signal: undefined, dispose() {} };
  const controller = new AbortController();
  const abort = () => controller.abort();
  activeSignals.forEach((signal) => {
    if (signal.aborted) abort();
    else signal.addEventListener('abort', abort, { once: true });
  });
  return {
    signal: controller.signal,
    dispose() {
      activeSignals.forEach((signal) => signal.removeEventListener('abort', abort));
    },
  };
}

export async function request(path, { method = 'GET', body, headers = {}, signal, responseType = 'json' } = {}) {
  const timeout = timeoutSignal();
  const combined = combineAbortSignals([signal, timeout.signal]);
  try {
    const response = await fetch(path, {
      method,
      headers: authHeaders(body === undefined ? headers : { ...JSON_HEADERS, ...headers }),
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      signal: combined.signal,
    });
    if (responseType === 'blob') {
      if (!response.ok) await parseResponse(response);
      return response;
    }
    return parseResponse(response);
  } finally {
    combined.dispose();
    timeout.clear();
  }
}

export function requestGet(path, options = {}) {
  return request(path, { ...options, method: 'GET' });
}

export function requestJson(path, payload = {}, options = {}) {
  return request(path, { ...options, method: options.method || 'POST', body: payload });
}

export function requestPost(path, payload = {}, options = {}) {
  return requestJson(path, payload, options);
}

export async function download(path, options = {}) {
  const response = await request(path, { ...options, responseType: 'blob' });
  const disposition = response.headers.get('Content-Disposition') || '';
  const filenameMatch = disposition.match(/filename="?([^";]+)"?/i);
  const blob = await response.blob();
  return { blob, filename: filenameMatch?.[1] || 'download' };
}
```

Create `cube_web/frontend/src/api/requestScope.js`:

```js
export function createRequestScope() {
  let controller = null;
  let token = 0;
  return {
    begin() {
      controller?.abort();
      controller = new AbortController();
      token += 1;
      return { token, signal: controller.signal };
    },
    isCurrent(candidate) {
      return candidate === token && !controller?.signal.aborted;
    },
    cancel() {
      controller?.abort();
      controller = null;
      token += 1;
    },
    dispose() {
      this.cancel();
    },
  };
}
```

Create `cube_web/frontend/src/api/pagination.js`:

```js
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
```

Create `cube_web/frontend/src/utils/status.js` with explicit labels/types for `pending`, `queued`, `running`, `completed`, `failed`, `cancelled`, `pass`, `warn`, `fail`, `error`, `publishing`, `active`, `withdrawing`, `withdrawn`, and `unpublished`. Export `statusPresentation(domain, value)` returning `{ label, type }`, with unknown values returning `{ label: value || '未知', type: 'info' }`. Do not map or retain `published`.

Create `AppTable.vue`, `DetailDrawer.vue`, and `StatusTag.vue` as slot-based Element Plus wrappers: `AppTable` forwards `current-change` and `size-change` from `<el-pagination>`; `DetailDrawer` exposes `v-model:visible`, accepts `title`, `loading`, and `size`, and renders a default slot; `StatusTag` calls `statusPresentation` and renders `<el-tag>`.

- [ ] **Step 4: Run the narrow infrastructure tests.**

Run:

```bash
npm --prefix cube_web/frontend ci
npm --prefix cube_web/frontend run test:unit -- tests/unit/client.spec.js tests/unit/requestScope.spec.js
```

Expected: PASS.

### Task 3: Install Pinia and Vue Router with portal-preserving authentication guards

**Files:**
- Modify: `cube_web/frontend/src/main.js`
- Modify: `cube_web/frontend/src/App.vue`
- Modify: `cube_web/frontend/src/data/navigation.js`
- Create: `cube_web/frontend/src/router/index.js`
- Create: `cube_web/frontend/src/layouts/AppLayout.vue`
- Create: `cube_web/frontend/tests/unit/router.spec.js`

**Interfaces:**
- Consumes: `authRequired()`, `loadAuthRuntimeConfig()`, `portalHomeUrl`, and `useSubUserStore()`.
- Produces: `router` with `/partition`, `/datasets`, `/quality`, `/encoding`, `/config`, and `/callback`; every protected route has `meta.requiresAuth: true`, administrator routes also have `meta.requiresAdmin: true`.
- Produces: `initializeApplicationAuth() -> Promise<void>` from `App.vue` for router guard readiness.

- [ ] **Step 1: Write failing router guard tests.**

```js
import { describe, expect, it, vi } from 'vitest';
import { createRouter, createMemoryHistory } from 'vue-router';
import { installGuards } from '@/router';

describe('router guards', () => {
  it('sends an unauthenticated protected route to the auth redirect with its local target', async () => {
    const redirectToAuth = vi.fn();
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [{ path: '/datasets', component: { template: '<div />' }, meta: { requiresAuth: true, requiresAdmin: true } }],
    });
    installGuards(router, {
      ready: () => true,
      authenticated: () => false,
      admin: () => false,
      redirectToAuth,
      portalHomeUrl: 'https://portal.example/#/home',
    });
    await router.push('/datasets?status=completed');
    expect(redirectToAuth).toHaveBeenCalledWith('/datasets?status=completed');
  });

  it('redirects a non-admin away from an administrator route without accepting an external target', async () => {
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [
        { path: '/partition', component: { template: '<div />' }, meta: { requiresAuth: true, requiresAdmin: true } },
        { path: '/encoding', component: { template: '<div />' } },
      ],
    });
    installGuards(router, {
      ready: () => true,
      authenticated: () => true,
      admin: () => false,
      redirectToAuth: vi.fn(),
      portalHomeUrl: 'https://portal.example/#/home',
    });
    await router.push('/partition');
    expect(router.currentRoute.value.fullPath).toBe('/encoding');
  });
});
```

- [ ] **Step 2: Run the router test to verify it fails.**

Run:

```bash
npm --prefix cube_web/frontend ci
npm --prefix cube_web/frontend run test:unit -- tests/unit/router.spec.js
```

Expected: FAIL because `@/router` and guarded routes do not exist.

- [ ] **Step 3: Implement routing, application initialization, layout, and route-aware navigation.**

Create `cube_web/frontend/src/router/index.js` with route records for the five application routes plus `/callback`. Use these metadata assignments:

```js
{ path: '/partition', name: 'partition', component: () => import('@/views/PartitionView.vue'), meta: { requiresAuth: true, requiresAdmin: true } },
{ path: '/datasets', name: 'datasets', component: () => import('@/views/DatasetsView.vue'), meta: { requiresAuth: true, requiresAdmin: true } },
{ path: '/quality', name: 'quality', component: () => import('@/views/QualityView.vue'), meta: { requiresAuth: true, requiresAdmin: true } },
{ path: '/encoding', name: 'encoding', component: () => import('@/views/EncodingView.vue'), meta: { requiresAuth: true } },
{ path: '/config', name: 'config', component: () => import('@/views/ConfigView.vue'), meta: { requiresAuth: true, requiresAdmin: true } },
{ path: '/callback', name: 'callback', component: () => import('@/views/PartitionView.vue') },
{ path: '/', redirect: '/partition' },
{ path: '/:pathMatch(.*)*', redirect: '/partition' },
```

Export `safeLocalTarget(value)`, `installGuards(router, dependencies)`, and the default `router`. `installGuards` must await readiness, call `redirectToAuth(to.fullPath)` for unauthenticated protected routes, redirect non-admin administrator routes to `{ name: 'encoding' }`, and not parse or navigate to externally supplied targets.

In `main.js`, install Pinia and the router before Element Plus and mount:

```js
import { createPinia } from 'pinia';
import router from './router';

createApp(App).use(createPinia()).use(router).use(ElementPlus).mount('#app');
```

Replace `App.vue` with a minimal readiness gate that loads runtime config, processes an OAuth `code` only on `/callback`, exchanges it through `subUser`, and `router.replace(safeLocalTarget(callbackStateTarget) || '/partition')`. It must remove invalid/external targets, clear invalid stored credentials after a failed `/api/me`, and render `<RouterView v-if="authReady" />` inside `AppLayout`.

Move the existing header/nav/logout markup from `App.vue` into `layouts/AppLayout.vue`; use `<RouterLink>` for internal links and existing `<a>` tags for external portal links. Update `data/navigation.js` so `localNavPaths` includes `数据集管理: '/datasets'`, `自动化质检: '/quality'`, and `系统配置: '/config'`, and `normalizePath` recognizes those paths rather than remapping them to `/partition`.

- [ ] **Step 4: Run the narrow router test.**

Run:

```bash
npm --prefix cube_web/frontend ci
npm --prefix cube_web/frontend run test:unit -- tests/unit/router.spec.js
```

Expected: PASS.

### Task 4: Implement the partition Pinia store and split the partition page

**Files:**
- Create: `cube_web/frontend/src/stores/partition.js`
- Modify: `cube_web/frontend/src/views/PartitionView.vue`
- Create: `cube_web/frontend/src/views/partition/GridParameters.vue`
- Create: `cube_web/frontend/src/views/partition/BatchAssetsPanel.vue`
- Create: `cube_web/frontend/src/views/partition/TaskQueuePanel.vue`
- Create: `cube_web/frontend/src/views/partition/ExecutionResultPanel.vue`
- Create: `cube_web/frontend/tests/unit/partitionStore.spec.js`
- Create: `cube_web/frontend/tests/unit/GridParameters.spec.js`

**Interfaces:**
- Consumes: `gridDefinition`, `nativeLevelLabel`, `derivedPartitionMethod`, `requestGet`, `requestPost`, `normalizePageResponse`, and `createRequestScope`.
- Produces: `usePartitionStore()` with `form`, `batches`, `tasks`, `result`, `loadBatches()`, `loadTasks()`, `submit()`, and `resetForm()`. `form.datasets` contains selected full normalized `DatasetInput` values; `form.batchId` is generated/provided before submission.
- Produces: `GridParameters` props `modelValue` and emits `update:modelValue`, `reset`, and `submit`.

- [ ] **Step 1: Write failing store and grid-parameter tests.**

```js
import { createPinia, setActivePinia } from 'pinia';
import { describe, expect, it, vi } from 'vitest';
import { usePartitionStore } from '@/stores/partition';

vi.mock('@/api/client', () => ({
  requestGet: vi.fn(),
  requestPost: vi.fn(),
}));

describe('partition store', () => {
  it('submits the exact M2 StrictPartitionRequest with normalized datasets and a derived method', async () => {
    setActivePinia(createPinia());
    const store = usePartitionStore();
    store.form.batchId = 'batch-frontend-001';
    store.form.gridType = 'isea4h';
    store.form.requestedGridLevel = 6;
    store.form.datasets = [{
      dataset_id: 'dataset-1', dataset_code: 'DS-1', dataset_title: 'Dataset 1', data_type: 'optical', product_type: 'L2A',
      assets: [{ source_asset_id: 'asset-1', cog_uri: 's3://cube/loader/dataset-1/asset-1.tif', checksum: 'a'.repeat(64), bbox: [100, 20, 101, 21], crs: 'EPSG:4326', time_start: '2026-07-01T00:00:00Z', time_end: '2026-07-01T00:05:00Z', attributes: {} }],
      bands: [{ source_asset_id: 'asset-1', band_code: 'B04', band_name: 'Red', band_type: 'spectral', unit: null, display_order: 4, attributes: {} }],
      attributes: {},
    }];
    await store.submit();
    const { requestPost } = await import('@/api/client');
    expect(requestPost).toHaveBeenCalledWith('/v1/partition/optical/tasks/run', expect.objectContaining({
      batch_id: 'batch-frontend-001',
      datasets: store.form.datasets,
      grid_type: 'isea4h',
      requested_grid_level: 6,
      partition_method: 'entity',
      cover_mode: 'intersect',
      time_granularity: 'day',
      max_cells_per_asset: 0,
    }), expect.any(Object));
    const body = requestPost.mock.calls[0][1];
    expect(body).not.toHaveProperty('dataset_ids');
    expect(body).not.toHaveProperty('grid_level_mode');
  });
});
```

```js
import { mount } from '@vue/test-utils';
import { afterEach, describe, expect, it } from 'vitest';
import GridParameters from '@/views/partition/GridParameters.vue';

const wrappers = [];
afterEach(() => wrappers.splice(0).forEach((wrapper) => wrapper.unmount()));

describe('GridParameters', () => {
  it('shows only production grids and a read-only derived method', () => {
    const wrapper = mount(GridParameters, { props: { modelValue: { gridType: 'mgrs', requestedGridLevel: 2 } } });
    wrappers.push(wrapper);
    expect(wrapper.text()).toContain('扩展 MGRS');
    expect(wrapper.text()).toContain('逻辑剖分');
    expect(wrapper.text()).not.toContain('S2');
    expect(wrapper.text()).not.toContain('平面格网');
  });
});
```

- [ ] **Step 2: Run the narrow partition tests to verify the failures.**

Run:

```bash
npm --prefix cube_web/frontend ci
npm --prefix cube_web/frontend run test:unit -- tests/unit/partitionStore.spec.js tests/unit/GridParameters.spec.js
```

Expected: FAIL because the store and split components do not exist.

- [ ] **Step 3: Implement the bounded partition domain and component split.**

Create `stores/partition.js` with form state shaped exactly as follows:

```js
const form = reactive({
  batchId: '',
  datasets: [],
  gridType: 'geohash',
  requestedGridLevel: 6,
  coverMode: 'intersect',
  timeGranularity: 'day',
  maxCellsPerAsset: 0,
});
```

`BatchAssetsPanel` obtains selected complete normalized `DatasetInput` objects from the loaded-dataset source, preserves every required dataset/asset/band field verbatim, and writes them to `form.datasets`; it may not reduce selections to IDs. `submit()` determines the one shared `data_type` from the selected datasets (reject mixed data types in this M4 UI), requires a non-empty `batchId` and at least one dataset with non-empty `assets` and `bands`, and sends this exact M2 production request to `POST /v1/partition/${dataType}/tasks/run`:

```js
{
  batch_id: form.batchId,
  datasets: form.datasets,
  grid_type: form.gridType,
  requested_grid_level: Number(form.requestedGridLevel),
  partition_method: derivedPartitionMethod(form.gridType),
  cover_mode: form.coverMode,
  time_granularity: form.timeGranularity,
  max_cells_per_asset: Number(form.maxCellsPerAsset),
}
```

Reject the submit before any request if the data type is mixed, `batchId` is blank, an asset/band set is empty, or `requestedGridLevel` lies outside `gridDefinition(form.gridType)`. Never send `dataset_ids`, `datasetIds`, `grid_level`, `grid_level_mode`, `selected_assets`, or client-side credential/runtime fields. Use a store-local request scope for `loadBatches`, `loadTasks`, and `submit`; only a current token may update `batches`, `tasks`, `result`, `error`, or `loading`.

Replace the giant `PartitionView.vue` with a composition root that imports the four new feature components and the store. `GridParameters` contains the grid select, native level control/label, map load action, read-only `partition_method`, and adjacent `重置` / `提交剖分` buttons. `BatchAssetsPanel` shows loaded compliant datasets/assets only. `TaskQueuePanel` owns the paginated current task table. `ExecutionResultPanel` shows task ID, partition status, `瓦片数据` count, `格网单元` count, index count, and server error; it must not render `生成 COG`, `COG 耗时`, or COG conversion stages.

- [ ] **Step 4: Run the narrow partition tests.**

Run:

```bash
npm --prefix cube_web/frontend ci
npm --prefix cube_web/frontend run test:unit -- tests/unit/partitionStore.spec.js tests/unit/GridParameters.spec.js
```

Expected: PASS.

### Task 5: Implement the dataset store, paginated table, and reset-safe detail drawer

**Files:**
- Create: `cube_web/frontend/src/stores/datasets.js`
- Create: `cube_web/frontend/src/views/DatasetsView.vue`
- Create: `cube_web/frontend/src/views/datasets/DatasetDetailDrawer.vue`
- Create: `cube_web/frontend/tests/unit/datasetsStore.spec.js`
- Create: `cube_web/frontend/tests/unit/DatasetDetailDrawer.spec.js`

**Interfaces:**
- Consumes: `GET /v1/partition/datasets`, all eight dataset detail endpoints from the normative API, `normalizePageResponse`, `pageQuery`, `createRequestScope`, `AppTable`, `DetailDrawer`, and `StatusTag`.
- Produces: `useDatasetsStore()` with `filters`, `pageState`, `selectedDatasetId`, `detail`, `loadList()`, `openDetail(datasetId)`, `loadDetailTab(tab)`, and `closeDetail()`.
- Produces: a `DatasetDetailDrawer` with tabs `overview`, `assets`, `bands`, `tiles`, `indexes`, `grid`, `quality`, and `publications`.

- [ ] **Step 1: Write failing token-race and drawer-reset tests.**

```js
import { createPinia, setActivePinia } from 'pinia';
import { describe, expect, it, vi } from 'vitest';
import { useDatasetsStore } from '@/stores/datasets';

const deferred = [];
vi.mock('@/api/client', () => ({
  requestGet: vi.fn(() => new Promise((resolve) => deferred.push(resolve))),
}));

describe('datasets store', () => {
  it('keeps only the most recently opened dataset detail', async () => {
    setActivePinia(createPinia());
    const store = useDatasetsStore();
    const first = store.openDetail('dataset-a');
    const second = store.openDetail('dataset-b');
    deferred[1]({ dataset_id: 'dataset-b', dataset_title: 'B' });
    deferred[0]({ dataset_id: 'dataset-a', dataset_title: 'A' });
    await Promise.all([first, second]);
    expect(store.selectedDatasetId).toBe('dataset-b');
    expect(store.detail.overview.dataset_title).toBe('B');
  });
});
```

```js
import { mount } from '@vue/test-utils';
import { afterEach, describe, expect, it } from 'vitest';
import DatasetDetailDrawer from '@/views/datasets/DatasetDetailDrawer.vue';

const wrappers = [];
afterEach(() => wrappers.splice(0).forEach((wrapper) => wrapper.unmount()));

describe('DatasetDetailDrawer', () => {
  it('renders all eight required detail tabs', () => {
    const wrapper = mount(DatasetDetailDrawer, { props: { visible: true, datasetId: 'dataset-1', detail: {} } });
    wrappers.push(wrapper);
    expect(wrapper.text()).toContain('资产');
    expect(wrapper.text()).toContain('波段');
    expect(wrapper.text()).toContain('瓦片');
    expect(wrapper.text()).toContain('索引');
    expect(wrapper.text()).toContain('格网');
    expect(wrapper.text()).toContain('质检');
    expect(wrapper.text()).toContain('发布');
  });
});
```

- [ ] **Step 2: Run the narrow dataset tests to verify the failures.**

Run:

```bash
npm --prefix cube_web/frontend ci
npm --prefix cube_web/frontend run test:unit -- tests/unit/datasetsStore.spec.js tests/unit/DatasetDetailDrawer.spec.js
```

Expected: FAIL because the dataset store and drawer do not exist.

- [ ] **Step 3: Implement the dataset store and views.**

Create the list filter shape in `stores/datasets.js`; `loadList()` serializes it to the exact M3 query names `keyword`, `data_type`, `product_type`, `batch_id`, `grid_type`, `partition_status`, `quality_status`, `publish_status`, `time_start`, `time_end`, `page`, `page_size`, `sort_by`, and `sort_order` (omitting undefined/empty optional values):

```js
const filters = reactive({
  keyword: '',
  dataType: '',
  productType: '',
  batchId: '',
  gridType: '',
  partitionStatus: '',
  qualityStatus: '',
  publishStatus: '',
  timeStart: '',
  timeEnd: '',
  sortBy: 'updated_at',
  sortOrder: 'desc',
});
```

`openDetail(datasetId)` must first call this reset routine, then start a new request scope token before requesting `GET /v1/partition/datasets/{datasetId}`:

```js
function resetDetail(nextDatasetId = '') {
  detailScope.cancel();
  selectedDatasetId.value = nextDatasetId;
  detailVisible.value = Boolean(nextDatasetId);
  activeTab.value = 'overview';
  detail.value = { overview: null, assets: null, bands: null, tiles: null, indexes: null, grid: null, quality: null, publications: null };
  selectedRows.value = [];
  Object.assign(tabPages, {
    assets: { page: 1, pageSize: 20 }, tiles: { page: 1, pageSize: 20 }, indexes: { page: 1, pageSize: 20 }, grid: { page: 1, pageSize: 20 }, quality: { page: 1, pageSize: 20 }, publications: { page: 1, pageSize: 20 },
  });
}
```

Every tab request must include the selected `dataset_id` in its URL and check both `selectedDatasetId.value === datasetId` and `detailScope.isCurrent(token)` before writing. Map the tabs to these exact endpoints: root detail, `/assets`, `/bands`, `/tiles`, `/indexes`, `/grid`, `/quality`, `/publications`.

Create `DatasetsView.vue` as a paginated Element Plus table using `AppTable`; include the specified dataset columns and an `详情` action that calls `openDetail(row.dataset_id)`. Create `DatasetDetailDrawer.vue` using `DetailDrawer`, render the eight named tabs, and keep each paginated detail collection in its own tab view. The overview displays requested grid level and the server-provided grid-level display name, not a fabricated uniform result level.

- [ ] **Step 4: Run the narrow dataset tests.**

Run:

```bash
npm --prefix cube_web/frontend ci
npm --prefix cube_web/frontend run test:unit -- tests/unit/datasetsStore.spec.js tests/unit/DatasetDetailDrawer.spec.js
```

Expected: PASS.

### Task 6: Implement the normalized quality store, all-records view, unified drawer, exports, and reruns

**Files:**
- Create: `cube_web/frontend/src/stores/quality.js`
- Create: `cube_web/frontend/src/views/QualityView.vue`
- Create: `cube_web/frontend/src/views/quality/QualityDetailDrawer.vue`
- Delete: `cube_web/frontend/src/components/QualityHistoryDrawer.vue`
- Create: `cube_web/frontend/tests/unit/qualityStore.spec.js`
- Create: `cube_web/frontend/tests/unit/QualityDetailDrawer.spec.js`

**Interfaces:**
- Consumes: `GET /v1/quality/records`, `GET /v1/quality/records/{quality_run_id}`, `GET /v1/quality/records/{quality_run_id}/results`, `GET /v1/quality/records/{quality_run_id}/errors`, both export routes, and `POST /v1/quality/runs`.
- Produces: `useQualityStore()` with `filters`, `records`, `openDetail(qualityRunId)`, `loadErrors()`, `exportErrors(format, filtered)`, and `rerun(datasetId, outputVersion)`.
- Produces: a unified detail drawer with `results` and `errors` tabs.

- [ ] **Step 1: Write failing quality-store tests for full versus filtered export and two-deferred-response stale-detail protection.**

```js
import { createPinia, setActivePinia } from 'pinia';
import { describe, expect, it, vi } from 'vitest';
import { useQualityStore } from '@/stores/quality';

const deferred = [];
vi.mock('@/api/client', () => ({
  requestGet: vi.fn(() => new Promise((resolve) => deferred.push(resolve))),
  requestPost: vi.fn(),
  download: vi.fn(),
}));

describe('quality store', () => {
  it('keeps the second quality detail when the first deferred response resolves last', async () => {
    setActivePinia(createPinia());
    const store = useQualityStore();
    const first = store.openDetail('quality-run-a');
    const second = store.openDetail('quality-run-b');
    deferred[1]({ quality_run_id: 'quality-run-b', status: 'pass', results_complete: true });
    deferred[0]({ quality_run_id: 'quality-run-a', status: 'fail', results_complete: true });
    await Promise.all([first, second]);
    expect(store.selectedQualityRunId).toBe('quality-run-b');
    expect(store.detail.quality_run_id).toBe('quality-run-b');
    expect(store.detail.status).toBe('pass');
  });

  it('exports all errors without filters and filtered errors without visible-page parameters', async () => {
    setActivePinia(createPinia());
    const store = useQualityStore();
    store.selectedQualityRunId = 'quality-run-a';
    store.errorFilters.ruleCode = 'asset_readability';
    store.errorPage = 3;
    store.errorPageSize = 50;
    await store.exportErrors('csv', false);
    await store.exportErrors('json', true);
    const { download } = await import('@/api/client');
    expect(download.mock.calls[0][0]).toContain('format=csv');
    expect(download.mock.calls[0][0]).not.toMatch(/rule_code=|page=|page_size=/);
    expect(download.mock.calls[1][0]).toContain('format=json');
    expect(download.mock.calls[1][0]).toContain('rule_code=asset_readability');
    expect(download.mock.calls[1][0]).not.toMatch(/(?:\?|&)page=|(?:\?|&)page_size=/);
  });
});
```

- [ ] **Step 2: Run the narrow quality tests to verify the failures.**

Run:

```bash
npm --prefix cube_web/frontend ci
npm --prefix cube_web/frontend run test:unit -- tests/unit/qualityStore.spec.js tests/unit/QualityDetailDrawer.spec.js
```

Expected: FAIL because the quality store and unified drawer do not exist.

- [ ] **Step 3: Implement the normalized quality store and UI, then remove the old quality UI.**

Create `stores/quality.js` with these explicit filters; `loadList()` serializes them to the exact M3 snake_case names `keyword`, `dataset_id`, `output_version`, `data_type`, `status`, `trigger`, `requested_by`, `current_only`, `started_from`, and `started_to`:

```js
const filters = reactive({
  keyword: '',
  datasetId: '',
  dataType: '',
  status: '',
  currentOnly: '',
  sortBy: 'generated_at',
  sortOrder: 'desc',
});
const errorFilters = reactive({ ruleCode: '', errorCode: '', field: '' });
```

`openDetail(qualityRunId)` resets `selectedQualityRunId`, detail, results, errors, selected error rows, active tab, error pagination, and the request token before calling the detail endpoint. It must request `/results` and `/errors` only for that selected run; stale responses cannot write after a different run is selected. The two-deferred-response test above is mandatory evidence: after `openDetail('quality-run-a')`, then `openDetail('quality-run-b')`, resolving B before A leaves `selectedQualityRunId` and detail equal to B.

Implement export paths from a query built with `pageQuery`:

```js
const basePath = `/v1/quality/records/${selectedQualityRunId.value}/errors/export`;
const query = pageQuery({ format, ...(filtered ? errorFilters : {}) });
const { blob, filename } = await download(`${basePath}?${query}`);
```

Create an object URL, click an `<a download>`, revoke it, and display the returned filename. `QualityDetailDrawer` must have a visible `导出全部` control and a separate `导出当前筛选结果` control; neither accepts or sends page/page_size. The drawer displays run sequence, rule-set version, output version, and an explicit current-quality indicator. Both `QualityView` and the drawer provide `重新质检`, posting `{ dataset_id, output_version }` to `/v1/quality/runs`.

Delete `QualityHistoryDrawer.vue`, remove its import and every `qualityHistory`, data-type-specific quality request, and quality-workspace template branch from the split `PartitionView.vue`. Search the frontend source to confirm no strings identify the old quality API family.

- [ ] **Step 4: Run the narrow quality tests.**

Run:

```bash
npm --prefix cube_web/frontend ci
npm --prefix cube_web/frontend run test:unit -- tests/unit/qualityStore.spec.js tests/unit/QualityDetailDrawer.spec.js
```

Expected: PASS.

### Task 7: Clean up encoding, configuration, navigation, styles, and obsolete frontend surfaces

**Files:**
- Modify: `cube_web/frontend/src/views/EncodingView.vue`
- Modify: `cube_web/frontend/src/views/ConfigView.vue`
- Modify: `cube_web/frontend/src/styles.css`
- Modify: `cube_web/frontend/src/data/navigation.js`
- Modify: `cube_web/frontend/tests/e2e/app.spec.js`
- Create: `cube_web/frontend/tests/unit/obsoleteSurface.spec.js`

**Interfaces:**
- Consumes: shared grid metadata and current M1 SDK routes.
- Produces: encoding/configuration controls limited to Geohash, MGRS, and ISEA4H with native-level constraints.

- [ ] **Step 1: Write a failing surface test that bans old grid and quality UI symbols.**

```js
import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';

const source = [
  'src/views/PartitionView.vue',
  'src/views/EncodingView.vue',
  'src/views/ConfigView.vue',
  'src/data/navigation.js',
].map((file) => readFileSync(new URL(`../../${file}`, import.meta.url), 'utf8')).join('\n');

describe('obsolete frontend surface removal', () => {
  it('does not retain old grids, quality history UI, or COG conversion progress copy', () => {
    expect(source).not.toMatch(/\bs2\b|tile_matrix|plane_grid|QualityHistoryDrawer|生成 COG|COG 耗时/);
  });
});
```

- [ ] **Step 2: Run the focused obsolete-surface test to verify it fails.**

Run:

```bash
npm --prefix cube_web/frontend ci
npm --prefix cube_web/frontend run test:unit -- tests/unit/obsoleteSurface.spec.js
```

Expected: FAIL because old grid options and conversion controls remain.

- [ ] **Step 3: Apply the targeted cleanup.**

Replace every encoding grid selector and grid label map with `gridDefinitions` from `utils/grid.js`; use the selected definition's `minLevel` and `maxLevel` for inputs. Use current `geohash`, `mgrs`, and `isea4h` ST-code prefixes and remove all old example codes (`s2:`, `tm:`, `pg:`, and `hx:`).

In `ConfigView.vue`, remove all COG conversion fields (`cog_workers`, compression, predictor, level, and thread controls), the old tile/S2/plane grid options, and the ingest-demo settings that imply frontend control of COG generation. Replace grid configuration fields with `grid_type` and `requested_grid_level`, constrain values with `gridDefinitions`, and show the derived method read-only.

Update responsive styles for `AppLayout`, tables, drawers, and partition controls so that below `768px` toolbar actions wrap, drawers use `100%` width, table containers scroll horizontally, and adjacent reset/submit controls remain individually reachable. Remove styles that existed only for the old quality workspace/history drawer.

- [ ] **Step 4: Run the focused obsolete-surface test.**

Run:

```bash
npm --prefix cube_web/frontend ci
npm --prefix cube_web/frontend run test:unit -- tests/unit/obsoleteSurface.spec.js
```

Expected: PASS.

### Task 8: Replace browser regression coverage with routed, teleported-control, reset-safe drawer, export, and responsive tests

**Files:**
- Modify: `cube_web/frontend/tests/e2e/app.spec.js`
- Create: `cube_web/frontend/tests/e2e/fixtures.js`

**Interfaces:**
- Consumes: the M2 partition submit route, M3 normalized API fixtures, router URLs, Element Plus dropdowns, and application tables/drawers.
- Produces: browser evidence for route navigation, native grid constraints, dataset/quality drawer reset behavior, filtered/full quality export requests, and desktop/mobile layout.
- Freezes the mandatory Playwright spec at `cube_web/frontend/tests/e2e/app.spec.js` and fixture module at `cube_web/frontend/tests/e2e/fixtures.js`; do not rename, split, dynamically select, or adapt an old browser spec.
- `installApiRoutes(page)` exports the named fixtures `datasetListFixture`, `datasetDetailFixtureA`, `datasetDetailFixtureB`, `datasetAssetsFixtureA`, `datasetGridFixtureA`, `qualityRecordsFixture`, `qualityDetailFixtureA`, `qualityDetailFixtureB`, `qualityResultsFixtureA`, `qualityErrorsFixtureA`, `qualityExportCsvFixture`, and `qualityExportJsonFixture`. Dataset list uses `dataset-a`/`Dataset A` and `dataset-b`/`Dataset B`; each list fixture is exactly `{ items, total, page, page_size }`. Dataset detail fixtures contain their matching `dataset_id`, `dataset_title`, `grid_type`, `requested_grid_level`, `partition_status`, `quality_status`, and derived `publish_status`; child fixtures are Page payloads. Quality records uses `quality-run-a` and `quality-run-b`; detail fixtures contain matching `quality_run_id`, `dataset_id`, `output_version`, `status`, `results_complete`, `quality_sequence`, and `is_current`; results/errors fixtures use Page payloads. CSV export returns `Content-Type: text/csv` and `Content-Disposition: attachment; filename="dataset-a_quality-run-a_errors.csv"`; JSON export returns `Content-Type: application/json` and an equivalent JSON filename.
- The application must expose these exact test IDs: `partition-grid-type`, `dataset-row-dataset-a`, `dataset-row-dataset-b`, `dataset-detail-drawer`, `dataset-detail-close`, `dataset-detail-tab-assets`, `dataset-detail-tab-grid`, `quality-row-quality-run-a`, `quality-row-quality-run-b`, `quality-detail-drawer`, `quality-detail-close`, `quality-detail-tab-results`, `quality-detail-tab-errors`, `quality-export-all`, and `quality-export-filtered`. The test IDs are stable browser-test interfaces, not arbitrary implementation detail.
- Exact browser locators are `page.getByTestId('partition-grid-type')` for the Element Plus control; `page.getByTestId('dataset-row-dataset-a')` / `page.getByTestId('dataset-row-dataset-b')` for dataset selection; `page.getByTestId('dataset-detail-drawer')` and `page.getByTestId('dataset-detail-close')` for opening/closing its drawer; `page.getByTestId('quality-row-quality-run-a')` / `page.getByTestId('quality-row-quality-run-b')`, `page.getByTestId('quality-detail-drawer')`, `page.getByTestId('quality-detail-close')`, `page.getByTestId('quality-detail-tab-results')`, `page.getByTestId('quality-detail-tab-errors')`, `page.getByTestId('quality-export-all')`, and `page.getByTestId('quality-export-filtered')` for quality. Use no adaptive text/legacy selectors for these interactions.

- [ ] **Step 1: Replace old batch-import browser setup with named M2/M3 API fixtures.**

Create `tests/e2e/fixtures.js` exporting `installApiRoutes(page)` plus every named fixture frozen in the Task 8 Interfaces. It must intercept the M2 submit route, exact M3 list/detail endpoints, and these delayed first-detail paths without fallback/adaptive handlers:

```js
await page.route('**/v1/partition/datasets?**', async (route) => route.fulfill({ contentType: 'application/json', body: JSON.stringify(datasetListFixture) }));
await page.route('**/v1/partition/datasets/dataset-a', async (route) => route.fulfill({ contentType: 'application/json', body: JSON.stringify(datasetDetailFixtureA) }));
await page.route('**/v1/partition/datasets/dataset-b', async (route) => route.fulfill({ contentType: 'application/json', body: JSON.stringify(datasetDetailFixtureB) }));
await page.route('**/v1/quality/records?**', async (route) => route.fulfill({ contentType: 'application/json', body: JSON.stringify(qualityRecordsFixture) }));
await page.route('**/v1/quality/records/quality-run-a', async (route) => route.fulfill({ contentType: 'application/json', body: JSON.stringify(qualityDetailFixtureA) }));
await page.route('**/v1/quality/records/quality-run-b', async (route) => route.fulfill({ contentType: 'application/json', body: JSON.stringify(qualityDetailFixtureB) }));
```

For the quality stale-selection spec, `installApiRoutes(page, { deferQualityRunA: true })` must retain the `quality-run-a` route callback's `route` in a named `releaseQualityRunA` closure, fulfill B immediately, and return `{ releaseQualityRunA }`; `releaseQualityRunA()` fulfills A only after B's drawer details are visible. Provide named handlers for each dataset detail subresource, `/v1/quality/records/quality-run-a/results`, `/v1/quality/records/quality-run-a/errors`, both export formats with the frozen headers, and `POST /v1/quality/runs`. The M2 partition fixture verifies a body containing `batch_id`, `datasets` with assets/bands, `grid_type`, `requested_grid_level`, and derived `partition_method`, and rejects `dataset_ids`/`grid_level_mode` with HTTP 400. Do not call old schema-import, batch-detail, history, latest, or report endpoints.

- [ ] **Step 2: Add the routed user-flow tests with frozen IDs and stale-race proof.**

Add a dataset test that visits `/datasets`, clicks `page.getByTestId('dataset-row-dataset-a')`, asserts `page.getByTestId('dataset-detail-drawer')` is visible, switches using `page.getByTestId('dataset-detail-tab-assets')` then `page.getByTestId('dataset-detail-tab-grid')`, asserts only `Dataset A` data is shown, closes with `page.getByTestId('dataset-detail-close')`, then opens `page.getByTestId('dataset-row-dataset-b')` and asserts Dataset B is shown. Add a test that clicks the Element Plus grid control exactly this way:

```js
await page.getByTestId('partition-grid-type').click();
const visibleOptions = page.locator('.el-select-dropdown:visible .el-select-dropdown__item');
await expect(visibleOptions).toHaveText(['Geohash', '扩展 MGRS', 'ISEA4H']);
await visibleOptions.filter({ hasText: 'ISEA4H' }).click();
await expect(page.getByText('实体剖分', { exact: true })).toBeVisible();
```

Add the mandatory quality stale-race test with no timing sleep:

```js
const { releaseQualityRunA } = await installApiRoutes(page, { deferQualityRunA: true });
await page.goto('/quality');
await page.getByTestId('quality-row-quality-run-a').click();
await page.getByTestId('quality-row-quality-run-b').click();
await expect(page.getByTestId('quality-detail-drawer')).toContainText('quality-run-b');
await releaseQualityRunA();
await expect(page.getByTestId('quality-detail-drawer')).toContainText('quality-run-b');
await expect(page.getByTestId('quality-detail-drawer')).not.toContainText('quality-run-a');
```

Add a quality export test that uses `page.getByTestId('quality-row-quality-run-a')`, `page.getByTestId('quality-detail-tab-errors')`, `page.getByTestId('quality-export-all')`, and `page.getByTestId('quality-export-filtered')`; assert the full URL contains `format=csv` and no `rule_code`, `page`, or `page_size`, while the filtered URL contains `rule_code` and still no `page`/`page_size`. Also click `quality-detail-tab-results` and assert its frozen result fixture renders. Add a responsive test using `page.setViewportSize({ width: 390, height: 844 })`, visits `/partition`, and asserts the reset and submit buttons are visible and non-overlapping by comparing their bounding boxes.

- [ ] **Step 3: Run only the Playwright frontend regression.**

Run:

```bash
npm --prefix cube_web/frontend ci
npm --prefix cube_web/frontend exec playwright test tests/e2e/app.spec.js
```

Expected: PASS with all routed frontend regression tests passing and no skipped legacy test.

### Task 9: M4 Completion Gate, adversarial evidence, scoped static checks, and milestone commit

**Files:**
- Modify: only Milestone 4 implementation files listed in Tasks 1–8.
- Verify: `docs/superpowers/plans/2026-07-14-m4-frontend-architecture.md`

**Interfaces:**
- Consumes: completed M1–M3 APIs and all frontend modules/tests from Tasks 1–8.
- Produces: one evidence record per technical L1–L4 gate, review-role evidence, and one local M4 milestone commit only after every blocking condition passes.

- [ ] **Step 1: Pass technical L1 (owned-slice self-test) and record implementer evidence.**

Each owner runs the narrow tests created by its assigned task in its own isolated worktree and records branch, exclusive file list, command, pass count, and static-check result. L1 passes only when every owned narrow test—including the Task 8 browser spec—is green; unavailable browser/runtime dependency, failure, skip, or unowned-file diff blocks integration. The implementer then self-reviews its owned diff; this review role is evidence, not an L1 substitute.

- [ ] **Step 2: Pass technical L2 (combined frontend verification) and independent review.**

In the Opus integration worktree, apply reviewed worker diffs without committing, then run the combined frontend unit suite, production/current-doc scan, build, and frozen browser spec. An independent Sonnet or Opus reviewer who did not implement the slice inspects router guards, M2 submission body, API method/cancellation behavior, token races, M3 dataset/quality filters, publication status mapping, old-quality removal, download authentication, and test teardown. Record files/commands, findings, severity, fixes, and rerun result. A high-severity finding blocks L3.

- [ ] **Step 3: Pass technical L3 (explicit rejection/race matrix) and adversarial validation.**

A separate adversarial validator runs and records these required cases: submit rejects blank `batch_id`, mixed dataset data types, empty assets/bands, `dataset_ids`, and `grid_level_mode`; every grid derives only its allowed method; rapid dataset A/B and quality-run A/B opening cannot show stale A; aborting a request does not update state; old `/quality/history` is not routable; external callback targets are rejected; full/filtered export omits visible-page limits; and the 390px viewport preserves usable actions. Each rejected request must be asserted as a deliberate negative test—not discovered by an unscoped grep. Fix every failure before L4.

- [ ] **Step 4: Pass technical L4 and run final verification exactly once.**

Run from the isolated integration worktree root:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests cube_split/tests
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests
python3.11 -m ruff check cube_encoder cube_split cube_web
python3.11 -m mypy cube_encoder/grid_core cube_split/cube_split cube_web/cube_web
npm --prefix cube_web/frontend ci
npm --prefix cube_web/frontend run test:unit
npm --prefix cube_web/frontend run build
npm --prefix cube_web/frontend exec playwright test tests/e2e/app.spec.js
rg -n --glob '!tests/**' --glob '!docs/**' 's2|tile_matrix|plane_grid|QualityHistoryDrawer|/history|/latest|/report|生成 COG|COG 耗时|published' cube_web/frontend/src
rg -n --glob '*.md' 's2|tile_matrix|plane_grid|QualityHistoryDrawer|/history|/latest|/report|生成 COG|COG 耗时|published' docs/superpowers/specs docs/superpowers/plans 2>/dev/null
python3.11 - <<'PY'
from pathlib import Path

plan = Path('docs/superpowers/plans/2026-07-14-m4-frontend-architecture.md').resolve()
for source in (plan,):
    text = source.read_text(encoding='utf-8')
    forbidden = '.claude/' + 'worktrees'
    assert forbidden not in text, f'{source} contains a harness worktree path'
PY
```

Expected: L4 passes only when all tests/build/type/lint/browser commands pass; the production-source scan has no matches; the current production-doc scan has no matches; explicit L3 rejection tests cover forbidden request/legacy-route behavior; and the Python assertion exits 0. The assertion reads only the absolute resolved plan path, constructs the forbidden harness-path token inside its own source, and never scans harness worktree directories.

- [ ] **Step 5: Review the final staged file set and create the one local M4 commit.**

Only the Opus integration worker performs this step after L1–L4 and all review roles are complete. From its isolated worktree, stage exactly the M4 implementation/test files after preserving review evidence outside the commit, verify no other worker/integration commit exists, and create the sole local M4 commit:

```bash
git status --short
git diff --check
git add cube_web/frontend/package.json cube_web/frontend/package-lock.json cube_web/frontend/vite.config.js cube_web/frontend/src cube_web/frontend/tests
git status --short
git commit -m "feat: refactor frontend dataset quality workspace"
```

Expected: the staged set contains only Milestone 4 frontend files, the commit succeeds locally as `feat: refactor frontend dataset quality workspace`, and no worker commit, remote push, branch creation, or pull request occurs.

---

## M5 Browser Handoff Contract

M5 consumes the literal browser interfaces frozen in Task 8 without adaptation: spec `cube_web/frontend/tests/e2e/app.spec.js`, fixtures `cube_web/frontend/tests/e2e/fixtures.js`, `installApiRoutes(page, { deferQualityRunA })`, named fixtures `datasetListFixture`, `datasetDetailFixtureA`, `datasetDetailFixtureB`, `datasetAssetsFixtureA`, `datasetGridFixtureA`, `qualityRecordsFixture`, `qualityDetailFixtureA`, `qualityDetailFixtureB`, `qualityResultsFixtureA`, `qualityErrorsFixtureA`, `qualityExportCsvFixture`, and `qualityExportJsonFixture`, and `releaseQualityRunA()` for the delayed-first-response race. It uses dataset IDs `dataset-a`/`dataset-b`, quality IDs `quality-run-a`/`quality-run-b`, exact M3 dataset and quality routes described in Task 8, and exact M2 submission route `POST /v1/partition/optical/tasks/run` for the optical fixture.

M5 uses only these exact locators: `partition-grid-type`, `dataset-row-dataset-a`, `dataset-row-dataset-b`, `dataset-detail-drawer`, `dataset-detail-close`, `dataset-detail-tab-assets`, `dataset-detail-tab-grid`, `quality-row-quality-run-a`, `quality-row-quality-run-b`, `quality-detail-drawer`, `quality-detail-close`, `quality-detail-tab-results`, `quality-detail-tab-errors`, `quality-export-all`, and `quality-export-filtered`. It opens Element Plus grid choices via `page.getByTestId('partition-grid-type')` then `.el-select-dropdown:visible .el-select-dropdown__item`; it does not use native `<select>`, `selectOption`, dynamic legacy test paths, or adaptive old-test selectors.

The mandatory M5 stale-selection proof opens A, opens B, waits until `quality-detail-drawer` contains `quality-run-b`, calls `releaseQualityRunA()`, and reasserts the drawer contains B and not A. Full export includes `format=csv` and no `rule_code`, `page`, or `page_size`; filtered export contains `rule_code` and no `page`/`page_size`. Both use the named Task 8 export fixtures and their frozen content types and filenames.
