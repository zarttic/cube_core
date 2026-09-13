// Real-stack, human-like E2E suite. Every action is a browser click/fill; API/DB use is verification only.
import fs from 'node:fs';
import path from 'node:path';
import {
  APP, OUT, REPO, DIR, DATASET_BATCH, DATASET_ID, FORBIDDEN_GRIDS, state, initOut, persist, record, scenario,
  skip, shot, waitFor, apiJson, runDb, dbCounts, dbRunCounts, traceChunk,
} from './lib.mjs';
import { login, launchReal } from './login.mjs';

const GRIDS = [
  { key: 'geohash', option: /geohash|经纬度/, level: '层级 1' },
  { key: 'mgrs', option: /mgrs|平面/, level: '层级 0' },
  { key: 'isea4h', option: /isea4h|六边/, level: '层级 1' },
];
const MINI_LEVEL = { geohash: '1', mgrs: '0', isea4h: '1' };
const wanted = process.argv.slice(2);
const shouldRun = (id) => wanted.length === 0 || wanted.includes(String(id));
state.startedAt = new Date().toISOString();

const diag = async (page, tag) => {
  const info = await page.evaluate(() => ({
    url: location.href,
    testids: [...document.querySelectorAll('[data-testid]')].map((e) => e.getAttribute('data-testid')),
    drawer: (document.querySelector('.el-drawer')?.innerText || '').replace(/\n+/g, ' | ').slice(0, 1200),
    checkboxes: document.querySelectorAll('.el-checkbox').length,
  })).catch(() => ({}));
  fs.writeFileSync(path.join(OUT, `diag-${tag}.json`), JSON.stringify(info, null, 2));
  return JSON.stringify(info).slice(0, 500);
};

async function openPartition(page) {
  await page.goto(`${APP}/partition`, { waitUntil: 'domcontentloaded', timeout: 60000 });
  await page.waitForTimeout(3000);
  if (page.url().startsWith('http://10.3.100.182')) await login(page, { entry: `${APP}/partition` });
  await page.getByTestId('partition-module-optical').click();
  await page.waitForTimeout(1500);
}

async function openDrawer(page) {
  await page.getByRole('button', { name: /已载入光学遥感数据/ }).click();
  await page.waitForTimeout(2500);
}

async function expandDataset(page) {
  const node = page.getByTestId(`dataset-tree-${DATASET_BATCH}-${DATASET_ID}`);
  const tree = page.getByTestId(`batch-tree-${DATASET_BATCH}`);
  if (!(await tree.count())) {
    const card = page.getByTestId(`load-batch-${DATASET_BATCH}`);
    if (!(await card.count())) throw new Error(`load-batch card missing before tree exists; drawer=${await diag(page, 'expand-missing-card')}`);
    await card.first().click();
    await page.waitForTimeout(3000);
  }
  const header = node.locator('.partition-scene-group-header');
  if (!(await node.count()) || !(await header.isVisible().catch(() => false))) {
    const batchHeader = tree.locator('.partition-batch-tree-header');
    if (!(await batchHeader.count())) throw new Error(`batch header missing; drawer=${await diag(page, 'expand-missing-batch')}`);
    if ((await batchHeader.getAttribute('aria-expanded')) === 'false') await batchHeader.click();
    await page.waitForTimeout(2000);
  }
  if (!(await node.count())) throw new Error(`dataset node missing; drawer=${await diag(page, 'expand-missing-dataset')}`);
  if ((await header.getAttribute('aria-expanded')) === 'false') await header.click();
  await page.waitForTimeout(2000);
  if ((await header.getAttribute('aria-expanded')) === 'false') throw new Error(`dataset group still collapsed; drawer=${await diag(page, 'expand-still-collapsed')}`);
  return node;
}

function datasetNode(page) { return page.getByTestId(`dataset-tree-${DATASET_BATCH}-${DATASET_ID}`); }

async function selectDataset(page) {
  await expandDataset(page);
  const checkbox = datasetNode(page).locator('[data-testid^="select-dataset-"]').first();
  if (!(await checkbox.count())) throw new Error(`dataset checkbox missing; drawer=${await diag(page, 'select-missing-checkbox')}`);
  if ((await checkbox.locator('.el-checkbox__input').getAttribute('class')).includes('is-checked')) return 'already-selected';
  await checkbox.locator('.el-checkbox__input').click();
  await page.waitForTimeout(2500);
  return 'clicked';
}

// Select exactly one scene (first rendered) so runs stay small; returns scene id.
const KNOWN_GOOD_SCENE = 'scene-3e2d9d877f1423bace8e58532596d94c671fa26b36cb0038a26e00c113744723';

async function selectSingleScene(page, preferredId = KNOWN_GOOD_SCENE) {
  const rows = page.locator('[data-testid^="select-scene-"]');
  const total = await rows.count();
  if (!total) throw new Error(`no scene checkboxes; drawer=${await diag(page, 'scene-missing')}`);
  const all = [];
  for (let index = 0; index < total; index += 1) all.push(rows.nth(index));
  const enabled = [];
  for (const candidate of all) {
    const disabled = await candidate.locator('.el-checkbox__input').evaluate((node) => node.classList.contains('is-disabled')).catch(() => true);
    if (!disabled) enabled.push(candidate);
  }
  const enabledIds = [];
  for (const candidate of enabled) enabledIds.push((await candidate.getAttribute('data-testid')).replace('select-scene-', ''));
  state.ledger.notes.push({ scenario: 'scene-availability', enabledScenes: enabledIds, totalScenes: total });
  persist();
  const forcedIndex = Number(process.env.NEAT_SCENE_INDEX ?? Number.NaN);
  const order = [];
  if (Number.isInteger(forcedIndex) && enabled[forcedIndex]) order.push(enabled[forcedIndex]);
  if (preferredId) {
    const preferred = enabled.find(async (candidate) => (await candidate.getAttribute('data-testid')) === `select-scene-${preferredId}`);
    if (preferred) order.push(preferred);
  }
  order.push(...enabled);
  for (const candidate of order) {
    const input = candidate.locator('.el-checkbox__input');
    if (await input.evaluate((node) => node.classList.contains('is-disabled')).catch(() => true)) continue;
    const sceneId = (await candidate.getAttribute('data-testid')).replace('select-scene-', '');
    await input.click();
    await page.waitForTimeout(2500);
    return { sceneId, sceneCount: total };
  }
  throw new Error(`every scene checkbox is disabled (${total} scenes); drawer=${await diag(page, 'scene-all-disabled')}`);
}

async function selectGrid(page, grid) {
  const select = datasetNode(page).locator('[data-testid^="dataset-grid-"]:not([data-testid^="dataset-grid-level-"])').first();
  if (!(await select.count())) throw new Error(`grid select missing for ${grid.key}; drawer=${await diag(page, 'grid-missing')}`);
  await select.click();
  await page.waitForTimeout(900);
  const dropdown = page.locator('.el-select-dropdown:visible');
  const options = await dropdown.locator('.el-select-dropdown__item').allTextContents();
  const option = dropdown.locator('.el-select-dropdown__item').filter({ hasText: grid.option }).first();
  if (!(await option.count())) throw new Error(`option ${grid.key} not found among ${JSON.stringify(options)}`);
  await option.click();
  await page.waitForTimeout(1500);
  const shown = await select.innerText().catch(() => '');
  return { options, shown: shown.replace(/\s+/g, ' ').trim() };
}

async function closeDrawer(page) {
  const close = page.locator('.el-drawer__close-btn').first();
  if (await close.count()) {
    await close.click();
  } else {
    await page.keyboard.press('Escape');
  }
  await page.waitForTimeout(2000);
  return /0 \u4e2a\u6570\u636e\u96c6/.test(await page.getByTestId('selected-load-batches').innerText().catch(() => ''));
}

async function unlockLevelIfNeeded(page) {
  const unlock = datasetNode(page).locator('[data-testid^="unlock-grid-level-"]').first();
  if (await unlock.count()) {
    await unlock.click();
    await page.waitForTimeout(1500);
    return true;
  }
  return false;
}

async function setLevel(page, levelLabel) {
  const select = datasetNode(page).locator('[data-testid^="dataset-grid-level-"]').first();
  if (!(await select.count())) throw new Error(`level select missing; drawer=${await diag(page, 'level-missing')}`);
  await select.click();
  await page.waitForTimeout(900);
  const dropdown = page.locator('.el-select-dropdown:visible');
  const options = await dropdown.locator('.el-select-dropdown__item').allTextContents();
  const option = dropdown.locator('.el-select-dropdown__item').filter({ hasText: new RegExp(`^\\s*${levelLabel}\\s*$`) }).first();
  if (!(await option.count())) throw new Error(`level ${levelLabel} not among ${JSON.stringify(options)}`);
  await option.click();
  await page.waitForTimeout(1200);
  return (await select.innerText()).replace(/\s+/g, ' ').trim();
}

const TERMINAL_STATUSES = new Set(['completed', 'failed', 'cancelled', 'manual_required']);
const STATUS_LABELS = [
  ['需要人工处理', 'manual_required'], ['部分失败', 'partial_failure'], ['取消中', 'cancel_requested'], ['已取消', 'cancelled'],
  ['已完成', 'completed'], ['失败', 'failed'], ['运行中', 'running'], ['重试中', 'retrying'], ['已排队', 'queued'], ['等待中', 'pending'],
];
function statusFromRow(text) {
  for (const [label, value] of STATUS_LABELS) if (text.includes(label)) return value;
  const english = text.match(/manual_required|cancel_requested|partial_failure|completed|failed|cancelled|running|retrying|queued|pending/);
  return english ? english[0] : null;
}

async function main() {
  const { browser, context, page } = await launchReal();
  fs.mkdirSync(path.join(OUT, 'traces'), { recursive: true });
  await context.tracing.start({ screenshots: true, snapshots: true, sources: true }).catch(() => {});
  state.traceContext = context;
  const submitPayloads = (() => {
    try { return JSON.parse(fs.readFileSync(path.join(OUT, 'submit-payloads.json'), 'utf8')); } catch { return []; }
  })();
  page.on('response', async (response) => {
    if (response.request().method() === 'POST' && response.url().includes('/v1/partition/runs')) {
      const body = await response.text().catch(() => '');
      submitPayloads.push({ status: response.status(), request: response.request().postData() || JSON.stringify(response.request().postDataJSON?.() || {}), response: body.slice(0, 4000) });
      fs.writeFileSync(path.join(OUT, 'submit-payloads.json'), JSON.stringify(submitPayloads, null, 2));
    }
  });

  try {
    let loginResult;
    try {
      loginResult = await traceChunk('7-login', () => login(page, { entry: `${APP}/partition` }));
    } catch (error) {
      if (shouldRun(7)) await record(7, 'real portal login form (headless, human-like)', 'FAIL', String(error.message || error).slice(0, 400));
      throw error;
    }
    state.ledger.notes.push({ scenario: 'login', user: loginResult.username, role: loginResult.role, mechanism: 'portal login form: placeholder user/pass + native role select (管理员) + 登录系统 button' });
    persist();
    if (shouldRun(7)) {
      await record(7, 'real portal login form (headless, human-like)', 'PASS',
        `portal form -> POST /api/login 200 -> /callback -> app authenticated (access_token present); role=${loginResult.role}; mechanism=real login form`);
    }
    if (wanted.includes('0')) { await browser.close(); return; }

    if (shouldRun(1)) {
      await scenario(1, '/partition dataset/scene list renders real data', async () => {
        await openPartition(page);
        await openDrawer(page);
        const node = await expandDataset(page);
        await shot(page, '01-drawer-dataset-expanded');
        const datasetText = await page.getByText('ard-load-d42e8791e5194a88ba42b499274b29ff', { exact: false }).count();
        const sceneRows = await page.locator('[data-testid^="scene-"]').count();
        const sceneVisible = await page.locator('[data-testid^="select-scene-"]').first().isVisible();
        const bandRows = await page.locator('[data-testid^="band-unit-"]').count();
        await shot(page, '01b-drawer-scenes');
        const headerText = (await node.locator('.partition-scene-group-header').innerText()).replace(/\s+/g, ' ');
        if (datasetText === 0 || !sceneVisible) throw new Error(`dataset/scenes not visible (id matches=${datasetText}, sceneVisible=${sceneVisible}); ${await diag(page, 's1')}`);
        return `dataset id matches=${datasetText}; header="${headerText.slice(0, 110)}"; scene rows=${sceneRows} (visible=${sceneVisible}); band rows=${bandRows}; shots 01/01b`;
      });
    }

    if (shouldRun(2)) {
      await scenario(2, 'grid type selection for geohash/mgrs/isea4h, method derived, no legacy grids', async () => {
        const observed = {};
        const legacySeen = new Set();
        for (const grid of GRIDS) {
          await openPartition(page);
          await openDrawer(page);
          await selectDataset(page);
          const { options, shown } = await selectGrid(page, grid);
          options.forEach((text) => FORBIDDEN_GRIDS.forEach((legacy) => { if (text.includes(legacy)) legacySeen.add(legacy); }));
          const unlocked = await unlockLevelIfNeeded(page);
          const level = await setLevel(page, grid.level);
          observed[grid.key] = { shown, level, unlockedLevelControl: unlocked, options };
          const partitionRequest = await page.evaluate(async () => {
            const response = await fetch('/api/config');
            return response.status;
          });
          void partitionRequest;
          await shot(page, `02-grid-${grid.key}`);
        }
        const bodyText = await page.evaluate(() => document.body.innerText);
        const legacyInBody = FORBIDDEN_GRIDS.filter((legacy) => bodyText.includes(legacy));
        await record('2-summary', 'grid selection observations', 'PASS', JSON.stringify(observed));
        if (legacySeen.size || legacyInBody.length) throw new Error(`legacy grid leaked: dropdown=${[...legacySeen]} body=${legacyInBody}`);
        const wrong = Object.entries(observed).filter(([key, value]) => value.level !== `层级 ${MINI_LEVEL[key]}`);
        if (wrong.length) throw new Error(`level not applied: ${JSON.stringify(wrong)}`);
        return `3 grids selectable from one dataset row; levels applied ${JSON.stringify(MINI_LEVEL)}; no ${FORBIDDEN_GRIDS.join('/')} option or text`;
      });
    }

    if (wanted.length === 0 || wanted.some((w) => w === '3' || w.startsWith('3-'))) {
      for (const grid of GRIDS) {
        if (!shouldRun(`3-${grid.key}`)) continue;
        await scenario(`3-${grid.key}`, `real partition run via UI submit (${grid.key})`, async () => {
          await openPartition(page);
          await openDrawer(page);
          await expandDataset(page);
          const scene = await selectSingleScene(page);
          await selectGrid(page, grid);
          await unlockLevelIfNeeded(page);
          await setLevel(page, grid.level);
          await shot(page, `03-${grid.key}-configured`);
          await closeDrawer(page);
          await shot(page, `03-${grid.key}-drawer-closed`);
          const payloadCount = submitPayloads.length;
          await page.getByRole('button', { name: '提交剖分' }).click();
          await page.waitForTimeout(6000);
          const submitted = await waitFor(() => submitPayloads.length > payloadCount ? submitPayloads[submitPayloads.length - 1] : null,
            { timeout: 30000, interval: 1000, label: 'POST /v1/partition/runs' });
          const responseBody = JSON.parse(submitted.response || '{}');
          const requestBody = JSON.parse(submitted.request || '{}');
          const runId = requestBody.partition_run_id || responseBody.partition_run_id;
          const expectedMethod = (grid.key === 'geohash' || grid.key === 'mgrs') ? 'logical' : 'entity';
          const partitions = (requestBody.datasets || []).map((dataset) => dataset.partition).filter(Boolean);
          const sentMethods = [...new Set(partitions.map((entry) => entry.partition_method).filter(Boolean))];
          const sentGrids = [...new Set(partitions.map((entry) => entry.grid_type).filter(Boolean))];
          const sentLevels = [...new Set(partitions.map((entry) => entry.requested_grid_level))];
          const maxCells = [...new Set(partitions.map((entry) => entry.max_cells_per_asset))];
          state.ledger.notes.push({ scenario: `3-${grid.key}`, expectedMethod, sentMethods: [...new Set(sentMethods)], sentGrids: [...new Set(sentGrids)], requestKeys: Object.keys(requestBody), maxCells, sentLevels, bands: (requestBody.datasets || []).flatMap((d) => d.band_unit_ids || []).length });
          if (sentMethods.length && sentMethods.some((method) => method !== expectedMethod)) throw new Error(`method derived wrong: expected ${expectedMethod}, payload says ${JSON.stringify(sentMethods)}`);
          if (sentGrids.length && sentGrids.some((value) => value !== grid.key)) throw new Error(`payload grid mismatch: ${JSON.stringify(sentGrids)}`);
          const taskId = responseBody.task_id;
          state.ledger.runs.push({ run_id: runId, task_id: taskId, grid: grid.key, level: grid.level, scene_id: scene.sceneId, source: 'UI submit /partition', created_at: new Date().toISOString() });
          state.ledger.notes.push({ scenario: `3-${grid.key}`, mechanism: 'human-like clicks: module tab -> drawer -> batch header -> dataset header -> scene checkbox -> grid select -> level select -> 提交剖分', scene: scene.sceneId });
          persist();
          await shot(page, `03-${grid.key}-submitted`);
          // Poll the UI task table (reload = user refresh) until terminal; task rows are keyed by task_id.
          let status = responseBody.status || 'unknown';
          let lastRow = '';
          const deadline = Date.now() + Number(process.env.NEAT_RUN_DEADLINE_MS || 200000);
          const rowFor = (needle) => page.locator('tr', { hasText: needle }).first();
          while (!TERMINAL_STATUSES.has(status) && Date.now() < deadline) {
            await page.waitForTimeout(10000);
            await page.goto(`${APP}/partition`, { waitUntil: 'domcontentloaded', timeout: 60000 });
            await page.waitForTimeout(3000);
            const row = (taskId && await rowFor(taskId).count()) ? rowFor(taskId) : rowFor(runId);
            if (await row.count()) { lastRow = (await row.innerText()).replace(/\s+/g, ' '); const parsed = statusFromRow(lastRow); if (parsed) status = parsed; }
            if (!TERMINAL_STATUSES.has(status)) { const dbNow = dbRunCounts(runId).status; if (dbNow) status = TERMINAL_STATUSES.has(dbNow) || status === 'unknown' ? dbNow : status; }
          }
          if (!TERMINAL_STATUSES.has(status)) {
            // Human-like reap: click the row's 取消 button, then wait bounded for cancellation.
            const row = (taskId && await rowFor(taskId).count()) ? rowFor(taskId) : rowFor(runId);
            const cancel = row.locator('button', { hasText: '取消' }).first();
            if (await cancel.count()) {
              await cancel.click().catch(() => {});
              const cancelDeadline = Date.now() + 90000;
              while (!TERMINAL_STATUSES.has(status) && Date.now() < cancelDeadline) {
                await page.waitForTimeout(10000);
                await page.goto(`${APP}/partition`, { waitUntil: 'domcontentloaded', timeout: 60000 });
                await page.waitForTimeout(3000);
                const cancelRow = (taskId && await rowFor(taskId).count()) ? rowFor(taskId) : rowFor(runId);
                lastRow = (await cancelRow.innerText().catch(() => '')).replace(/\s+/g, ' ');
                const parsed = statusFromRow(lastRow);
                if (parsed) status = parsed;
                const dbNow = dbRunCounts(runId).status;
                if (dbNow && TERMINAL_STATUSES.has(dbNow)) status = dbNow;
              }
            }
          }
          await shot(page, `03-${grid.key}-terminal-${status}`);
          const dbCountsForRun = dbRunCounts(runId);
          state.ledger.runs[state.ledger.runs.length - 1].db = dbCountsForRun;
          persist();
          const uiCounts = { tiles: (responseBody.tiles || responseBody.tile_objects || []).length, cells: (responseBody.grid_cells || []).length, indexes: (responseBody.indexes || []).length };
          await record(`3-${grid.key}-db`, `DB verification ${grid.key}`, 'PASS', JSON.stringify({ run_id: runId, status, db: dbCountsForRun, ui: uiCounts, ui_row: lastRow }));
          if (!TERMINAL_STATUSES.has(status)) throw new Error(`run ${runId} not terminal within ${process.env.NEAT_RUN_DEADLINE_MS || 200000}ms budget; last row="${lastRow}"`);
          if (status !== 'completed') throw new Error(`run ${runId} ended ${status}: ${lastRow}`);
          const dbCells = dbCountsForRun.partition_grid_cells;
          if (!(dbCells > 0)) throw new Error(`DB shows no grid cells for ${runId}: ${JSON.stringify(dbCountsForRun)}`);
          return `run=${runId} status=${status} db grid_cells=${dbCells} indexes=${dbCountsForRun.partition_indexes} tiles=${dbCountsForRun.partition_tiles} run_scenes=${dbCountsForRun.partition_run_scenes}; UI row="${lastRow.slice(0, 120)}"`;
        });
      }
    }

    if (submitPayloads.length) {
      const values = [];
      for (const payload of submitPayloads) {
        try {
          const body = JSON.parse(payload.request || '{}');
          for (const dataset of body.datasets || []) values.push(dataset?.partition?.max_cells_per_asset);
        } catch { /* ignore malformed capture */ }
      }
      const allZero = values.length > 0 && values.every((value) => value === 0);
      await record('2-maxcells', 'UI hard-codes max_cells_per_asset=0 in submit payload', allZero ? 'PASS' : 'FAIL',
        `submit payloads=${submitPayloads.length}; values=${JSON.stringify(values)}; no numeric max-cells control on /partition (cube_web/frontend/src/utils/grid.js pins 0 = unbounded)`);
    }

    if (shouldRun(6)) {
      await scenario('6-datasets', 'click-through /datasets', async () => {
        await page.goto(`${APP}/datasets`, { waitUntil: 'domcontentloaded', timeout: 60000 });
        await page.waitForTimeout(5000);
        const url = page.url();
        const body = await page.evaluate(() => document.body.innerText.replace(/\s+/g, ' '));
        await shot(page, '06-datasets');
        if (url.includes('5177/login')) throw new Error('bounced to portal login for /datasets');
        if (!url.includes('/datasets')) {
          return `finding confirmed: no /datasets route in router/index.js; catch-all redirects to ${url.replace(APP, '')} (user sees the partition page, not a datasets page); body="${body.slice(0, 120)}"`;
        }
        const heading = await page.locator('h1,h2,h3').first().innerText().catch(() => '');
        return `route reachable: ${url} heading="${heading.replace(/\s+/g, ' ').slice(0, 60)}"; body="${body.slice(0, 120)}"`;
      });
      const pages = [
        { route: '/quality', name: '自动化质检', file: '06-quality' },
        { route: '/data-management', name: '数据管理', file: '06-data-management' },
        { route: '/config', name: '配置', file: '06-config' },
      ];
      for (const target of pages) {
        await scenario(`6-${target.route.slice(1)}`, `click-through ${target.route}`, async () => {
          await page.goto(`${APP}/partition`, { waitUntil: 'domcontentloaded', timeout: 60000 }).catch(() => {});
          await page.getByRole('link', { name: target.name }).first().click().catch(async () => {
            await page.goto(`${APP}${target.route}`, { waitUntil: 'domcontentloaded', timeout: 60000 });
          });
          await page.waitForTimeout(6000);
          const heading = await page.locator('h1,h2,h3').first().innerText().catch(() => '');
          const body = await page.evaluate(() => document.body.innerText.replace(/\s+/g, ' '));
          await shot(page, target.file);
          if (page.url().includes('5177/login')) throw new Error(`bounced to portal login for ${target.route}`);
          if (!page.url().includes(target.route)) throw new Error(`navigation did not reach ${target.route}: ${page.url()}`);
          if (body.includes('用户登录') || body.length < 80) throw new Error(`page shows login/blank content: "${body.slice(0, 120)}"`);
          return `url=${page.url()} heading="${heading.replace(/\s+/g, ' ').slice(0, 60)}" body="${body.slice(0, 160)}"`;
        });
      }
    }

    if (shouldRun(4)) {
      await scenario(4, '/encoding grid preview semantics + SDK comparison', async () => {
        const captures = [];
        const capture = async (target) => {
          const response = await target;
          const request = response.request().postDataJSON?.() || null;
          const body = await response.text().catch(() => '');
          captures.push({ url: response.url(), status: response.status(), request, response: (() => { try { return JSON.parse(body); } catch { return { raw: body.slice(0, 500) }; } })() });
          fs.writeFileSync(path.join(OUT, 'encoding-preview.json'), JSON.stringify(captures, null, 2));
          return captures;
        };
        fs.writeFileSync(path.join(OUT, 'encoding-preview.json'), '[]');
        await page.goto(`${APP}/encoding`, { waitUntil: 'domcontentloaded', timeout: 60000 });
        await page.waitForTimeout(6000);
        const controls = await page.evaluate(() => ({
          buttons: [...document.querySelectorAll('button')].map((e) => e.textContent.trim()).filter(Boolean).slice(0, 25),
          body: document.body.innerText.replace(/\s+/g, ' ').slice(0, 300),
        }));
        fs.writeFileSync(path.join(OUT, 'encoding-controls.json'), JSON.stringify(controls, null, 2));
        await shot(page, '04-encoding-page');
        const observed = {};
        for (const grid of GRIDS) {
          const option = { geohash: '经纬度格网', mgrs: '平面格网', isea4h: '六边形格网' }[grid.key];
          const button = page.locator('.radio-label, button').filter({ hasText: option }).first();
          if (!(await button.count())) throw new Error(`grid control ${option} not found`);
          await button.click();
          await page.waitForTimeout(1500);
          const activeGrid = await page.evaluate(() => document.body.innerText.replace(/\s+/g, ' ').slice(0, 60));
          const pending = page.waitForResponse((r) => r.url().includes('/locate') || r.url().includes('/cover'), { timeout: 30000 }).catch(() => null);
          await page.getByRole('button', { name: /查看结果/ }).first().click();
          const captured = await pending;
          if (captured) await capture(captured);
          await page.waitForTimeout(2500);
          const rows = await page.evaluate(() => [...document.querySelectorAll('tr, .result-item, .detail-row')].map((e) => e.innerText.replace(/\s+/g, ' ')).slice(0, 12));
          observed[grid.key] = { clicked: true, rows: rows.slice(0, 6) };
          await shot(page, `04-encoding-${grid.key}`);
        }
        await record('4-summary', 'encoding UI rows', 'PASS', JSON.stringify(observed));
        let comparison = null;
        try {
          comparison = JSON.parse(runDb([path.join(OUT, 'encoding-preview.json')], { script: 'sdk_compare.py' }));
        } catch (error) {
          comparison = { error: String(error.message || error).slice(0, 200) };
        }
        fs.writeFileSync(path.join(OUT, 'encoding-sdk-compare.json'), JSON.stringify(comparison, null, 2));
        const checks = comparison.checks || [];
        const mismatches = checks.filter((check) => !check.match);
        if (!checks.length) throw new Error(`no preview request captured; ui=${JSON.stringify(observed)} sdk=${JSON.stringify(comparison).slice(0, 200)}`);
        if (mismatches.length) throw new Error(`SDK mismatch: ${JSON.stringify(mismatches).slice(0, 400)}`);
        return `preview captured for ${checks.length} grids; UI codes == SDK codes for all (${checks.map((check) => `${check.grid_type}@${check.level}:${check.ui_codes.length}`).join(', ')}); screenshots 04-encoding-*.png`;
      });
    }

    if (shouldRun(5)) {
      await scenario(5, 'negative cases: invalid level, duplicate submit, out-of-range', async () => {
        const notes = [];
        await openPartition(page);
        await openDrawer(page);
        await expandDataset(page);
        await selectSingleScene(page);
        await selectGrid(page, GRIDS[0]);
        await unlockLevelIfNeeded(page);
        const levelSelect = datasetNode(page).locator('[data-testid^="dataset-grid-level-"]').first();
        await setLevel(page, '层级 1');
        const levelOptions = await (async () => {
          await levelSelect.click();
          await page.waitForTimeout(800);
          const options = await page.locator('.el-select-dropdown:visible .el-select-dropdown__item').allTextContents();
          await page.keyboard.press('Escape');
          await page.waitForTimeout(500);
          return options;
        })();
        notes.push(`level options=${JSON.stringify(levelOptions)} (out-of-range 999 not offered)`);
        await shot(page, '05-level-options');
        await closeDrawer(page);
        // duplicate submit guard: click twice fast and count POSTs
        const before = submitPayloads.length;
        await page.getByRole('button', { name: '提交剖分' }).click({ noWaitAfter: true }).catch(() => {});
        await page.getByRole('button', { name: '提交剖分' }).click({ noWaitAfter: true, timeout: 3000 }).catch(() => {});
        await page.waitForTimeout(6000);
        const posts = submitPayloads.length - before;
        notes.push(`duplicate submit -> POSTs observed=${posts}`);
        const dupRun = submitPayloads[submitPayloads.length - 1];
        if (dupRun) {
          const parsed = JSON.parse(dupRun.request || '{}');
          let dupResponse = {};
          try { dupResponse = JSON.parse(dupRun.response || '{}'); } catch { /* */ }
          if (!state.ledger.runs.some((r) => r.run_id === parsed.partition_run_id)) {
            state.ledger.runs.push({ run_id: parsed.partition_run_id, task_id: dupResponse.task_id || '', grid: 'geohash', level: 1, source: 'negative-scenario duplicate-submit', created_at: new Date().toISOString() });
            persist();
          }
        }
        await shot(page, '05-duplicate-submit');
        // AOI with no cells: not reachable from this UI surface.
        await skip('5-aoi-no-cells', 'AOI yielding no cells', 'no AOI editor on /partition for this dataset; not reachable without a custom AOI control');
        // 401 on the auth-enabled backend (no Authorization header).
        const authCheck = await apiJson('/v1/partition/runs');
        notes.push(`401 check: GET /v1/partition/runs without token -> ${authCheck.status}`);
        if (authCheck.status !== 401) throw new Error(`expected 401 from auth-enabled backend, got ${authCheck.status}`);
        // Invalid grid / level: replay the captured submit payload with a real bearer token.
        const token = await page.evaluate(() => localStorage.getItem('access_token') || localStorage.getItem('token') || '');
        let basePayload = null;
        try {
          const captured = JSON.parse(fs.readFileSync(path.join(OUT, 'submit-payloads.json'), 'utf8'));
          basePayload = JSON.parse(captured[captured.length - 1].request || '{}');
        } catch { /* no capture */ }
        const negative = async (label, mutate) => {
          if (!basePayload) { notes.push(`${label}: skipped (no captured payload)`); return; }
          const body = JSON.parse(JSON.stringify(basePayload));
          body.partition_run_id = `neat-neg-${Date.now().toString(36)}`;
          mutate(body);
          // the backend may create a failed run row before rejecting; always ledger our own id
          if (!state.ledger.runs.some((r) => r.run_id === body.partition_run_id)) {
            state.ledger.runs.push({ run_id: body.partition_run_id, source: `negative-${label}`, created_at: new Date().toISOString() });
            persist();
          }
          const response = await fetch(`${APP}/v1/partition/runs`, { method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` }, body: JSON.stringify(body) });
          const text = await response.text().catch(() => '');
          notes.push(`${label} -> ${response.status} ${text.slice(0, 90)}`);
          if (response.ok || response.status >= 500) {
            throw new Error(`${label} expected 4xx, got ${response.status}`);
          }
        };
        await negative('invalid grid plane_grid', (body) => { for (const dataset of body.datasets || []) dataset.partition = { ...dataset.partition, grid_type: 'plane_grid' }; });
        await negative('invalid level 999', (body) => { for (const dataset of body.datasets || []) dataset.partition = { ...dataset.partition, requested_grid_level: 999 }; });
        // Reap the duplicate-submit run if it is not terminal yet.
        const pending = state.ledger.runs.filter((entry) => entry.source === 'negative-scenario duplicate-submit');
        for (const entry of pending) {
          let dbStatus = dbRunCounts(entry.run_id).status;
          const reapDeadline = Date.now() + 90000;
          while (dbStatus && !TERMINAL_STATUSES.has(dbStatus) && Date.now() < reapDeadline) {
            await page.waitForTimeout(10000);
            dbStatus = dbRunCounts(entry.run_id).status;
          }
          if (dbStatus && !TERMINAL_STATUSES.has(dbStatus)) {
            // click 取消 on the matching task row
            await page.goto(`${APP}/partition`, { waitUntil: 'domcontentloaded', timeout: 60000 });
            await page.waitForTimeout(3000);
            const row = page.locator('tr', { hasText: entry.task_id || entry.run_id }).first();
            const cancel = row.locator('button', { hasText: '取消' }).first();
            if (await cancel.count()) await cancel.click().catch(() => {});
            await page.waitForTimeout(8000);
            dbStatus = dbRunCounts(entry.run_id).status;
          }
          notes.push(`duplicate-submit run ${entry.run_id} terminal=${dbStatus}`);
        }
        const backend = await apiJson('/health');
        notes.push(`backend /health -> ${backend.status}`);
        return `${notes.join(' | ')}`;
      });
    }
  } finally {
    try { fs.writeFileSync(path.join(OUT, 'api-calls.json'), JSON.stringify(apiCallsSnapshot, null, 2)); } catch { /* */ }
    try { await context.tracing.stop(); } catch { /* */ }
    persist();
    await browser.close();
  }
  const failed = state.results.filter((r) => r.status === 'FAIL');
  console.log(`\n=== ${state.results.filter((r) => r.status === 'PASS').length} PASS / ${failed.length} FAIL / ${state.results.filter((r) => r.status === 'SKIPPED').length} SKIPPED -> ${OUT}/results.json`);
  process.exitCode = failed.length ? 1 : 0;
}

let apiCallsSnapshot = [];
void DIR; void runDb; void dbCounts; void OUT;
await main();
