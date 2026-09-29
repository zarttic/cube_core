// Real-stack click-through for the pre-ingest record merge window and manual ingest.
// Every mutation is a browser click; API/DB reads are verification only.
//
// Target defaults to the radar scene used in production testing; override with
// NEAT_E2E_BATCH / NEAT_E2E_SCENE when the fixture changes.
import {
  APP, state, initOut, persist, record, scenario, shot, waitFor, apiJson, dbRunCounts,
} from './lib.mjs';
import { login, launchReal } from './login.mjs';

const MODULE = process.env.NEAT_E2E_MODULE || 'radar';
const MODULE_LABEL = process.env.NEAT_E2E_MODULE_LABEL || '雷达遥感';
const BATCH = process.env.NEAT_E2E_BATCH || 'ard-radar-ard-load-851346e432a14ac8b74f9dbaa801f038';
const SCENE = process.env.NEAT_E2E_SCENE || 'scene-dc998ae39f79da48fa453ae5a004c7fb5c95b6d54f1d0c55b8ae463f5036dc9b';
const GRID = { key: 'mgrs', option: /mgrs|平面/ };
const LEVEL_A = '层级 0';
const LEVEL_B = '层级 1';
const RUN_DEADLINE_MS = Number(process.env.NEAT_RUN_DEADLINE_MS || 300000);
const INGEST_DEADLINE_MS = Number(process.env.NEAT_INGEST_DEADLINE_MS || 300000);

async function openPartition(page) {
  await page.goto(`${APP}/partition`, { waitUntil: 'domcontentloaded', timeout: 60000 });
  await page.waitForTimeout(3000);
  if (page.url().startsWith('http://10.3.100.182')) await login(page, { entry: `${APP}/partition` });
  await page.getByTestId(`partition-module-${MODULE}`).click();
  await page.waitForTimeout(1500);
}

async function openQueueDrawer(page) {
  await page.getByRole('button', { name: `已载入${MODULE_LABEL}数据` }).click();
  await page.waitForTimeout(2500);
}

async function expandHeader(page, header) {
  if ((await header.getAttribute('aria-expanded')) === 'false') {
    await header.click();
    await page.waitForTimeout(2000);
  }
}

async function prepareTarget(page) {
  const pinned = page.locator(`[data-testid="load-batch-${BATCH}"]`);
  const anyCard = page.locator('[data-testid^="load-batch-"]:not([data-testid="load-batch-selector"])');
  await waitFor(async () => (await pinned.count()) + (await anyCard.count()) > 0, { timeout: 30000, label: 'pending load-batch card' });
  const card = (await pinned.count()) ? pinned.first() : anyCard.first();
  const batchId = (await card.getAttribute('data-testid')).replace('load-batch-', '');
  state.ledger.notes.push({ scenario: 'merge-ingest target', batch_id: batchId, pinned: batchId === BATCH });
  persist();
  const tree = page.locator(`[data-testid="batch-tree-${batchId}"]`);
  if (!(await tree.count())) {
    await card.click();
    await page.waitForTimeout(4000);
  }
  await expandHeader(page, tree.locator('.partition-batch-tree-header'));
  const datasetNode = tree.locator('[data-testid^="dataset-tree-"]').first();
  if (!(await datasetNode.count())) throw new Error(`no dataset node under batch ${batchId}`);
  await expandHeader(page, datasetNode.locator('.partition-scene-group-header'));
  return { batchId, datasetNode };
}

async function selectScene(page, preferredSceneId) {
  const preferred = page.locator(`[data-testid="select-scene-${preferredSceneId}"]`);
  const candidates = [];
  if (await preferred.count()) candidates.push(preferred.first());
  const all = page.locator('[data-testid^="select-scene-"]');
  const total = await all.count();
  for (let index = 0; index < total; index += 1) candidates.push(all.nth(index));
  for (const candidate of candidates) {
    const input = candidate.locator('.el-checkbox__input');
    if (await input.evaluate((node) => node.classList.contains('is-disabled')).catch(() => true)) continue;
    const sceneId = (await candidate.getAttribute('data-testid')).replace('select-scene-', '');
    await input.click();
    await page.waitForTimeout(2500);
    return sceneId;
  }
  throw new Error('no enabled scene checkbox');
}

async function selectGrid(page, datasetNode) {
  const select = datasetNode.locator('[data-testid^="dataset-grid-"]:not([data-testid^="dataset-grid-level-"])').first();
  await select.click();
  await page.waitForTimeout(900);
  const dropdown = page.locator('.el-select-dropdown:visible');
  const options = await dropdown.locator('.el-select-dropdown__item').allTextContents();
  const option = dropdown.locator('.el-select-dropdown__item').filter({ hasText: GRID.option }).first();
  if (!(await option.count())) throw new Error(`grid ${GRID.key} not among ${JSON.stringify(options)}`);
  await option.click();
  await page.waitForTimeout(1500);
}

async function unlockLevelIfNeeded(page, datasetNode) {
  const unlock = datasetNode.locator('[data-testid^="unlock-grid-level-"]').first();
  if (await unlock.count()) {
    await unlock.click();
    await page.waitForTimeout(1500);
  }
}

async function setLevel(page, datasetNode, levelLabel) {
  const select = datasetNode.locator('[data-testid^="dataset-grid-level-"]').first();
  await select.click();
  await page.waitForTimeout(900);
  const dropdown = page.locator('.el-select-dropdown:visible');
  const options = await dropdown.locator('.el-select-dropdown__item').allTextContents();
  const option = dropdown.locator('.el-select-dropdown__item').filter({ hasText: new RegExp(`^\\s*${levelLabel}\\s*$`) }).first();
  if (!(await option.count())) throw new Error(`level ${levelLabel} not among ${JSON.stringify(options)}`);
  await option.click();
  await page.waitForTimeout(1200);
}

async function closeDrawer(page) {
  const close = page.locator('.el-drawer__close-btn').first();
  if (await close.count()) await close.click();
  else await page.keyboard.press('Escape');
  await page.waitForTimeout(2000);
}

async function configureAndSubmit(page, levelLabel, captured) {
  await openPartition(page);
  await openQueueDrawer(page);
  const { batchId, datasetNode } = await prepareTarget(page);
  const sceneId = await selectScene(page, SCENE);
  await selectGrid(page, datasetNode);
  await unlockLevelIfNeeded(page, datasetNode);
  await setLevel(page, datasetNode, levelLabel);
  await shot(page, `merge-ingest-configured-${levelLabel.replace(/\s+/g, '')}`);
  await closeDrawer(page);
  const submit = page.getByRole('button', { name: '提交剖分' });
  await waitFor(async () => !(await submit.isDisabled()), { timeout: 30000, label: 'submit button enabled' });
  const before = captured.length;
  await submit.click();
  await waitFor(() => captured.length > before, { timeout: 30000, label: 'POST /v1/partition/runs' });
  const post = captured[captured.length - 1];
  const body = JSON.parse(post.response || '{}');
  if (post.status !== 202 || !body.partition_run_id) {
    throw new Error(`submit failed: HTTP ${post.status} ${String(post.response).slice(0, 300)}`);
  }
  return { body, request: JSON.parse(post.request || '{}'), batchId, sceneId };
}

async function waitRun(page, runId, { expectStatus = 'completed' } = {}) {
  const deadline = Date.now() + RUN_DEADLINE_MS;
  let counts = null;
  while (Date.now() < deadline) {
    counts = dbRunCounts(runId);
    if (['completed', 'failed', 'cancelled', 'manual_required'].includes(counts.status)) break;
    await page.waitForTimeout(5000);
  }
  if (!counts || !['completed', 'failed', 'cancelled', 'manual_required'].includes(counts.status)) {
    throw new Error(`run ${runId} not terminal within ${RUN_DEADLINE_MS}ms (status=${counts?.status})`);
  }
  if (expectStatus && counts.status !== expectStatus) {
    throw new Error(`run ${runId} ended ${counts.status}: ${JSON.stringify(counts)}`);
  }
  return counts;
}

async function waitIngestCompleted(page, runId, token) {
  const deadline = Date.now() + INGEST_DEADLINE_MS;
  let mine = [];
  while (Date.now() < deadline) {
    const listed = await apiJson(`/v1/ingest-runs?keyword=${encodeURIComponent(runId)}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const items = listed.body?.items || listed.body?.records || [];
    mine = items.filter((item) => String(item.partition_run_id) === String(runId));
    if (mine.some((item) => item.status === 'completed')) return mine;
    if (mine.some((item) => ['failed', 'cancelled'].includes(item.status))) {
      throw new Error(`ingest run ended ${mine[0].status}: ${JSON.stringify(mine[0]).slice(0, 300)}`);
    }
    await page.waitForTimeout(5000);
  }
  throw new Error(`ingest for ${runId} not completed within ${INGEST_DEADLINE_MS}ms`);
}

async function main() {
  initOut();
  const { browser, context, page } = await launchReal();
  await context.tracing.start({ screenshots: true, snapshots: true, sources: true }).catch(() => {});
  state.traceContext = context;
  const captured = [];
  page.on('response', async (response) => {
    if (response.request().method() === 'POST' && response.url().includes('/v1/partition/runs')) {
      captured.push({ status: response.status(), request: response.request().postData() || '', response: await response.text().catch(() => '') });
    }
  });

  try {
    await login(page, { entry: `${APP}/partition` });
    await record('me-0', 'real portal login', 'PASS', 'portal login form -> authenticated app session');

    let runId = null;
    let datasetId = null;
    let bandUnitId = null;
    let firstAttempt = 0;
    await scenario('me-1', 'partition submit through real clicks (mgrs L0; an open record is reused)', async () => {
      const submitted = await configureAndSubmit(page, LEVEL_A, captured);
      runId = submitted.body.partition_run_id;
      firstAttempt = Number(submitted.body.attempt_no) || 1;
      datasetId = submitted.request.datasets?.[0]?.dataset_id || null;
      bandUnitId = submitted.request.datasets?.[0]?.band_unit_ids?.[0] || null;
      state.ledger.runs.push({ run_id: runId, task_id: submitted.body.task_id, grid: GRID.key, level: LEVEL_A, batch: submitted.batchId, scene: submitted.sceneId, source: 'neat merge_ingest.mjs', created_at: new Date().toISOString() });
      persist();
      const counts = await waitRun(page, runId, { expectStatus: 'completed' });
      await shot(page, 'merge-ingest-me-1-completed');
      return `run=${runId} merged=${submitted.body.merged === true} attempt_no=${firstAttempt} dataset=${datasetId} scene=${submitted.sceneId} status=completed grid_cells=${counts.partition_grid_cells} submission_count=${counts.submission_count}`;
    }, { stopOnFail: true });

    await scenario('me-2', 'same data with another level merges into the same record', async () => {
      const submitted = await configureAndSubmit(page, LEVEL_B, captured);
      if (submitted.body.partition_run_id !== runId) throw new Error(`expected merge into ${runId}, got ${submitted.body.partition_run_id}`);
      if (submitted.body.merged !== true) throw new Error(`expected merged=true: ${JSON.stringify(submitted.body).slice(0, 200)}`);
      const expectedAttempt = firstAttempt + 1;
      if (Number(submitted.body.attempt_no) !== expectedAttempt) throw new Error(`expected attempt_no=${expectedAttempt}, got ${submitted.body.attempt_no}`);
      const counts = await waitRun(page, runId, { expectStatus: 'completed' });
      if (Number(counts.submission_count) < expectedAttempt) throw new Error(`DB submission_count=${counts.submission_count}, expected >= ${expectedAttempt}`);
      await shot(page, 'merge-ingest-me-2-merged');
      return `merged into ${runId}; attempt_no=${submitted.body.attempt_no}; submission_count=${counts.submission_count}; merge_state=${counts.merge_state}`;
    }, { stopOnFail: true });

    await scenario('me-3', 'record detail shows both submissions and the level difference', async () => {
      await page.goto(`${APP}/quality`, { waitUntil: 'domcontentloaded', timeout: 60000 });
      await page.waitForTimeout(5000);
      const detailButton = page.getByTestId(`quality-task-detail-${runId}`);
      await waitFor(async () => (await detailButton.count()) > 0, { timeout: 60000, label: `quality row ${runId}` });
      await detailButton.first().click();
      await page.waitForTimeout(4000);
      const summary = page.locator('[data-testid="partition-submissions"] summary');
      await waitFor(async () => (await summary.count()) > 0, { timeout: 30000, label: 'submission history' });
      await summary.click();
      await page.waitForTimeout(1000);
      const historyText = (await page.locator('[data-testid="partition-submissions"]').innerText()).replace(/\s+/g, ' ');
      const lastAttempt = firstAttempt + 1;
      const changesLocator = page.locator(`[data-testid="partition-submission-changes-${lastAttempt}"]`);
      await waitFor(async () => (await changesLocator.count()) > 0, { timeout: 15000, label: `changes for attempt ${lastAttempt}` });
      const changesText = (await changesLocator.innerText()).replace(/\s+/g, ' ');
      await shot(page, 'merge-ingest-me-3-history');
      const historyCount = Number((historyText.match(/剖分提交历史\s*(\d+)\s*次/) || [])[1] || 0);
      if (historyCount < lastAttempt) throw new Error(`expected >= ${lastAttempt} submissions: ${historyText.slice(0, 300)}`);
      if (!changesText.includes('格网层级：0 → 1')) throw new Error(`expected level diff: ${changesText}`);
      if (datasetId && changesText.includes(datasetId)) throw new Error(`dataset id leaked into change labels: ${changesText}`);
      await page.keyboard.press('Escape');
      await page.waitForTimeout(1500);
      return `history shows ${historyCount} submissions; attempt ${lastAttempt} changes="${changesText}"`;
    }, { stopOnFail: true });

    await scenario('me-4', 'manual ingest through 数据管理与入库 -> 数据入库 clicks', async () => {
      await page.goto(`${APP}/partition`, { waitUntil: 'domcontentloaded', timeout: 60000 });
      await page.waitForTimeout(3000);
      await page.getByTestId('partition-module-ingest').click();
      await page.waitForTimeout(5000);
      await page.getByRole('tab', { name: '数据入库' }).first().click();
      await page.waitForTimeout(6000);
      const row = page.locator('.pending-ingest-row', { hasText: runId }).first();
      const deadline = Date.now() + RUN_DEADLINE_MS;
      while (Date.now() < deadline) {
        if (await row.count()) break;
        await page.getByRole('button', { name: '刷新' }).first().click().catch(() => {});
        await page.waitForTimeout(8000);
      }
      if (!(await row.count())) throw new Error(`pending ingest collection for ${runId} never appeared (quality gate)`);
      await shot(page, 'merge-ingest-me-4-collection');
      await row.getByRole('button', { name: '选择数据入库' }).click();
      const dialog = page.locator('.el-dialog:visible').first();
      await waitFor(async () => (await dialog.count()) > 0 && (await dialog.innerText()).includes('手动入库'), { timeout: 30000, label: 'manual ingest dialog' });
      const selectAll = dialog.locator('[data-testid^="manual-select-dataset-"]').first();
      await waitFor(async () => (await selectAll.count()) > 0, { timeout: 30000, label: 'manual dataset checkbox' });
      await selectAll.locator('.el-checkbox__input').click();
      await page.waitForTimeout(1500);
      await shot(page, 'merge-ingest-me-4-selection');
      await dialog.getByRole('button', { name: '提交入库' }).click();
      await waitFor(() => dbRunCounts(runId).ingest_runs >= 1, { timeout: 60000, label: 'ingest run row in DB' });
      const sealed = dbRunCounts(runId);
      if (sealed.merge_state !== 'closed') throw new Error(`record not sealed after ingest: merge_state=${sealed.merge_state}`);
      const token = await page.evaluate(() => localStorage.getItem('access_token') || localStorage.getItem('token') || '');
      const completed = await waitIngestCompleted(page, runId, token);
      await page.locator('.el-segmented__item', { hasText: '入库记录' }).first().click().catch(async () => {
        await page.getByText('入库记录', { exact: true }).first().click();
      });
      await page.waitForTimeout(4000);
      const historyRow = page.locator('tr', { hasText: runId }).first();
      await waitFor(async () => (await historyRow.count()) > 0, { timeout: 30000, label: 'ingest history row' });
      await shot(page, 'merge-ingest-me-4-history');
      return `ingest runs completed=${completed.length}; record merge_state=closed; ingest_run=${completed[0]?.ingest_run_id}`;
    }, { stopOnFail: true });

    await scenario('me-5', 'after ingest the pending list no longer offers the consumed band', async () => {
      await openPartition(page);
      await openQueueDrawer(page);
      const card = page.locator(`[data-testid="load-batch-${BATCH}"]`);
      await page.waitForTimeout(3000);
      const stillListed = await card.count();
      await shot(page, 'merge-ingest-me-5-pending-after-ingest');
      if (stillListed) throw new Error(`batch ${BATCH} still offered as pending for an ingested band (band=${bandUnitId})`);
      await page.keyboard.press('Escape');
      return `batch ${BATCH} removed from the pending list after ingest (band=${bandUnitId})`;
    });
  } finally {
    try { await context.tracing.stop(); } catch { /* */ }
    persist();
    await browser.close();
  }
  const failed = state.results.filter((result) => result.status === 'FAIL');
  console.log(`\n=== merge/ingest click-through: ${state.results.filter((r) => r.status === 'PASS').length} PASS / ${failed.length} FAIL / ${state.results.length} records`);
  process.exitCode = failed.length ? 1 : 0;
}

await main();
