import { chromium } from '@playwright/test';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const PORTAL = process.env.NEAT_PORTAL || 'http://10.3.100.182:5177';
export const APP = process.env.NEAT_APP || 'http://10.3.100.179:50040';
export const API = process.env.NEAT_API || APP;
export const REPO = process.env.NEAT_REPO || '/home/lyajun/projects/cube_project';
export const OUT = process.env.NEAT_OUT || `/tmp/e2e-neat-b-${Date.now()}`;
export const shotsDir = path.join(OUT, 'screenshots');
export const tracesDir = path.join(OUT, 'traces');
export const DIR = path.dirname(fileURLToPath(import.meta.url));

export const DATASET_BATCH = 'ard-load-d42e8791e5194a88ba42b499274b29ff';
export const DATASET_ID = 'ard-optical-ard-load-d42e8791e5194a88ba42b499274b29ff';
export const FORBIDDEN_GRIDS = ['s2', 'tile_matrix', 'plane_grid'];

export const state = { results: [], ledger: { runs: [], keys: [], notes: [] }, traceContext: null };

function writeJson(name, value) { fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(value, null, 2)}\n`); }
function readJson(name, fallback) {
  try { return JSON.parse(fs.readFileSync(path.join(OUT, name), 'utf8')); } catch { return fallback; }
}
function mergeById(previous, current, key) {
  const merged = new Map();
  for (const item of [...(previous || []), ...(current || [])]) {
    if (!item || !item[key]) continue;
    const prior = merged.get(item[key]);
    if (!prior || String(item.at || '') >= String(prior.at || '')) merged.set(item[key], item);
  }
  return [...merged.values()];
}
function dedupeJson(items) {
  const seen = new Set();
  const result = [];
  for (const item of items || []) {
    let key;
    try { key = JSON.stringify(item); } catch { continue; }
    if (seen.has(key)) continue;
    seen.add(key);
    result.push(item);
  }
  return result;
}
export function persist() {
  const priorResults = readJson('results.json', { scenarios: [] });
  const priorLedger = readJson('ledger.json', { runs: [], notes: [], keys: [] });
  const scenarios = mergeById(priorResults.scenarios, state.results, 'id');
  const runs = mergeById(priorLedger.runs, state.ledger.runs, 'run_id');
  // notes/keys must be idempotent: prior + current, deduped, never re-appended on the next call.
  const notes = dedupeJson([...(priorLedger.notes || []), ...(state.ledger.notes || [])]);
  const keys = dedupeJson([...(priorLedger.keys || []), ...(state.ledger.keys || [])]);
  writeJson('results.json', { started_at: priorResults.started_at || state.startedAt, updated_at: new Date().toISOString(), scenarios });
  writeJson('ledger.json', { runs, notes, keys });
  state.results = scenarios;
  state.ledger = { runs, notes, keys };
}
export function initOut() {
  fs.mkdirSync(shotsDir, { recursive: true });
  fs.mkdirSync(tracesDir, { recursive: true });
  persist();
}
export async function record(id, name, status, detail, extra = {}) {
  const entry = { id, name, status, detail, at: new Date().toISOString(), ...extra };
  state.results = [...state.results.filter((r) => r.id !== id), entry];
  persist();
  fs.appendFileSync(path.join(OUT, 'run.log'), `${entry.at} [${status}] ${id} ${name} :: ${typeof detail === 'string' ? detail : JSON.stringify(detail)}\n`);
  console.log(`[${status}] ${id} ${name} :: ${typeof detail === 'string' ? detail : JSON.stringify(detail)}`);
}
export async function traceChunk(id, fn) {
  const context = state.traceContext;
  let tracePath = null;
  if (context) {
    try {
      fs.mkdirSync(tracesDir, { recursive: true });
      await context.tracing.startChunk({ title: String(id) });
      tracePath = path.join(tracesDir, `${id}.zip`);
    } catch (error) {
      tracePath = null;
      try { fs.appendFileSync(path.join(OUT, 'trace-errors.log'), `${new Date().toISOString()} startChunk ${id}: ${error.message}\n`); } catch { /* */ }
    }
  }
  try {
    return await fn();
  } finally {
    if (tracePath) {
      try { await context.tracing.stopChunk({ path: tracePath }); }
      catch (error) {
        try { fs.appendFileSync(path.join(OUT, 'trace-errors.log'), `${new Date().toISOString()} stopChunk ${id}: ${error.message}\n`); } catch { /* */ }
      }
    }
  }
}

export async function scenario(id, name, fn, opts = {}) {
  const started = Date.now();
  const meta = {};
  if (state.traceContext) meta.trace = `${id}.zip`;
  try {
    const detail = await traceChunk(id, fn);
    await record(id, name, 'PASS', detail || 'ok', { duration_ms: Date.now() - started, ...meta, ...opts.meta });
  } catch (error) {
    await record(id, name, 'FAIL', String(error.message || error).slice(0, 800), { duration_ms: Date.now() - started, ...meta, ...opts.meta });
    if (opts.stopOnFail) throw error;
  }
}
export async function skip(id, name, reason) { await record(id, name, 'SKIPPED', reason); }

export async function shot(page, name) {
  await page.screenshot({ path: path.join(shotsDir, `${name}.png`) });
  return `${name}.png`;
}
export async function waitFor(fn, { timeout = 30000, interval = 1000, label = 'condition' } = {}) {
  const deadline = Date.now() + timeout;
  let last;
  for (;;) {
    last = await fn();
    if (last) return last;
    if (Date.now() > deadline) throw new Error(`timeout ${timeout}ms waiting for ${label}`);
    await new Promise((r) => setTimeout(r, interval));
  }
}
export async function apiJson(pathname, options = {}) {
  const response = await fetch(`${API}${pathname}`, options);
  const text = await response.text();
  let body = null; try { body = JSON.parse(text); } catch { /* */ }
  return { status: response.status, body, text };
}
export function runDb(args, { timeout = 120000, script = 'db.py' } = {}) {
  const out = execFileSync('python3.11', [path.join(DIR, script), ...args], {
    cwd: REPO, timeout, encoding: 'utf8',
    env: { ...process.env, PYTHONPATH: 'cube_encoder:cube_split:cube_web' },
  });
  return out.trim();
}
export function dbCounts(tables) {
  const raw = runDb(['counts', ...tables]);
  return JSON.parse(raw.split('\n').filter(Boolean).pop());
}
export function dbRunCounts(runId) { return JSON.parse(runDb(['run', runId]).split('\n').filter(Boolean).pop()); }

export async function launchReal() {
  fs.mkdirSync(shotsDir, { recursive: true });
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ viewport: { width: 1680, height: 1050 }, ignoreHTTPSErrors: true });
  const page = await context.newPage();
  return { browser, context, page };
}
