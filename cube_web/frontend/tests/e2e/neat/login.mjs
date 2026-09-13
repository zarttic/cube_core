// Real-stack login: drives the portal login form (native <select> for 登录角色) like a human,
// then follows the OAuth redirect chain back to the app. No credential is stored in this file.
import { chromium } from '@playwright/test';
import fs from 'node:fs';
import path from 'node:path';

export const PORTAL = process.env.NEAT_PORTAL || 'http://10.3.100.182:5177';
export const APP = process.env.NEAT_APP || 'http://10.3.100.179:50040';
export const OUT = process.env.NEAT_OUT || `/tmp/e2e-neat-b-${Date.now()}`;
export const shotsDir = path.join(OUT, 'screenshots');

export async function login(page, { entry = `${APP}/partition` } = {}) {
  const password = process.env.NEAT_ADMIN_PASSWORD;
  const username = process.env.NEAT_ADMIN_USER || 'admin';
  const role = process.env.NEAT_ADMIN_ROLE || '管理员';
  if (!password) throw new Error('NEAT_ADMIN_PASSWORD not set in process env');
  const steps = [];
  const shot = async (name) => { await page.screenshot({ path: path.join(shotsDir, name) }); return name; };

  // 1. Enter the app like a user; the app bounces to the portal login with OAuth params.
  await page.goto(entry, { waitUntil: 'domcontentloaded', timeout: 60000 });
  await page.waitForTimeout(6000);
  steps.push(`entry=${page.url().slice(0, 120)} shot=${await shot('login-01-bounced.png')}`);

  // 2. Username.
  const userField = page.getByPlaceholder(/用户名|user|account/i).or(page.locator('input:not([type=password]):visible').first());
  await userField.first().click();
  await userField.first().fill(username);
  steps.push(`username=${username} shot=${await shot('login-02-username.png')}`);

  // 3. Password.
  const passField = page.getByPlaceholder(/密码|password/i).or(page.locator('input[type=password]'));
  await passField.first().click();
  await passField.first().fill(password);
  steps.push(`password filled shot=${await shot('login-03-password.png')}`);

  // 4. Role: native <select> — click it, then pick the option by label.
  const roleSelect = page.locator('select').first();
  if (!(await roleSelect.count())) throw new Error('role <select> not found on login form');
  await roleSelect.click();
  await page.waitForTimeout(400);
  await roleSelect.selectOption({ label: role });
  const roleValue = await roleSelect.inputValue();
  steps.push(`role=${roleValue} shot=${await shot('login-04-role.png')}`);
  if (roleValue !== role) throw new Error(`role select did not apply (got ${roleValue})`);

  // 5. Submit.
  const loginPost = page.waitForResponse((r) => r.url().includes('/api/login') && r.request().method() === 'POST', { timeout: 30000 });
  const submit = page.getByRole('button', { name: /登录系统|登录|登 录|login/i }).first();
  await submit.click();
  steps.push(`submit=click shot=${await shot('login-05-submitted.png')}`);
  const postResponse = await loginPost.catch(() => null);
  steps.push(`POST /api/login -> ${postResponse ? postResponse.status() : 'no-response'}`);
  if (postResponse && postResponse.status() !== 200) {
    const body = await postResponse.text().catch(() => '');
    throw new Error(`login rejected: ${postResponse.status()} ${body.slice(0, 200)}`);
  }

  // 6. Follow OAuth redirect chain: portal -> /api/authorize -> app /callback -> app page.
  await page.waitForURL(/\/callback|\/partition|\/encoding|\/$/, { timeout: 45000 }).catch(() => {});
  await page.waitForTimeout(5000);
  steps.push(`redirected=${page.url().slice(0, 120)} shot=${await shot('login-06-redirect.png')}`);

  if (page.url().includes('/callback')) {
    await page.waitForTimeout(6000);
    steps.push(`after-callback=${page.url().slice(0, 120)} shot=${await shot('login-07-after-callback.png')}`);
  }

  // 7. Assert authenticated.
  const token = await page.evaluate(() => localStorage.getItem('access_token') || localStorage.getItem('token') || '');
  const keys = await page.evaluate(() => Object.keys(localStorage));
  await shot('login-08-authenticated.png');
  steps.push(`app_url=${page.url().slice(0, 120)} storage_keys=${keys.join(',')} token_len=${token.length}`);
  if (!token) throw new Error(`no access_token after login; steps=${steps.join(' | ')}`);
  return { token, steps, role: roleValue, username };
}

export async function launchReal() {
  fs.mkdirSync(shotsDir, { recursive: true });
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ viewport: { width: 1680, height: 1050 }, ignoreHTTPSErrors: true });
  const page = await context.newPage();
  return { browser, context, page };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const { browser, page } = await launchReal();
  try {
    const result = await login(page);
    console.log(`LOGIN OK token_len=${result.token.length} role=${result.role}`);
    console.log(result.steps.join('\n'));
  } catch (error) {
    console.log(`LOGIN FAILED: ${error.message}`);
    fs.writeFileSync(path.join(OUT, 'login-failure.json'), JSON.stringify({ error: error.message, url: page.url() }, null, 2));
    await page.screenshot({ path: path.join(shotsDir, 'login-failure.png') }).catch(() => {});
  } finally {
    await browser.close();
  }
}
