import { test, expect, request as apiRequest, type Page } from '@playwright/test'

/**
 * E2E — Consumption / usage UI (Ship 1).
 *
 * Exercises the two new pages against the live api-gateway + frontend:
 *   - /usage        (per-workspace: plan meter + transfer tiles + per-pipeline table)
 *   - /admin/usage  (platform: by-workspace / by-user consumption rollups)
 *
 * Auth: log in via the api-gateway (real session token), then drop it as the
 * `auth_token` cookie with the gateway's own attributes (host-scoped 'localhost',
 * HttpOnly, Lax) so it reaches BOTH the :3000 middleware presence check and the
 * :5001 gateway session validation. Pin the active workspace via localStorage so
 * authFetch sends X-Workspace-ID.
 *
 * Prereqs (must be up): api-gateway on :5001 with the /usage + /admin/usage routes,
 * postgres with pipeline_run_table_stats, frontend on :3000. Creds + workspace are
 * env-overridable for CI.
 */

const HOST_GATEWAY = process.env.USAGE_E2E_GATEWAY || 'http://localhost:5001'
const EMAIL = process.env.USAGE_E2E_EMAIL || 'default@rsync-ai.local'
const PASSWORD = process.env.USAGE_E2E_PASSWORD || 'password123'
const WORKSPACE_ID = process.env.USAGE_E2E_WS || 'd43bccb5-4ad7-431b-8ce2-649184fc8a71'

let token = process.env.USAGE_E2E_TOKEN || ''

test.beforeAll(async () => {
  // Prefer a pre-supplied session token (CI seeds one) to avoid the auth-login
  // rate limiter; otherwise log in with the dev creds.
  if (token) return
  const ctx = await apiRequest.newContext({ baseURL: HOST_GATEWAY })
  const res = await ctx.post('/api/v1/auth/login', { data: { email: EMAIL, password: PASSWORD } })
  if (!res.ok()) throw new Error(`login failed: ${res.status()} ${await res.text()}`)
  token = (await res.json()).token
  await ctx.dispose()
})

async function authenticate(page: Page) {
  await page.context().addCookies([
    { name: 'auth_token', value: token, domain: 'localhost', path: '/', httpOnly: true, sameSite: 'Lax' },
  ])
  await page.addInitScript((ws) => {
    localStorage.setItem('rsync_active_workspace_id', ws)
  }, WORKSPACE_ID)
}

test.describe('Consumption / usage UI', () => {
  test('workspace usage page renders plan meter, transfer tiles, per-pipeline table', async ({ page }) => {
    await authenticate(page)
    await page.goto('/usage', { waitUntil: 'domcontentloaded' })

    await expect(page.getByRole('heading', { name: /^Usage$/i }).first()).toBeVisible({ timeout: 30000 })
    await expect(page.getByText(/Current plan/i).first()).toBeVisible({ timeout: 20000 })
    await expect(page.getByText(/Records processed/i).first()).toBeVisible()
    await expect(page.getByText(/Data transfer \(this month\)/i).first()).toBeVisible()
    await expect(page.getByText(/NL→SQL queries \(this month\)/i).first()).toBeVisible()
    await expect(page.getByText(/Per-pipeline transfer/i).first()).toBeVisible()

    await page.screenshot({ path: 'e2e/__screens__/usage-workspace.png', fullPage: true })
  })

  test('admin usage page renders workspace + user rollups', async ({ page }) => {
    await authenticate(page)
    await page.goto('/admin/usage', { waitUntil: 'domcontentloaded' })

    await expect(page.getByRole('heading', { name: /Platform admin/i }).first()).toBeVisible({ timeout: 30000 })
    await expect(page.getByRole('button', { name: /By workspace/i }).first()).toBeVisible({ timeout: 20000 })
    await expect(page.getByRole('button', { name: /By user/i }).first()).toBeVisible()

    await page.screenshot({ path: 'e2e/__screens__/usage-admin-workspaces.png', fullPage: true })

    await page.getByRole('button', { name: /By user/i }).first().click()
    await expect(page.getByText(/@/).first()).toBeVisible({ timeout: 10000 })
    await page.screenshot({ path: 'e2e/__screens__/usage-admin-users.png', fullPage: true })
  })
})
