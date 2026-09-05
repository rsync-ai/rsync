/**
 * E2E Test: Execution + Pipeline Controls
 * Covers: cancel execution, refresh list, pause/resume/stop from pipeline detail (regular ETL pipelines)
 *
 * Assumes services are already running:
 * - Frontend: http://localhost:3000
 * - API Gateway: http://localhost:5001
 */

import { test, expect, type Page } from '@playwright/test'

const API_URL = process.env.API_URL || 'http://localhost:5001'
const DEV_USER_ID = process.env.E2E_USER_ID || '00000000-0000-0000-0000-000000000000'

async function ensureLoggedIn(page: Page) {
  await page.goto('/login')
  await page.waitForLoadState('networkidle')
  const useBtn = page.locator('button:has-text("Use")')
  if (await useBtn.isVisible({ timeout: 2000 }).catch(() => false)) {
    await useBtn.click()
  } else {
    await page.getByLabel(/Email/i).fill('default@rsync-ai.local')
    await page.getByLabel(/Password/i).fill('password123')
  }
  await page.getByRole('button', { name: /Sign in/i }).click()
  await page.waitForURL((url) => !url.pathname.includes('/login'), { timeout: 15000 })

  // Confirm we got into the app shell (sidebar link exists)
  await expect(page.locator('a:has-text("Agentic Data Pipeline")')).toBeVisible({ timeout: 30000 })

  // Ensure user_id exists for client-side API calls (avoids relying on /auth/me).
  const existingUserId = await page.evaluate(() => localStorage.getItem('user_id') || '')
  if (!existingUserId) {
    await page.evaluate((uid) => localStorage.setItem('user_id', uid), DEV_USER_ID)
  }
}

async function waitForStatus<T extends { status?: string }>(
  fetchFn: () => Promise<T>,
  desired: string,
  timeoutMs = 30000
): Promise<T> {
  const start = Date.now()
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const obj = await fetchFn()
    if ((obj.status || '').toLowerCase() === desired.toLowerCase()) return obj
    if (Date.now() - start > timeoutMs) return obj
    await new Promise((r) => setTimeout(r, 1000))
  }
}

async function waitForExecutionExists(page: Page, executionId: string, timeoutMs = 30000): Promise<void> {
  const start = Date.now()
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const r = await page.request.get(`${API_URL}/api/v1/executions/${executionId}`)
    if (r.ok()) return
    if (Date.now() - start > timeoutMs) return
    await new Promise((res) => setTimeout(res, 1000))
  }
}

async function getApiAuthHeaders(page: Page): Promise<Record<string, string>> {
  let userId = await page.evaluate(() => localStorage.getItem('user_id') || '')
  const token = await page.evaluate(() => localStorage.getItem('auth_token') || '')
  if (!userId) {
    userId = DEV_USER_ID
    await page.evaluate((uid) => localStorage.setItem('user_id', uid), userId)
  }
  const headers: Record<string, string> = {}
  if (userId) headers['X-User-ID'] = userId
  if (token) headers['Authorization'] = token
  return headers
}

test.describe('RSYNC AI - Execution Controls', () => {
  test.beforeEach(async ({ page }) => {
    await ensureLoggedIn(page)
  })

  test('Executions page: cancel works and status becomes cancelled', async ({ page }) => {
    const apiHeaders = await getApiAuthHeaders(page)

    // Create a unique pipeline so we can search by name reliably in the UI.
    const pipelineName = `E2E Exec Controls ${Date.now()}`
    const createResp = await page.request.post(`${API_URL}/api/v1/pipelines`, {
      headers: { ...apiHeaders, 'Content-Type': 'application/json' },
      data: { name: pipelineName, description: 'e2e execution controls', request: 'sync mysql to s3' },
    })
    expect(createResp.ok()).toBeTruthy()
    const created = await createResp.json().catch(() => null)
    const pipelineId = created?.id
    test.skip(!pipelineId, 'Failed to create pipeline for execution controls test')

    const runResp = await page.request.post(`${API_URL}/api/v1/pipelines/${pipelineId}/run`, { headers: apiHeaders })
    expect(runResp.ok()).toBeTruthy()
    const runJson = await runResp.json()
    const execId: string = runJson.execution_id
    expect(execId).toBeTruthy()

    // Ensure the execution is queryable before relying on UI list rendering.
    await waitForExecutionExists(page, execId, 30000)

    // Open executions list and search by pipeline name (stable UI anchor)
    await page.goto('/executions')
    const search = page.locator('input[placeholder*="Search by pipeline name or execution ID"]')
    await expect(search).toBeVisible({ timeout: 30000 })

    await search.fill(pipelineName)

    // Wait for the execution card to appear and cancel it
    const refreshBtn = page.getByRole('button', { name: /refresh executions/i })
    // Some environments need an explicit refresh to pull latest executions.
    for (let i = 0; i < 10; i++) {
      await refreshBtn.click().catch(() => {})
      if (await page.getByRole('link', { name: pipelineName }).first().isVisible().catch(() => false)) break
      await new Promise((r) => setTimeout(r, 1000))
    }
    await expect(page.getByRole('link', { name: pipelineName }).first()).toBeVisible({ timeout: 30000 })

    await page.locator('button:has-text("Cancel")').first().click()

    // Verify via API (source of truth)
    const execJson = await waitForStatus(
      async () => {
        const r = await page.request.get(`${API_URL}/api/v1/executions/${execId}`, { headers: apiHeaders })
        expect(r.ok()).toBeTruthy()
        return await r.json()
      },
      'cancelled',
      60000
    )
    expect(execJson.status).toBe('cancelled')

    // UI should now show Cancelled somewhere (best-effort; list refresh is async)
    await expect(page.locator('text=Cancelled').first()).toBeVisible({ timeout: 30000 })
  })

  test('Pipeline detail: pause → resume → stop (best-effort)', async ({ page }) => {
    // Pick an existing pipeline and start it via API so the UI shows controls
    const pipelinesResp = await page.request.get(`${API_URL}/api/v1/pipelines?limit=1`)
    expect(pipelinesResp.ok()).toBeTruthy()
    const pipelinesJson = await pipelinesResp.json()
    const pipelineId = pipelinesJson.pipelines?.[0]?.id
    test.skip(!pipelineId, 'No pipelines available to test controls')

    const runResp = await page.request.post(`${API_URL}/api/v1/pipelines/${pipelineId}/run`)
    expect(runResp.ok()).toBeTruthy()

    // Open pipeline detail
    await page.goto(`/pipelines/${pipelineId}`)
    await expect(page.locator('button:has-text("Pause")').first()).toBeVisible({ timeout: 30000 })

    // Pause
    await page.locator('button:has-text("Pause")').first().click()
    const pausedPipeline = await waitForStatus(
      async () => {
        const r = await page.request.get(`${API_URL}/api/v1/pipelines/${pipelineId}`)
        expect(r.ok()).toBeTruthy()
        return await r.json()
      },
      'paused',
      60000
    )
    expect(pausedPipeline.status).toBe('paused')
    await expect(page.locator('button:has-text("Resume")').first()).toBeVisible({ timeout: 30000 })

    // Resume
    await page.locator('button:has-text("Resume")').first().click()
    const runningPipeline = await waitForStatus(
      async () => {
        const r = await page.request.get(`${API_URL}/api/v1/pipelines/${pipelineId}`)
        expect(r.ok()).toBeTruthy()
        return await r.json()
      },
      'running',
      60000
    )
    expect(runningPipeline.status).toBe('running')
    await expect(page.locator('button:has-text("Pause")').first()).toBeVisible({ timeout: 30000 })

    // Stop
    await page.locator('button:has-text("Stop")').first().click()
    const stoppedPipeline = await waitForStatus(
      async () => {
        const r = await page.request.get(`${API_URL}/api/v1/pipelines/${pipelineId}`)
        expect(r.ok()).toBeTruthy()
        return await r.json()
      },
      'stopped',
      60000
    )
    expect(stoppedPipeline.status).toBe('stopped')
  })
})


