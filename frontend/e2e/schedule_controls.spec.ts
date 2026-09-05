/**
 * E2E Test: Scheduler UI (Pipeline Schedules)
 * Covers: create schedule in UI, verify it appears, trigger now, verify scheduled execution exists, delete schedule.
 */

import { test, expect, type Dialog, type Page, type Response } from '@playwright/test'

const API_URL = process.env.API_URL || 'http://localhost:5001'
const DEV_USER_ID = process.env.E2E_USER_ID || '00000000-0000-0000-0000-000000000000'
const API_HEADERS = { 'X-User-ID': DEV_USER_ID }

async function ensureLoggedIn(page: Page) {
  await page.goto('/dashboard')
  const onLogin = await page.locator('text=Welcome to Rsync').isVisible().catch(() => false)
  if (onLogin) {
    const useBtn = page.locator('button:has-text("Use")')
    if (await useBtn.isVisible().catch(() => false)) await useBtn.click()
    await page.locator('button:has-text("Sign in")').click()
  }
  await expect(page.locator('a:has-text("Agentic Data Pipeline")')).toBeVisible({ timeout: 30000 })
}

type Execution = { id?: string; trigger_source?: string; schedule_id?: string } & Record<string, unknown>
type Schedule = {
  schedule_id?: string
  schedule_spec?: { timezone?: string } & Record<string, unknown>
} & Record<string, unknown>

async function waitForScheduledExecution(
  page: Page,
  pipelineId: string,
  scheduleId: string,
  timeoutMs = 60000
): Promise<string | null> {
  const start = Date.now()
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const r = await page.request.get(`${API_URL}/api/v1/executions?pipeline_id=${pipelineId}&limit=20`, { headers: API_HEADERS })
    if (r.ok()) {
      const j = await r.json()
      const exec = ((j.executions || []) as Execution[]).find(
        (e) => e.trigger_source === 'scheduled' && e.schedule_id === scheduleId
      )
      if (exec?.id) return exec.id
    }
    if (Date.now() - start > timeoutMs) return null
    await new Promise((res) => setTimeout(res, 1500))
  }
}

async function waitForSchedule(page: Page, pipelineId: string, timeoutMs = 30000): Promise<Schedule | null> {
  const start = Date.now()
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const r = await page.request.get(`${API_URL}/api/v1/pipelines/${pipelineId}/schedules`, { headers: API_HEADERS })
    if (r.ok()) {
      const j = await r.json().catch(() => null)
      const schedule = (j?.schedules?.[0] || null) as Schedule | null
      if (schedule?.schedule_id) return schedule
    }
    if (Date.now() - start > timeoutMs) return null
    await new Promise((res) => setTimeout(res, 1000))
  }
}

test.describe('RSYNC AI - Scheduler UI', () => {
  test.beforeEach(async ({ page }) => {
    await ensureLoggedIn(page)
  })

  test('CDC pipeline must not show scheduler UI; schedule API must reject', async ({ page }) => {
    // Create a CDC pipeline (pipeline-level override).
    const createPipelineResp = await page.request.post(`${API_URL}/api/v1/pipelines`, {
      headers: { ...API_HEADERS, 'Content-Type': 'application/json' },
      data: {
        name: `E2E CDC Pipeline ${Date.now()}`,
        description: 'CDC pipeline created by Playwright scheduler gating test',
        request: 'replicate changes in real time (cdc) every hour',
        sync_mode: 'cdc',
        cdc_mode: 'streaming_only',
        sync_mode_source: 'user_manual',
      },
    })
    expect(createPipelineResp.ok()).toBeTruthy()
    const createdPipeline = await createPipelineResp.json()
    const pipelineId = createdPipeline?.id
    test.skip(!pipelineId, 'Failed to create CDC pipeline for E2E')

    // Navigate to pipeline detail
    await page.goto(`/pipelines/${pipelineId}`)

    // CDC pipelines should not render the schedules panel at all.
    await expect(page.locator('text=Pipeline Schedules')).toHaveCount(0)
    await expect(page.locator('button:has-text("Create Schedule")')).toHaveCount(0)

    // API must also reject schedules for CDC pipelines (server-side enforcement).
    const createScheduleResp = await page.request.post(`${API_URL}/api/v1/pipelines/${pipelineId}/schedules`, {
      headers: { ...API_HEADERS, 'Content-Type': 'application/json' },
      data: { schedule_type: 'interval', schedule_spec: { every_seconds: 60, timezone: 'UTC' } },
    })
    expect([400, 403].includes(createScheduleResp.status())).toBeTruthy()
    if (createScheduleResp.status() === 400) {
      const j = await createScheduleResp.json()
      expect(String(j?.error || '')).toContain('schedules_not_supported_for_cdc')
    }
  })

  test('Batch pipeline auto-schedules from NL (every hour)', async ({ page }) => {
    // Create a batch pipeline whose NL includes a schedule intent.
    const createPipelineResp = await page.request.post(`${API_URL}/api/v1/pipelines`, {
      headers: { ...API_HEADERS, 'Content-Type': 'application/json' },
      data: {
        name: `E2E AutoSchedule Pipeline ${Date.now()}`,
        description: 'Batch pipeline created by Playwright auto-schedule test',
        request: 'sync some data every hour',
        sync_mode: 'batch',
        sync_mode_source: 'user_manual',
      },
    })
    expect(createPipelineResp.ok()).toBeTruthy()
    const createdPipeline = await createPipelineResp.json()
    const pipelineId = createdPipeline?.id
    test.skip(!pipelineId, 'Failed to create batch pipeline for auto-schedule E2E')

    // Poll schedules endpoint; auto-schedule should appear quickly.
    const start = Date.now()
    let scheduleId: string | null = null
    while (Date.now() - start < 30000) {
      const r = await page.request.get(`${API_URL}/api/v1/pipelines/${pipelineId}/schedules`, { headers: API_HEADERS })
      if (r.ok()) {
        const j = await r.json()
        const s = j?.schedules?.[0]
        if (s?.schedule_id) {
          scheduleId = s.schedule_id
          break
        }
      }
      await new Promise((res) => setTimeout(res, 1000))
    }
    expect(scheduleId).toBeTruthy()
  })

  test('Create schedule → trigger now → verify scheduled execution → delete schedule', async ({ page }) => {
    // Create a fresh pipeline for this test to avoid interference from prior scheduled executions.
    const createPipelineResp = await page.request.post(`${API_URL}/api/v1/pipelines`, {
      headers: { ...API_HEADERS, 'Content-Type': 'application/json' },
      data: {
        name: `E2E Schedule Pipeline ${Date.now()}`,
        description: 'Pipeline created by Playwright scheduler E2E',
        request: 'sync some data for scheduler e2e',
      },
    })
    expect(createPipelineResp.ok()).toBeTruthy()
    const createdPipeline = await createPipelineResp.json()
    const pipelineId = createdPipeline?.id
    test.skip(!pipelineId, 'Failed to create pipeline for E2E')

    // Ensure no schedules before starting (best-effort cleanup)
    const existing = await page.request.get(`${API_URL}/api/v1/pipelines/${pipelineId}/schedules`, { headers: API_HEADERS })
    if (existing.ok()) {
      const j = await existing.json()
      const schedules = j.schedules || []
      for (const s of schedules) {
        await page.request.delete(`${API_URL}/api/v1/pipelines/${pipelineId}/schedules/${s.schedule_id}`, { headers: API_HEADERS })
      }
    }

    // Navigate to pipeline detail
    await page.goto(`/pipelines/${pipelineId}`)

    // Open Create Schedule dialog
    await page.locator('button:has-text("Create Schedule")').first().click()
    await expect(page.locator('text=Configure a recurring schedule for this pipeline')).toBeVisible({ timeout: 30000 })
    await expect(page.locator('input[placeholder="3600"]')).toBeVisible({ timeout: 30000 })
    await expect(page.locator('input[list="rsync-timezones"]')).toBeVisible({ timeout: 30000 })

    // Set interval 60s, timezone Asia/Kolkata (tests full timezone support)
    await page.locator('input[placeholder="3600"]').fill('60')
    const tzInput = page.locator('input[list="rsync-timezones"]').first()
    await tzInput.fill('Asia/Kolkata')

    // Create
    await page.locator('button:has-text("Create Schedule")').last().click()

    // Verify schedule created via API (authoritative), then assert matching schedule card renders in UI.
    const createdSchedule = await waitForSchedule(page, pipelineId, 30000)
    expect(createdSchedule?.schedule_id).toBeTruthy()
    const createdTimezone = createdSchedule?.schedule_spec?.timezone || 'UTC'
    const createdScheduleId = createdSchedule?.schedule_id
    if (!createdScheduleId) throw new Error('Expected created schedule to have a schedule_id')

    await expect(page.locator('text=Pipeline Schedules')).toBeVisible()
    await expect(page.locator(`[data-schedule-id="${createdScheduleId}"]`)).toBeVisible({ timeout: 30000 })
    await expect(page.locator(`[data-schedule-id="${createdScheduleId}"]`)).toContainText('Timezone:', { timeout: 30000 })

    // Trigger now from the schedule card (play icon button)
    // We click the first icon-only button in the schedule actions cluster by using the title attribute.
    await page.locator('button[title="Trigger now"]').first().click()

    // Verify via API that a scheduled execution was created
    const scheduledExecId = await waitForScheduledExecution(page, pipelineId, createdScheduleId, 120000)
    expect(scheduledExecId).toBeTruthy()

    // Go to Executions page; verify Scheduled badge title contains timezone, then click to deep-link back to schedule
    await page.goto('/executions')
    const shortId = (scheduledExecId as string).slice(0, 8)
    const search = page.locator('input[placeholder*="Search by pipeline name or execution ID"]')
    await expect(search).toBeVisible({ timeout: 30000 })
    await search.fill(shortId)
    await expect(page.locator(`text=#${shortId}`).first()).toBeVisible({ timeout: 30000 })

    const scheduledBadgeLink = page.locator('a:has-text("Scheduled")').first()
    await expect(scheduledBadgeLink).toBeVisible({ timeout: 30000 })

    // Sanity: the scheduled badge deep-link should point at the created schedule id
    const href = await scheduledBadgeLink.getAttribute('href')
    expect(href).toContain(`scheduleId=${createdScheduleId}`)

    // Hover to trigger lazy schedule fetch; assert the network call succeeds and then title includes timezone.
    const schedulesPath = `/api/v1/pipelines/${pipelineId}/schedules`
    const respPromise = page.waitForResponse(
      (r: Response) => r.request().method() === 'GET' && r.url().includes(schedulesPath),
      { timeout: 30000 }
    )
    await scheduledBadgeLink.hover()
    const resp = await respPromise
    expect(resp.status()).toBe(200)

    // Wait for title to eventually include timezone (async state update)
    await expect.poll(async () => (await scheduledBadgeLink.getAttribute('title')) || '', {
      timeout: 30000,
    }).toContain(`Timezone: ${createdTimezone}`)

    // Click the Scheduled badge (it should deep-link)
    await scheduledBadgeLink.click()

    // Assert we landed on pipeline page schedules section
    await expect(page).toHaveURL(/\/pipelines\//)
    await expect(page).toHaveURL(/scheduleId=/)
    await expect(page.locator('text=Pipeline Schedules')).toBeVisible({ timeout: 30000 })

    // Delete schedule from UI (trash icon button)
    page.once('dialog', (d: Dialog) => d.accept())
    await page.locator('button[title="Delete"]').first().click()

    // Verify schedule removed
    await expect(page.locator('text=No schedules configured')).toBeVisible({ timeout: 30000 })
  })
})


