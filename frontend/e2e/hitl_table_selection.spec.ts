/**
 * E2E Test: HITL Table Selection UI
 * Covers: pipeline detail shows "Select tables" CTA and table selector dialog when state is waiting_for_user/table_selection.
 *
 * Strategy: create a fresh pipeline, then stub the pipeline state endpoint to deterministically return available tables.
 * We also intercept the HITL resume endpoint to validate payload and return success.
 */

import { test, expect, type Page, type Route } from '@playwright/test'

const API_URL = process.env.API_URL || 'http://localhost:5001'
const DEV_USER_ID = process.env.E2E_USER_ID || '00000000-0000-0000-0000-000000000000'
const API_HEADERS = { 'X-User-ID': DEV_USER_ID }

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
  // Sidebar nav entry (label was updated as part of PRD alignment).
  await expect(page.locator('a:has-text("Agentic Data Pipeline")')).toBeVisible({ timeout: 30000 })
}

type HitlTablesPayload = {
  selected_tables?: string[]
} & Record<string, unknown>

test.describe('RSYNC AI - HITL Table Selection', () => {
  test.beforeEach(async ({ page }) => {
    await ensureLoggedIn(page)
  })

  test('Pipeline detail shows table selector and submits selected tables', async ({ page }) => {
    // Create a fresh pipeline for this test (no need to run it).
    const createPipelineResp = await page.request.post(`${API_URL}/api/v1/pipelines`, {
      headers: { ...API_HEADERS, 'Content-Type': 'application/json' },
      data: {
        name: `E2E HITL Tables ${Date.now()}`,
        description: 'Pipeline created by HITL table selection E2E',
        request: 'sync mysql tables to s3',
      },
    })
    expect(createPipelineResp.ok()).toBeTruthy()
    const createdPipeline = await createPipelineResp.json()
    const pipelineId: string | undefined = createdPipeline?.id
    test.skip(!pipelineId, 'Failed to create pipeline for E2E')

    const stateUrl = `${API_URL}/api/v1/pipelines/${pipelineId}/state`
    const hitlUrl = `${API_URL}/api/v1/pipelines/${pipelineId}/hitl/tables`

    // Stub pipeline state to force HITL table selection.
    await page.route(stateUrl, async (route: Route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          schema_version: 2,
          pipeline_id: pipelineId,
          execution_id: 'e2e-exec-00000001',
          status: 'waiting_for_user',
          current_stage: 'table_selection',
          message: 'Select table(s) to sync (77 available)',
          progress: { percent: 75, current_step: 7, total_steps: 8, stage: 'table_selection' },
          blocking_reason: {
            type: 'table_selection',
            description: 'Select table(s) to sync (77 available)',
            available_tables: [
              { name: 'users', schema: 'public', row_count: 100, columns: 10 },
              { name: 'orders', schema: 'public', row_count: 500, columns: 18 },
            ],
            details: {
              source_type: 'MySQL',
              // AI table suggestions injected by the backend at park time
              // (pipeline_state.go attachTableSuggestions).
              suggested_tables: [
                { name: 'users', schema: 'public', reason: 'Matches intent: user data sync', confidence: 0.92 },
              ],
              suggestions_source: 'llm',
            },
          },
        }),
      })
    })

    // Intercept submit so we can validate payload and return success without relying on backend workflow state.
    let submittedBody: HitlTablesPayload | null = null
    await page.route(hitlUrl, async (route: Route) => {
      const req = route.request()
      if (req.method().toUpperCase() === 'POST') {
        try {
          // postDataJSON() returns a parsed object; may throw if body isn't JSON.
          submittedBody = (await req.postDataJSON()) as HitlTablesPayload
        } catch {
          submittedBody = null
        }
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            success: true,
            pipeline_id: pipelineId,
            execution_id: 'e2e-exec-00000001',
            signal: 'tables_selected',
          }),
        })
        return
      }
      await route.fallback()
    })

    // Navigate to pipeline detail (this will mount PipelineLiveStatePanel and fetch /state).
    await page.goto(`/pipelines/${pipelineId}`)

    // Verify HITL panel appears (UI copy may evolve; anchor on the canonical header).
    await expect(page.locator('text=Pipeline Paused - Action Required')).toBeVisible({ timeout: 30000 })

    // The modal may auto-open; if not, click CTA.
    const modalTitle = page.locator('text=Select Tables to Sync')
    if (!(await modalTitle.isVisible().catch(() => false))) {
      await page.locator('button:has-text("Select tables")').first().click()
    }
    await expect(modalTitle).toBeVisible({ timeout: 30000 })

    // AI suggestions: `public.users` was suggested by the backend, so it must
    // be pre-selected with an "AI suggested" badge and the one-line hint.
    await expect(page.locator('text=AI pre-selected 1 table — adjust freely')).toBeVisible({ timeout: 30000 })
    await expect(page.locator('text=AI suggested').first()).toBeVisible()
    await expect(page.locator('#tbl-public\\.users')).toHaveAttribute('data-state', 'checked')
    // The non-suggested table stays unchecked.
    await expect(page.locator('#tbl-public\\.orders')).toHaveAttribute('data-state', 'unchecked')

    // User override always wins: ADD a non-suggested table on top of the AI
    // pre-selection (clicking `public.users` would deselect it — also allowed).
    await page.locator('label:has-text("public.orders")').first().click()

    // Continue should reflect selection count (pre-selected + user pick) and submit.
    const continueBtn = page.locator('button:has-text("Sync 2 tables")').first()
    await expect(continueBtn).toBeEnabled({ timeout: 30000 })
    await continueBtn.click()

    // Validate payload was submitted: AI pre-selection + the user's addition.
    await expect.poll(() => submittedBody, { timeout: 30000 }).not.toBeNull()
    // Cast: TS can't see the closure assignment inside the route handler.
    const submitted = submittedBody as HitlTablesPayload | null
    expect(submitted?.selected_tables || []).toContain('public.users')
    expect(submitted?.selected_tables || []).toContain('public.orders')

    // Modal should close after success.
    await expect(modalTitle).toBeHidden({ timeout: 30000 })
  })
})


