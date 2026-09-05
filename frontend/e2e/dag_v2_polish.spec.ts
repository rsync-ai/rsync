import { test, expect, type Page, request as apiRequest } from '@playwright/test'

/**
 * Polish-pass E2E — verifies the new DAGVisualizationV2 + StageDetailPanel +
 * PipelineInsightsBar + PipelineCopilotDock surfaces.
 *
 * Auth strategy: register a real user on the running api-gateway, set the
 * returned token as a cookie. Server-side auth checks then succeed.
 *
 * Data strategy: use a real pipeline (created via API once at setup), but
 * intercept the browser-side /state call to inject rich test data
 * (PII metadata, anomalous duration, parseable record counts).
 */

const HOST_GATEWAY = 'http://localhost:5001' // api-gateway exposed on host
const TEST_EMAIL = `e2e-polish-${Date.now()}@example.com`
const TEST_PASSWORD = 'E2eTest!2024'

let token = ''
let pipelineId = ''

test.beforeAll(async () => {
  const ctx = await apiRequest.newContext({ baseURL: HOST_GATEWAY })

  // Register
  const reg = await ctx.post('/api/v1/auth/register', {
    data: { email: TEST_EMAIL, password: TEST_PASSWORD, name: 'E2E Polish' },
  })
  if (!reg.ok()) {
    throw new Error(`Register failed: ${reg.status()} ${await reg.text()}`)
  }
  const regJson = await reg.json()
  token = regJson.token

  // Create a pipeline owned by this user (so the dashboard server fetch finds it)
  const pipe = await ctx.post('/api/v1/pipelines', {
    headers: { Authorization: token },
    data: {
      name: 'E2E DAG Polish',
      request: 'sync mysql customers to s3',
      source: 'mysql',
      destination: 'aws-s3',
    },
  })
  if (!pipe.ok()) {
    throw new Error(`Pipeline create failed: ${pipe.status()} ${await pipe.text()}`)
  }
  const pipeJson = await pipe.json()
  pipelineId = pipeJson.id
  await ctx.dispose()
})

async function setupAuthAndMocks(page: Page) {
  // Real auth token cookie — server-side auth check will pass
  await page.context().addCookies([
    {
      name: 'auth_token',
      value: token,
      url: 'http://localhost:3000',
    },
  ])
  await page.addInitScript((t) => {
    localStorage.setItem('auth_token', t)
  }, token)

  // Mock the state endpoint with rich Phase 1+2+3 test data
  await page.route(`**/api/v1/pipelines/${pipelineId}/state`, async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        execution_id: 'exec-1',
        current_state: 'COMPLETED',
        execution_plan: {
          pipeline_id: pipelineId,
          mode: 'dag',
          stages: [
            {
              id: 'src',
              display_name: 'Extract from MySQL',
              description: 'Pull customer rows',
              status: 'complete',
              node_kind: 'source',
              dependencies: [],
              actual_duration: 1200,
              result_summary: '12,450 customers extracted',
              metadata: {
                node_config: { connector_type: 'mysql' },
                output_schema: [
                  { name: 'id', type: 'BIGINT', nullable: false },
                  { name: 'email', type: 'VARCHAR(255)', nullable: false },
                  { name: 'phone', type: 'VARCHAR(20)', nullable: true },
                  { name: 'created_at', type: 'DATETIME', nullable: false },
                ],
              },
            },
            {
              id: 'tx_pii',
              display_name: 'Mask PII',
              description: 'Redact email + phone',
              status: 'complete',
              node_kind: 'transform',
              dependencies: ['src'],
              actual_duration: 1500,
              result_summary: 'Processed 12,450 records',
              metadata: { pii_fields: ['email', 'phone', 'ssn'] },
            },
            {
              id: 'tx_slow',
              display_name: 'Aggregate Stats',
              description: 'Compute customer aggregations',
              status: 'complete',
              node_kind: 'transform',
              dependencies: ['tx_pii'],
              actual_duration: 9000,
              result_summary: 'Aggregated 12,450 records',
            },
            {
              id: 'dst',
              display_name: 'Load to S3',
              description: 'Write parquet to S3',
              status: 'complete',
              node_kind: 'destination',
              dependencies: ['tx_slow'],
              actual_duration: 1800,
              result_summary: '1.2M rows loaded',
              metadata: { node_config: { connector_type: 'aws-s3' } },
            },
          ],
          metadata: { is_dag: true, node_count: 4, edge_count: 3 },
        },
      }),
    })
  })

  // Empty events list (status comes from the plan stages)
  await page.route('**/api/v1/pipelines/*/events**', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ events: [] }),
    })
  })

  // Schema changes — one pending approval to exercise SchemaEvolutionPanel
  await page.route(`**/api/v1/pipelines/${pipelineId}/schema-changes`, async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        schema_changes: [
          {
            id: 'sc-1',
            pipeline_id: pipelineId,
            change_type: 'add_column',
            table_name: 'customers',
            ddl: 'ALTER TABLE customers ADD COLUMN loyalty_tier VARCHAR(20) NULL;',
            reasoning: 'New column added upstream; safe to auto-propagate.',
            risks: '[]',
            user_message: 'New column detected in source schema.',
            status: 'pending',
            reviewed_by: null,
            reviewed_at: null,
            applied_at: null,
            error_message: null,
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString(),
          },
        ],
      }),
    })
  })
}

test.describe('DAG V2 polish surfaces', () => {
  test('renders insights bar, animated nodes, edge badges, detail panel, copilot dock', async ({ page }) => {
    const errors: string[] = []
    page.on('pageerror', (err) => errors.push(`pageerror: ${err.message}`))

    await setupAuthAndMocks(page)

    await page.goto(`/pipelines/${pipelineId}?tab=steps`)
    await page.waitForLoadState('networkidle')

    // Click Steps tab if not already there
    const stepsTab = page.getByRole('tab', { name: /^Steps/i }).first()
    if (await stepsTab.isVisible({ timeout: 4000 }).catch(() => false)) {
      await stepsTab.click()
      await page.waitForTimeout(300)
    }

    // ── 1. Insights Bar ──────────────────────────────────────────────────────
    await expect(page.locator('text=/Ask about this pipeline/i').first()).toBeVisible({ timeout: 10000 })
    await expect(page.locator('text=/1\\.2M records loaded/i').first()).toBeVisible()
    await expect(page.getByRole('button', { name: /Explain this pipeline/i }).first()).toBeVisible()
    // Anomaly chip — tx_slow takes 9s vs ~1.5s peers (6x median)
    await expect(page.getByRole('button', { name: /slow|anomal/i }).first()).toBeVisible()
    // PII chip
    await expect(page.getByRole('button', { name: /Audit PII/i }).first()).toBeVisible()

    await page.screenshot({ path: 'test-results/01-insights-bar.png', fullPage: false })

    // ── 2. Schema Evolution Panel — pending approval card ───────────────────
    await expect(page.locator('text=/Schema Evolution/i').first()).toBeVisible({ timeout: 8000 })
    await expect(page.locator('text=/1 pending/i').first()).toBeVisible()
    await expect(page.locator('text=/customers/i').first()).toBeVisible()
    // Approve button should be present
    await expect(page.getByRole('button', { name: /Apply migration/i }).first()).toBeVisible()

    await page.screenshot({ path: 'test-results/01b-schema-evolution-panel.png', fullPage: false })

    // ── 3. Edge row-count badge ──────────────────────────────────────────────
    const edgeBadge = page.locator('text=/(12\\.[0-9]K|1\\.2M) →/').first()
    await expect(edgeBadge).toBeVisible({ timeout: 8000 })

    // ── 3a. Edge schema preview — hover the src→tx_pii edge badge ───────────
    // The src stage has output_schema in metadata; hovering the edge label
    // reveals the column list in the popover.
    const srcEdgeBadge = page.locator('text=/(12\\.[0-9]K) →/').first()
    if (await srcEdgeBadge.isVisible({ timeout: 3000 }).catch(() => false)) {
      await srcEdgeBadge.hover()
      // Column names from the mock output_schema
      await expect(page.locator('text=/email/i').first()).toBeVisible({ timeout: 3000 })
      await expect(page.locator('text=/BIGINT|VARCHAR/i').first()).toBeVisible({ timeout: 2000 })
      await page.screenshot({ path: 'test-results/02a-edge-schema-hover.png', fullPage: false })
    }

    // ── 3b. Anomaly badge on the slow node ──────────────────────────────────
    const anomalyBadge = page.locator('text=/^[3-9]\\.[0-9]×$|^[1-9][0-9]\\.[0-9]×$/').first()
    await expect(anomalyBadge).toBeVisible({ timeout: 8000 })

    await page.screenshot({ path: 'test-results/02-graph-with-particles.png', fullPage: false })

    // ── 4. Click destination node → detail panel ────────────────────────────
    // ReactFlow assigns [data-id="<id>"] to each node container — robust selector
    const dstNode = page.locator('[data-id="dst"]').first()
    await dstNode.scrollIntoViewIfNeeded()
    await dstNode.click({ force: true })
    await expect(page.locator('text=/Records loaded/i').first()).toBeVisible({ timeout: 5000 })
    await expect(page.locator('text=/1,200,000/').first()).toBeVisible()
    await expect(page.getByRole('button', { name: /Ask about this node/i })).toBeVisible()

    await page.screenshot({ path: 'test-results/03-detail-panel-destination.png', fullPage: false })

    // ── 5. Click PII node → PII protection callout ──────────────────────────
    const piiNode = page.locator('[data-id="tx_pii"]').first()
    await piiNode.scrollIntoViewIfNeeded()
    await piiNode.click({ force: true })
    await expect(page.locator('text=/PII protection/i').first()).toBeVisible({ timeout: 5000 })
    await expect(page.locator('text=/3 fields masked/i').first()).toBeVisible()

    await page.screenshot({ path: 'test-results/04-detail-panel-pii.png', fullPage: false })

    // ── 6. Copilot dock toggle and content ──────────────────────────────────
    const copilotTab = page.getByRole('button', { name: /Open Pipeline Copilot/i })
    await expect(copilotTab).toBeVisible()
    await copilotTab.click()

    await expect(page.locator('text=/Pipeline Copilot/i').first()).toBeVisible({ timeout: 5000 })
    await expect(page.locator('text=/E2E DAG Polish/i').first()).toBeVisible()
    await expect(page.locator('text=/Suggested questions/i')).toBeVisible()
    await expect(
      page.getByRole('button', { name: /Why is "Aggregate Stats" slow/i }),
    ).toBeVisible()
    await expect(page.locator('textarea[placeholder*="Ask about this pipeline"]')).toBeVisible()

    await page.screenshot({ path: 'test-results/05-copilot-dock.png', fullPage: false })

    // No critical runtime errors
    const critical = errors.filter(
      (e) =>
        !e.includes('Failed to load resource') &&
        !e.includes('NEXT_REDIRECT') &&
        !e.includes('hydration'),
    )
    if (critical.length > 0) {
      console.log('Captured errors:\n' + critical.join('\n'))
    }
    expect(critical.slice(0, 3)).toHaveLength(0)
  })

  test('clicking an Insights bar chip lands in chat and the prompt is sent', async ({ page }) => {
    await setupAuthAndMocks(page)

    // Capture the chat-message POST so we can confirm the message was actually sent
    let chatBody: any = null
    await page.route('**/api/v1/chat/message', async (route) => {
      try {
        chatBody = route.request().postDataJSON()
      } catch {
        chatBody = route.request().postData()
      }
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          message_id: 'msg-1',
          response_type: 'text',
          content: 'Got it — explaining now.',
          ts: Date.now(),
        }),
      })
    })

    await page.goto(`/pipelines/${pipelineId}?tab=steps`)
    await page.waitForLoadState('networkidle')

    // Wait for the Insights bar to render
    await expect(page.locator('text=/Ask about this pipeline/i').first()).toBeVisible({ timeout: 10000 })

    // Click "Explain this pipeline" chip
    await page.getByRole('button', { name: /Explain this pipeline/i }).first().click()

    // We should be on /chat
    await page.waitForURL(/\/chat/, { timeout: 5000 })

    // The chat should auto-send the seeded prompt → message body should reach the API
    await expect.poll(() => chatBody, { timeout: 8000 }).not.toBeNull()
    const seededText = JSON.stringify(chatBody)
    expect(seededText).toMatch(/Explain my pipeline/i)
    // Confirm stage names made it into the seeded prompt
    expect(seededText).toMatch(/Extract from MySQL/i)
    expect(seededText).toMatch(/Mask PII/i)

    // The seeded prompt should NOT remain stuck in the input (it must have been
    // consumed by autosend; the chat moves on to render agent response state).
    // The home-state pre-filled banner should be gone.
    await expect(page.locator('text=/Try "sync mysql to s3"/i')).not.toBeVisible({ timeout: 3000 })

    await page.screenshot({ path: 'test-results/06-chat-after-chip-click.png', fullPage: false })
  })
})
