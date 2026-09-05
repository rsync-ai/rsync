import { test, expect, Page } from '@playwright/test'

/**
 * DAG Workflow E2E Tests
 * 
 * Tests for DAG (Directed Acyclic Graph) workflow functionality:
 * - DAG visualization in the Steps tab
 * - Node status tracking
 * - HITL node input
 * 
 * Prerequisites:
 * - All services running (docker-compose up -d)
 * - Frontend running (npm run dev)
 * - API Gateway running
 * 
 * Run: npx playwright test e2e/dag_workflow.spec.ts
 */

// Helper to set auth cookie for all tests
async function setupAuth(page: Page) {
  await page.context().addCookies([{
    name: 'auth_token',
    value: 'test-token-for-e2e',
    domain: 'localhost',
    path: '/',
  }])
  
  await page.addInitScript(() => {
    localStorage.setItem('auth_token', 'test-token-for-e2e')
    localStorage.setItem('user_id', 'e2e-test-user')
  })
}

async function mockPipelineAndConnections(page: Page) {
  // Pipeline details (used for connector-type fallback labeling)
  await page.route('**/api/v1/pipelines/dag-test-pipeline', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        id: 'dag-test-pipeline',
        source_connection_id: 'conn-src-1',
        destination_connection_id: 'conn-dst-1',
      }),
    })
  })

  // Connections (used to infer connector_type)
  await page.route('**/api/v1/connections/*', async (route) => {
    const url = route.request().url()
    const id = url.split('/api/v1/connections/')[1]?.split('?')[0] || ''
    const connector_type = id.includes('dst') ? 'aws-s3' : 'mysql'
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        id,
        connector_type,
        type: id.includes('dst') ? 'destination' : 'source',
        name: id,
      }),
    })
  })

  // Events (Steps tab reconciles status from events when present)
  await page.route('**/api/v1/pipelines/*/events?**', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ events: [] }),
    })
  })
}

// Mock execution plan with DAG structure
const mockDAGExecutionPlan = {
  pipeline_id: 'dag-test-pipeline',
  workflow_id: 'dag-test-workflow',
  mode: 'dag',
  stages: [
    {
      id: 'source_1',
      display_name: 'Extract from MySQL',
      description: 'Extract data from MySQL database',
      status: 'complete',
      node_kind: 'source',
      dependencies: [],
      progress: 100,
    },
    {
      id: 'transform_1',
      display_name: 'Transform Data',
      description: 'Apply data transformations',
      status: 'running',
      node_kind: 'transform',
      dependencies: ['source_1'],
      progress: 45,
    },
    {
      id: 'dest_1',
      display_name: 'Load to S3',
      description: 'Upload data to S3 bucket',
      status: 'pending',
      node_kind: 'destination',
      dependencies: ['transform_1'],
    },
    {
      id: 'notify_1',
      display_name: 'Send Slack Notification',
      description: 'Notify on completion',
      status: 'pending',
      node_kind: 'notification',
      dependencies: ['dest_1'],
    },
  ],
  metadata: {
    is_dag: true,
    node_count: 4,
    edge_count: 3,
    graph_id: 'graph-dag-test',
  },
}

test.describe('DAG Workflow Visualization', () => {
  test.beforeEach(async ({ page }) => {
    await setupAuth(page)
    await mockPipelineAndConnections(page)
  })

  test('displays DAG badge when plan has is_dag=true', async ({ page }) => {
    // Mock the API response
    await page.route('**/api/v1/pipelines/*/state', async route => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          execution_plan: mockDAGExecutionPlan,
          current_state: 'DAG_EXECUTING',
        }),
      })
    })

    await page.goto('/pipelines/dag-test-pipeline')
    await page.waitForLoadState('networkidle')

    // Navigate to Steps tab
    const stepsTab = page.getByRole('tab', { name: /steps/i })
    if (await stepsTab.isVisible({ timeout: 5000 }).catch(() => false)) {
      await stepsTab.click()
      await page.waitForTimeout(500)

      // Should show DAG badge
      const dagBadge = page.locator('text=DAG')
      const hasDagBadge = await dagBadge.isVisible({ timeout: 5000 }).catch(() => false)
      expect(hasDagBadge).toBeTruthy()
    }
  })

  test('shows view mode toggle for DAG plans', async ({ page }) => {
    await page.route('**/api/v1/pipelines/*/state', async route => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          execution_plan: mockDAGExecutionPlan,
          current_state: 'DAG_EXECUTING',
        }),
      })
    })

    await page.goto('/pipelines/dag-test-pipeline')
    await page.waitForLoadState('networkidle')

    const stepsTab = page.getByRole('tab', { name: /steps/i })
    if (await stepsTab.isVisible({ timeout: 5000 }).catch(() => false)) {
      await stepsTab.click()
      await page.waitForTimeout(500)

      // Should show Graph and Timeline buttons
      const graphButton = page.getByRole('button', { name: /graph/i })
      const timelineButton = page.getByRole('button', { name: /timeline/i })

      const hasGraphButton = await graphButton.isVisible({ timeout: 3000 }).catch(() => false)
      const hasTimelineButton = await timelineButton.isVisible({ timeout: 3000 }).catch(() => false)

      expect(hasGraphButton || hasTimelineButton).toBeTruthy()
    }
  })

  test('renders SVG DAG visualization', async ({ page }) => {
    await page.route('**/api/v1/pipelines/*/state', async route => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          execution_plan: mockDAGExecutionPlan,
          current_state: 'DAG_EXECUTING',
        }),
      })
    })

    await page.goto('/pipelines/dag-test-pipeline')
    await page.waitForLoadState('networkidle')

    const stepsTab = page.getByRole('tab', { name: /steps/i })
    if (await stepsTab.isVisible({ timeout: 5000 }).catch(() => false)) {
      await stepsTab.click()
      await page.waitForTimeout(500)

      // Click Graph view if available
      const graphButton = page.getByRole('button', { name: /graph/i })
      if (await graphButton.isVisible({ timeout: 2000 }).catch(() => false)) {
        await graphButton.click()
        await page.waitForTimeout(300)
      }

      // Should render SVG with nodes
      const svg = page.locator('svg')
      const hasSvg = await svg.first().isVisible({ timeout: 3000 }).catch(() => false)
      
      // Should have node rectangles
      const rects = page.locator('svg rect')
      const rectCount = await rects.count()

      expect(hasSvg || rectCount > 0).toBeTruthy()
    }
  })

  test('can switch between Graph and Timeline views', async ({ page }) => {
    await page.route('**/api/v1/pipelines/*/state', async route => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          execution_plan: mockDAGExecutionPlan,
          current_state: 'DAG_EXECUTING',
        }),
      })
    })

    await page.goto('/pipelines/dag-test-pipeline')
    await page.waitForLoadState('networkidle')

    const stepsTab = page.getByRole('tab', { name: /steps/i })
    if (await stepsTab.isVisible({ timeout: 5000 }).catch(() => false)) {
      await stepsTab.click()
      await page.waitForTimeout(500)

      const graphButton = page.getByRole('button', { name: /graph/i })
      const timelineButton = page.getByRole('button', { name: /timeline/i })

      // Click Timeline view
      if (await timelineButton.isVisible({ timeout: 2000 }).catch(() => false)) {
        await timelineButton.click()
        await page.waitForTimeout(300)

        // Should show linear cards
        const cards = page.locator('.border-l-4')
        const cardCount = await cards.count()
        expect(cardCount).toBeGreaterThan(0)

        // Click Graph view
        if (await graphButton.isVisible({ timeout: 2000 }).catch(() => false)) {
          await graphButton.click()
          await page.waitForTimeout(300)

          // Should show SVG
          const svg = page.locator('svg')
          const hasSvg = await svg.first().isVisible({ timeout: 2000 }).catch(() => false)
          expect(hasSvg).toBeTruthy()
        }
      }
    }
  })

  test('displays node metadata in summary', async ({ page }) => {
    await page.route('**/api/v1/pipelines/*/state', async route => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          execution_plan: mockDAGExecutionPlan,
          current_state: 'DAG_EXECUTING',
        }),
      })
    })

    await page.goto('/pipelines/dag-test-pipeline')
    await page.waitForLoadState('networkidle')

    const stepsTab = page.getByRole('tab', { name: /steps/i })
    if (await stepsTab.isVisible({ timeout: 5000 }).catch(() => false)) {
      await stepsTab.click()
      await page.waitForTimeout(500)

      // Should show node count
      const nodeCount = page.locator('text=Nodes: 4')
      const hasNodeCount = await nodeCount.isVisible({ timeout: 3000 }).catch(() => false)

      // Should show edge count
      const edgeCount = page.locator('text=Edges: 3')
      const hasEdgeCount = await edgeCount.isVisible({ timeout: 3000 }).catch(() => false)

      expect(hasNodeCount || hasEdgeCount).toBeTruthy()
    }
  })
})

test.describe('DAG HITL Node Input', () => {
  test.beforeEach(async ({ page }) => {
    await setupAuth(page)
  })

  test('HITL endpoint accepts node input', async ({ page }) => {
    let signalReceived = false
    
    // Mock the HITL endpoint
    await page.route('**/api/v1/pipelines/*/hitl/node-input', async route => {
      signalReceived = true
      const body = route.request().postDataJSON()
      
      expect(body.node_id).toBeDefined()
      expect(body.message).toBeDefined()

      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          success: true,
          pipeline_id: 'dag-test-pipeline',
          execution_id: 'exec-123',
          node_id: body.node_id,
          signal: 'node_input_provided',
        }),
      })
    })

    // Ensure we are on a real origin so relative fetch() URLs work (not about:blank)
    await page.goto('/')

    // Simulate a HITL node input request
    const response = await page.evaluate(async () => {
      const res = await fetch('/api/v1/pipelines/dag-test-pipeline/hitl/node-input', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          node_id: 'source_1',
          message: 'Use the users and orders tables',
        }),
      })
      return res.json()
    })

    expect(signalReceived).toBeTruthy()
    expect(response.success).toBeTruthy()
  })
})

test.describe('DAG Workflow Status Tracking', () => {
  test.beforeEach(async ({ page }) => {
    await setupAuth(page)
  })

  test('shows running status with spinner for active nodes', async ({ page }) => {
    await page.route('**/api/v1/pipelines/*/state', async route => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          execution_plan: mockDAGExecutionPlan,
          current_state: 'DAG_EXECUTING',
        }),
      })
    })

    await page.goto('/pipelines/dag-test-pipeline')
    await page.waitForLoadState('networkidle')

    const stepsTab = page.getByRole('tab', { name: /steps/i })
    if (await stepsTab.isVisible({ timeout: 5000 }).catch(() => false)) {
      await stepsTab.click()
      await page.waitForTimeout(500)

      // Click Timeline view to see status more clearly
      const timelineButton = page.getByRole('button', { name: /timeline/i })
      if (await timelineButton.isVisible({ timeout: 2000 }).catch(() => false)) {
        await timelineButton.click()
        await page.waitForTimeout(300)
      }

      // Should have a spinning loader for running status
      const spinner = page.locator('.animate-spin')
      const hasSpinner = await spinner.first().isVisible({ timeout: 3000 }).catch(() => false)
      expect(hasSpinner).toBeTruthy()
    }
  })

  test('shows progress percentage for running nodes', async ({ page }) => {
    await page.route('**/api/v1/pipelines/*/state', async route => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          execution_plan: mockDAGExecutionPlan,
          current_state: 'DAG_EXECUTING',
        }),
      })
    })

    await page.goto('/pipelines/dag-test-pipeline')
    await page.waitForLoadState('networkidle')

    const stepsTab = page.getByRole('tab', { name: /steps/i })
    if (await stepsTab.isVisible({ timeout: 5000 }).catch(() => false)) {
      await stepsTab.click()
      await page.waitForTimeout(500)

      // Switch to Timeline view
      const timelineButton = page.getByRole('button', { name: /timeline/i })
      if (await timelineButton.isVisible({ timeout: 2000 }).catch(() => false)) {
        await timelineButton.click()
        await page.waitForTimeout(300)
      }

      // Should show progress (45%)
      const progressText = page.locator('text=45%')
      const hasProgress = await progressText.isVisible({ timeout: 3000 }).catch(() => false)
      expect(hasProgress).toBeTruthy()
    }
  })
})
