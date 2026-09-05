/**
 * Demo Diagnostic Test
 * - Logs in with real dev credentials (default@rsync-ai.local / password123)
 * - Screenshots key pages for demo review
 * - Tests MySQL → PostgreSQL connection creation and pipeline flow
 *
 * Run: npx playwright test e2e/demo_diagnostic.spec.ts --headed
 */

import { test, expect, type Page } from '@playwright/test'
import * as path from 'path'
import * as fs from 'fs'

const SCREENSHOTS_DIR = path.join(__dirname, '..', 'demo-screenshots')
const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:5001'

async function loginWithRealCredentials(page: Page) {
  await page.goto('/login')
  await page.waitForLoadState('networkidle')

  // Use dev quick-login button if available
  const useBtn = page.locator('button:has-text("Use")')
  if (await useBtn.isVisible({ timeout: 2000 }).catch(() => false)) {
    await useBtn.click()
  } else {
    await page.getByLabel(/Email/i).fill('default@rsync-ai.local')
    await page.getByLabel(/Password/i).fill('password123')
  }

  await page.getByRole('button', { name: /Sign in/i }).click()
  // Wait for redirect away from login
  await page.waitForURL((url) => !url.pathname.includes('/login'), { timeout: 15000 })
}

function saveScreenshot(name: string) {
  if (!fs.existsSync(SCREENSHOTS_DIR)) fs.mkdirSync(SCREENSHOTS_DIR, { recursive: true })
  return path.join(SCREENSHOTS_DIR, `${name}.png`)
}

test.describe('Demo Diagnostic - Full UI Flow', () => {
  test.setTimeout(120000)

  test('01 - Login page loads and dev login works', async ({ page }) => {
    await page.goto('/login')
    await page.waitForLoadState('networkidle')
    await page.screenshot({ path: saveScreenshot('01-login-page'), fullPage: true })

    // Use the dev quick-fill button
    const useBtn = page.locator('button:has-text("Use")')
    await expect(useBtn).toBeVisible({ timeout: 5000 })
    await useBtn.click()

    await page.getByRole('button', { name: /Sign in/i }).click()
    await page.waitForURL((url) => !url.pathname.includes('/login'), { timeout: 15000 })
    await page.screenshot({ path: saveScreenshot('02-after-login'), fullPage: true })
    console.log('✅ Login successful, redirected to:', page.url())
  })

  test('02 - Dashboard page', async ({ page }) => {
    await loginWithRealCredentials(page)
    await page.goto('/dashboard')
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(2000)
    await page.screenshot({ path: saveScreenshot('03-dashboard'), fullPage: true })

    const body = page.locator('body')
    await expect(body).toBeVisible()
    console.log('✅ Dashboard loaded at:', page.url())
    console.log('   Page title:', await page.title())
  })

  test('03 - Home / overview page', async ({ page }) => {
    await loginWithRealCredentials(page)
    await page.goto('/')
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(2000)
    await page.screenshot({ path: saveScreenshot('04-home'), fullPage: true })
    console.log('✅ Home loaded at:', page.url())
  })

  test('04 - Chat interface', async ({ page }) => {
    await loginWithRealCredentials(page)
    await page.goto('/chat')
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(2000)
    await page.screenshot({ path: saveScreenshot('05-chat'), fullPage: true })

    const textarea = page.locator('textarea').first()
    const hasInput = await textarea.isVisible({ timeout: 5000 }).catch(() => false)
    console.log('✅ Chat page loaded, has textarea:', hasInput)
  })

  test('05 - Connectors page', async ({ page }) => {
    await loginWithRealCredentials(page)
    await page.goto('/connectors')
    await page.waitForLoadState('networkidle')
    await page.waitForSelector('.animate-spin', { state: 'hidden', timeout: 15000 }).catch(() => {})
    await page.waitForTimeout(2000)
    await page.screenshot({ path: saveScreenshot('06-connectors'), fullPage: true })

    const cards = page.locator('.rounded-xl.border.cursor-pointer, [data-testid="connector-card"]')
    const count = await cards.count()
    console.log('✅ Connectors page loaded, connector cards found:', count)
  })

  test('06 - Connections page', async ({ page }) => {
    await loginWithRealCredentials(page)
    await page.goto('/connections')
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(2000)
    await page.screenshot({ path: saveScreenshot('07-connections'), fullPage: true })
    console.log('✅ Connections page loaded')
  })

  test('07 - New connection wizard', async ({ page }) => {
    await loginWithRealCredentials(page)
    await page.goto('/connections/new')
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(2000)
    await page.screenshot({ path: saveScreenshot('08-new-connection'), fullPage: true })
    console.log('✅ New connection page at:', page.url())
    // Print visible text to understand current UI
    const headings = await page.locator('h1, h2, h3').allTextContents()
    console.log('   Headings:', headings)
    const buttons = await page.locator('button').allTextContents()
    console.log('   Buttons:', buttons.slice(0, 10))
  })

  test('08 - Pipelines page', async ({ page }) => {
    await loginWithRealCredentials(page)
    await page.goto('/pipelines')
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(2000)
    await page.screenshot({ path: saveScreenshot('09-pipelines'), fullPage: true })
    console.log('✅ Pipelines page loaded')
  })

  test('09 - API health check (connections, connectors, pipelines)', async ({ page }) => {
    await loginWithRealCredentials(page)

    // Get the auth cookie
    const cookies = await page.context().cookies()
    const authCookie = cookies.find(c => c.name === 'auth_token')
    const token = authCookie?.value || ''

    const headers: Record<string, string> = token
      ? { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' }
      : { 'X-User-ID': '00000000-0000-0000-0000-000000000000', 'Content-Type': 'application/json' }

    // Check connectors
    const connResp = await page.request.get(`${API_URL}/api/v1/connectors`, { headers })
    console.log('Connectors API:', connResp.status(), connResp.ok() ? '✅' : '❌')
    if (connResp.ok()) {
      const data = await connResp.json()
      const connectors = Array.isArray(data) ? data : data.connectors || data.data || []
      console.log('  Connector count:', connectors.length)
      const mysqlConn = connectors.find((c: { type?: string; id?: string }) => c.type?.toLowerCase().includes('mysql') || c.id?.toLowerCase().includes('mysql'))
      console.log('  MySQL connector available:', !!mysqlConn)
    }

    // Check connections
    const connectionsResp = await page.request.get(`${API_URL}/api/v1/connections`, { headers })
    console.log('Connections API:', connectionsResp.status(), connectionsResp.ok() ? '✅' : '❌')

    // Check pipelines
    const pipelinesResp = await page.request.get(`${API_URL}/api/v1/pipelines`, { headers })
    console.log('Pipelines API:', pipelinesResp.status(), pipelinesResp.ok() ? '✅' : '❌')
    if (pipelinesResp.ok()) {
      const data = await pipelinesResp.json()
      const pipelines = Array.isArray(data) ? data : data.pipelines || data.data || []
      console.log('  Existing pipelines:', pipelines.length)
    }
  })

  test('10 - Create MySQL source connection via API', async ({ page }) => {
    await loginWithRealCredentials(page)
    const cookies = await page.context().cookies()
    const authCookie = cookies.find(c => c.name === 'auth_token')
    const token = authCookie?.value || ''
    const headers: Record<string, string> = token
      ? { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' }
      : { 'X-User-ID': '00000000-0000-0000-0000-000000000000', 'Content-Type': 'application/json' }

    // Create MySQL source connection
    const mysqlResp = await page.request.post(`${API_URL}/api/v1/connections`, {
      headers,
      data: {
        name: `E2E Demo MySQL ${Date.now()}`,
        connector_id: 'mysql-v1-0-0',
        direction: 'source',
        config: {
          host: 'mysql-e2e',
          port: 3306,
          database: 'e2e_db',
          user: 'e2e_user',
          password: 'e2e_password',
        },
      },
    })
    console.log('Create MySQL connection:', mysqlResp.status(), mysqlResp.ok() ? '✅' : '❌')
    if (!mysqlResp.ok()) {
      const body = await mysqlResp.text()
      console.log('  Error:', body.slice(0, 200))
      return
    }
    const mysqlConn = await mysqlResp.json()
    const mysqlConnId = mysqlConn?.id || mysqlConn?.connection?.id
    console.log('  MySQL connection ID:', mysqlConnId)

    // Create PostgreSQL destination connection
    const pgResp = await page.request.post(`${API_URL}/api/v1/connections`, {
      headers,
      data: {
        name: `E2E Demo Postgres ${Date.now()}`,
        connector_id: 'postgresql-v1-0-0',
        direction: 'destination',
        config: {
          host: 'postgres-e2e',
          port: 5432,
          database: 'e2e_db',
          user: 'e2e_user',
          password: 'e2e_password',
        },
      },
    })
    console.log('Create PostgreSQL connection:', pgResp.status(), pgResp.ok() ? '✅' : '❌')
    if (!pgResp.ok()) {
      const body = await pgResp.text()
      console.log('  Error:', body.slice(0, 200))
      return
    }
    const pgConn = await pgResp.json()
    const pgConnId = pgConn?.id || pgConn?.connection?.id
    console.log('  PostgreSQL connection ID:', pgConnId)

    // Now navigate to connections page to see them
    await page.goto('/connections')
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(2000)
    await page.screenshot({ path: saveScreenshot('10-connections-with-e2e'), fullPage: true })
    console.log('✅ Connections created and listed')
  })

  test('11 - Chat pipeline creation prompt', async ({ page }) => {
    await loginWithRealCredentials(page)
    await page.goto('/chat?prompt=sync+mysql+to+postgresql')
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(3000)
    await page.screenshot({ path: saveScreenshot('11-chat-pipeline-prompt'), fullPage: true })
    console.log('✅ Chat pipeline prompt triggered at:', page.url())

    // Wait to see if pipeline tabs appear
    const pipelineTab = page.getByRole('tab', { name: /Pipeline/i })
    const hasPipelineTab = await pipelineTab.isVisible({ timeout: 30000 }).catch(() => false)
    console.log('  Pipeline tab appeared:', hasPipelineTab)
    if (hasPipelineTab) {
      await page.screenshot({ path: saveScreenshot('11b-chat-pipeline-tabs'), fullPage: true })
    }
  })
})
