/**
 * Capture all 6 product feature screenshots for the rsync.ai marketing website.
 *
 * Saves directly to: rsync-ai-website/public/screenshots/ as *-v3.png
 * Each screenshot is clipped to exactly 1440x900 — no Retina scaling issues.
 *
 * Run: npx playwright test e2e/website_screenshots.spec.ts --reporter=list
 */

import { test, type Page, type BrowserContext } from '@playwright/test'
import * as path from 'path'

const OUT = path.join(__dirname, '..', '..', '..', 'rsync-ai-website', 'public', 'screenshots')
const VIEWPORT = { width: 1440, height: 900 }
const COMPLETED_ID = '74b9f49c-3506-4779-9357-27d1ae879388'

async function login(page: Page) {
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
  await page.waitForLoadState('networkidle')
  await page.waitForTimeout(400)
}

async function shot(page: Page, filename: string) {
  await page.waitForTimeout(800)
  // Hide trace toasts, dev indicators, and any bottom-left overlays before capturing
  await page.evaluate(() => {
    // Next.js dev indicator (the "N" badge)
    const devIndicator = document.querySelector('nextjs-portal, [data-nextjs-dialog-overlay], #__next-build-watcher')
    if (devIndicator) (devIndicator as HTMLElement).style.display = 'none'
    // Any toast/notification containers at the bottom
    document.querySelectorAll<HTMLElement>(
      '[class*="toast"], [class*="Toast"], [class*="trace"], [class*="Trace"], [class*="notification"], [data-sonner-toaster], [id*="toast"]'
    ).forEach(el => { el.style.display = 'none' })
    // Target the specific rsync.ai trace badge (fixed bottom-left)
    document.querySelectorAll<HTMLElement>('body > *').forEach(el => {
      const style = window.getComputedStyle(el)
      if (style.position === 'fixed' && parseInt(style.bottom) < 100 && parseInt(style.left) < 200) {
        el.style.display = 'none'
      }
    })
  })
  await page.screenshot({
    path: path.join(OUT, filename),
    clip: { x: 0, y: 0, width: 1440, height: 900 },
  })
  console.log(`✓  saved: ${filename}`)
}

test.setTimeout(180000)

test('capture-all-screenshots', async ({ browser }) => {
  const context: BrowserContext = await browser.newContext({ viewport: VIEWPORT })
  const page = await context.newPage()
  await login(page)

  // ── 1. Pipeline Creation — Confirm Pipeline dialog ─────────────────
  await page.goto('/chat')
  await page.waitForLoadState('networkidle')
  await page.keyboard.press('Escape')
  await page.waitForTimeout(400)
  const chatInput = page.locator('textarea, input[placeholder*="Describe"], input[placeholder*="sync"], input[placeholder*="Try"]').last()
  await chatInput.click()
  await chatInput.fill('')
  await chatInput.pressSequentially('sync shopify to postgresql', { delay: 30 })
  await page.keyboard.press('Enter')
  await page.locator('text=/Confirm Pipeline/i').first()
    .waitFor({ state: 'visible', timeout: 30000 }).catch(() => {})
  await page.waitForTimeout(600)
  await shot(page, 'pipeline-creation-v3.png')

  // ── 2. Schema Discovery — completed pipeline Table Statistics tab ──
  // Shows all discovered tables (orders, customers, products...) with row counts
  await page.goto(`/pipelines/${COMPLETED_ID}`)
  await page.waitForLoadState('networkidle')
  await page.waitForTimeout(500)
  const tableStatsTab = page.locator('[role="tab"], button').filter({ hasText: /Table statistics/i }).first()
  if (await tableStatsTab.isVisible({ timeout: 5000 }).catch(() => false)) {
    await tableStatsTab.click()
    await page.waitForSelector('.animate-spin', { state: 'hidden', timeout: 8000 }).catch(() => {})
    await page.locator('text=/shopify|orders|customers/i').first()
      .waitFor({ state: 'visible', timeout: 8000 }).catch(() => {})
    await page.waitForTimeout(800)
  } else {
    await page.waitForTimeout(1000)
  }
  await shot(page, 'schema-discovery-v3.png')

  // ── 3. Live Monitoring — Executions page ──────────────────────────
  await page.goto('/executions')
  await page.waitForLoadState('networkidle')
  await page.waitForSelector('.animate-spin', { state: 'hidden', timeout: 8000 }).catch(() => {})
  await page.waitForTimeout(600)
  await shot(page, 'monitoring-v3.png')

  // ── 4. Data Explorer — orders table with row data ─────────────────
  await page.goto('/explorer')
  await page.waitForLoadState('networkidle')
  await page.waitForTimeout(500)
  const connDropdown = page.locator('[data-testid="connection-select"]').first()
  if (await connDropdown.isVisible({ timeout: 2000 }).catch(() => false)) {
    await connDropdown.click()
    await page.waitForTimeout(300)
    const pgOption = page.locator('[role="option"]').filter({ hasText: /pg-sink|shopify|postgres/i }).first()
    const firstOpt = page.locator('[role="option"]').first()
    const target = await pgOption.isVisible({ timeout: 800 }).catch(() => false) ? pgOption : firstOpt
    await target.click()
    await page.waitForTimeout(800)
  }
  const ordersRow = page.locator('li, [role="row"], .cursor-pointer, button')
    .filter({ hasText: /^orders/i }).first()
  if (await ordersRow.isVisible({ timeout: 5000 }).catch(() => false)) {
    await ordersRow.click()
    await page.waitForSelector('.animate-spin', { state: 'hidden', timeout: 8000 }).catch(() => {})
    await page.waitForTimeout(800)
  }
  await shot(page, 'data-explorer-v3.png')

  // ── 5. AI Connector Builder — connectors marketplace (all connectors) ─
  await page.goto('/connectors')
  await page.waitForLoadState('networkidle')
  await page.waitForSelector('.animate-spin', { state: 'hidden', timeout: 8000 }).catch(() => {})
  await page.waitForTimeout(800)
  await shot(page, 'connector-gen-v3.png')

  // ── 6. MCP Server Generator — generate connector form ─────────────
  // Shows the form to generate a new MCP-compliant connector from API docs
  await page.goto('/connectors/generate')
  await page.waitForLoadState('networkidle')
  await page.waitForSelector('.animate-spin', { state: 'hidden', timeout: 8000 }).catch(() => {})
  await page.waitForTimeout(800)
  await shot(page, 'mcp-generator-v3.png')

  await context.close()
  console.log(`\nAll 6 screenshots saved to: ${OUT}`)
})
