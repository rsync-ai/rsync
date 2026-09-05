/**
 * Demo Screenshots - captures key UI states for recording prep
 * Run: npx playwright test e2e/demo_screenshots.spec.ts --reporter=list
 */

import { test, type Page } from '@playwright/test'
import * as path from 'path'
import * as fs from 'fs'

const OUT = path.join(__dirname, '..', 'demo-screenshots')

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
}

async function ss(page: Page, name: string) {
  if (!fs.existsSync(OUT)) fs.mkdirSync(OUT, { recursive: true })
  await page.screenshot({ path: path.join(OUT, `${name}.png`), fullPage: true })
}

test.describe('Demo screenshots', () => {
  test.setTimeout(180000)

  test('01-login', async ({ page }) => {
    await page.goto('/login')
    await page.waitForLoadState('networkidle')
    await ss(page, '01-login')
  })

  test('02-home-dashboard', async ({ page }) => {
    await login(page)
    await page.goto('/')
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(1500)
    await ss(page, '02-home-dashboard')
  })

  test('03-connectors', async ({ page }) => {
    await login(page)
    await page.goto('/connectors')
    await page.waitForLoadState('networkidle')
    await page.waitForSelector('.animate-spin', { state: 'hidden', timeout: 10000 }).catch(() => {})
    await page.waitForTimeout(1000)
    await ss(page, '03-connectors')
  })

  test('04-connectors-mysql-modal', async ({ page }) => {
    await login(page)
    await page.goto('/connectors')
    await page.waitForSelector('.animate-spin', { state: 'hidden', timeout: 10000 }).catch(() => {})
    await page.waitForTimeout(500)
    // Search for MySQL
    const search = page.getByPlaceholder('Search connectors...')
    await search.fill('mysql')
    await page.waitForTimeout(500)
    // Open first result
    const card = page.locator('.rounded-xl.border.cursor-pointer').first()
    if (await card.isVisible({ timeout: 3000 }).catch(() => false)) {
      await card.click()
      await page.waitForTimeout(500)
    }
    await ss(page, '04-connectors-mysql-modal')
  })

  test('05-connections-list', async ({ page }) => {
    await login(page)
    await page.goto('/connections')
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(1500)
    await ss(page, '05-connections-list')
  })

  test('06-new-connection-direction', async ({ page }) => {
    await login(page)
    await page.goto('/connections/new')
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(1000)
    await ss(page, '06-new-connection-direction')
  })

  test('07-new-connection-mysql-config', async ({ page }) => {
    await login(page)
    await page.goto('/connections/new')
    await page.waitForLoadState('networkidle')
    // Select Source
    await page.locator('button').filter({ hasText: /Source.*connectors/i }).first().click()
    await page.waitForSelector('.animate-spin', { state: 'hidden', timeout: 10000 }).catch(() => {})
    await page.waitForTimeout(500)
    // Click MySQL connector
    const mysqlBtn = page.locator('button.text-left').filter({ hasText: /mysql/i }).first()
    if (await mysqlBtn.isVisible({ timeout: 5000 }).catch(() => false)) {
      await mysqlBtn.click()
      await page.waitForTimeout(500)
    }
    await ss(page, '07-new-connection-mysql-config')
  })

  test('08-chat-home', async ({ page }) => {
    await login(page)
    await page.goto('/chat')
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(2000)
    await ss(page, '08-chat-home')
  })

  test('09-chat-pipeline-creation', async ({ page }) => {
    await login(page)
    await page.goto('/chat')
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(2000)
    // Click MySQL → Postgres quick action
    const btn = page.locator('button').filter({ hasText: /MySQL.*Postgres/i }).first()
    if (await btn.isVisible({ timeout: 5000 }).catch(() => false)) {
      await btn.click()
      await page.waitForTimeout(2000)
    }
    await ss(page, '09-chat-mysql-postgres-selected')
    // Select Batch mode
    const batchBtn = page.getByRole('button', { name: /Batch.*historical|Batch.*incremental/i }).first()
    if (await batchBtn.isVisible({ timeout: 10000 }).catch(() => false)) {
      await ss(page, '09b-chat-sync-mode-selection')
      await batchBtn.click()
      await page.waitForTimeout(500)
      // Start pipeline
      const startBtn = page.getByRole('button', { name: /Start pipeline/i })
      if (await startBtn.isEnabled({ timeout: 3000 }).catch(() => false)) {
        await startBtn.click()
        await page.waitForTimeout(3000)
        await ss(page, '09c-chat-pipeline-starting')
        // Wait for pipeline tab
        const pipelineTab = page.getByRole('tab', { name: 'Pipeline' })
        if (await pipelineTab.isVisible({ timeout: 15000 }).catch(() => false)) {
          await ss(page, '09d-chat-pipeline-running')
        }
      }
    }
  })

  test('10-pipelines-list', async ({ page }) => {
    await login(page)
    await page.goto('/pipelines')
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(1500)
    await ss(page, '10-pipelines-list')
  })

  test('11-cdc-pipeline', async ({ page }) => {
    await login(page)
    await page.goto('/chat')
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(2000)
    // Click MySQL → Postgres quick action
    const btn = page.locator('button').filter({ hasText: /MySQL.*Postgres/i }).first()
    if (await btn.isVisible({ timeout: 5000 }).catch(() => false)) {
      await btn.click()
      await page.waitForTimeout(2000)
    }
    // Select CDC mode
    const cdcBtn = page.getByRole('button', { name: /CDC.*snapshot.*changes/i }).first()
    if (await cdcBtn.isVisible({ timeout: 10000 }).catch(() => false)) {
      await ss(page, '11a-chat-cdc-mode-visible')
      await cdcBtn.click()
      await page.waitForTimeout(500)
      const startBtn = page.getByRole('button', { name: /Start pipeline/i })
      if (await startBtn.isEnabled({ timeout: 3000 }).catch(() => false)) {
        await startBtn.click()
        await page.waitForTimeout(3000)
        await ss(page, '11b-chat-cdc-pipeline-starting')
        const pipelineTab = page.getByRole('tab', { name: 'Pipeline' })
        if (await pipelineTab.isVisible({ timeout: 15000 }).catch(() => false)) {
          await ss(page, '11c-chat-cdc-pipeline-running')
        }
      }
    }
  })
})
