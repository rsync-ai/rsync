/**
 * Demo video recording — captures a real product walkthrough as a WebM video.
 * Run: npx playwright test e2e/record_demo_video.spec.ts --reporter=list
 * Output: frontend/website-screenshots/demo-raw.webm (then converted to mp4)
 */

import { test, chromium } from '@playwright/test'
import * as path from 'path'
import * as fs from 'fs'

const OUT = path.join(__dirname, '..', 'website-screenshots')

test('record product demo', async () => {
  if (!fs.existsSync(OUT)) fs.mkdirSync(OUT, { recursive: true })

  // Remove any existing demo-raw.webm so the rename step can find the new recording unambiguously
  const existingRaw = path.join(OUT, 'demo-raw.webm')
  if (fs.existsSync(existingRaw)) fs.unlinkSync(existingRaw)

  const browser = await chromium.launch({ headless: true })
  const context = await browser.newContext({
    viewport: { width: 1280, height: 720 },
    recordVideo: {
      dir: OUT,
      size: { width: 1280, height: 720 },
    },
  })
  const page = await context.newPage()

  // ── 1. Login ──
  await page.goto('http://localhost:3000/login')
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
  await page.waitForTimeout(1500)

  // ── 2. Chat home — show quick-start grid ──
  await page.goto('http://localhost:3000/chat')
  await page.waitForLoadState('networkidle')
  await page.waitForTimeout(2500)

  // ── 3. Click MySQL → Postgres quick action ──
  const mysqlBtn = page.locator('button').filter({ hasText: /MySQL.*Postgres/i }).first()
  if (await mysqlBtn.isVisible({ timeout: 5000 }).catch(() => false)) {
    await mysqlBtn.click()
  }
  await page.waitForTimeout(2500)

  // ── 4. Wait for HITL sync mode panel ──
  const batchBtn = page.getByRole('button', { name: /Batch.*historical|Batch.*incremental/i }).first()
  const hitlVisible = await batchBtn.isVisible({ timeout: 60000 }).catch(() => false)
  if (hitlVisible) {
    await page.waitForTimeout(2000) // let user "read" the panel
    await batchBtn.click()
    await page.waitForTimeout(1000)

    // ── 5. Start pipeline ──
    const startBtn = page.getByRole('button', { name: /Start pipeline/i })
    if (await startBtn.isEnabled({ timeout: 5000 }).catch(() => false)) {
      await startBtn.click()
      await page.waitForTimeout(3000)
    }
  }

  // ── 6. Pipeline running — Pipeline tab ──
  const pipelineTab = page.getByRole('tab', { name: 'Pipeline' })
  if (await pipelineTab.isVisible({ timeout: 20000 }).catch(() => false)) {
    await page.waitForTimeout(3000)
  }

  // ── 7. Connectors catalog ──
  await page.goto('http://localhost:3000/connectors')
  await page.waitForLoadState('networkidle')
  await page.waitForSelector('.animate-spin', { state: 'hidden', timeout: 10000 }).catch(() => {})
  await page.waitForTimeout(2000)

  // ── 8. Pipelines list ──
  await page.goto('http://localhost:3000/pipelines')
  await page.waitForLoadState('networkidle')
  await page.waitForTimeout(2000)

  await context.close()
  await browser.close()

  // Find the newly recorded .webm (demo-raw.webm was deleted above, so any .webm here is new)
  const videos = fs.readdirSync(OUT).filter(f => f.endsWith('.webm') && f !== 'demo-raw.webm')
  if (videos.length > 0) {
    // Pick most recently modified in case multiple exist
    const newest = videos
      .map(f => ({ f, mtime: fs.statSync(path.join(OUT, f)).mtimeMs }))
      .sort((a, b) => b.mtime - a.mtime)[0].f
    fs.renameSync(path.join(OUT, newest), path.join(OUT, 'demo-raw.webm'))
    console.log('Video saved: demo-raw.webm')
  } else {
    console.error('No new .webm found — recording may have failed')
  }
})
