import { test, expect, type Page } from "@playwright/test"

async function loginWithRealCredentials(page: Page) {
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

test.describe("Chat-first pipeline flow", () => {
  test("prompt → pipeline view renders (no draft flow)", async ({ page }) => {
    page.on("console", (msg) => {
      // eslint-disable-next-line no-console
      console.log("browser:", msg.type(), msg.text())
    })
    page.on("pageerror", (err) => {
      // eslint-disable-next-line no-console
      console.log("pageerror:", err.message)
    })

    await loginWithRealCredentials(page)
    await page.goto("/chat")
    await page.waitForLoadState("networkidle")
    // Wait for home hero to render
    await page.waitForTimeout(2000)

    // Click the MySQL → Postgres quick pipeline button (fastest path to pipeline creation)
    const mysqlPostgresBtn = page.locator('button').filter({ hasText: /MySQL.*Postgres|Postgres.*MySQL/i }).first()
    const hasMysqlPostgres = await mysqlPostgresBtn.isVisible({ timeout: 5000 }).catch(() => false)

    if (hasMysqlPostgres) {
      await mysqlPostgresBtn.click()
    } else {
      // Fallback: use the home input form
      const chatInput = page.getByPlaceholder(/sync mysql to s3|describe what data/i).first()
      await expect(chatInput).toBeVisible({ timeout: 10000 })
      await chatInput.fill("sync mysql to postgresql")
      await page.keyboard.press("Enter")
    }

    // Wait for the HITL sync mode selection card to appear
    const batchBtn = page.getByRole("button", { name: /Batch.*historical|Batch.*incremental/i }).first()
    await expect(batchBtn).toBeVisible({ timeout: 60_000 })

    // Select "Batch" mode
    await batchBtn.click()

    // Start pipeline button should now be enabled
    const startBtn = page.getByRole("button", { name: /Start pipeline/i })
    await expect(startBtn).toBeEnabled({ timeout: 5000 })
    await startBtn.click()

    // Pipeline tab should appear in right panel once pipeline is created
    await expect(page.getByRole("tab", { name: "Pipeline" })).toBeVisible({ timeout: 60_000 })

    // Chat message confirms pipeline started
    await expect(page.getByText(/Pipeline started|Syncing.*MySQL.*PostgreSQL/i).first()).toBeVisible({ timeout: 10_000 })
  })
})

