import { test, expect, Page } from "@playwright/test"

async function setupAuth(page: Page) {
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

test.describe("E2E - PostgreSQL (docker) destination: Start & Test + Save Connection", () => {
  test.beforeEach(async ({ page }) => {
    await setupAuth(page)
    await page.goto("/connectors")
    await page.waitForLoadState("networkidle")
  })

  test("configures postgres-e2e destination and save succeeds", async ({ page }) => {
    // Filter to PostgreSQL connector
    const searchInput = page.getByPlaceholder("Search connectors...")
    await expect(searchInput).toBeVisible({ timeout: 10_000 })
    await searchInput.fill("postgres")

    // Open the PostgreSQL connector modal (card click)
    const pgCard = page.locator(".rounded-xl.border.cursor-pointer").first()
    await expect(pgCard).toBeVisible({ timeout: 10_000 })
    await pgCard.click()

    // Fill connection name
    const connectionName = `e2e-postgres-dest-${Date.now()}`
    await page.getByLabel(/Connection Name/i).fill(connectionName)

    // Choose Destination (Write Data)
    // Note: Connection type is a Radix Select.
    await page.getByText(/Connection Type/i).scrollIntoViewIfNeeded()
    await page.locator('[role="combobox"]').first().click()
    await page.getByRole("option", { name: /Destination/i }).click()

    // Fill Postgres config for the docker-compose.e2e.yml container
    await page.getByLabel(/^Host/i).fill("postgres-e2e")
    await page.getByLabel(/^Port/i).fill("5432")
    await page.getByLabel(/^Database/i).fill("e2e_db")
    await page.getByLabel(/^User/i).fill("e2e_user")
    await page.getByLabel(/^Password/i).fill("e2e_password")

    // Click Start & Test / Test Connection
    const testBtn = page.getByRole("button", { name: /Start & Test Connection|Test Connection/i })
    await expect(testBtn).toBeEnabled()
    await testBtn.click()

    // Wait for a success or failure banner to render
    const successBanner = page.getByText(/Connection test successful|Connection successful/i)
    const failureBanner = page.getByText(/Connection test failed|Test service returned error/i)

    await Promise.race([
      successBanner.waitFor({ state: "visible", timeout: 60_000 }),
      failureBanner.waitFor({ state: "visible", timeout: 60_000 }),
    ])

    await expect(failureBanner).toHaveCount(0)
    await expect(successBanner).toBeVisible()

    // Save the connection
    const saveBtn = page.getByRole("button", { name: /Save Connection/i })
    await expect(saveBtn).toBeEnabled()
    const createRespPromise = page.waitForResponse(
      (r) => r.url().includes("/api/v1/connections") && r.request().method() === "POST",
      { timeout: 30_000 }
    )
    await saveBtn.click()

    // Log the create response for easier debugging in CI/local runs
    const createResp = await createRespPromise
    const createBodyText = await createResp.text()
    if (process.env.E2E_DEBUG) {
      // eslint-disable-next-line no-console
      console.log(`[e2e] create connection status=${createResp.status()} body=${createBodyText}`)
    }

    // Redirect to /connections after save
    await expect(page).toHaveURL(/\/connections/i, { timeout: 30_000 })

    // Verify the new connection appears (best-effort; UI may paginate)
    await expect(page.getByText(connectionName)).toBeVisible({ timeout: 30_000 })
  })
})

