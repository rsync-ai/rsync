import { test, expect, Page } from '@playwright/test'

async function setupAuth(page: Page) {
  await page.goto('/login')
  await page.waitForLoadState('networkidle')
  // Use dev quick-fill if available, otherwise fill manually
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

test.describe('E2E - MySQL (docker) Start & Test Connection', () => {
  test.beforeEach(async ({ page }) => {
    await setupAuth(page)
    await page.goto('/connectors')
    await page.waitForLoadState('networkidle')
  })

  test('configures mysql-e2e and test succeeds (no 404)', async ({ page }) => {
    // Filter to MySQL connector
    const searchInput = page.getByPlaceholder('Search connectors...')
    await expect(searchInput).toBeVisible({ timeout: 10_000 })
    await searchInput.fill('mysql')

    // Open the MySQL connector modal
    const mysqlCard = page.locator('.rounded-xl.border.cursor-pointer').first()
    await expect(mysqlCard).toBeVisible({ timeout: 10_000 })
    await mysqlCard.click()

    // Fill connection name
    const connectionName = `e2e-mysql-${Date.now()}`
    await page.getByLabel(/Connection Name/i).fill(connectionName)

    // Choose Source (Read Data)
    // Note: Connection type is a Radix Select.
    await page.getByText(/Connection Type/i).scrollIntoViewIfNeeded()
    await page.locator('[role="combobox"]').first().click()
    await page.getByRole('option', { name: /Source/i }).click()

    // Fill MySQL config for the docker-compose.e2e.yml container (reachable from orchestrator network)
    await page.getByLabel(/^Host/i).fill('mysql-e2e')
    await page.getByLabel(/^Port/i).fill('3306')
    await page.getByLabel(/^Database/i).fill('e2e_db')
    await page.getByLabel(/^User/i).fill('e2e_user')
    await page.getByLabel(/^Password/i).fill('e2e_password')

    // Click Start & Test / Test Connection
    const testBtn = page.getByRole('button', { name: /Start & Test Connection|Test Connection/i })
    await expect(testBtn).toBeEnabled()
    await testBtn.click()

    // Assert we do not regress to 404
    await expect(page.getByText(/404 page not found|HTTP 404/i)).toHaveCount(0)

    // Wait for a success or failure banner to render
    const successBanner = page.getByText(/Connection test successful|Connection successful/i)
    const failureBanner = page.getByText(/Connection test failed|Test service returned error/i)

    await Promise.race([
      successBanner.waitFor({ state: 'visible', timeout: 60_000 }),
      failureBanner.waitFor({ state: 'visible', timeout: 60_000 }),
    ])

    await expect(failureBanner).toHaveCount(0)
    await expect(successBanner).toBeVisible()
  })
})

