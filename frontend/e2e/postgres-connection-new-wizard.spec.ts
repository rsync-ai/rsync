import { test, expect, Page } from "@playwright/test"

async function setupAuth(page: Page) {
  await page.context().addCookies([
    {
      name: "auth_token",
      value: "Bearer dev-token",
      domain: "localhost",
      path: "/",
    },
  ])

  await page.addInitScript(() => {
    localStorage.setItem("auth_token", "Bearer dev-token")
    localStorage.setItem("user_id", "00000000-0000-0000-0000-000000000001")
  })
}

test.describe("Connections wizard - PostgreSQL destination", () => {
  test.beforeEach(async ({ page }) => {
    await setupAuth(page)
  })

  test("creates a postgres destination via /connections/new", async ({ page }) => {
    if (process.env.E2E_DEBUG) {
      page.on("request", (r) => {
        if (/\/api\/v1\/connections$/.test(r.url()) && r.method() === "POST") {
          // eslint-disable-next-line no-console
          console.log(`[e2e] -> ${r.method()} ${r.url()}`)
        }
      })
      page.on("response", (r) => {
        if (/\/api\/v1\/connections$/.test(r.url()) && r.request().method() === "POST") {
          // eslint-disable-next-line no-console
          console.log(`[e2e] <- ${r.status()} ${r.request().method()} ${r.url()}`)
        }
      })
    }

    await page.goto("/connections/new?connector_type=postgresql&type=destination")

    await expect(page.getByText("Connection Details")).toBeVisible({ timeout: 30_000 })

    const name = `e2e-postgres-wizard-${Date.now()}`
    await page.getByPlaceholder(/My .* Connection/i).fill(name)
    await page.getByLabel(/^Host/i).fill("postgres-e2e")
    await page.getByLabel(/^Port/i).fill("5432")
    await page.getByLabel(/^Database/i).fill("e2e_db")
    await page.getByLabel(/^User/i).fill("e2e_user")
    await page.getByLabel(/^Password/i).fill("e2e_password")

    const testBtn = page.getByRole("button", { name: /Test Connection/i })
    await expect(testBtn).toBeEnabled()
    await testBtn.click()
    await expect(page.getByRole("heading", { name: /Connection Test Successful/i })).toBeVisible({ timeout: 60_000 })

    const createReqPromise = page.waitForRequest(
      (r) => /\/api\/v1\/connections$/.test(r.url()) && r.method() === "POST",
      { timeout: 30_000 }
    )
    // Sonner toasts can overlay fixed buttons; dispatchEvent bypasses hit-testing.
    await page.getByRole("button", { name: /Save Connection/i }).dispatchEvent("click")

    const createReq = await createReqPromise
    const createResp = await createReq.response()
    expect(createResp?.ok()).toBeTruthy()

    // Wizard default returnTo is /connections
    await expect(page).toHaveURL(/\/connections/i, { timeout: 30_000 })
    await expect(page.getByText(name)).toBeVisible({ timeout: 30_000 })
  })
})

