import { test, expect } from "@playwright/test"

test.describe("Chat: multiple prompts in one session", () => {
  test.beforeEach(async ({ page }) => {
    // Bypass auth in local/dev.
    await page.addInitScript(() => {
      localStorage.setItem("auth_token", "Bearer dev-token")
      localStorage.setItem("user_id", "00000000-0000-0000-0000-000000000001")
    })
  })

  async function sendChat(page: any, msg: string) {
    const input = page.getByPlaceholder("Describe what data you want to move…")
    await expect(input).toBeVisible()
    await input.fill(msg)
    await page.keyboard.press("Enter")
  }

  test("new pipeline prompt overrides old confirmation intent", async ({ page }) => {
    await page.goto("/chat")

    // 1) Start a pipeline request (lands in confirmation state)
    await sendChat(page, "sync mysql to postgresql")
    await expect(page.getByText(/I understood:.*MySQL.*PostgreSQL/i)).toBeVisible({ timeout: 30_000 })
    await expect(page.getByText("Choose Sync Mode").first()).toBeVisible({ timeout: 30_000 })

    // 2) Without answering Yes/No, send a new prompt with a multi-word connector.
    await sendChat(page, "mysql  to  aws  s3")

    // Must NOT remain stuck on Postgres.
    await expect(page.getByText(/I understood:.*MySQL.*Amazon S3/i)).toBeVisible({ timeout: 30_000 })
  })

  test("picking a mode does not auto-start pipeline", async ({ page }) => {
    await page.goto("/chat")
    await sendChat(page, "sync mysql to postgresql")
    await expect(page.getByText("Choose Sync Mode").first()).toBeVisible({ timeout: 30_000 })

    // Click an option
    await page.getByText("CDC (changes only)").first().click()

    // Start button should now be enabled, but pipeline should NOT start until clicked.
    const startBtn = page.getByRole("button", { name: "Start pipeline" }).first()
    await expect(startBtn).toBeEnabled()
    await expect(page.getByText(/Pipeline started!/i)).toHaveCount(0)
  })
})

