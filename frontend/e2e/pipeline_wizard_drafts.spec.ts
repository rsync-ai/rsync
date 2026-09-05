import { expect, test } from "@playwright/test"

test("pipelines/new redirects to chat (wizard flow removed)", async ({ page }) => {
  // Login (dev default user from migrations)
  await page.goto("/login")

  const useDevButton = page.getByRole("button", { name: /^Use$/i })
  if (await useDevButton.count()) {
    await useDevButton.first().click()
  } else {
    await page.getByLabel(/email/i).fill("default@rsync-ai.local")
    await page.getByLabel(/password/i).fill("password123")
  }

  await page.getByRole("button", { name: /sign in/i }).click()
  await page.waitForLoadState("networkidle")

  // Legacy route -> chat-first creation flow
  await page.goto("/pipelines/new")
  await page.waitForURL(/\/chat/i)

  // Chat input should be present
  await expect(page.getByPlaceholder("Describe what data you want to move…")).toBeVisible()
})

