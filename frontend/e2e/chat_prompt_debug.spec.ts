import { test } from "@playwright/test"

test("debug: /chat?prompt should not auto-create pipeline", async ({ page }) => {
  page.on("console", (msg) => {
    // eslint-disable-next-line no-console
    console.log("browser:", msg.type(), msg.text())
  })

  page.on("request", (req) => {
    const url = req.url()
    if (
      req.method() !== "GET" ||
      url.includes("/api/") ||
      url.includes("/api/v1/") ||
      url.includes("pipeline") ||
      url.includes("draft")
    ) {
      // eslint-disable-next-line no-console
      console.log("req:", req.method(), url, req.postData() || "")
    }
  })

  page.on("response", async (res) => {
    const url = res.url()
    if (
      url.includes("/api/") ||
      url.includes("/api/v1/") ||
      url.includes("pipeline") ||
      url.includes("draft")
    ) {
      // eslint-disable-next-line no-console
      console.log("res:", res.status(), url)
    }
  })

  await page.addInitScript(() => {
    localStorage.setItem("auth_token", "Bearer dev-token")
    localStorage.setItem("user_id", "00000000-0000-0000-0000-000000000001")
  })

  await page.goto("/chat?prompt=sync%20mysql%20to%20s3")
  await page.waitForTimeout(5000)
})

