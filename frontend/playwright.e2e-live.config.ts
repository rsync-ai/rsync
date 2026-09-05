// TEMP config: run e2e specs against an ALREADY-RUNNING staging stack (no webServer).
import { defineConfig, devices } from "@playwright/test"

export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  workers: 1,
  reporter: [["list"]],
  use: {
    baseURL: process.env.FRONTEND_URL || "http://localhost:8080",
    trace: "off",
    screenshot: "only-on-failure",
    viewport: { width: 1280, height: 720 },
    actionTimeout: 15000,
    navigationTimeout: 30000,
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  timeout: 600_000,
  expect: { timeout: 15000 },
})
