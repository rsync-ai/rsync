/**
 * E2E: Missing-config (connection_config) UI loop
 *
 * Validates the UI "observe → fix → validate → resume" path for missing destination connection:
 * - Pipeline detail shows HITL panel + connection selector modal when state is waiting_for_user/connection_config
 * - User can create the missing destination connection via /connections/new deep-link (prefilled)
 * - After returning, user can select source+dest and submit, calling HITL resume endpoint
 *
 * We stub the pipeline state endpoint to deterministically force the HITL state,
 * but we use the real Connections API for creating connections.
 */

import { test, expect, type Page, type Route } from "@playwright/test"

const API_URL = process.env.API_URL || "http://localhost:5001"
const DEV_USER_ID = process.env.E2E_USER_ID || "00000000-0000-0000-0000-000000000001"

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

test.describe("HITL - connection_config (missing destination)", () => {
  test.beforeEach(async ({ page }) => {
    await setupAuth(page)
  })

  test("create missing Postgres destination and resume pipeline connections", async ({ page }) => {
    // Create a source connection (MySQL) via API so the selector can proceed.
    const mysqlName = `e2e-mysql-src-${Date.now()}`
    const mysqlResp = await page.request.post(`${API_URL}/api/v1/connections`, {
      headers: {
        "Content-Type": "application/json",
        "X-User-ID": DEV_USER_ID,
        Authorization: "Bearer dev-token",
      },
      data: {
        name: mysqlName,
        connection_type: "source",
        connector_type: "mysql",
        sync_mode: "batch",
        config: {
          host: "mysql-e2e",
          port: 3306,
          database: "e2e_db",
          user: "e2e_user",
          password: "e2e_password",
        },
      },
    })
    expect(mysqlResp.ok()).toBeTruthy()
    const mysqlConn = await mysqlResp.json()
    const mysqlId: string | undefined = mysqlConn?.id
    test.skip(!mysqlId, "Failed to create MySQL source connection")

    // Create a pipeline shell (we won't run it; we stub the state endpoint).
    const createPipelineResp = await page.request.post(`${API_URL}/api/v1/pipelines`, {
      headers: { "Content-Type": "application/json", "X-User-ID": DEV_USER_ID, Authorization: "Bearer dev-token" },
      data: {
        name: `E2E HITL Connections ${Date.now()}`,
        description: "Pipeline created by HITL connections E2E",
        request: "sync mysql to postgresql",
      },
    })
    expect(createPipelineResp.ok()).toBeTruthy()
    const createdPipeline = await createPipelineResp.json()
    const pipelineId: string | undefined = createdPipeline?.id
    test.skip(!pipelineId, "Failed to create pipeline for E2E")

    const stateUrl = `${API_URL}/api/v1/pipelines/${pipelineId}/state`
    const hitlUrl = `${API_URL}/api/v1/pipelines/${pipelineId}/hitl/connections`

    // Force pipeline into waiting_for_user with missing destination connection.
    await page.route(stateUrl, async (route: Route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          schema_version: 2,
          pipeline_id: pipelineId,
          execution_id: "e2e-exec-00000001",
          status: "waiting_for_user",
          current_stage: "connection_validation",
          message: "Configure required connections to continue",
          progress: { percent: 40, current_step: 4, total_steps: 10, stage: "connection_validation" },
          blocking_reason: {
            type: "connection_config",
            description: "Missing destination connection. Please create/select a PostgreSQL destination.",
            details: {
              user_request: "sync mysql to postgresql",
              source_type: "mysql",
              destination_type: "postgresql",
              missing_connections: [{ direction: "destination", connector_type: "postgresql" }],
              ui_hints: { widget: "connection_configurator" },
            },
          },
        }),
      })
    })

    // Capture resume payload and return success without depending on workflow internals.
    let submitted: any | null = null
    await page.route(hitlUrl, async (route: Route) => {
      const req = route.request()
      if (req.method().toUpperCase() === "POST") {
        try {
          submitted = (await req.postDataJSON()) as any
        } catch {
          submitted = null
        }
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            success: true,
            pipeline_id: pipelineId,
            execution_id: "e2e-exec-00000001",
            signal: "connections_configured",
          }),
        })
        return
      }
      await route.fallback()
    })

    // Go to pipeline detail; the connection selector should auto-open.
    await page.goto(`/pipelines/${pipelineId}`)
    await expect(page.locator("text=Pipeline Paused - Action Required")).toBeVisible({ timeout: 30_000 })

    const modalTitle = page.getByRole("heading", { name: "Configure Required Connections" })
    await expect(modalTitle).toBeVisible({ timeout: 30_000 })

    // Navigate to create a PostgreSQL destination connection.
    // - If there are no Postgres destinations, we show a dedicated CTA button.
    // - If there are existing ones, we still show a "Create new PostgreSQL connection" row.
    const createPgBtn = page.getByRole("button", { name: /Create PostgreSQL Connection/i }).first()
    const createPgRow = page.getByText(/Create new PostgreSQL connection/i).first()

    await Promise.race([
      createPgBtn.waitFor({ state: "visible", timeout: 10_000 }),
      createPgRow.waitFor({ state: "visible", timeout: 10_000 }),
    ])

    if (await createPgBtn.isVisible().catch(() => false)) {
      await createPgBtn.click()
    } else {
      await createPgRow.scrollIntoViewIfNeeded()
      await createPgRow.click()
    }

    // Should deep-link into /connections/new and preselect postgres + destination.
    await expect(page).toHaveURL(/\/connections\/new\?/i, { timeout: 30_000 })
    await expect(page.getByText("Connection Details")).toBeVisible({ timeout: 30_000 })
    await expect(page.getByRole("button", { name: "Change" })).toBeVisible({ timeout: 30_000 })
    // Best-effort: connector display name varies ("PostgreSQL", "Postgres", etc.)
    await expect(page.getByRole("heading", { name: /postgre/i }).first()).toBeVisible({ timeout: 30_000 })

    // Fill the connection details
    const pgName = `e2e-postgres-dest-${Date.now()}`
    await page.getByPlaceholder(/My .* Connection/i).fill(pgName)
    await page.getByLabel(/^Host/i).fill("postgres-e2e")
    await page.getByLabel(/^Port/i).fill("5432")
    await page.getByLabel(/^Database/i).fill("e2e_db")
    await page.getByLabel(/^User/i).fill("e2e_user")
    await page.getByLabel(/^Password/i).fill("e2e_password")

    // Test then save
    const testBtn = page.getByRole("button", { name: /Test Connection/i })
    await expect(testBtn).toBeEnabled()
    await testBtn.click()
    await expect(page.getByRole("heading", { name: /Connection Test Successful/i })).toBeVisible({ timeout: 60_000 })

    const saveBtn = page.getByRole("button", { name: /Save Connection/i })
    await expect(saveBtn).toBeEnabled()
    const createReqPromise = page.waitForRequest(
      (r) => /\/api\/v1\/connections$/.test(r.url()) && r.method() === "POST",
      { timeout: 30_000 }
    )
    // Sonner toasts can overlay fixed buttons; dispatchEvent bypasses hit-testing.
    await saveBtn.dispatchEvent("click")

    const createReq = await createReqPromise
    const createResp = await createReq.response()
    const status = createResp?.status() ?? -1
    const body = createResp ? await createResp.text() : ""
    if (process.env.E2E_DEBUG) {
      // eslint-disable-next-line no-console
      console.log(`[e2e] save connection response status=${status} body=${body}`)
    }
    expect(createResp?.ok()).toBeTruthy()

    // Should return to pipeline detail (returnTo from modal)
    await expect(page).toHaveURL(new RegExp(`/pipelines/${pipelineId}$`), { timeout: 30_000 })

    // Connection selector modal should re-appear (state still says waiting_for_user).
    await expect(modalTitle).toBeVisible({ timeout: 30_000 })

    // Continue should be enabled once both connections are selected; source should already exist.
    // Select the newly created Postgres destination if it's not auto-selected.
    const destOption = page.locator(`label:has-text("${pgName}")`).first()
    if (await destOption.isVisible().catch(() => false)) {
      await destOption.click()
    }

    const continueBtn = page.getByRole("button", { name: /Start Pipeline/i }).first()
    await expect(continueBtn).toBeEnabled({ timeout: 30_000 })
    await continueBtn.click()

    // Validate payload was submitted.
    await expect.poll(() => submitted, { timeout: 30_000 }).not.toBeNull()
    expect(String(submitted?.source_connection_id || "")).toContain(String(mysqlId))
    expect(String(submitted?.destination_connection_id || "")).toMatch(/[0-9a-f-]{36}/i)

    // Modal should close after success.
    await expect(modalTitle).toBeHidden({ timeout: 30_000 })
  })
})

