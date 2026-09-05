import { test, expect, type Page, type Route, type Request } from "@playwright/test"

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

type StubConnection = {
  id: string
  name: string
  connector_type: string
  type: "source" | "destination"
  status: string
}

function json(route: Route, status: number, body: unknown, extraHeaders?: Record<string, string>) {
  return route.fulfill({
    status,
    contentType: "application/json",
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type, Authorization, X-User-ID, X-Trace-ID, traceparent, Accept",
      "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
      ...extraHeaders,
    },
    body: JSON.stringify(body),
  })
}

test.describe("Explorer UI: NL → SQL corner cases (stubbed)", () => {
  test.beforeEach(async ({ page }) => {
    await setupAuth(page)

    const connections: StubConnection[] = [
      {
        id: "11111111-1111-1111-1111-111111111111",
        name: "MySQL E2E (stub)",
        connector_type: "mysql",
        type: "source",
        status: "active",
      },
    ]

    // Stub API gateway connections list (CORS + preflight).
    await page.route("**/api/v1/connections**", async (route: Route) => {
      const req = route.request()
      if (req.method() === "OPTIONS") {
        return route.fulfill({
          status: 204,
          headers: {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type, Authorization, X-User-ID, X-Trace-ID, traceparent, Accept",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
          },
        })
      }
      if (req.method() !== "GET") return json(route, 405, { error: "method not allowed" })
      return json(route, 200, { connections })
    })

    // Stub schema index for the selected connection.
    await page.route("**/api/explorer/schema-index/**", async (route: Route) => {
      return json(route, 200, {
        tables: [
          {
            name: "users",
            schema: "public",
            row_count: 1000,
            columns: [
              { name: "id", type: "int", is_primary_key: true },
              { name: "email", type: "varchar" },
              { name: "revenue", type: "decimal" },
              { name: "created_at", type: "timestamp" },
            ],
          },
          {
            name: "orders",
            schema: "public",
            row_count: 5000,
            columns: [
              { name: "id", type: "int", is_primary_key: true },
              { name: "user_id", type: "int" },
              { name: "amount", type: "decimal" },
              { name: "created_at", type: "timestamp" },
            ],
          },
        ],
        foreign_keys: [
          {
            from_schema: "public",
            from_table: "orders",
            from_column: "user_id",
            to_schema: "public",
            to_table: "users",
            to_column: "id",
            confidence: 0.95,
          },
        ],
      })
    })

    let lastResolveQuestion: string | null = null
    let lastGenerateQuestion: string | null = null
    let lastGenerateTables: string | null = null

    // Table resolver: force HITL when question includes "ambiguous".
    await page.route("**/api/explorer/nl/resolve-tables", async (route: Route) => {
      let body: any = {}
      try {
        body = route.request().postDataJSON() as any
      } catch {
        body = {}
      }
      lastResolveQuestion = String(body?.question || "")
      if (lastResolveQuestion.toLowerCase().includes("ambiguous")) {
        return json(route, 200, {
          needs_hitl: true,
          hitl_reason: "Ambiguous table match",
          confidence_overall: 0.55,
          candidates: [
            // High confidence so picker auto-selects this table (reduces flaky click targeting).
            { table: "users", schema_name: "public", confidence: 0.8, reason: "mentions users" },
            { table: "orders", schema_name: "public", confidence: 0.58, reason: "mentions orders" },
          ],
        })
      }

      // Deterministic switching: if the question mentions "orders" strongly, prefer orders.
      if (lastResolveQuestion.toLowerCase().includes("orders")) {
        return json(route, 200, {
          needs_hitl: false,
          confidence_overall: 0.92,
          candidates: [{ table: "orders", schema_name: "public", confidence: 0.92, reason: "mentions orders" }],
        })
      }

      return json(route, 200, {
        needs_hitl: false,
        confidence_overall: 0.9,
        candidates: [{ table: "users", schema_name: "public", confidence: 0.9, reason: "default" }],
      })
    })

    // SQL generator: echo tables, and return unsafe SQL if asked to "drop".
    await page.route("**/api/sql/generate", async (route: Route) => {
      let body: any = {}
      try {
        body = route.request().postDataJSON() as any
      } catch {
        body = {}
      }
      lastGenerateQuestion = String(body?.question || "")
      lastGenerateTables = String(body?.tables || "")
      const q = lastGenerateQuestion.toLowerCase()

      if (q.includes("drop")) {
        return json(route, 200, { sql: "DROP TABLE users;", confidence: 0.9, explanation: "unsafe" })
      }

      // Default safe SQL without LIMIT; UI will inject LIMIT from NL.
      if (lastGenerateTables.toLowerCase().includes("orders")) {
        return json(route, 200, {
          sql: "SELECT id, user_id, amount FROM orders ORDER BY amount DESC",
          confidence: 0.82,
          explanation: `tables=${lastGenerateTables}`,
        })
      }
      return json(route, 200, {
        sql: "SELECT id, email, revenue FROM users ORDER BY revenue DESC",
        confidence: 0.82,
        explanation: `tables=${lastGenerateTables}`,
      })
    })

    // Query execution stub (includes warnings + truncated + long cell).
    await page.route("**/api/explorer/query", async (route: Route) => {
      let body: any = {}
      try {
        body = route.request().postDataJSON() as any
      } catch {
        body = {}
      }
      const sql = String(body?.sql || "")
      return json(route, 200, {
        columns: ["id", "email", "note"],
        rows: [
          {
            id: 1,
            email: "a@example.com",
            note: "x".repeat(260),
          },
        ],
        row_count: 1,
        execution_time_ms: 12,
        truncated: true,
        warnings: [`Stubbed execution for sql=${sql.slice(0, 24)}...`],
      })
    })

    // Expose captured values to the test via window for assertions.
    // Store captured values after navigation (Playwright closures are fine; we assert via locators below).
    ;(page as any).__getCaptured = () => ({ lastResolveQuestion, lastGenerateQuestion, lastGenerateTables })
  })

  test("multiline NL generates SQL with correct LIMIT + clamps huge limits", async ({ page }) => {
    await page.goto("/explorer")

    const nl = page.locator("#explorer-nl")
    const sql = page.locator("#explorer-sql")
    const generateBtn = page.getByRole("button", { name: "Generate SQL", exact: true })

    await expect(nl).toBeVisible({ timeout: 30_000 })
    // Ensure schema UI is loaded from the stubbed schema-index.
    await expect(page.getByText("users").first()).toBeVisible({ timeout: 30_000 })

    const multiline = [
      "Show me the top 250 users by revenue",
      "- only for 2024",
      "- include email",
    ].join("\n")

    await nl.fill(multiline)
    await generateBtn.click()

    await expect(sql).toContainText(/LIMIT 250/i)

    // Verify the server saw newlines in the question.
    const captured = (page as any).__getCaptured?.()
    expect(String(captured?.lastGenerateQuestion || "")).toContain("\n")

    // Huge limit should clamp to 1000.
    await nl.fill("show me 999999 rows from users")
    await generateBtn.click()
    await expect(sql).toContainText(/LIMIT 1000/i)
  })

  test("empty/whitespace NL cannot generate; Cmd+Enter generates when present", async ({ page }) => {
    await page.goto("/explorer")

    const nl = page.locator("#explorer-nl")
    const sql = page.locator("#explorer-sql")
    const generateBtn = page.getByRole("button", { name: "Generate SQL", exact: true })

    await expect(nl).toBeVisible({ timeout: 30_000 })
    await nl.fill("   ")
    await expect(generateBtn).toBeDisabled()

    await nl.fill("Show me top 10 users")
    await nl.focus()
    await page.keyboard.press("Meta+Enter")
    await expect(sql).toContainText(/SELECT/i)
  })

  test("multiple NL prompts switch tables (no sticky previous selection)", async ({ page }) => {
    await page.goto("/explorer")

    const nl = page.locator("#explorer-nl")
    const sql = page.locator("#explorer-sql")
    const generateBtn = page.getByRole("button", { name: "Generate SQL", exact: true })

    await nl.fill("top 10 users by revenue")
    await generateBtn.click()
    await expect(sql).toContainText(/FROM users/i)
    const selectedSection = page.locator("div").filter({ hasText: /^Selected:/ }).first()
    await expect(selectedSection.getByText("public.users", { exact: true })).toBeVisible()

    await nl.fill("top 10 orders by amount")
    await generateBtn.click()
    // Because the user already had a selection, the UI should ask to confirm switching tables.
    await expect(page.getByRole("heading", { name: "Select Tables" })).toBeVisible({ timeout: 30_000 })
    await page.getByRole("button", { name: /Confirm Selection/i }).dispatchEvent("click")

    await expect(sql).toContainText(/FROM orders/i)
    await expect(selectedSection.getByText("public.orders", { exact: true })).toBeVisible()
  })

  test("metric ambiguity: 'total orders' asks count vs sum (HITL)", async ({ page }) => {
    await page.goto("/explorer")

    const nl = page.locator("#explorer-nl")
    const sql = page.locator("#explorer-sql")
    const generateBtn = page.getByRole("button", { name: "Generate SQL", exact: true })

    await nl.fill("show me total orders per month")
    await generateBtn.click()

    await expect(page.getByRole("heading", { name: "Quick clarification" })).toBeVisible({ timeout: 30_000 })
    // Default is COUNT (recommended). Proceed.
    await page.getByRole("button", { name: "Use this metric" }).dispatchEvent("click")

    await expect(sql).toContainText(/SELECT/i)
    // With our stubs, orders should be selected for "orders" questions.
    await expect(sql).toContainText(/FROM orders/i)
  })

  test("unsafe generated SQL is blocked from execution", async ({ page }) => {
    await page.goto("/explorer")

    const nl = page.locator("#explorer-nl")
    const sql = page.locator("#explorer-sql")
    const runBtn = page.getByRole("button", { name: "Run Query" })

    await nl.fill("drop users table")
    await page.getByRole("button", { name: "Generate SQL" }).click()

    await expect(sql).toContainText(/DROP TABLE/i)
    await expect(runBtn).toBeDisabled()
    await expect(page.getByText(/Only SELECT queries can be executed/i)).toBeVisible()
  })

  test("HITL table picker opens for ambiguous prompts and continues to SQL generation", async ({ page }) => {
    await page.goto("/explorer")

    const nl = page.locator("#explorer-nl")
    const sql = page.locator("#explorer-sql")

    await nl.fill("ambiguous: show totals by user and order")
    await page.getByRole("button", { name: "Generate SQL", exact: true }).click()

    // HITL picker appears: pick "public.users" then confirm.
    await expect(page.getByRole("heading", { name: "Select Tables" })).toBeVisible({ timeout: 30_000 })
    const confirmBtn = page.getByRole("button", { name: /Confirm Selection/i })
    await expect(confirmBtn).toBeEnabled({ timeout: 30_000 })
    await confirmBtn.dispatchEvent("click")

    await expect(sql).toContainText(/SELECT/i)
  })

  test("execute query renders results, truncated badge, warnings, and cell viewer for long values", async ({ page }) => {
    await page.goto("/explorer")

    const nl = page.locator("#explorer-nl")
    const sql = page.locator("#explorer-sql")

    await nl.fill("Show me 10 users")
    await page.getByRole("button", { name: "Generate SQL" }).click()

    // Ensure SQL is runnable
    await expect(page.getByRole("button", { name: "Run Query" })).toBeEnabled()
    await page.getByRole("button", { name: "Run Query" }).click()

    await expect(page.getByText("Results")).toBeVisible({ timeout: 30_000 })
    await expect(page.getByText("Truncated")).toBeVisible()

    // Warning block should render
    await expect(page.getByText(/Stubbed execution/)).toBeVisible()

    // Long cell opens viewer
    const longCell = page.locator('td[title="Click to view full value"]').first()
    await expect(longCell).toBeVisible()
    await longCell.click()
    await expect(page.getByText("Full cell value")).toBeVisible()
  })

  test("query history: executing adds entry and clicking it loads NL + SQL", async ({ page }) => {
    await page.goto("/explorer")

    const nl = page.locator("#explorer-nl")
    const sql = page.locator("#explorer-sql")
    const generateBtn = page.getByRole("button", { name: "Generate SQL", exact: true })

    const question = "Show me 5 users"
    await nl.fill(question)
    await generateBtn.click()
    await page.getByRole("button", { name: "Run Query" }).click()

    await expect(page.getByText("Results")).toBeVisible({ timeout: 30_000 })

    // Open the Query History tab and click the newest entry.
    await page.getByRole("tab", { name: "Query History" }).click()
    const entry = page.getByRole("button").filter({ hasText: question }).first()
    await expect(entry).toBeVisible({ timeout: 30_000 })
    await entry.click()

    // Loaded values should appear in editors.
    await expect(nl).toHaveValue(question)
    await expect(sql).toContainText(/SELECT/i)
  })
})

