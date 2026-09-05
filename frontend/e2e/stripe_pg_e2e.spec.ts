/**
 * TEMP verification spec (Stripe source -> SQL destination, full E2E).
 * Drives the REAL staging UI at FRONTEND_URL (default :8080) and proves data lands.
 * Destination is env-parameterized so the same spec proves pg_dest AND mysql_dest.
 *
 * Strategy (deterministic, avoids NL-parse + table-selection HITL footguns):
 *   1. UI login (best-effort; dev-bypass tolerated).
 *   2. Create pipeline via authenticated request context with EXPLICIT
 *      source_connection=stripe, destination_connection=$DEST_CONN, sync_mode=batch,
 *      selected_tables=<all 8>, destination_namespace=$DEST_SCHEMA (isolated).
 *   3. Open /pipelines/{id} in the UI, screenshot.
 *   4. Trigger run (allow_draft for first run of a brand-new connector).
 *   5. Poll /state to terminal status (completed/failed/stopped).
 *   6. Discover landed schema + verify rows per resource via explorer/query.
 *
 * Run (Postgres):
 *   cd frontend && FRONTEND_URL=http://localhost:8080 \
 *     DEST_CONN=pg_dest DEST_CONN_ID=4be2d91c-74c0-43d7-9322-7fbb37c8d6cf DEST_SCHEMA=stripe_e2e \
 *     npx playwright test e2e/stripe_pg_e2e.spec.ts --config=playwright.e2e-live.config.ts --reporter=list
 * Run (MySQL):
 *   ... DEST_CONN=mysql_dest DEST_CONN_ID=79eb1ac6-f448-4e97-b49a-a3f540c9c161 DEST_SCHEMA=stripe_e2e ...
 */
import { test, expect, type Page, type APIRequestContext } from "@playwright/test"

const DEST_CONN = process.env.DEST_CONN || "pg_dest"
const DEST_CONN_ID = process.env.DEST_CONN_ID || "4be2d91c-74c0-43d7-9322-7fbb37c8d6cf"
const DEST_SCHEMA = process.env.DEST_SCHEMA || "stripe_e2e"
const RESOURCES = [
  "customers", "products", "prices", "charges",
  "payment_intents", "invoices", "subscriptions", "refunds",
] as const
// Source-of-truth counts captured live from Stripe immediately before this run.
const SOURCE_COUNTS: Record<string, number> = {
  customers: 12, products: 8, prices: 12, charges: 15,
  payment_intents: 11, invoices: 7, subscriptions: 3, refunds: 3,
}

async function bestEffortLogin(page: Page) {
  try {
    await page.goto("/login", { waitUntil: "domcontentloaded", timeout: 20000 })
    const useBtn = page.locator('button:has-text("Use")')
    if (await useBtn.isVisible({ timeout: 3000 }).catch(() => false)) {
      await useBtn.click()
    } else {
      const email = page.getByLabel(/Email/i)
      if (await email.isVisible({ timeout: 2000 }).catch(() => false)) {
        await email.fill("default@rsync-ai.local")
        await page.getByLabel(/Password/i).fill("password123")
      }
    }
    const signIn = page.getByRole("button", { name: /Sign in/i })
    if (await signIn.isVisible({ timeout: 2000 }).catch(() => false)) {
      await signIn.click()
      await page.waitForURL((u) => !u.pathname.includes("/login"), { timeout: 15000 }).catch(() => {})
    }
    console.log("[login] completed (or dev-bypass; proceeding)")
  } catch (e) {
    console.log("[login] skipped:", (e as Error).message)
  }
}

async function queryRows(req: APIRequestContext, sql: string): Promise<Record<string, unknown>[] | null> {
  const r = await req.post("/api/v1/explorer/query", { data: { connection_id: DEST_CONN_ID, sql } })
  if (!r.ok()) {
    console.log(`[explorer] ${r.status()} for: ${sql} -> ${(await r.text()).slice(0, 200)}`)
    return null
  }
  return (await r.json()).rows || []
}

async function queryCount(req: APIRequestContext, schemaTable: string): Promise<number | null> {
  const rows = await queryRows(req, `SELECT count(*) AS c FROM ${schemaTable}`)
  if (rows === null) return null
  if (!rows.length) return 0
  const v = Object.values(rows[0])[0]
  return typeof v === "number" ? v : parseInt(String(v), 10)
}

test.describe(`Stripe -> ${DEST_CONN} E2E (real run)`, () => {
  test(`create, run, and verify all 8 Stripe resources land in ${DEST_CONN}`, async ({ page, request }) => {
    test.setTimeout(600_000) // 10 min: real batch run of 8 resources

    page.on("pageerror", (err) => console.log("pageerror:", err.message))

    await bestEffortLogin(page)

    // ---- 1. Create pipeline (deterministic; allow_draft for brand-new connector) ----
    const name = `Stripe->${DEST_CONN} E2E ${Date.now()}`
    const createResp = await request.post("/api/v1/pipelines?allow_draft=true", {
      data: {
        name,
        description: `TEMP E2E: prove Stripe source lands all 8 resources in ${DEST_CONN}`,
        request:
          "Sync all Stripe resources (customers, products, prices, charges, payment_intents, invoices, subscriptions, refunds) as a batch load.",
        source_connection: "stripe",
        destination_connection: DEST_CONN,
        sync_mode: "batch",
        selected_tables: [...RESOURCES],
        destination_namespace: DEST_SCHEMA,
      },
    })
    console.log("[create] status", createResp.status())
    expect(createResp.ok(), `create failed: ${await createResp.text()}`).toBeTruthy()
    const created = await createResp.json()
    const pipelineId: string = created.id || created.pipeline_id || created.pipeline?.id
    console.log("[create] pipeline_id", pipelineId)
    expect(pipelineId, "no pipeline id returned").toBeTruthy()

    // ---- 2. Open pipeline detail in the UI ----
    await page.goto(`/pipelines/${pipelineId}`, { waitUntil: "domcontentloaded" })
    await page.waitForTimeout(3000)
    await page.screenshot({ path: `e2e/_artifacts/stripe_${DEST_CONN}_01_created.png`, fullPage: true }).catch(() => {})

    // ---- 3. Trigger run (deterministic; run endpoint also gates on draft) ----
    const runResp = await request.post(`/api/v1/pipelines/${pipelineId}/run?allow_draft=true`, {
      data: { run_mode: "reload", ack_warnings: true },
    })
    console.log("[run] status", runResp.status(), (await runResp.text()).slice(0, 300))
    expect(runResp.ok(), "run trigger failed").toBeTruthy()
    await page.waitForTimeout(2000)
    await page.screenshot({ path: `e2e/_artifacts/stripe_${DEST_CONN}_02_running.png`, fullPage: true }).catch(() => {})

    // ---- 4. Poll to terminal status ----
    const terminal = new Set(["completed", "failed", "stopped"])
    let status = "unknown"
    let lastBody: Record<string, unknown> | null = null
    const deadline = Date.now() + 8 * 60 * 1000
    while (Date.now() < deadline) {
      const sr = await request.get(`/api/v1/pipelines/${pipelineId}/state`)
      if (sr.ok()) {
        const sj = await sr.json()
        lastBody = sj
        status = (sj.status || sj.state || "").toString()
        console.log(`[poll] status=${status} stage=${sj.current_stage || ""} pct=${sj.progress?.percent ?? ""}`)
        if (terminal.has(status)) break
      } else {
        console.log("[poll] state http", sr.status())
      }
      await page.waitForTimeout(5000)
    }
    await page.screenshot({ path: `e2e/_artifacts/stripe_${DEST_CONN}_03_final.png`, fullPage: true }).catch(() => {})
    console.log("[state-final]", JSON.stringify(lastBody).slice(0, 500))

    // ---- 5. Discover landed schema + verify rows per resource ----
    const inList = RESOURCES.map((r) => `'${r}'`).join(",")
    const discRows = await queryRows(
      request,
      `SELECT table_schema, table_name FROM information_schema.tables WHERE table_name IN (${inList})`
    )
    const schemaOf: Record<string, string> = {}
    for (const row of discRows || []) {
      const tn = String(row.table_name ?? row.TABLE_NAME)
      const ts = String(row.table_schema ?? row.TABLE_SCHEMA)
      if (!schemaOf[tn] || ts === DEST_SCHEMA) schemaOf[tn] = ts
    }
    const landed: Record<string, number | null> = {}
    for (const res of RESOURCES) {
      const sch = schemaOf[res] || DEST_SCHEMA
      landed[res] = await queryCount(request, `${sch}.${res}`)
    }

    const summary = {
      pipeline_id: pipelineId,
      destination: DEST_CONN,
      final_status: status,
      landed_schemas: schemaOf,
      comparison: RESOURCES.map((r) => ({
        resource: r,
        source: SOURCE_COUNTS[r],
        landed: landed[r],
        match: landed[r] === SOURCE_COUNTS[r],
      })),
    }
    console.log("\n===== STRIPE->" + DEST_CONN.toUpperCase() + " E2E SUMMARY =====")
    console.log(JSON.stringify(summary, null, 2))
    console.log("===== END SUMMARY =====\n")

    // ---- 6. Assertions ----
    expect(status, `pipeline did not complete (status=${status})`).toBe("completed")
    for (const res of RESOURCES) {
      expect(landed[res], `resource ${res} landed 0 / missing`).toBeGreaterThan(0)
    }
  })
})
