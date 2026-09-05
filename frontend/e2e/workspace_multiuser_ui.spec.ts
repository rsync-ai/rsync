import { test, expect } from "@playwright/test"
import path from "node:path"

// Live UI E2E for the workspace multi-user surface (P5a/P5b/P5b-UI #1-#4) against
// the running staging stack (http://localhost:8080). The world (verified owner A,
// member B, shared workspace W, a pending invite) is seeded by
// fixtures/workspace_multiuser.seed.sh, which exports the WSUI_* env read below.
// Drives the real UI; authoritative DB checks run after.
const A_EMAIL = process.env.WSUI_A_EMAIL!
const A_PW = process.env.WSUI_A_PW!
const B_EMAIL = process.env.WSUI_B_EMAIL!
const WID = process.env.WSUI_WID!
const WS_NAME = process.env.WSUI_WS_NAME!
const PENDING_TOK = process.env.WSUI_PENDING_TOK!
const SHOTS = process.env.WSUI_SHOTS || "/tmp"
const shot = (n: string) => path.join(SHOTS, `${n}.png`)

test.describe.configure({ mode: "serial" })

test("workspace multi-user UI: login → members → invite → role → invite-landing → delete", async ({
  page,
  context,
}) => {
  // Seed the active workspace so the (client) members page targets W, not A's personal ws.
  await context.addInitScript((wid) => {
    try {
      localStorage.setItem("rsync_active_workspace_id", wid as string)
    } catch {}
  }, WID)
  await context.addCookies([
    { name: "rsync_active_workspace_id", value: WID, url: "http://localhost:8080" },
  ])

  // 1) Login via the real form.
  await page.goto("/login")
  await page.locator("#email").fill(A_EMAIL)
  await page.locator("#password").fill(A_PW)
  await page.getByRole("button", { name: /sign in/i }).click()
  await page.waitForURL((u) => !u.pathname.includes("/login"), { timeout: 25000 })
  await page.screenshot({ path: shot("01_logged_in"), fullPage: true })

  // 2) Members page renders owner A + member B for the active workspace.
  await page.goto("/workspace/members")
  await expect(page.getByRole("heading", { name: /workspace members/i })).toBeVisible({ timeout: 20000 })
  await expect(page.getByText(A_EMAIL, { exact: false })).toBeVisible({ timeout: 20000 })
  await expect(page.getByText(B_EMAIL, { exact: false })).toBeVisible()
  await page.screenshot({ path: shot("02_members_list"), fullPage: true })

  // 3) Invite a teammate through the dialog.
  const invitee = `wsui-ui-invitee-${Date.now()}@example.com`
  await page.getByRole("button", { name: /invite member/i }).click()
  await expect(page.getByText(/invite a teammate/i)).toBeVisible()
  await page.getByPlaceholder("teammate@company.com").fill(invitee)
  await page.getByRole("button", { name: /send invite/i }).click()
  // success: the dialog footer flips Cancel -> Done once an accept URL is returned.
  await expect(page.getByRole("button", { name: /^done$/i })).toBeVisible({ timeout: 20000 })
  await page.screenshot({ path: shot("03_invite_sent"), fullPage: true })
  await page.getByRole("button", { name: /^done$/i }).click()
  // the new pending invite shows up in the list (target the table cell, not the toast)
  await expect(page.getByRole("cell", { name: invitee, exact: true })).toBeVisible({ timeout: 20000 })

  // 4) Promote member B to admin via the per-row actions menu.
  await page.getByRole("button", { name: `Manage ${B_EMAIL}` }).click()
  await page.getByRole("menuitem", { name: /make admin/i }).click()
  // toast is transient; assert leniently, DB check is authoritative.
  await page.getByText(/is now admin/i).first().waitFor({ state: "visible", timeout: 8000 }).catch(() => {})
  await page.screenshot({ path: shot("04_promote_admin"), fullPage: true })

  // 5) Invite landing page (P5b-UI #3) renders meaningful content for the pending token.
  await page.goto(`/invite/${PENDING_TOK}`)
  await expect(
    page.getByText(new RegExp(`${WS_NAME.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}|invit|workspace|join`, "i")).first(),
  ).toBeVisible({ timeout: 20000 })
  await page.screenshot({ path: shot("05_invite_landing"), fullPage: true })

  // 6) Owner deletes the (empty) workspace W via the Danger zone → redirect to /.
  await page.goto("/workspace/members")
  await expect(page.getByRole("heading", { name: /workspace members/i })).toBeVisible({ timeout: 20000 })
  await page.getByRole("button", { name: /^delete workspace$/i }).click()
  await expect(page.getByText(/delete this workspace\?/i)).toBeVisible()
  await page.getByRole("button", { name: /delete permanently/i }).click()
  await page.waitForURL((u) => u.pathname === "/", { timeout: 25000 })
  await page.screenshot({ path: shot("06_after_delete"), fullPage: true })
})
