/**
 * THE TRANSFORM BUILDER PUSHED ITS OWN BUTTONS OUT OF THE CARD.
 *
 * Reported from the live app ("producer transformation ui is buggy"), and the
 * screenshot showed it: on the Pre-Kafka Transforms list the settings and
 * delete buttons were missing from the "Rename Columns" card and crowding the
 * edge on three others. The controls were not hidden by a conditional — they
 * had been pushed past the card's right edge.
 *
 * The cause is one missing class. The row is
 * `grip | title (flex-1) | controls`, and a flex child defaults to
 * `min-width: auto`, which refuses to shrink below its own min-content width.
 * So the title block could not give way: the longer the operation name, the
 * further right the controls were shoved. Measured in the deployed page
 * (app.rsync.ai, 2026-09-23), as pixels past the card's right edge:
 *
 *     Hash Column        inside by 25px   (the only one that fit)
 *     Handle Nulls       over by  23px
 *     Type Conversion    over by  41px
 *     Select Columns     over by  58px
 *     Rename Columns     over by 125px    <- gear and trash unreachable
 *
 * Applying `min-w-0` to the title block and `shrink-0` to the control cluster
 * in that same live DOM put all five at "inside by 25px" — the card's own
 * padding — and left the one card that already fit exactly where it was.
 *
 * jsdom computes no layout, so what this file can prove is the class contract;
 * the pixels above were measured in the browser. What it CAN prove outright is
 * the fourth defect in the same block: the config summary ran the config
 * through `.slice(0, 50)` and then appended "..." unconditionally, so
 * `{"column":"password"}` — 21 characters — rendered as truncated when nothing
 * had been cut. The element already carries `truncate`; CSS does that job.
 */

import { beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"
import "@testing-library/jest-dom"

import TransformsPage from "@/app/(dashboard)/transforms/page"
import { authFetch } from "@/lib/api/auth-fetch"

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), info: vi.fn(), message: vi.fn() },
}))
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn(), replace: vi.fn() }),
  usePathname: () => "/transforms",
  useSearchParams: () => new URLSearchParams(),
}))
vi.mock("@/contexts/WorkspaceContext", () => ({
  useWorkspaceRole: () => ({
    role: "admin",
    isLoading: false,
    error: false,
    activeWorkspace: null,
    can: () => true,
    meets: () => true,
  }),
}))

const mockFetch = authFetch as unknown as Mock
const PIPELINE = "e676b9ee-c41b-400f-9b79-4106e297c22f"

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as unknown as Response
}

// The five rules from the reported screenshot, in the order they were shown.
// The operation names are what drive the defect, so they are not simplified.
const PRODUCER_RULES = [
  { operation: "hash", column: "password" },
  { operation: "rename", mappings: "email:user_email, name:full_name" },
  { operation: "type_convert", column: "age", to_type: "integer" },
  { operation: "null_handle", action: "skip", column: "email" },
  { operation: "select", columns: "id, user_email, full_name" },
].map((config, i) => ({
  id: `0000000${i}-0000-4000-8000-00000000000${i}`,
  pipeline_id: PIPELINE,
  transform_type: "producer",
  transform_order: i,
  transform_config: config,
  enabled: true,
}))

beforeEach(() => {
  mockFetch.mockReset()
  // The page reads the id off window.location itself, not through useSearchParams.
  window.history.replaceState({}, "", `/transforms?pipeline=${PIPELINE}`)
  mockFetch.mockImplementation(async (url: string) => {
    if (url.includes("/transforms/pipeline/"))
      return res(200, { pipeline_id: PIPELINE, producer_transforms: PRODUCER_RULES, consumer_transforms: [] })
    if (url.includes("/pipelines")) return res(200, { pipelines: [{ id: PIPELINE, name: "cdc-dedup-skip-proof pg-to-mongo" }] })
    return res(200, {})
  })
})

async function cards() {
  const rows = await screen.findAllByTestId("transform-card-row")
  // Arms every assertion below: with no cards rendered, a `for` over an empty
  // list passes while proving nothing, and the mock above would be the bug.
  expect(rows.length).toBe(PRODUCER_RULES.length)
  return rows
}

describe("a transform card's controls stay inside the card", () => {
  it("lets the title block shrink, so a long operation name cannot shove the controls out", async () => {
    render(<TransformsPage />)
    for (const row of await cards()) {
      const title = row.querySelector('[data-testid="transform-card-title"]')!
      const classes = Array.from(title.classList)
      // It takes the free space...
      expect({ title: title.className, grows: classes.includes("flex-1") }).toEqual({
        title: title.className,
        grows: true,
      })
      // ...and, the part that was missing, it gives space back. Without this
      // the block's min-width is `auto` and the controls are pushed off-card.
      expect({ title: title.className, canShrink: classes.includes("min-w-0") }).toEqual({
        title: title.className,
        canShrink: true,
      })
    }
  })

  it("never lets the control cluster be the thing that gives way", async () => {
    render(<TransformsPage />)
    for (const row of await cards()) {
      const controls = row.querySelector('[data-testid="transform-card-controls"]')!
      expect({ controls: controls.className, fixed: Array.from(controls.classList).includes("shrink-0") }).toEqual({
        controls: controls.className,
        fixed: true,
      })
      // The five controls the screenshot lost: toggle, up, down, settings,
      // delete. The toggle is a Radix Switch, which is itself a <button>, so
      // the four icon buttons are counted apart from it.
      expect(controls.querySelector('[role="switch"]')).not.toBeNull()
      const iconButtons = Array.from(controls.querySelectorAll("button")).filter(
        (b) => b.getAttribute("role") !== "switch"
      )
      expect(iconButtons.length).toBe(4)
    }
  })

  it("truncates the operation name rather than the card", async () => {
    render(<TransformsPage />)
    for (const row of await cards()) {
      const title = row.querySelector('[data-testid="transform-card-title"]')!
      const truncating = Array.from(title.querySelectorAll("span")).some((s) =>
        s.classList.contains("truncate")
      )
      expect({ title: title.className, truncating }).toEqual({ title: title.className, truncating: true })
    }
  })

  // #1166 put these five controls back inside the card; it did not give them
  // names. Every one is icon-only, so each announced as a bare "button" — five
  // per card, and with five cards open a screen reader offered twenty-five
  // indistinguishable buttons, four of which delete a rule. The name has to
  // carry the operation itself: the title cell next door is the one thing on
  // this row that truncates, so it cannot be relied on to say which rule the
  // buttons belong to.
  it("gives every control an accessible name that says which rule it acts on", async () => {
    render(<TransformsPage />)
    const announced = new Set<string>()
    for (const row of await cards()) {
      const controls = row.querySelector('[data-testid="transform-card-controls"]')!
      const title = row
        .querySelector('[data-testid="transform-card-title"]')!
        .querySelector("span.truncate")!.textContent!.trim()
      expect(title).not.toBe("")
      // Switch + up + down + settings + delete. Radix renders the Switch as a
      // <button role="switch">, so it is in this list and needs a name too.
      const buttons = Array.from(controls.querySelectorAll("button"))
      expect(buttons.length).toBe(5)
      for (const b of buttons) {
        const name = b.getAttribute("aria-label")
        // stringContaining does both jobs at once: absent/empty fails, and so
        // does a generic name like "Delete" that does not identify the rule.
        expect({ html: b.outerHTML, name }).toEqual({
          html: b.outerHTML,
          name: expect.stringContaining(title),
        })
        // Sighted mouse users get the same sentence — the icons are no more
        // self-explanatory to them.
        expect({ html: b.outerHTML, title: b.getAttribute("title") }).toEqual({
          html: b.outerHTML,
          title: name,
        })
        announced.add(name!)
      }
    }
    // Five rules x five controls, every one distinct: no two controls anywhere
    // on the page announce the same way.
    expect(announced.size).toBe(PRODUCER_RULES.length * 5)
  })

  // KI-TRANSFORM-NAME-TRUNCATES-TO-NOTHING: every sibling of the name on its
  // line was shrink-0, so on a narrow card the name was the only thing that
  // could give way and reached 0px. jsdom computes no layout, so this pins the
  // structure that prevents it: nothing but the icon shares the name's line.
  it("gives the operation name a line of its own, with the badges wrapping below", async () => {
    const original = PRODUCER_RULES[1].transform_config
    // `sql` is in the dialog's catalogue but no engine runs it: the card that
    // carries the "No engine can run this" badge.
    PRODUCER_RULES[1].transform_config = { ...original, operation: "sql" }
    try {
      render(<TransformsPage />)
      const rows = await cards()
      for (const row of rows) {
        const nameLine = row.querySelector('[data-testid="transform-card-name"]')!
        const name = nameLine.querySelector("span.truncate")!
        expect(name.textContent!.trim()).not.toBe("")
        // Hover still shows the whole name when it does truncate.
        expect(name.getAttribute("title")).toBe(name.textContent!.trim())
        // Icon + name, and nothing else competing for the width.
        expect(nameLine.children.length).toBe(2)
        const badges = row.querySelector('[data-testid="transform-card-badges"]')!
        expect(Array.from(badges.classList)).toContain("flex-wrap")
        expect(badges.textContent).toMatch(/producer/)
      }
      // The card that most needs identifying: the destructive badge is on the
      // badge line, not beside the name.
      const blocked = rows.filter((r) => /No engine can run this/.test(r.textContent ?? ""))
      expect(blocked.length).toBe(1)
      expect(blocked[0].querySelector('[data-testid="transform-card-name"]')!.textContent).not.toMatch(/No engine/)
      expect(blocked[0].querySelector('[data-testid="transform-card-badges"]')!.textContent).toMatch(/No engine can run this/)
    } finally {
      PRODUCER_RULES[1].transform_config = original
    }
  })

  it("does not claim a short config was truncated", async () => {
    render(<TransformsPage />)
    await cards()
    // `{"column":"password"}` is 21 characters, well under the old 50-char
    // slice, so the trailing "..." the old code appended was a lie about the
    // data. This is the one assertion here that fails on behaviour, not classes.
    // (`operation` is not in this string because `fromDefinitions` lifts it out
    // of the stored config into its own field before the card ever sees it.)
    const summary = await waitFor(() => {
      const el = screen.getByText(/"column":"password"/)
      expect(el).toBeInTheDocument()
      return el
    })
    expect(summary.textContent).not.toMatch(/\.\.\.$/)
    expect(summary.textContent).toBe('{"column":"password"}')
    expect(Array.from(summary.classList)).toContain("truncate")
  })
})
