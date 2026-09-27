/**
 * THE EXPLORER WAS A TALL PAGE OF SHORT BOXES, AND IT NEVER SAID WHICH DATABASE
 * IT WAS LOOKING AT.
 *
 * Two defects, reported together from the live app:
 *
 * 1. "schema and table showing tab length is not full based on page" — the
 *    schema tree lived in a hardcoded `h-[340px]` box and the result grid in
 *    `h-[400px]`, so on a 1440px-tall window roughly half the viewport was
 *    empty while both lists scrolled inside a letterbox. Height is now taken
 *    from the window: the shell is `lg:h-[calc(100vh-8rem)]` (the shell that
 *    `DashboardShell`'s `pt-16` + `p-6` leaves) and every pane below it grows
 *    into it. The invariant this file pins is the one that is easy to break by
 *    accident: a `flex-1` child of a column flex parent overflows its parent
 *    unless it also carries `min-h-0`, because the default `min-height: auto`
 *    refuses to shrink below the content. One missing `min-h-0` anywhere on the
 *    chain and the tree grows past the bottom of the window again.
 *
 * 2. "we have mongodb destination created but in explorer its not showing" —
 *    discovery is hard-pinned to the one database the connection names
 *    (`connectionScope`, backend-orchestrator .../discovery_scope.go: a
 *    connector whose tables live in databases and whose config names one
 *    ignores the connection's Scope entirely). The destination named an empty
 *    database, so `schema-index` answered 200 with `table_count: 0` — success,
 *    no error, no tables — and the UI said only "No tables found", which reads
 *    as "the Explorer cannot see this connection". The name of the database is
 *    now on screen before the query, and the empty state names it too.
 */

import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

import ExplorerPage from "@/app/(dashboard)/explorer/page"
import { authFetch } from "@/lib/api/auth-fetch"

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), info: vi.fn(), message: vi.fn() },
}))
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn(), replace: vi.fn() }),
  usePathname: () => "/explorer",
  useSearchParams: () => new URLSearchParams(),
}))
vi.mock("next-themes", () => ({
  useTheme: () => ({ resolvedTheme: "light", setTheme: vi.fn() }),
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
vi.mock("@/contexts/CurrentUserContext", () => ({
  useCurrentUser: () => ({ user: { id: "u-1" } }),
}))

const mockFetch = authFetch as unknown as Mock
const CONN_ID = "11111111-1111-1111-1111-111111111111"

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as unknown as Response
}

function connection(over: Record<string, unknown> = {}) {
  return {
    id: CONN_ID,
    name: "warehouse",
    connector_type: "postgresql",
    type: "source",
    status: "active",
    is_connected: true,
    supports_explorer: true,
    explorer_mode: "sql",
    sql_dialect: "postgresql",
    config: { database: "analytics" },
    ...over,
  }
}

const TABLES = {
  connection_id: CONN_ID,
  table_count: 1,
  tables: [{ name: "orders", schema: "shop", columns: [{ name: "id", type: "bigint" }] }],
  foreign_keys: [],
}

function serve(conn: Record<string, unknown>, schema: unknown = TABLES) {
  mockFetch.mockImplementation(async (url: string) => {
    if (url.includes("/schema-index")) return res(200, schema)
    if (/\/api\/v1\/connections$/.test(url)) return res(200, { connections: [conn] })
    return res(200, {})
  })
}

beforeAll(() => {
  // CodeMirror measures text through Range rects, which jsdom does not implement.
  const empty = () => Object.assign([], { item: () => null }) as unknown as DOMRectList
  Range.prototype.getClientRects ||= empty
  Range.prototype.getBoundingClientRect ||= () => new DOMRect(0, 0, 0, 0)
})

beforeEach(() => {
  mockFetch.mockReset()
})

// A Tailwind class list, as the class strings a single element carries.
function classesOf(el: Element): string[] {
  return Array.from(el.classList)
}

// `h-[340px]`, `h-96`, `max-h-[70vh]` at the `lg` breakpoint and up — anything
// that fixes a pane's desktop height instead of deriving it from the window.
function fixedDesktopHeight(el: Element): string[] {
  return classesOf(el).filter((c) => /^(lg:|xl:|2xl:)?(max-)?h-(\[\d+(px|rem)\]|\d+)$/.test(c))
}

function grows(el: Element): boolean {
  return classesOf(el).some((c) => c === "flex-1" || c === "lg:flex-1")
}

// A deliberate floor in rem, as opposed to the default `min-height: auto`.
const EXPLICIT_FLOOR = /^(?:lg:)?min-h-\[(\d+(?:\.\d+)?)rem\]$/

// `min-h-0` is the usual form. A declared floor counts too: the thing that
// breaks this layout is `min-height: auto`, which refuses to shrink below the
// content at any size. A floor's *value* can still be wrong, but that is not
// something this predicate can judge — the floors are pinned to ranges by the
// per-pane tests below, which is where a runaway floor fails.
function canShrink(el: Element): boolean {
  return classesOf(el).some(
    (c) =>
      c === "min-h-0" ||
      c === "lg:min-h-0" ||
      c === "h-full" ||
      c === "lg:h-full" ||
      EXPLICIT_FLOOR.test(c)
  )
}

// The rem value of an element's explicit desktop floor, or 0 if it has none.
function floorRem(el: Element): number {
  for (const c of classesOf(el)) {
    const m = EXPLICIT_FLOOR.exec(c)
    if (m) return Number(m[1])
  }
  return 0
}

describe("the Explorer fills the window", () => {
  it("takes the schema tree's height from the window, not from a fixed box", async () => {
    serve(connection())
    render(<ExplorerPage />)

    const shell = await screen.findByTestId("explorer-shell")
    // The one height on the page that is allowed to be absolute: the shell's,
    // because it is the window minus the chrome DashboardShell reserves.
    expect(classesOf(shell)).toContain("lg:h-[calc(100vh-8rem)]")

    const pane = await screen.findByTestId("schema-tree-pane")
    expect(fixedDesktopHeight(pane)).toEqual([])
    expect(grows(pane)).toBe(true)
  })

  it("keeps min-h-0 on every flex parent between the shell and the tree", async () => {
    serve(connection())
    render(<ExplorerPage />)

    const shell = await screen.findByTestId("explorer-shell")
    const pane = await screen.findByTestId("schema-tree-pane")

    const chain: Element[] = []
    for (let el: Element | null = pane; el && el !== shell; el = el.parentElement) chain.push(el)
    // Guard the walk itself: if the pane ever stops being a descendant of the
    // shell, the loop above would run to <html> and this test would pass empty.
    expect(shell.contains(pane)).toBe(true)
    expect(chain.length).toBeGreaterThan(2)

    for (const el of chain) {
      expect({ el: el.className, fixed: fixedDesktopHeight(el) }).toEqual({ el: el.className, fixed: [] })
      if (grows(el)) {
        // A flex-1 child of a column flex parent overflows unless it may shrink.
        expect({ el: el.className, canShrink: canShrink(el) }).toEqual({ el: el.className, canShrink: true })
      }
    }
  })
})

describe("the Explorer names the one database it browses", () => {
  it("shows the pinned database for a source connection", async () => {
    serve(connection())
    render(<ExplorerPage />)

    const chip = await screen.findByText("Browsing")
    expect(chip.parentElement).toHaveTextContent("analytics")
    expect(chip.parentElement?.getAttribute("title")).toMatch(/pinned to this one database/i)
  })

  it("marks a destination and says an empty tree is not a broken pipeline", async () => {
    serve(
      connection({
        name: "mongo warehouse",
        connector_type: "mongodb",
        type: "destination",
        explorer_mode: "document",
        config: { database: "demo" },
      }),
      { connection_id: CONN_ID, table_count: 0, tables: [], foreign_keys: [] },
    )
    render(<ExplorerPage />)

    const chip = await screen.findByText("Browsing")
    expect(chip.parentElement).toHaveTextContent("demo")
    expect(chip.parentElement).toHaveTextContent("destination")

    const empty = await screen.findByText(/No collections in "demo"/)
    expect(empty).toHaveTextContent(/the one database this connection browses/i)
    expect(empty).toHaveTextContent(/does not mean the pipeline is not landing data/i)
  })

  it("drops the pinned-database chip when the connection names no database", async () => {
    serve(connection({ config: {} }), { connection_id: CONN_ID, table_count: 0, tables: [], foreign_keys: [] })
    render(<ExplorerPage />)

    expect(await screen.findByText("No tables found")).toBeInTheDocument()
    expect(screen.queryByText("Browsing")).not.toBeInTheDocument()
  })
})

/**
 * ...AND THEN THE SQL EDITOR HAD NO HEIGHT AT ALL.
 *
 * Reported from prod once the panes were window-sized: "sql editor is not
 * showing correctly default". Measured in the deployed page: the compose pane
 * took its height from the dragged ratio alone (`--compose-basis`, 45% by
 * default), and everything above the editor inside it — the question box (138px),
 * its label, the Run row (40px) — is `shrink-0`. On a 771px-tall window that
 * left the editor's own block 0px of a 182px card body, so CodeMirror rendered
 * at height 0: a "SQL Query" label with nothing under it. The ratio needed a
 * ~1050px-tall viewport before the editor got a usable box, which is taller
 * than most laptops.
 *
 * The fix is a floor in three places, and this is what these tests pin. jsdom
 * computes no layout, so what is provable here is the class contract, not the
 * pixels; the pixels were measured in the browser (0px -> 94px).
 */
describe("the SQL editor cannot be squeezed to nothing", () => {
  it("gives the editor's block a floor instead of only letting it shrink", async () => {
    serve(connection())
    render(<ExplorerPage />)

    const pane = await screen.findByTestId("sql-editor-pane")
    // It must still be allowed to shrink with the pane...
    expect(classesOf(pane)).toContain("min-h-0")
    expect(classesOf(pane)).toContain("flex-1")
    // ...but not below something you can read SQL in.
    const f = floorRem(pane)
    expect({ pane: pane.className, floorRem: f >= 6 && f <= 10 }).toEqual({
      pane: pane.className,
      floorRem: true,
    })
  })

  it("floors the compose pane at its content, because a % basis cannot", async () => {
    serve(connection())
    render(<ExplorerPage />)

    const compose = await screen.findByTestId("compose-pane")
    // The basis is still the dragged split; the floor is what stops the drag
    // (and a short window) from collapsing the pane's own content.
    expect(classesOf(compose)).toContain("lg:basis-[var(--compose-basis)]")
    // 25rem in ask mode was the measured no-clip minimum; below it the Run row
    // was cut off. The ceiling is here so a "fix" cannot just take the window.
    const f = floorRem(compose)
    expect({ compose: compose.className, floorRem: f >= 20 && f <= 30 }).toEqual({
      compose: compose.className,
      floorRem: true,
    })
  })

  it("keeps the min-h-0 chain unbroken between the shell and the editor", async () => {
    serve(connection())
    render(<ExplorerPage />)

    const shell = await screen.findByTestId("explorer-shell")
    const pane = await screen.findByTestId("sql-editor-pane")

    const chain: Element[] = []
    for (let el: Element | null = pane; el && el !== shell; el = el.parentElement) chain.push(el)
    expect(shell.contains(pane)).toBe(true)
    expect(chain.length).toBeGreaterThan(2)

    for (const el of chain) {
      expect({ el: el.className, fixed: fixedDesktopHeight(el) }).toEqual({ el: el.className, fixed: [] })
      if (grows(el)) {
        expect({ el: el.className, canShrink: canShrink(el) }).toEqual({ el: el.className, canShrink: true })
      }
    }
  })

  it("leaves the results pane room for a row when the editor takes its floor", async () => {
    serve(connection())
    render(<ExplorerPage />)

    const card = await screen.findByTestId("results-pane")
    // Not `lg:min-h-0`: with the compose pane floored, min-h-0 here dropped the
    // results card to 62px on a 706px-tall window, which renders a 2px grid
    // scroller — the first fix traded one collapsed pane for another. Measured
    // on the deployed page at that height with the masked-columns notice
    // showing (two lines above the grid, and the case a demo hits first): the
    // floor buys 43px of scroller at 15rem, 91px at 18rem, 123px at 20rem, and
    // the column header alone is 48px — so 15rem could not show the header.
    // 18rem is the header plus a row. The range is what this test pins; the
    // exact value is a judgement call recorded at the Card.
    expect(classesOf(card)).toContain("lg:flex-1")
    const f = floorRem(card)
    expect({ card: card.className, floorRem: f >= 12 && f <= 20 }).toEqual({
      card: card.className,
      floorRem: true,
    })
  })
})

/**
 * A MANUAL RUN FINISHED ON THE STEP TIMELINE, WITH ITS OWN GRID OUT OF SIGHT.
 *
 * Found while testing the deployed build by hand: running SQL landed on Steps
 * and stayed there after "Completed 178ms", so the rows were one click away in
 * a pane nobody had asked for. `executeQuery` selects Results deliberately —
 * the comment there says so, this had been fixed once already — but it did so
 * *before* the `if (!currentRun)` block, and `startExplorationRun` selects
 * Steps itself. The later call won. Ordering bugs of this shape leave no trace
 * in review, so the behaviour is pinned here rather than the line order.
 */
describe("a manual run leaves its own results in front", () => {
  it("selects Results after the run, not the step timeline", async () => {
    mockFetch.mockImplementation(async (url: string) => {
      if (url.includes("/schema-index")) return res(200, TABLES)
      if (/\/api\/v1\/connections$/.test(url)) return res(200, { connections: [connection()] })
      if (url.includes("/explorer/query"))
        return res(200, {
          // `columns` is a plain string[] on the wire (QueryResult, page.tsx:181)
          // and reaches ShareToSlackDialog uncoerced; objects here crash it.
          columns: ["id"],
          rows: [{ id: 1 }],
          row_count: 1,
          execution_time_ms: 4,
        })
      return res(200, {})
    })
    // Seeding history is how the SQL reaches the editor: CodeMirror needs a
    // layout to be typed into and jsdom has none, but a history entry sets the
    // same state through `loadFromHistory`.
    window.localStorage.setItem(
      `rsync_explorer_history_v1:${CONN_ID}`,
      JSON.stringify([
        { id: "h1", sql: "select 1", timestamp: new Date().toISOString(), executionTimeMs: 4, rowCount: 1 },
      ])
    )
    const user = userEvent.setup()
    render(<ExplorerPage />)

    await user.click(await screen.findByRole("tab", { name: /History/i }))
    await user.click(await screen.findByRole("button", { name: /select 1/i }))
    await user.click(await screen.findByRole("button", { name: /Run Query/i }))

    // The run has to have actually produced rows, or "Results is selected"
    // would also be true of every failure path.
    await screen.findByTestId("results-grid-scroller")
    await waitFor(() =>
      expect(screen.getByRole("tab", { name: /Results/i })).toHaveAttribute("data-state", "active")
    )
    expect(screen.getByRole("tab", { name: /Steps/i })).toHaveAttribute("data-state", "inactive")
  })
  it("fills the step timeline even though the editor never lost focus", async () => {
    mockFetch.mockImplementation(async (url: string) => {
      if (url.includes("/schema-index")) return res(200, TABLES)
      if (/\/api\/v1\/connections$/.test(url)) return res(200, { connections: [connection()] })
      if (url.includes("/explorer/query"))
        return res(200, { columns: ["id"], rows: [{ id: 1 }], row_count: 1, execution_time_ms: 4 })
      return res(200, {})
    })
    window.localStorage.setItem(
      `rsync_explorer_history_v1:${CONN_ID}`,
      JSON.stringify([
        { id: "h1", sql: "select 1", timestamp: new Date().toISOString(), executionTimeMs: 4, rowCount: 1 },
      ])
    )
    const user = userEvent.setup()
    render(<ExplorerPage />)

    await user.click(await screen.findByRole("tab", { name: /History/i }))
    await user.click(await screen.findByRole("button", { name: /select 1/i }))

    // The defect needs the editor to hold focus across the whole run: step
    // updates are parked while it does, and the queue was flushed from nowhere
    // but the two editors' own blur handlers. So a run fired with Cmd+Enter
    // from the editor — no blur anywhere — left the Steps tab reading
    // "Completed · 0ms total" with every step blank, next to a grid full of
    // rows. Real `.focus()` rather than `fireEvent.focus`, which dispatches
    // "focus" and not the "focusin" React listens for, and `fireEvent.click`
    // rather than userEvent for the button, because userEvent moves focus and
    // the blur would do the flushing for us.
    const nl = screen.getByLabelText(/Natural Language Query/i)
    nl.focus()
    expect(document.activeElement).toBe(nl)

    fireEvent.click(screen.getByRole("button", { name: /Run Query/i }))
    await screen.findByTestId("results-grid-scroller")

    // `mouseDown` is how Radix switches tabs, and unlike a real click it does
    // not move focus in jsdom. Both assertions are preconditions, not
    // decoration: if focus had left the editor, the blur handler would flush
    // and this test would pass against the unfixed page.
    expect(document.activeElement).toBe(nl)
    fireEvent.mouseDown(screen.getByRole("tab", { name: /Steps/i }), { button: 0 })
    const panel = await screen.findByTestId("results-pane")
    expect(document.activeElement).toBe(nl)

    const row = (title: string) => {
      const el = screen.getByText(title).closest("button")
      expect(el).not.toBeNull()
      expect(panel.contains(el!)).toBe(true)
      return el!
    }
    // A step that never landed carries no duration at all, so this is the
    // assertion that fails when the queue is not flushed.
    expect(row("Fetch Schema").textContent).toContain("0ms")
    expect(row("Execute Query").textContent).toMatch(/\d+ms/)
    // And a direct query does not identify tables or map columns, so those two
    // say so instead of showing a clock face on a finished run.
    expect(row("Identify Tables").textContent).toContain("Skipped")
    expect(row("Map Columns").textContent).toContain("Skipped")
  })
})

describe("the separator moves the split by the distance dragged", () => {
  it("does not jump to the pointer when the handle is grabbed away from the ratio", async () => {
    window.localStorage.removeItem("explorer.composeRatio")
    serve(connection())
    render(<ExplorerPage />)

    const sep = await screen.findByRole("separator", { name: /Resize the editor/i })
    expect(sep).toHaveAttribute("aria-valuenow", "45")

    // The handle does not sit at 45% of the column: both panes carry minimum
    // heights, and on a short window one of them binds, which moves the handle
    // away from the ratio it represents. jsdom has no layout, so the column's
    // box is stated outright: 600px tall, and the handle is grabbed at y=500 —
    // which an absolute mapping reads as 83%, clamped to the 75% maximum.
    const column = sep.parentElement!
    expect(column.tagName).toBe("MAIN")
    column.getBoundingClientRect = () =>
      ({
        top: 0,
        bottom: 600,
        height: 600,
        left: 0,
        right: 0,
        width: 0,
        x: 0,
        y: 0,
        toJSON: () => ({}),
      }) as DOMRect

    fireEvent.pointerDown(sep, { clientY: 500 })
    // Grabbing the handle is not a resize.
    expect(sep).toHaveAttribute("aria-valuenow", "45")

    // 60px down a 600px column is ten points of ratio, from wherever it was.
    act(() => {
      window.dispatchEvent(new MouseEvent("pointermove", { clientY: 560 }))
    })
    await waitFor(() => expect(sep).toHaveAttribute("aria-valuenow", "55"))

    act(() => {
      window.dispatchEvent(new MouseEvent("pointerup", { clientY: 560 }))
    })
    expect(Number(window.localStorage.getItem("explorer.composeRatio"))).toBeCloseTo(0.55, 5)

    // Released: the window listeners are gone and later pointer traffic is not
    // a drag. Without this the test would pass on a handler that never unbinds.
    act(() => {
      window.dispatchEvent(new MouseEvent("pointermove", { clientY: 200 }))
    })
    expect(sep).toHaveAttribute("aria-valuenow", "55")
  })
})
