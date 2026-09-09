/**
 * Two compounding defects, both found on a clean-room self-host install where
 * the frontend (:3000) and the api-gateway (:5001) are separate origins.
 *
 * 1. Nine client-side reads used a bare relative `fetch("/api/v1/...")`. A
 *    relative URL resolves against the *page* origin, so every one of them went
 *    to Next.js, which serves no /api/v1 route, and 404'd without ever reaching
 *    the backend. It works in `next dev` and behind a single-origin reverse
 *    proxy, which is why it survived: the two deployments anyone tests on are
 *    exactly the two where the bug is invisible.
 *
 * 2. The 404 was then swallowed into a zero. `r.ok ? r.json() : null` followed
 *    by `payload?.total ?? 0` makes an unreachable backend indistinguishable
 *    from an empty workspace, and both components then *overwrote* their
 *    server-rendered values with that zero. The `catch` blocks that claimed to
 *    "keep SSR values on failure" could never fire — `Promise.allSettled` does
 *    not reject, and a non-2xx response is not a thrown error. So the single
 *    failure mode these components exist to defend against, a count silently
 *    reading zero, was the failure mode they produced.
 *
 * The consequence users saw is in the onboarding case below: a workspace that
 * had completed every step got its counts zeroed on mount and was shown the
 * "Get your first sync running" checklist again — the exact outcome
 * FirstRunOnboarding's own header comment says must never happen.
 *
 * Each behavioural case below carries a control that fails if the component
 * simply stopped re-fetching, because "never asks" also produces "never writes
 * a zero" and would otherwise pass silently.
 */

import { describe, it, expect, vi, beforeEach, type Mock } from "vitest"
import { readdirSync, readFileSync, statSync } from "node:fs"
import { join, resolve } from "node:path"
import { render, screen, waitFor, act } from "@testing-library/react"
import "@testing-library/jest-dom"

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
  usePathname: () => "/",
}))
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))

import { authFetch } from "@/lib/api/auth-fetch"
import { readStatOrUnknown } from "@/lib/api/read-stat"
import { DashboardStatsRefresher } from "@/components/dashboard/DashboardStatsRefresher"
import { FirstRunOnboarding } from "@/components/onboarding/FirstRunOnboarding"
import { emitPipelineRefresh } from "@/lib/events/pipelineRefresh"

const mockFetch = authFetch as unknown as Mock

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: String(status),
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as unknown as Response
}

/** Every read 404s — what a separate-origin install did to all nine call sites. */
const allReadsNotFound = () => mockFetch.mockImplementation(async () => res(404, {}))

beforeEach(() => {
  mockFetch.mockReset()
  vi.spyOn(console, "warn").mockImplementation(() => {})
})

describe("readStatOrUnknown", () => {
  it("reports an unreadable stat as undefined, not as a zero-shaped body", async () => {
    allReadsNotFound()
    await expect(readStatOrUnknown("/api/v1/pipelines")).resolves.toBeUndefined()
  })

  it("reports a thrown request as undefined too", async () => {
    mockFetch.mockRejectedValue(new Error("Failed to fetch"))
    await expect(readStatOrUnknown("/api/v1/pipelines")).resolves.toBeUndefined()
  })

  // The control. Without it, a helper that returned undefined unconditionally
  // would satisfy both cases above — and a real empty workspace must still be
  // reported as empty, not as unknown.
  it("still reports a genuine zero as a zero", async () => {
    mockFetch.mockResolvedValue(res(200, { total: 0, pipelines: [] }))
    await expect(readStatOrUnknown("/api/v1/pipelines")).resolves.toEqual({
      total: 0,
      pipelines: [],
    })
  })

  it("goes through the API base resolver rather than the page origin", async () => {
    mockFetch.mockResolvedValue(res(200, { total: 1 }))
    await readStatOrUnknown("/api/v1/pipelines")
    expect(mockFetch).toHaveBeenCalledWith("/api/v1/pipelines")
  })
})

describe("DashboardStatsRefresher", () => {
  const cards = [
    { title: "Pipelines", value: 5, iconName: "GitBranch", color: "", bgColor: "", subtitle: "1 running" },
    { title: "Executions", value: 12, iconName: "History", color: "", bgColor: "", subtitle: "12 successful" },
    { title: "Sources", value: 3, iconName: "Database", color: "", bgColor: "" },
    { title: "Destinations", value: 2, iconName: "ArrowRightLeft", color: "", bgColor: "" },
  ]

  it("leaves the rendered counts alone when the refresh cannot be read", async () => {
    allReadsNotFound()
    render(<DashboardStatsRefresher initialCards={cards} looksEmpty={false} />)

    await act(async () => {
      emitPipelineRefresh("p1")
    })
    await waitFor(() => expect(mockFetch).toHaveBeenCalled())

    for (const value of ["5", "12", "3", "2"]) {
      expect(screen.getByText(value)).toBeInTheDocument()
    }
    expect(screen.queryByText("0")).not.toBeInTheDocument()
  })

  // The control: the same event with readable responses must move the numbers.
  // Without it, a component that had simply stopped refreshing would pass the
  // case above while being broken in a different direction.
  it("does update the counts when the refresh can be read", async () => {
    mockFetch.mockImplementation(async (path: string) => {
      if (path.startsWith("/api/v1/pipelines")) return res(200, { total: 9, pipelines: [] })
      if (path.startsWith("/api/v1/executions")) return res(200, { total: 20, executions: [] })
      return res(200, { total: 7, connections: [] })
    })
    render(<DashboardStatsRefresher initialCards={cards} looksEmpty={false} />)

    await act(async () => {
      emitPipelineRefresh("p1")
    })
    await waitFor(() => expect(screen.getByText("9")).toBeInTheDocument())
    expect(screen.getByText("20")).toBeInTheDocument()
  })
})

describe("FirstRunOnboarding", () => {
  const activated = { sourceCount: 2, destinationCount: 1, pipelineCount: 1, queryCount: 4 }

  it("stays retired for an activated workspace whose re-verification cannot be read", async () => {
    allReadsNotFound()
    render(<FirstRunOnboarding initial={activated} />)

    await waitFor(() => expect(mockFetch).toHaveBeenCalled())
    await waitFor(() => expect(screen.queryByText(/Get your first sync running/)).not.toBeInTheDocument())
    // Give the effect's setState a chance to land before declaring it absent.
    await act(async () => {
      await Promise.resolve()
    })
    expect(screen.queryByText(/Get your first sync running/)).not.toBeInTheDocument()
  })

  // The control: a workspace that really is empty must still see the checklist,
  // and it must be the *re-verification* that reveals it — the SSR counts here
  // say the workspace is activated, so this only passes if the fetch ran and its
  // zeros were believed.
  it("shows the checklist when the reads succeed and really report zero", async () => {
    mockFetch.mockImplementation(async (path: string) => {
      if (path.startsWith("/api/v1/demo/status")) return res(200, { available: false })
      return res(200, { total: 0, connections: [], pipelines: [], queries_used: 0 })
    })
    render(<FirstRunOnboarding initial={activated} />)

    await waitFor(() => expect(screen.getByText(/Get your first sync running/)).toBeInTheDocument())
  })
})

/**
 * Source census for defect 1.
 *
 * The behavioural cases above are pinned to three components. This one closes
 * the class: any client module that reaches the api-gateway through a bare
 * relative path is the same bug, wherever it is added next. It is also how the
 * tenth call site was found — `src/app/(dashboard)/explorer/page.tsx` split its
 * `fetch(` and its URL across two lines, so a line-oriented grep of the same
 * pattern reported nine offenders and this walk reported ten.
 *
 * `/proxy/...` is deliberately allowed — those are Next.js route handlers under
 * `src/app/proxy`, served by the frontend's own origin, where a relative URL is
 * the correct address rather than a mistake.
 */
describe("no client module addresses the api-gateway by a relative path", () => {
  const SRC = resolve(__dirname, "..")

  function sourceFiles(dir: string, acc: string[] = []): string[] {
    for (const entry of readdirSync(dir)) {
      if (entry === "node_modules" || entry === "__tests__") continue
      const full = join(dir, entry)
      if (statSync(full).isDirectory()) sourceFiles(full, acc)
      else if (/\.tsx?$/.test(entry) && !/\.(test|spec)\.tsx?$/.test(entry)) acc.push(full)
    }
    return acc
  }

  /**
   * Drop whole-line comments before scanning.
   *
   * Needed because this defect's own documentation quotes the offending shape —
   * `read-stat.ts` explains it at length — and a scanner that cannot tell code
   * from prose reports the explanation as the bug.
   *
   * Deliberately NOT a file-level exemption: `read-stat.ts` is a place a real
   * offender could be added tomorrow, and exempting the file would be exempting
   * exactly the set the guard exists to catch. Dropping only lines that *begin*
   * with `//` or `*` can never hide a call site, because a call site never
   * begins its line that way. It is also not a general comment stripper: a `//`
   * appearing mid-line is left alone, so no `https://` inside a string is
   * mangled into a false match.
   */
  function withoutCommentLines(source: string): string {
    return source
      .split("\n")
      .filter((line) => {
        const t = line.trimStart()
        return !t.startsWith("//") && !t.startsWith("*") && !t.startsWith("/*")
      })
      .join("\n")
  }

  // A bare `fetch(` — not authFetch/refetch/prefetch/x.fetch — whose first
  // argument is a string or template literal opening with `/api/`. `\s*` spans
  // newlines on purpose: the tenth offender wrapped its argument onto the next
  // line.
  const RELATIVE_API_FETCH = /(?<![A-Za-z0-9_$.])fetch\(\s*["'`]\/api\//

  const files = sourceFiles(SRC)

  // The denominator. A walk that silently found nothing would make every
  // assertion below vacuous, and this suite would stay green through a
  // reintroduction of all ten call sites.
  it("scans a real corpus", () => {
    expect(files.length).toBeGreaterThan(100)
    for (const known of [
      "components/dashboard/DashboardStatsRefresher.tsx",
      "components/onboarding/FirstRunOnboarding.tsx",
      "app/oauth/callback/page.tsx",
      "app/(dashboard)/explorer/page.tsx",
    ]) {
      expect(files.some((f) => f.endsWith(known))).toBe(true)
    }
  })

  // The pattern itself, checked against the code that was actually removed, so
  // a regex that had stopped matching anything could not pass as a clean repo.
  it("recognises the shape that was removed", () => {
    expect(RELATIVE_API_FETCH.test(`fetch("/api/v1/pipelines", { credentials: "include" })`)).toBe(true)
    expect(RELATIVE_API_FETCH.test("fetch(`/api/v1/oauth/callback/${p}?code=${c}`, {")).toBe(true)
    expect(RELATIVE_API_FETCH.test("const res = await fetch(\n  `/api/v1/explorer/connections/x`\n)")).toBe(true)
    expect(RELATIVE_API_FETCH.test(`authFetch("/api/v1/pipelines")`)).toBe(false)
    expect(RELATIVE_API_FETCH.test(`fetch("/proxy/explorer/export", {`)).toBe(false)
  })

  // The comment filter, measured both ways: it must hide the prose that quotes
  // the defect and must not hide the code that commits it.
  it("tells the documented shape from the committed one", () => {
    expect(RELATIVE_API_FETCH.test(withoutCommentLines(` * used a bare relative \`fetch("/api/v1/x")\``))).toBe(false)
    expect(RELATIVE_API_FETCH.test(withoutCommentLines(`  // fetch("/api/v1/x") is wrong`))).toBe(false)
    expect(RELATIVE_API_FETCH.test(withoutCommentLines(`  const r = await fetch("/api/v1/x")`))).toBe(true)
    // Mid-line `//` must survive, or a URL in a string could be mangled.
    expect(withoutCommentLines(`const u = "https://api.example/x"`)).toContain("https://api.example/x")
    // And the filter must not gut the corpus it is applied to.
    const remaining = files
      .map((f) => withoutCommentLines(readFileSync(f, "utf8")))
      .join("\n")
      .match(/fetch\(/g)
    expect(remaining?.length ?? 0).toBeGreaterThan(20)
  })

  it("finds none left", () => {
    const offenders = files.filter((f) => RELATIVE_API_FETCH.test(withoutCommentLines(readFileSync(f, "utf8"))))
    expect(offenders.map((f) => f.slice(SRC.length + 1))).toEqual([])
  })
})
