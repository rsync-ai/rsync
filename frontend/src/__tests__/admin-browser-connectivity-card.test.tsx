import { beforeEach, describe, expect, it, vi } from "vitest"
import { render, screen, within } from "@testing-library/react"

import { BrowserConnectivityCard } from "@/components/admin/BrowserConnectivityCard"

// The probes themselves are covered in browser-connectivity-probes.test.ts; here they are
// stand-ins so the card's own logic (overall verdict, per-row result, re-run) is what fails.
const { apiRun, wsRun } = vi.hoisted(() => ({ apiRun: vi.fn(), wsRun: vi.fn() }))
vi.mock("@/lib/diagnostics/browser-connectivity", () => ({
  BROWSER_PROBES: [
    { id: "api-health", name: "API gateway", target: (c: { apiUrl: string }) => `${c.apiUrl}/api/health`, run: apiRun },
    { id: "websocket", name: "Live updates (WebSocket)", target: (c: { wsUrl: string }) => c.wsUrl, run: wsRun },
  ],
}))

function row(id: string): HTMLElement {
  const el = document.querySelector<HTMLElement>(`li[data-probe="${id}"]`)
  if (!el) throw new Error(`no row for ${id}`)
  return el
}

beforeEach(() => {
  apiRun.mockReset()
  wsRun.mockReset()
})

describe("BrowserConnectivityCard", () => {
  it("says Reachable when both probes pass", async () => {
    apiRun.mockResolvedValue("Reachable")
    wsRun.mockResolvedValue("Connected")
    render(<BrowserConnectivityCard refreshToken={0} />)

    expect(await screen.findByText("Reachable", { selector: "div" })).toBeInTheDocument()
    expect(within(row("websocket")).getByText(/^Connected · \d+ ms$/)).toBeInTheDocument()
  })

  it("says Blocked and shows the reason when one probe fails", async () => {
    apiRun.mockResolvedValue("Reachable")
    wsRun.mockRejectedValue(new Error("No answer in 5s"))
    render(<BrowserConnectivityCard refreshToken={0} />)

    expect(await screen.findByText("Blocked")).toBeInTheDocument()
    expect(within(row("websocket")).getByText(/^No answer in 5s · \d+ ms$/)).toBeInTheDocument()
    expect(within(row("api-health")).getByText(/^Reachable · \d+ ms$/)).toBeInTheDocument()
  })

  it("re-runs on refresh and keeps the last answer until the new one lands", async () => {
    apiRun.mockResolvedValue("Reachable")
    wsRun.mockResolvedValueOnce("Connected")
    const { rerender } = render(<BrowserConnectivityCard refreshToken={0} />)
    await screen.findByText("Reachable", { selector: "div" })

    // The second WebSocket answer never arrives; the row must not fall back to "Checking…".
    wsRun.mockReturnValueOnce(new Promise(() => {}))
    rerender(<BrowserConnectivityCard refreshToken={1} />)

    expect(apiRun).toHaveBeenCalledTimes(2)
    expect(wsRun).toHaveBeenCalledTimes(2)
    expect(within(row("websocket")).getByText(/^Connected · \d+ ms$/)).toBeInTheDocument()
    expect(within(row("websocket")).queryByText("Checking…")).toBeNull()
  })
})
