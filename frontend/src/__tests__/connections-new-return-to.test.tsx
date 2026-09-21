import { describe, expect, it, vi } from "vitest"
import { render, screen } from "@testing-library/react"

import NewConnectionPage from "@/app/(dashboard)/connections/new/page"

// returnTo arrives in the URL and feeds both the Back link and the post-create
// redirect, so a crafted link must not send the user off the app.

const nav = vi.hoisted(() => ({ query: "" }))

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  useSearchParams: () => new URLSearchParams(nav.query),
}))
vi.mock("@/lib/api/mcp-connectors", () => ({
  fetchMCPConnectors: vi.fn(async () => ({ connectors: [] })),
  fetchMCPConnector: vi.fn(),
  saveConnection: vi.fn(),
}))
vi.mock("sonner", () => ({ toast: { error: vi.fn(), success: vi.fn() } }))

function backHref(query: string) {
  nav.query = query
  const { unmount } = render(<NewConnectionPage />)
  const href = screen.getByRole("link", { name: "Back" }).getAttribute("href")
  unmount()
  return href
}

describe("New connection — returnTo", () => {
  it("follows an app-relative path", () => {
    expect(backHref("returnTo=/pipelines/abc")).toBe("/pipelines/abc")
  })

  it("falls back to the list for an off-app or script URL", () => {
    for (const bad of ["//evil.example", "/\\evil.example", "https://evil.example", "javascript:alert(1)"]) {
      expect(backHref(`returnTo=${encodeURIComponent(bad)}`)).toBe("/connections")
    }
  })

  it("defaults to the list when absent", () => {
    expect(backHref("")).toBe("/connections")
  })
})
