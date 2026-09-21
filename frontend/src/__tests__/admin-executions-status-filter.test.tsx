import { beforeAll, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"

import AdminExecutionsPage from "@/app/(dashboard)/admin/executions/page"
import { authFetch } from "@/lib/api/auth-fetch"

// #53: the Status filter was a free-text box and the search hint was cut off.

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
  usePathname: () => "/admin/executions",
}))
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))

const mockFetch = authFetch as unknown as Mock

beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
})

describe("Admin → Executions filters", () => {
  it("offers the status as a fixed list and keeps the search hint short", async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      headers: { get: () => null },
      json: async () => ({ data: [], total: 0, limit: 50, offset: 0 }),
    } as unknown as Response)
    render(<AdminExecutionsPage />)

    await waitFor(() => expect(mockFetch).toHaveBeenCalled())
    expect(String(mockFetch.mock.calls[0][0])).not.toMatch(/status=/)

    expect(screen.queryByPlaceholderText("Status (optional)")).toBeNull()
    const status = screen.getByRole("combobox", { name: "Status" })
    expect(status).toHaveTextContent("All statuses")

    const search = screen.getByRole("textbox", { name: "Search executions" })
    expect((search as HTMLInputElement).placeholder.length).toBeLessThanOrEqual(30)
    expect(search).toHaveAttribute("title", expect.stringMatching(/execution id, pipeline id, pipeline name, or email/))

    expect(screen.getByRole("heading", { name: "Admin" })).toBeInTheDocument()
  })
})
