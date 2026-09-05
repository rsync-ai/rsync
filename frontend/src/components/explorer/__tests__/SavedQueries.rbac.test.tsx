import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"

// Prod regression (workspace `demo`, real viewer account): the Saved Queries panel
// rendered Schedule / Edit / Delete fully enabled for a viewer. Every one of those
// mutations 403s at the api-gateway with "insufficient workspace role", so no data
// was ever at risk — but the viewer was handed three buttons that could only fail.
// `Run Query`, `Generate SQL` and `Save current` were gated; these three were not.
//
// The role is a mutable let so one file can render the same panel as viewer, member
// and admin without a separate module mock per role.
let mockRole = "viewer"
let mockUserId: string | undefined = "99999999-9999-9999-9999-999999999999"

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() } }))
vi.mock("@/contexts/WorkspaceContext", () => ({
  useWorkspaceRole: () => ({
    role: mockRole,
    isLoading: false,
    error: false,
    activeWorkspace: null,
    // Delegate to the real table so this test can't drift from roles.ts.
    can: (action: string) => realCan(mockRole, action as never),
    meets: () => false,
  }),
}))
vi.mock("@/contexts/CurrentUserContext", () => ({
  useCurrentUser: () => ({
    user: mockUserId ? { id: mockUserId, email: "v@example.com", name: "V", role: "user", status: "active" } : null,
    role: "user",
    isLoading: false,
  }),
}))

import { can as realCan } from "@/lib/workspace/roles"
import { authFetch } from "@/lib/api/auth-fetch"
import { SavedQueries, type SavedQuery } from "@/components/explorer/SavedQueries"

const CONNECTION_ID = "44444444-4444-4444-4444-444444444444"
const OWNER_ID = "11111111-1111-1111-1111-111111111111"

const mockFetch = authFetch as unknown as ReturnType<typeof vi.fn>

function res(status: number, body: unknown) {
  return { ok: status >= 200 && status < 300, status, json: async () => body } as unknown as Response
}

function query(overrides: Partial<SavedQuery> = {}): SavedQuery {
  return {
    id: "33333333-3333-3333-3333-333333333333",
    connection_id: CONNECTION_ID,
    name: "Viewer RBAC probe",
    sql_text: "UPDATE demo_products SET sku = sku WHERE 1 = 0",
    statement_class: "dml_write",
    visibility: "workspace",
    created_by: OWNER_ID,
    created_at: "2026-08-17T00:00:00Z",
    updated_at: "2026-08-17T00:00:00Z",
    ...overrides,
  }
}

async function renderAs(role: string, items: SavedQuery[], userId?: string) {
  mockRole = role
  mockUserId = userId ?? "99999999-9999-9999-9999-999999999999"
  mockFetch.mockResolvedValue(res(200, { saved_queries: items, count: items.length }))
  render(<SavedQueries connectionId={CONNECTION_ID} currentSql="SELECT 1" onLoad={vi.fn()} />)
  await waitFor(() => expect(screen.getByText("Viewer RBAC probe")).toBeInTheDocument())
}

const schedule = () => screen.getByRole("button", { name: /^Schedule Viewer RBAC probe$/ })
const edit = () => screen.getByRole("button", { name: /^Edit Viewer RBAC probe$/ })
const del = () => screen.getByRole("button", { name: /^Delete Viewer RBAC probe$/ })
const saveCurrent = () => screen.getByRole("button", { name: /Save current/ })

beforeEach(() => {
  vi.clearAllMocks()
  mockRole = "viewer"
  mockUserId = "99999999-9999-9999-9999-999999999999"
})

describe("SavedQueries — viewer sees no enabled mutation controls", () => {
  it("disables Schedule, Edit, Delete and Save current for a viewer", async () => {
    await renderAs("viewer", [query()])
    expect(schedule()).toBeDisabled()
    expect(edit()).toBeDisabled()
    expect(del()).toBeDisabled()
    expect(saveCurrent()).toBeDisabled()
  })

  it("explains why rather than silently doing nothing", async () => {
    await renderAs("viewer", [query()])
    expect(schedule().getAttribute("title")).toMatch(/admin role or higher.*viewer/i)
    expect(edit().getAttribute("title")).toMatch(/creator or a workspace admin.*viewer/i)
    expect(saveCurrent().getAttribute("title")).toMatch(/member role or higher.*viewer/i)
  })
})

describe("SavedQueries — the gate is per-role, not a blanket hide", () => {
  it("lets a member save and manage their own row, but not schedule it", async () => {
    const mine = "99999999-9999-9999-9999-999999999999"
    await renderAs("member", [query({ created_by: mine })], mine)
    expect(saveCurrent()).toBeEnabled()
    expect(edit()).toBeEnabled()
    expect(del()).toBeEnabled()
    // Scheduling runs the SQL unattended under the creator's authority — admin+ only.
    expect(schedule()).toBeDisabled()
  })

  it("stops a member editing somebody else's saved query", async () => {
    await renderAs("member", [query({ created_by: OWNER_ID })], "99999999-9999-9999-9999-999999999999")
    expect(edit()).toBeDisabled()
    expect(del()).toBeDisabled()
  })

  it("leaves every control enabled for an admin", async () => {
    await renderAs("admin", [query()])
    expect(schedule()).toBeEnabled()
    expect(edit()).toBeEnabled()
    expect(del()).toBeEnabled()
    expect(saveCurrent()).toBeEnabled()
  })
})
