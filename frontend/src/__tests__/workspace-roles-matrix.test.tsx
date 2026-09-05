import { describe, expect, it } from "vitest"
import { render, screen } from "@testing-library/react"

import { WorkspaceRolesMatrix } from "@/components/workspace/WorkspaceRolesMatrix"
import { WORKSPACE_CAPABILITY_ROWS, ROLE_LABELS } from "@/lib/workspace/roles"

// P0 Roles & permissions tab: a read-only matrix of capability (rows) x role
// (columns) so members understand what each role can do. Pure render driven by the
// shared roles module — no fetching, no mutation.

describe("WorkspaceRolesMatrix", () => {
  it("renders a column header for every role", () => {
    render(<WorkspaceRolesMatrix />)
    // Each role appears as a column header (and again in the legend), so assert on
    // the header cell specifically rather than the ambiguous label text.
    expect(screen.getByTestId("role-col-owner").textContent).toContain(ROLE_LABELS.owner)
    expect(screen.getByTestId("role-col-admin").textContent).toContain(ROLE_LABELS.admin)
    expect(screen.getByTestId("role-col-member").textContent).toContain(ROLE_LABELS.member)
    expect(screen.getByTestId("role-col-viewer").textContent).toContain(ROLE_LABELS.viewer)
  })

  it("renders a row for every capability", () => {
    render(<WorkspaceRolesMatrix />)
    for (const row of WORKSPACE_CAPABILITY_ROWS) {
      expect(screen.getByText(row.label)).toBeInTheDocument()
    }
  })

  it("owner is allowed every capability; viewer only the lowest", () => {
    render(<WorkspaceRolesMatrix />)
    // delete/transfer is owner-only
    expect(screen.getByTestId("cap-delete-owner").textContent).toContain("Allowed")
    expect(screen.getByTestId("cap-delete-viewer").textContent).toContain("Not allowed")
    expect(screen.getByTestId("cap-delete-admin").textContent).toContain("Not allowed")
    // view is allowed for everyone including viewer
    expect(screen.getByTestId("cap-view-viewer").textContent).toContain("Allowed")
  })

  it("managing members is admin+ but not member/viewer", () => {
    render(<WorkspaceRolesMatrix />)
    expect(screen.getByTestId("cap-members-admin").textContent).toContain("Allowed")
    expect(screen.getByTestId("cap-members-member").textContent).toContain("Not allowed")
  })

  it("marks the caller's current role column", () => {
    render(<WorkspaceRolesMatrix currentRole="admin" />)
    expect(screen.getByTestId("role-col-admin").getAttribute("data-current")).toBe("true")
    expect(screen.getByTestId("role-col-owner").getAttribute("data-current")).toBe("false")
  })
})
