import { describe, expect, it } from "vitest"
import {
  roleRank,
  meetsRole,
  can,
  canEditSavedQuery,
  roleHasCapability,
  ROLE_LABELS,
  ROLE_DESCRIPTIONS,
  WORKSPACE_CAPABILITY_ROWS,
  WORKSPACE_ROLES,
  type WorkspaceRole,
  type WorkspaceAction,
} from "@/lib/workspace/roles"

// P0 keystone: a single, pure source of truth for workspace role/permission
// decisions, mirroring the backend hierarchy owner(4) > admin(3) > member(2) >
// viewer(1) and the backend's fail-closed `Role.Meets` (unknown role => level 0).
// Every role-gated control in the UI defers to `can()` / `meetsRole()` here so the
// rules can't drift per-component (the old WorkspaceMembers had its own roleRank).

describe("workspace roles — roleRank", () => {
  it("ranks the four roles in the backend order", () => {
    expect(roleRank("owner")).toBe(4)
    expect(roleRank("admin")).toBe(3)
    expect(roleRank("member")).toBe(2)
    expect(roleRank("viewer")).toBe(1)
  })

  it("fails closed: an unknown / empty role ranks 0 (below viewer)", () => {
    expect(roleRank("superuser")).toBe(0)
    expect(roleRank("")).toBe(0)
    expect(roleRank("OWNER")).toBe(0) // case-sensitive — backend roles are lowercase
  })
})

describe("workspace roles — meetsRole", () => {
  it("is true when the role meets or exceeds the minimum", () => {
    expect(meetsRole("owner", "admin")).toBe(true)
    expect(meetsRole("admin", "admin")).toBe(true)
    expect(meetsRole("admin", "member")).toBe(true)
  })

  it("is false when the role is below the minimum", () => {
    expect(meetsRole("member", "admin")).toBe(false)
    expect(meetsRole("viewer", "member")).toBe(false)
  })

  it("fails closed for an unknown role against any minimum", () => {
    expect(meetsRole("nonsense", "viewer")).toBe(false)
  })
})

describe("workspace roles — can(role, action)", () => {
  it("read access (view) is granted to viewer and up", () => {
    for (const r of WORKSPACE_ROLES) expect(can(r, "view")).toBe(true)
    expect(can("ghost", "view")).toBe(false)
  })

  it("creating/running pipelines requires member or higher", () => {
    expect(can("owner", "create_pipeline")).toBe(true)
    expect(can("admin", "create_pipeline")).toBe(true)
    expect(can("member", "create_pipeline")).toBe(true)
    expect(can("viewer", "create_pipeline")).toBe(false)
  })

  it("managing members and renaming require admin or higher (mirrors backend owner/admin gate)", () => {
    for (const action of ["manage_members", "rename_workspace"] as WorkspaceAction[]) {
      expect(can("owner", action)).toBe(true)
      expect(can("admin", action)).toBe(true)
      expect(can("member", action)).toBe(false)
      expect(can("viewer", action)).toBe(false)
    }
  })

  it("deleting the workspace is owner-only (mirrors backend owner-only gate)", () => {
    expect(can("owner", "delete_workspace")).toBe(true)
    expect(can("admin", "delete_workspace")).toBe(false)
    expect(can("member", "delete_workspace")).toBe(false)
    expect(can("viewer", "delete_workspace")).toBe(false)
  })

  it("any current member (including a viewer) may leave a workspace", () => {
    for (const r of WORKSPACE_ROLES) expect(can(r, "leave_workspace")).toBe(true)
    expect(can("", "leave_workspace")).toBe(false) // not a member => can't leave
  })

  it("fails closed for an unknown action", () => {
    expect(can("owner", "obliterate" as WorkspaceAction)).toBe(false)
  })
})

// Regression: the saved-query and schedule controls shipped ungated. On prod a real
// viewer saw enabled Schedule / Edit / Delete buttons on both the Explorer panel and
// /explorer/schedules; every one of them 403'd server-side ("insufficient workspace
// role"), so the boundary held — but the UI invited a dead end. These pin the two
// gates the controls now use.
describe("workspace roles — saved queries & schedules", () => {
  it("save_query needs member+ (CreateSavedQuery requires WSMember)", () => {
    expect(can("viewer", "save_query")).toBe(false)
    expect(can("member", "save_query")).toBe(true)
    expect(can("admin", "save_query")).toBe(true)
    expect(can("owner", "save_query")).toBe(true)
  })

  it("schedule_query needs admin+ (saved_query_models.go modelRunMinRole = WSAdmin)", () => {
    expect(can("viewer", "schedule_query")).toBe(false)
    expect(can("member", "schedule_query")).toBe(false)
    expect(can("admin", "schedule_query")).toBe(true)
    expect(can("owner", "schedule_query")).toBe(true)
  })

  it("fails closed on an unknown role for both actions", () => {
    for (const bogus of ["", "  ", "Viewer", "superuser"]) {
      expect(can(bogus, "save_query")).toBe(false)
      expect(can(bogus, "schedule_query")).toBe(false)
    }
  })
})

describe("canEditSavedQuery — creator OR workspace admin", () => {
  const ME = "11111111-1111-1111-1111-111111111111"
  const SOMEONE_ELSE = "22222222-2222-2222-2222-222222222222"

  it("lets the creator edit their own row whatever their role", () => {
    for (const r of WORKSPACE_ROLES) {
      expect(canEditSavedQuery(r, ME, ME)).toBe(true)
    }
  })

  it("lets admins and owners edit somebody else's row", () => {
    expect(canEditSavedQuery("admin", SOMEONE_ELSE, ME)).toBe(true)
    expect(canEditSavedQuery("owner", SOMEONE_ELSE, ME)).toBe(true)
  })

  it("refuses viewers and members on somebody else's row", () => {
    expect(canEditSavedQuery("viewer", SOMEONE_ELSE, ME)).toBe(false)
    expect(canEditSavedQuery("member", SOMEONE_ELSE, ME)).toBe(false)
  })

  it("fails closed while the current user is still loading", () => {
    // user?.id is undefined until /auth/me resolves. A blank id must never be
    // treated as matching a blank created_by, or the control flashes enabled.
    expect(canEditSavedQuery("viewer", SOMEONE_ELSE, undefined)).toBe(false)
    expect(canEditSavedQuery("viewer", null, null)).toBe(false)
    expect(canEditSavedQuery("viewer", "", "")).toBe(false)
    expect(canEditSavedQuery("viewer", "   ", "   ")).toBe(false)
  })
})

describe("workspace roles — labels & descriptions", () => {
  it("provides a human label and description for every role", () => {
    for (const r of WORKSPACE_ROLES) {
      expect(ROLE_LABELS[r]).toBeTruthy()
      expect(ROLE_DESCRIPTIONS[r]).toBeTruthy()
    }
  })
})

describe("workspace roles — capability matrix (read-only Roles tab)", () => {
  it("exposes ordered capability rows from least to most privileged", () => {
    const ranks = WORKSPACE_CAPABILITY_ROWS.map((row) => roleRank(row.minRole))
    expect(ranks.length).toBeGreaterThanOrEqual(4)
    const sorted = [...ranks].sort((a, b) => a - b)
    expect(ranks).toEqual(sorted)
  })

  it("roleHasCapability matches meetsRole against each row's minRole", () => {
    for (const row of WORKSPACE_CAPABILITY_ROWS) {
      for (const r of WORKSPACE_ROLES) {
        expect(roleHasCapability(r, row.minRole)).toBe(meetsRole(r, row.minRole))
      }
    }
  })

  it("the most-privileged row (delete/transfer) is owner-only", () => {
    const last = WORKSPACE_CAPABILITY_ROWS[WORKSPACE_CAPABILITY_ROWS.length - 1]
    expect(last.minRole).toBe<WorkspaceRole>("owner")
    expect(roleHasCapability("admin", last.minRole)).toBe(false)
  })
})
