import { describe, expect, it } from "vitest"

import { canGenerateConnectors } from "@/contexts/CurrentUserContext"

// Connector generation is gated on the WORKSPACE axis (owner/admin), mirroring
// the api-gateway WorkspaceGeneratorMiddleware. This locks the contract so a
// future refactor cannot silently re-admit the old global-role values
// (power_user/user) or leak generation to members/viewers.
describe("canGenerateConnectors (workspace-role gate)", () => {
  it("allows workspace owner and admin", () => {
    expect(canGenerateConnectors("owner")).toBe(true)
    expect(canGenerateConnectors("admin")).toBe(true)
    // case/whitespace tolerant, matching the backend normalization
    expect(canGenerateConnectors(" Owner ")).toBe(true)
    expect(canGenerateConnectors("ADMIN")).toBe(true)
  })

  it("denies workspace member and viewer", () => {
    expect(canGenerateConnectors("member")).toBe(false)
    expect(canGenerateConnectors("viewer")).toBe(false)
  })

  it("denies the retired global-role values", () => {
    // These are platform-staff roles, not workspace roles — they must NOT pass
    // the workspace gate now that the axis has changed.
    expect(canGenerateConnectors("power_user")).toBe(false)
    expect(canGenerateConnectors("user")).toBe(false)
  })

  it("fails closed for unknown / empty / nullish roles", () => {
    expect(canGenerateConnectors("")).toBe(false)
    expect(canGenerateConnectors("superuser")).toBe(false)
    expect(canGenerateConnectors(null)).toBe(false)
    expect(canGenerateConnectors(undefined)).toBe(false)
  })
})
