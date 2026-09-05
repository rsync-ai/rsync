import { describe, expect, it } from "vitest"
import { toWorkspaceSlug } from "@/lib/workspace/slug"

// Mirrors the api-gateway slug derivation so the New Workspace dialog can show a
// live preview of the slug the backend will actually store. Backend
// (handlers/workspaces.go toSlug + CreateWorkspace fallback):
//   lowercase -> replace [^a-z0-9]+ with "-" -> trim "-" -> cap 60 chars;
//   an empty result becomes "workspace".

describe("toWorkspaceSlug", () => {
  it("lowercases and hyphenates words", () => {
    expect(toWorkspaceSlug("Acme Corp")).toBe("acme-corp")
    expect(toWorkspaceSlug("UPPER")).toBe("upper")
  })

  it("collapses runs of non-alphanumerics into a single hyphen", () => {
    expect(toWorkspaceSlug("Foo___Bar!!!Baz")).toBe("foo-bar-baz")
    expect(toWorkspaceSlug("a  b   c")).toBe("a-b-c")
  })

  it("trims leading and trailing separators and whitespace", () => {
    expect(toWorkspaceSlug("  Hello  World  ")).toBe("hello-world")
    expect(toWorkspaceSlug("--hi--")).toBe("hi")
  })

  it("leaves an already-valid slug stable (idempotent)", () => {
    expect(toWorkspaceSlug("already-a-slug")).toBe("already-a-slug")
    expect(toWorkspaceSlug(toWorkspaceSlug("Acme Corp"))).toBe("acme-corp")
  })

  it("falls back to 'workspace' when nothing slug-able remains", () => {
    expect(toWorkspaceSlug("")).toBe("workspace")
    expect(toWorkspaceSlug("   ")).toBe("workspace")
    expect(toWorkspaceSlug("日本語")).toBe("workspace")
    expect(toWorkspaceSlug("!!!")).toBe("workspace")
  })

  it("caps the slug at 60 characters (matching the backend)", () => {
    const long = "a".repeat(80)
    expect(toWorkspaceSlug(long)).toHaveLength(60)
  })
})
