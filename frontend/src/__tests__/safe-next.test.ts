import { describe, expect, it } from "vitest"

import { safeNextPath } from "@/lib/auth/safe-next"

// P5b-UI #3 (review remediation): the shared return-url guard used by login + signup.
// A safe next is an app-relative PATH. Protocol-relative ("//evil.com") and
// backslash-smuggled ("/\\evil.com") values pass a naive startsWith("/") check but a
// browser resolves them to an EXTERNAL origin — an open-redirect / phishing vector.

describe("safeNextPath", () => {
  it("allows app-relative paths", () => {
    expect(safeNextPath("/")).toBe("/")
    expect(safeNextPath("/invite/tok-1")).toBe("/invite/tok-1")
    expect(safeNextPath("/workspace/members?x=1")).toBe("/workspace/members?x=1")
  })

  it("rejects protocol-relative and backslash-smuggled URLs (open redirect)", () => {
    expect(safeNextPath("//evil.com")).toBe("/")
    expect(safeNextPath("//evil.example.com/path")).toBe("/")
    expect(safeNextPath("/\\evil.com")).toBe("/")
  })

  it("rejects absolute URLs and empty/missing values", () => {
    expect(safeNextPath("https://evil.com")).toBe("/")
    expect(safeNextPath("http://evil.com")).toBe("/")
    expect(safeNextPath("javascript:alert(1)")).toBe("/")
    expect(safeNextPath("")).toBe("/")
    expect(safeNextPath(null)).toBe("/")
    expect(safeNextPath(undefined)).toBe("/")
  })
})
