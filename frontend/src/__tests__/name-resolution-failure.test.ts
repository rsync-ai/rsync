import { readFileSync } from "node:fs"
import { resolve } from "node:path"
import { describe, expect, it } from "vitest"
import { isNameResolutionFailure } from "@/lib/errors/name-resolution"

/**
 * The explorer's schema-load failure had one hint for "the hostname could not be
 * resolved" and a generic one for everything else, and it reached the first only
 * on the literal text "no such host" -- which Go emits for NXDOMAIN and nothing
 * else. A resolver answering SERVFAIL (`server misbehaving`) therefore landed on
 * "check that the database connection is active, then click refresh", advice
 * that cannot fix a name which will not resolve, and the user retries forever.
 *
 * The cases below are not the one wording that was observed on CI. They are
 * every error the Go resolver itself defines plus the glibc getaddrinfo wording,
 * so the class is closed against the resolver rather than patched against the
 * instance.
 */
describe("isNameResolutionFailure", () => {
  const resolverFailures = [
    'Post "http://rsync-ai-postgresql-mcp:8000/mcp": dial tcp: lookup rsync-ai-postgresql-mcp on 127.0.0.53:53: no such host',
    "dial tcp: lookup db.internal: Name or service not known",
    'dial tcp: lookup postgresql-mcp on 127.0.0.53:53: server misbehaving',
    "dial tcp: lookup postgresql-mcp on 10.96.0.10:53: no answer from DNS server",
    "dial tcp: lookup postgresql-mcp on 10.96.0.10:53: lame referral",
    "dial tcp: lookup postgresql-mcp on 10.96.0.10:53: invalid DNS response",
    "dial tcp: lookup postgresql-mcp on 10.96.0.10:53: cannot unmarshal DNS message",
    "dial tcp: lookup postgresql-mcp on 10.96.0.10:53: cannot marshal DNS message",
    "dest error: [Errno -3] Temporary failure in name resolution",
  ]

  it.each(resolverFailures)("treats %s as a resolver failure", (text) => {
    expect(isNameResolutionFailure(text)).toBe(true)
  })

  // Capitals matter: only NXDOMAIN's Go wording is already lowercase. Every
  // other line above carries "DNS" or a leading capital that err.Error()
  // preserves verbatim, so a caller passing raw text must still match.
  it("is case-insensitive, because the resolver's other wordings carry capitals", () => {
    expect(isNameResolutionFailure("Cannot Unmarshal DNS Message")).toBe(true)
  })

  // The control. Without it a predicate that simply returned true would pass
  // every case above.
  it.each([
    "connection refused",
    "i/o timeout",
    'pq: duplicate key value violates unique constraint "connections_name_key"',
    "permission denied for table orders",
    "",
  ])("does not claim %s is a resolver failure", (text) => {
    expect(isNameResolutionFailure(text)).toBe(false)
  })
})

/**
 * Wiring guard. The predicate above is only worth anything if the branch that
 * chooses the hint actually calls it -- the defect being fixed was an inline
 * one-marker test at exactly that site, and re-introducing one there would leave
 * every assertion above green.
 */
describe("the explorer schema hint routes through the shared predicate", () => {
  const page = readFileSync(
    resolve(__dirname, "../app/(dashboard)/explorer/page.tsx"),
    "utf-8",
  )

  it("calls isNameResolutionFailure rather than testing for one wording inline", () => {
    expect(page).toContain("const isDnsFailure = isNameResolutionFailure(")
    expect(page).not.toMatch(/const isDnsFailure = lower\.includes/)
  })
})
