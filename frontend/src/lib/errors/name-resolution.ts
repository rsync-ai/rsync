/**
 * Does this error text mean DNS declined to produce an address?
 *
 * "no such host" is only NXDOMAIN. Go words every one of its other resolver
 * failures differently -- the error block in net/dnsclient_unix.go defines six
 * more -- and the cgo resolver relays glibc's getaddrinfo wording instead. A UI
 * that tests for NXDOMAIN alone therefore misses most real resolver outages and
 * tells the user to check that their connection is active, advice that cannot
 * fix a hostname that will not resolve.
 *
 * Caught on CI 2026-09-06 by a runner whose resolver answered SERVFAIL:
 * `lookup rsync-ai-postgresql-mcp on 127.0.0.53:53: server misbehaving`.
 *
 * The list is enumerated from the resolver's own error block rather than from
 * the one wording that was observed, so it closes the class instead of patching
 * the instance. Kept in lockstep with three backend classifiers that decide what
 * the user is told and whether the row is retried:
 *   - api-gateway `internal/handlers/errors.go` isNameResolutionFailure
 *   - backend-orchestrator `pkg/diagnose/diagnose.go` transient network set
 *   - kafka-mcp-sink worker `infra_fault.go` destInfraFaultMarkers
 */
const RESOLVER_FAILURE_MARKERS = [
  // NXDOMAIN, in Go's wording and in glibc's.
  "no such host",
  "name or service not known",
  // net/dnsclient_unix.go's remaining six.
  "server misbehaving",
  "no answer from dns server",
  "lame referral",
  "invalid dns response",
  "cannot unmarshal dns message",
  "cannot marshal dns message",
  // getaddrinfo EAI_AGAIN as glibc words it. The BSD/macOS wording for the same
  // errno is the bare "try again", too generic to match on without sweeping up
  // unrelated text, and Linux is what runs in the cluster.
  "temporary failure in name resolution",
] as const

export function isNameResolutionFailure(text: string): boolean {
  const lower = text.toLowerCase()
  return RESOLVER_FAILURE_MARKERS.some((marker) => lower.includes(marker))
}
