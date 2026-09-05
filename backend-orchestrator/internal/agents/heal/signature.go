package heal

// signature.go — the stable identity of a failure.
//
// Every other part of the heal loop already had an identity: an execution has a
// UUID, a pipeline has a UUID, an event has an event_id. The FAILURE did not.
// That absence is what kept the healer memoryless — it could not ask "have I seen
// this before, and did what I tried last time work?", so it re-derived an action
// from a substring match on every single occurrence and learned nothing across
// them.
//
// A signature is deliberately COARSER than the error string and FINER than the
// category:
//
//   - coarser than the error string, because "connection refused to 10.0.3.7:5432"
//     and "connection refused to 10.0.3.9:5432" are the same failure and must
//     recall the same remedy;
//   - finer than the category, because CategoryNetwork on a postgresql source is
//     not the same operational problem as CategoryNetwork on an s3 destination,
//     and an action that fixed one says nothing about the other.
//
// It is a plain readable string, not a hash. When an operator reads a heal_attempts
// row they should be able to see WHY two incidents were considered the same
// without running the hash themselves.

import (
	"regexp"
	"strings"

	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

// Normalisation rules, applied in order. Each collapses one class of
// per-incident noise that would otherwise make every occurrence unique.
//
// Ordering matters: UUIDs and timestamps are matched before the bare-number rule,
// because the number rule would otherwise shred them into partial tokens that no
// longer match anything.
var signatureScrubbers = []struct {
	re   *regexp.Regexp
	repl string
}{
	// UUIDs — execution/pipeline/correlation ids embedded in error text.
	{regexp.MustCompile(`(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`), "<uuid>"},
	// RFC3339-ish timestamps.
	{regexp.MustCompile(`\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?\b`), "<ts>"},
	// host:port and bare IPv4 — the same dependency failing from a different pod.
	{regexp.MustCompile(`\b\d{1,3}(\.\d{1,3}){3}(:\d+)?\b`), "<addr>"},
	// URLs — keep the scheme+host shape, drop the path/query which carries ids.
	{regexp.MustCompile(`\b(https?)://[^\s"',)]+`), "$1://<url>"},
	// Hex blobs (offsets, LSNs, checksums) before the decimal rule.
	{regexp.MustCompile(`\b0x[0-9a-fA-F]+\b`), "<hex>"},
	// Any remaining run of digits: row counts, ports, offsets, durations.
	{regexp.MustCompile(`\d+`), "<n>"},
	// Collapse whitespace last so the replacements above line up.
	{regexp.MustCompile(`\s+`), " "},
}

// maxSignatureErrorLen bounds the normalized error fragment. Long stack traces and
// multi-statement SQL echoes diverge in their tails while sharing the operationally
// meaningful head; truncating keeps recurrences of one fault matching each other.
const maxSignatureErrorLen = 160

// NormalizeErrorForSignature strips per-incident noise from an error string so two
// occurrences of the same fault produce identical text. Exported for tests and for
// any future caller that wants to group errors without minting a full signature.
func NormalizeErrorForSignature(msg string) string {
	s := strings.ToLower(strings.TrimSpace(msg))
	if s == "" {
		return ""
	}
	// Drop a leading status prefix ("silent_drop_detected: ..."), which is already
	// carried as its own signature field and would double-count here.
	if i := strings.Index(s, ": "); i > 0 && i < 40 && !strings.ContainsAny(s[:i], " \t") {
		s = s[i+2:]
	}
	for _, sc := range signatureScrubbers {
		s = sc.re.ReplaceAllString(s, sc.repl)
	}
	s = strings.TrimSpace(s)
	if len(s) > maxSignatureErrorLen {
		s = s[:maxSignatureErrorLen]
	}
	return s
}

// FailureSignature builds the stable recall key for one incident.
//
// Shape: "<category>|<executor_status>|<source>-><dest>|<normalized error>".
// The diagnosis Category is included but the suggested ACTION is not — the whole
// point of recall is to discover that the action the rules suggest is not the
// action that works, which is impossible if the action is baked into the key.
func FailureSignature(sig diagnose.Signal, dx diagnose.Diagnosis) string {
	part := func(s, fallback string) string {
		s = strings.ToLower(strings.TrimSpace(s))
		if s == "" {
			return fallback
		}
		return s
	}
	return strings.Join([]string{
		part(string(dx.Category), "unknown"),
		part(sig.ExecutorStatus, "unknown"),
		part(sig.SourceType, "?") + "->" + part(sig.DestinationType, "?"),
		NormalizeErrorForSignature(sig.ErrorMessage),
	}, "|")
}
