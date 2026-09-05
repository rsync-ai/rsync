package main

// Row-value / credential scrubber for sink log output.
//
// PORTED FROM backend-orchestrator/pkg/llmscrub/scrub.go — KEEP IN LOCKSTEP.
// The sink worker is a separate Go module and cannot import llmscrub without
// pulling the whole backend-orchestrator module into its isolated Docker build
// context, so the row-value patterns are duplicated here. When you change the
// canonical llmscrub regex set (or the Python mirror in llm-service
// masking.py), update this file too.
//
// Why the sink needs it: destination DB drivers embed offending ROW VALUES in
// their error text (Postgres "DETAIL: Key (email)=(alice@example.com)…",
// "Failing row contains (…)"; MySQL "Duplicate entry 'jane@acme.com'…"), and
// the sink logs those errors verbatim on DLQ-routing / flush failures. Those
// lines ship to SigNoz, so they must be scrubbed first.
//
// The lockstep is enforced, not just asserted: scrub_golden_parity_test.go pins
// scrubLog to shared/scrubber_golden.json, the same fixture the orchestrator and
// llm-service scrubbers are pinned to. Patching two of the three now fails CI.

import "regexp"

const scrubRedacted = "[redacted]"

var (
	reScrubFailingRow  = regexp.MustCompile(`(?is)\bFailing row contains\b.*`)
	reScrubValues      = regexp.MustCompile(`(?is)\bVALUES\b\s*\(.*`)
	reScrubKeyDetail   = regexp.MustCompile(`(?i)(\bKey\s*\([^)]*\)=)\((?:[^()]|\([^)]*\))*\)`)
	reScrubURLCred     = regexp.MustCompile(`(\w+://)[^/\s:@]+:[^/\s@]*@`)
	reScrubBearer      = regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9._~+/=\-]+`)
	reScrubKVSensitive = regexp.MustCompile(`(?i)\b(password|passwd|pwd|secret|token|api[_-]?key|apikey|authorization|bearer|access[_-]?key|private[_-]?key|client[_-]?secret)\b\s*[=:]\s*\S+`)
	// Credential words fused into a longer key name — KAFKA_SASL_PASSWORD,
	// sasl_plain_password, OAuthClientSecret:… — which the \b-anchored rule
	// above cannot see. The sink dials Kafka itself, so its own connect errors
	// are a first-party carrier for the broker credential. See
	// llmscrub.reCompoundCred for the full rationale.
	reScrubCompoundCred = regexp.MustCompile(`(?i)\b([A-Za-z0-9_.\-]*(?:password|passwd|pwd|secret|token|api[_.\-]?key|access[_.\-]?key|private[_.\-]?key))\b\s*[=:]\s*\S+`)
	reScrubJWT          = regexp.MustCompile(`\beyJ[A-Za-z0-9_\-]{4,}(?:\.[A-Za-z0-9_\-]+){1,2}`)
	reScrubColonQuoted  = regexp.MustCompile(`(:\s*)"(?:[^"\\]|\\.)*"`)
	reScrubJSONNum      = regexp.MustCompile(`("[\w\-]+"\s*:\s*)-?\d[\d.eE+\-]*`)
	// Single-quoted literals, paired or left open by upstream truncation, as ONE
	// rule. This file carried the pre-fix two-pass form long after llmscrub and
	// masking.py dropped it, and both halves of that form are actively wrong:
	//
	//   * without the leading `(^|[^A-Za-z0-9_])`, the apostrophe in "Couldn't"
	//     opened a literal, so "Couldn't route to DLQ for pipeline 'orders-sync'."
	//     became `Couldn'[redacted]'orders-sync'[redacted]` — the prose redacted
	//     and the pipeline name left in the clear, the contract exactly inverted;
	//   * the separate `'…$` pass re-read the `'[redacted]'` the first pass had
	//     just written and appended another `'[redacted]`, so `value was
	//     'topsecret' in the row` lost its diagnostic tail.
	//
	// Replacements are not rescanned, so one pass cannot do either to itself.
	reScrubQuotedLiteral = regexp.MustCompile(`(?s)(^|[^A-Za-z0-9_])'(?:[^'\\]|\\.)*(?:'|$)`)
	reScrubEmail         = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`)
	// IPv4 — personal data under GDPR and not needed for diagnosis. Absent from
	// this copy entirely until the golden parity test went looking for it.
	reScrubIPv4       = regexp.MustCompile(`\b(?:(?:25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\b`)
	reScrubBase64     = regexp.MustCompile(`\b[A-Za-z0-9+/]{40,}={0,2}\b`)
	reScrubSSN        = regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`)
	reScrubPhone      = regexp.MustCompile(`\b\d{3}[-.]\d{3}[-.]\d{4}\b`)
	reScrubLongDigits = regexp.MustCompile(`\d{7,}`)
)

// scrubLog masks likely customer data (row values, credentials, PII) in sink
// log text. Safe on empty input. Rule order mirrors llmscrub.Scrub.
func scrubLog(s string) string {
	if s == "" {
		return s
	}
	s = reScrubFailingRow.ReplaceAllString(s, "Failing row contains ("+scrubRedacted+")")
	s = reScrubValues.ReplaceAllString(s, "VALUES ("+scrubRedacted+")")
	s = reScrubKeyDetail.ReplaceAllString(s, "${1}("+scrubRedacted+")")
	s = reScrubURLCred.ReplaceAllString(s, "${1}"+scrubRedacted+"@")
	s = reScrubBearer.ReplaceAllString(s, "Bearer "+scrubRedacted)
	s = reScrubKVSensitive.ReplaceAllString(s, "${1}="+scrubRedacted)
	s = reScrubCompoundCred.ReplaceAllString(s, "${1}="+scrubRedacted)
	s = reScrubJWT.ReplaceAllString(s, scrubRedacted)
	s = reScrubColonQuoted.ReplaceAllString(s, `${1}"`+scrubRedacted+`"`)
	s = reScrubJSONNum.ReplaceAllString(s, "${1}[num-redacted]")
	s = reScrubQuotedLiteral.ReplaceAllString(s, "${1}'"+scrubRedacted+"'")
	s = reScrubEmail.ReplaceAllString(s, "[email-redacted]")
	s = reScrubIPv4.ReplaceAllString(s, "[ip-redacted]")
	s = reScrubBase64.ReplaceAllString(s, scrubRedacted)
	s = reScrubSSN.ReplaceAllString(s, "[num-redacted]")
	s = reScrubPhone.ReplaceAllString(s, "[num-redacted]")
	s = reScrubLongDigits.ReplaceAllString(s, "[num-redacted]")
	return s
}
