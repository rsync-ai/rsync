// Package llmscrub masks likely customer data in free-form error/log text
// before that text is included in an LLM prompt.
//
// Contract (CLAUDE.md "LLM data privacy"): the LLM may see schema metadata
// (table/column names, data types, row counts) and user-authored text — never
// row values, credentials, or PII. Database and sink error strings are the main
// leak path (failed INSERT value lists, duplicate-key details), so every
// free-text field that leaves the platform for an LLM must pass through Scrub.
//
// Scrub is intentionally lossy: when in doubt it removes content. Losing a
// fragment of an error message costs diagnosis quality; leaking a row value
// breaks the privacy contract. Double-quoted identifiers (Postgres-style) are
// preserved — they are schema metadata, which the contract allows. Small
// integers (ports, counts, percentages) are preserved; digit runs of 7+ are
// masked as likely record identifiers.
package llmscrub

import (
	"regexp"
)

const redacted = "[redacted]"

var (
	// Postgres not-null/check-constraint DETAIL dumps the ENTIRE row:
	// "Failing row contains (42, Jane Doe, 555-0182, ...)." Greedy to end of
	// string so a truncated dump can't leak a partial row.
	reFailingRow = regexp.MustCompile(`(?is)\bFailing row contains\b.*`)
	// Everything after a SQL VALUES keyword is row data, including multi-tuple
	// inserts: INSERT INTO t (a,b) VALUES (1,'x'),(2,'y') ...
	reValues = regexp.MustCompile(`(?is)\bVALUES\b\s*\(.*`)
	// Postgres duplicate-key detail: Key (email)=(user@x.com) already exists.
	// Column names (group 1) are schema metadata and kept; values are masked.
	// One nesting level of parentheses inside the value is tolerated.
	reKeyDetail = regexp.MustCompile(`(?i)(\bKey\s*\([^)]*\)=)\((?:[^()]|\([^)]*\))*\)`)
	// Credentials embedded in URLs: scheme://user:pass@host
	reURLCred = regexp.MustCompile(`(\w+://)[^/\s:@]+:[^/\s@]*@`)
	// HTTP credential headers: "Authorization: Bearer <token>". Must run before
	// reKVSensitive, whose \S+ would otherwise consume only the word "Bearer"
	// and leave the token itself in the text.
	reBearer = regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9._~+/=\-]+`)
	// key=value / key: value pairs with credential-bearing key names.
	reKVSensitive = regexp.MustCompile(`(?i)\b(password|passwd|pwd|secret|token|api[_-]?key|apikey|authorization|bearer|access[_-]?key|private[_-]?key|client[_-]?secret)\b\s*[=:]\s*\S+`)
	// The same key=value shape when the credential word is fused into a longer
	// key name. reKVSensitive is anchored with \b, and an underscore is a word
	// character, so KAFKA_SASL_PASSWORD, SASL_PASSWORD, sasl_plain_password,
	// KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET and the OAuthClientSecret field of
	// kafkaclient.Config under %#v had no word boundary before the credential
	// word and matched nothing at all. (Config.String() redacts, so %v/%+v are
	// already safe; %#v and reflect-based dumps bypass it.)
	//
	// Kafka is where that bites hardest: every SASL credential this platform
	// reads is spelled that way, and a broker credential is shared across all
	// tenants rather than scoped to one, so a single connect error carrying an
	// env dump leaks a cluster-wide secret. AWS_SECRET_ACCESS_KEY /
	// AWS_SESSION_TOKEN ride the same shape on the MSK IAM path.
	//
	// The key name is kept: it is configuration metadata, and it names the
	// variable an operator has to rotate. Bare "key" is deliberately NOT a
	// suffix — masking it would eat the Postgres "Key (email)=" detail and
	// schema metadata like partition_key. Character classes are spelled out
	// rather than using \w because Python's \w is Unicode-aware and Go's is not.
	reCompoundCred = regexp.MustCompile(`(?i)\b([A-Za-z0-9_.\-]*(?:password|passwd|pwd|secret|token|api[_.\-]?key|access[_.\-]?key|private[_.\-]?key))\b\s*[=:]\s*\S+`)
	// Bare JWTs (base64url segments evade reBase64's charset/length): eyJ = {"
	reJWT = regexp.MustCompile(`\beyJ[A-Za-z0-9_\-]{4,}(?:\.[A-Za-z0-9_\-]+){1,2}`)
	// Double-quoted string directly after a colon — Postgres quotes offending
	// VALUES this way ('invalid input syntax for type integer: "jane"'), and it
	// also catches JSON string values ("name": "Jane"). Identifier quoting
	// (relation "public.orders", constraint "users_pkey") never follows a colon
	// and is preserved.
	reColonQuoted = regexp.MustCompile(`(:\s*)"(?:[^"\\]|\\.)*"`)
	// JSON numeric values ("age": 41) — quoted key + colon + number.
	reJSONNum = regexp.MustCompile(`("[\w\-]+"\s*:\s*)-?\d[\d.eE+\-]*`)
	// Single-quoted literals — SQL string values. MySQL quotes values this way
	// ("Duplicate entry 'x' for key 'y'"); identifiers occasionally caught here
	// are an accepted loss (fail-closed). The trailing `(?:'|$)` also covers a
	// literal left OPEN by upstream truncation: log pipelines cut the text before
	// we scrub (e.g. SigNoz substring(body,1,2000)), so the closing quote may be
	// gone.
	//
	// The leading `(^|[^A-Za-z0-9_])` is what makes this safe on prose. A SQL
	// literal always opens at the start of the text or after a delimiter — space,
	// `=`, `(`, `,`, `:`. An apostrophe sitting BETWEEN word characters is a
	// contraction or possessive ("couldn't", "the user's"), and treating it as an
	// opener inverted this scrubber's contract: on
	//
	//   Couldn't apply schema change to pipeline 'orders-sync'.
	//
	// the run from "Couldn'" to the quote before orders-sync was consumed as the
	// literal, so the PROSE was redacted and the pipeline name LEAKED verbatim
	// (`Couldn'[redacted]'orders-sync'[redacted]`). This was reaching users: it is
	// the mangled `…pipeline '[redacted]` body seen in the notification bell.
	//
	// Paired and unpaired are one rule rather than two sequential passes for the
	// same reason — a second `'…$` pass re-read the `'[redacted]'` this pass had
	// just written and appended another `'[redacted]` to it, corrupting text that
	// was already correctly redacted (and eating the diagnostic tail after it).
	// Replacements are not rescanned, so one pass cannot do that to itself.
	reQuotedLiteral = regexp.MustCompile(`(?s)(^|[^A-Za-z0-9_])'(?:[^'\\]|\\.)*(?:'|$)`)
	// Email addresses appearing outside quotes.
	reEmail = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`)
	// IPv4 addresses — client/host IPs are personal data under GDPR and are not
	// needed for diagnosis. Octets validated 0-255 so 3-part version strings
	// (8.0.32) and ISO dates don't match; a 4-part dotted-quad is an accepted
	// loss. MUST stay lockstep with Python masking._SCRUB_PATTERNS.
	reIPv4 = regexp.MustCompile(`\b(?:(?:25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\b`)
	// Long base64-ish blobs (tokens, keys). 40+ chars keeps 32-hex trace ids.
	reBase64 = regexp.MustCompile(`\b[A-Za-z0-9+/]{40,}={0,2}\b`)
	// Dashed SSN / US-phone shapes (ISO dates \d{4}-\d{2}-\d{2} don't match —
	// timestamps must survive for diagnosis).
	reSSN   = regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`)
	rePhone = regexp.MustCompile(`\b\d{3}[-.]\d{3}[-.]\d{4}\b`)
	// Digit runs of 7+ — likely record ids, SSNs, phone numbers. Ports and row
	// counts (≤6 digits) survive.
	reLongDigits = regexp.MustCompile(`\d{7,}`)
)

// Scrub masks likely customer data (row values, credentials, PII) in error/log
// text. Safe on empty input; idempotent for practical purposes. Rule order is
// load-bearing (documented on each pattern above).
func Scrub(s string) string {
	if s == "" {
		return s
	}
	s = reFailingRow.ReplaceAllString(s, "Failing row contains ("+redacted+")")
	s = reValues.ReplaceAllString(s, "VALUES ("+redacted+")")
	s = reKeyDetail.ReplaceAllString(s, "${1}("+redacted+")")
	s = reURLCred.ReplaceAllString(s, "${1}"+redacted+"@")
	s = reBearer.ReplaceAllString(s, "Bearer "+redacted)
	s = reKVSensitive.ReplaceAllString(s, "${1}="+redacted)
	s = reCompoundCred.ReplaceAllString(s, "${1}="+redacted)
	s = reJWT.ReplaceAllString(s, redacted)
	s = reColonQuoted.ReplaceAllString(s, `${1}"`+redacted+`"`)
	s = reJSONNum.ReplaceAllString(s, "${1}[num-redacted]")
	s = reQuotedLiteral.ReplaceAllString(s, "${1}'"+redacted+"'")
	s = reEmail.ReplaceAllString(s, "[email-redacted]")
	s = reIPv4.ReplaceAllString(s, "[ip-redacted]")
	s = reBase64.ReplaceAllString(s, redacted)
	s = reSSN.ReplaceAllString(s, "[num-redacted]")
	s = rePhone.ReplaceAllString(s, "[num-redacted]")
	s = reLongDigits.ReplaceAllString(s, "[num-redacted]")
	return s
}

// ScrubMax scrubs and then truncates to at most max runes (0 or negative
// disables truncation). Truncation happens after scrubbing so a cut can never
// split a masked token back open.
func ScrubMax(s string, max int) string {
	s = Scrub(s)
	if max > 0 {
		r := []rune(s)
		if len(r) > max {
			return string(r[:max]) + "…"
		}
	}
	return s
}
