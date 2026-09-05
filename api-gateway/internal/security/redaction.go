package security

import (
	"fmt"
	"regexp"
	"strings"
)

// RedactionPolicyVersion tracks the version of redaction rules applied
// Increment when redaction logic changes to enable re-redaction of historical events
//
// v1.1.0 — code detection is anchored to statement position instead of a bare
// substring, and the phone rule no longer matches timestamps. See looksLikeSQL.
const RedactionPolicyVersion = "v1.1.0"

// RedactAny recursively masks sensitive values inside arbitrarily nested JSON-like structures.
// It is safe to use on untrusted event payloads before logging, broadcasting over websockets,
// or persisting in DB.
//
// Supported shapes:
// - map[string]interface{}
// - []interface{}
// - string (best-effort masking of JSON-ish strings and connection strings)
//
// NOTE: This is intentionally conservative: if we can't parse/understand a value, we return it as-is.
func RedactAny(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		return RedactMap(t)
	case []interface{}:
		out := make([]interface{}, 0, len(t))
		for _, item := range t {
			out = append(out, RedactAny(item))
		}
		return out
	case string:
		return redactString(t)
	default:
		return v
	}
}

// RedactMapWithVersion applies redaction and adds policy version metadata
func RedactMapWithVersion(m map[string]interface{}) map[string]interface{} {
	redacted := RedactMap(m)
	if redacted == nil {
		redacted = make(map[string]interface{})
	}
	
	// Add redaction metadata (non-intrusive key)
	redacted["_redaction_policy_version"] = RedactionPolicyVersion
	
	return redacted
}

// RedactMap masks sensitive keys and recursively redacts nested objects/arrays.
func RedactMap(m map[string]interface{}) map[string]interface{} {
	if m == nil {
		return nil
	}

	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		if isSensitiveKey(k) {
			out[k] = MaskedValue
			continue
		}
		out[k] = RedactAny(v)
	}

	return out
}

func redactString(s string) string {
	if s == "" {
		return s
	}

	// Mask common connection string patterns.
	if strings.Contains(strings.ToLower(s), "password=") ||
		strings.Contains(strings.ToLower(s), "pwd=") ||
		strings.Contains(strings.ToLower(s), "secret=") ||
		strings.Contains(strings.ToLower(s), "token=") {
		return MaskConnectionString(s)
	}

	// If this looks like JSON, mask common secret fields.
	trim := strings.TrimSpace(s)
	if strings.HasPrefix(trim, "{") || strings.HasPrefix(trim, "[") {
		return MaskJSON(s)
	}

	// If the string is a header-like value, mask bearer tokens.
	if strings.HasPrefix(strings.ToLower(trim), "bearer ") && len(trim) > len("bearer ")+4 {
		return fmt.Sprintf("Bearer %s", MaskedValue)
	}

	// Code-aware redaction for SQL/Python snippets
	if looksLikeSQL(s) {
		return redactSQL(s)
	}
	if looksLikePython(s) {
		return redactPython(s)
	}

	// PII pattern redaction
	return redactPII(s)
}

// A statement, not a word. Both detectors below decide whether to replace an
// ENTIRE string with a placeholder, and the redacted copy is the only one kept —
// the original is not stored anywhere. So a false positive permanently destroys
// an operator-facing message, and the words that used to trigger one ("update",
// "delete", "from", "class") are ordinary English.
//
// Two things are required of a match now:
//
//   - a statement *shape*, not just a keyword — `DELETE FROM <table> WHERE`,
//     not any sentence containing "delete from";
//   - statement *position* — start of the string, start of a line, or straight
//     after ": " / "; ", which is where a query embedded in an error message
//     actually appears ("query failed: SELECT ... FROM ...").
//
// Anchoring to those positions is what separates "Delete from the list any API
// token you no longer need" from "DELETE FROM sessions WHERE token = 'abc'":
// both begin with the verb and contain the clause keyword, and only the second
// has a table name where a table name belongs.
var sqlStatementRe = regexp.MustCompile(`(?im)(?:^|:[ \t]|;[ \t])[ \t]*(?:` +
	`select\s+.+\s+from\s+[\w".]+` +
	`|insert\s+into\s+[\w".]+` +
	`|update\s+[\w".]+\s+set\s` +
	`|delete\s+from\s+[\w".]+\s*(?:where\b|using\b|returning\b|;|$)` +
	`|create\s+table\s+[\w".]+` +
	`|alter\s+table\s+[\w".]+` +
	`)`)

// Python is line-oriented, so the anchor is the start of a line (leading
// indentation allowed — a `def` inside a class is still a def) and the match is
// case-sensitive, because Python keywords are. "Could not import the secret
// store configuration" starts with "Could"; `import os` starts with `import`.
var pythonStatementRe = regexp.MustCompile(`(?m)^[ \t]*(?:` +
	`def\s+\w+\s*\(` +
	`|class\s+\w+\s*[:(]` +
	`|import\s+[\w.]+` +
	`|from\s+[\w.]+\s+import\s` +
	`)`)

// looksLikeSQL reports whether a string contains an actual SQL statement, at a
// position where a statement can begin.
func looksLikeSQL(s string) bool {
	return sqlStatementRe.MatchString(s)
}

// looksLikePython reports whether a string contains an actual Python statement
// at the start of a line.
func looksLikePython(s string) bool {
	return pythonStatementRe.MatchString(s)
}

// redactSQL masks sensitive parts of SQL queries
// Safe fallback: if unsure, return masked placeholder
func redactSQL(sql string) string {
	// Simple heuristic: mask string literals that look like credentials
	// For safety, if the SQL contains password/secret/token keywords, redact the entire query
	lower := strings.ToLower(sql)
	if strings.Contains(lower, "password") ||
		strings.Contains(lower, "secret") ||
		strings.Contains(lower, "token") ||
		strings.Contains(lower, "api_key") {
		return "[SQL query redacted - contains sensitive patterns]"
	}
	
	// Otherwise, return as-is (basic queries are safe)
	return sql
}

// redactPython masks sensitive parts of Python code
// Safe fallback: if unsure, return masked placeholder
func redactPython(code string) string {
	// If code contains credential assignments, redact entire block
	lower := strings.ToLower(code)
	if strings.Contains(lower, "password") ||
		strings.Contains(lower, "secret") ||
		strings.Contains(lower, "token") ||
		strings.Contains(lower, "api_key") ||
		strings.Contains(lower, "credentials") {
		return "[Python code redacted - contains sensitive patterns]"
	}
	
	// Otherwise, return as-is
	return code
}

// redactPII masks common PII patterns
func redactPII(s string) string {
	// Email pattern.
	//
	// Deliberately broad, and load-bearing beyond email: a DSN with inline
	// credentials — postgres://svc:hunter2@db.internal:5432/demo — matches none
	// of the "password=" / "pwd=" / "secret=" / "token=" patterns that route a
	// string to MaskConnectionString above, so this branch is the only thing
	// standing between that password and the event store. Narrowing it means
	// handling that case first. See TestRedactString_DSNWithInlineCredentials…
	if strings.Contains(s, "@") && strings.Contains(s, ".") {
		// Basic email detection - replace with masked version
		parts := strings.Split(s, "@")
		if len(parts) == 2 && !strings.Contains(s, " ") {
			return "[email redacted]"
		}
	}

	// Phone number pattern (10+ digits with optional separators).
	//
	// The separators have to be the ONLY thing between the digits. The old rule
	// asked whether the string contained any of "()-. " anywhere, which a
	// Postgres text timestamp satisfies: "2026-08-05 13:41:52" is 19 characters
	// with 14 digits and a hyphen, so every stored timestamp rendered as text was
	// one step from becoming "[phone redacted]". A colon, a "T", or a leading "v"
	// now disqualifies it, because none of them appear in a phone number.
	digitCount := 0
	for _, ch := range s {
		if ch >= '0' && ch <= '9' {
			digitCount++
			continue
		}
		if !strings.ContainsRune("()+-. ", ch) {
			return s // not phone-shaped; nothing else to mask here
		}
	}
	if digitCount >= 10 && len(s) < 20 {
		return "[phone redacted]"
	}

	return s
}


