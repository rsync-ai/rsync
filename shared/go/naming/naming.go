// Package naming holds the single source of truth for validating user- and
// NL-supplied SQL identifiers (table names, destination namespaces).
//
// It exists because PR #130 ("the" disaster) showed the same article/stopword
// guard was needed in three places — the executor's NL table parse, the
// kafka-mcp-sink's destCfg override, and now the destination-mapping HITL's
// namespace field. Duplicated word lists drift; this package keeps them aligned.
package naming

import "strings"

// suspiciousIdentifiers are English articles / filler words a loose NL pattern
// may capture, plus pipeline-vocabulary words, none of which is ever a real
// table or namespace name. Accepting one (e.g. "the") silently misroutes every
// pipeline's rows into a junk table/schema — see PR #130.
var suspiciousIdentifiers = map[string]struct{}{
	"the": {}, "a": {}, "an": {}, "to": {}, "into": {}, "from": {}, "as": {},
	"table": {}, "tables": {}, "source": {}, "destination": {}, "dest": {},
	"sink": {}, "target": {}, "connection": {}, "it": {}, "this": {}, "that": {},
	"these": {}, "those": {}, "my": {}, "your": {}, "our": {}, "their": {},
}

// IsSuspiciousIdentifier reports whether s is an article / filler / pipeline
// vocabulary word that is never a legitimate table or namespace name. The match
// is case-insensitive, trimmed, and tolerant of surrounding double quotes so it
// works on both raw NL captures and already-quoted SQL identifiers.
func IsSuspiciousIdentifier(s string) bool {
	clean := strings.ToLower(strings.TrimSpace(strings.Trim(strings.TrimSpace(s), `"`)))
	_, bad := suspiciousIdentifiers[clean]
	return bad
}

// maxNamespaceLen bounds a namespace name well under the tightest engine limit
// (PostgreSQL identifiers cap at 63 bytes; MySQL databases at 64).
const maxNamespaceLen = 63

// ValidateNamespace checks a user-supplied destination namespace name and
// returns a human-readable reason if it must be rejected, or "" if it is
// acceptable. It enforces: non-empty, length bound, a conservative
// identifier charset (letters, digits, underscore; not leading with a digit),
// and the article/stopword guard. The charset is intentionally strict — the
// name flows into CREATE SCHEMA / CREATE DATABASE / dataset paths unquoted in
// some engines, so we reject anything that would need quoting or could inject.
func ValidateNamespace(s string) (reason string) {
	name := strings.TrimSpace(s)
	if name == "" {
		return "namespace name is empty"
	}
	if len(name) > maxNamespaceLen {
		return "namespace name is too long (max 63 characters)"
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
			// always allowed
		case r >= '0' && r <= '9':
			if i == 0 {
				return "namespace name must not start with a digit"
			}
		default:
			return "namespace name may only contain letters, digits, and underscores"
		}
	}
	if IsSuspiciousIdentifier(name) {
		return "\"" + name + "\" is a reserved/filler word and is never a valid namespace name"
	}
	return ""
}
