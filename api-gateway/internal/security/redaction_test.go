package security

import (
	"strings"
	"testing"
)

// The redactor runs over every string in every event payload before it is
// stored (projector/event_projector.go:608) and before it is broadcast over
// websockets (websocket/kafka_bridge.go:188 and :233). It stores the redacted
// copy — the original is not kept anywhere — so a false positive is permanent:
// the operator-facing message is gone from the UI and from the database.
//
// F-277: `looksLikeSQL` (redaction.go:107) was true on a bare "update " /
// "delete " / "select ", and `looksLikePython` (:118) on a bare "from " /
// "import " / "class ". `redactSQL` / `redactPython` then replace the ENTIRE
// string when it also contains password|secret|token|api_key. Both conditions
// are met by ordinary English.
//
// Measured on prod 2026-08-05, before the fix: 0 of 1614 stored payloads
// contain any of these placeholders, against a working positive control (1252
// of 1614 contain "message"). This is a trap being closed before it fires, not
// damage being repaired — only 3 STAGE_FAILED events exist, so the error path
// that produces these strings is barely exercised.

const (
	sqlPlaceholder    = "[SQL query redacted - contains sensitive patterns]"
	pythonPlaceholder = "[Python code redacted - contains sensitive patterns]"
)

// Ordinary English an operator needs to read. Each one contains a bare SQL or
// Python keyword AND a word from the sensitive list, which is all the old
// heuristic required to replace the whole sentence with a placeholder that also
// lies about what the string was.
func TestRedactString_OperatorProseIsNotMistakenForCode(t *testing.T) {
	cases := []struct{ name, in string }{
		// The exact shape built at backend-orchestrator/internal/workers/executor.go:533.
		{"update+token", "Data transfer failed: could not update orders: token expired"},
		{"delete+api_key", "Cannot delete pipeline: an api_key is still attached"},
		{"from+token", "Waiting for a token from the identity provider"},
		{"import+secret", "Could not import the secret store configuration"},
		{"select+password", "Select a destination before saving the password policy"},
		{"class+credentials", "This plan class does not include credentials management"},
		// Keyword pairs that co-occur in prose the way they do in a query.
		{"select..from+token", "Could not select rows from the source table: token expired"},
		{"delete..from+token", "Delete from the list any API token you no longer need"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := redactString(tc.in); got != tc.in {
				t.Fatalf("operator message was rewritten\n   in: %q\n  out: %q", tc.in, got)
			}
		})
	}
}

// The bound on the fix above: narrowing detection must not switch redaction
// off. Real statements carrying a credential keyword must still be replaced.
func TestRedactString_GenuineCodeWithCredentialKeywordsStaysRedacted(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"select", "SELECT id, password_hash FROM users WHERE id = 1", sqlPlaceholder},
		{"update", "UPDATE users SET password_hash = 'x' WHERE id = 1", sqlPlaceholder},
		{"delete", "DELETE FROM sessions WHERE token = 'abc'", sqlPlaceholder},
		{"insert", "INSERT INTO creds (api_key) VALUES ('sk-live-1')", sqlPlaceholder},
		{"create", "CREATE TABLE secrets (token text)", sqlPlaceholder},
		{"lowercase select", "select api_key from connections where id = 1", sqlPlaceholder},
		{"import", "import os\napi_key = os.environ['SECRET']", pythonPlaceholder},
		{"def", "def connect():\n    password = 'hunter2'", pythonPlaceholder},
		{"from-import", "from creds import password", pythonPlaceholder},
		{"class", "class Vault:\n    secret = 'x'", pythonPlaceholder},
		{"indented def", "    def login(self):\n        token = 'x'", pythonPlaceholder},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := redactString(tc.in); got != tc.want {
				t.Fatalf("code was not redacted\n   in: %q\n  out: %q\n want: %q", tc.in, got, tc.want)
			}
		})
	}
}

// Pre-existing contract, restated so the narrowing does not quietly change it:
// a statement with no credential keyword is recognised as code and passed
// through untouched.
func TestRedactString_HarmlessSQLIsRecognisedAndLeftAlone(t *testing.T) {
	for _, in := range []string{
		"SELECT id FROM orders",
		"DELETE FROM staging_rows WHERE id < 100",
	} {
		if got := redactString(in); got != in {
			t.Fatalf("harmless query was rewritten\n   in: %q\n  out: %q", in, got)
		}
	}
}

// redactPII's phone rule fires on any string with 10+ digits and fewer than 20
// characters that contains one of "()-. ". A Postgres text timestamp is exactly
// that: "2026-08-05 13:41:52" is 19 characters with 14 digits and a hyphen.
func TestRedactPII_PhoneRuleDoesNotEatTimestampsAndIDs(t *testing.T) {
	for _, in := range []string{
		"2026-08-05 13:41:52",
		"2026-08-05T13:41:52",
		"v1.2.3-20260805.14",
	} {
		if got := redactString(in); got != in {
			t.Fatalf("non-PII value was rewritten\n   in: %q\n  out: %q", in, got)
		}
	}
}

// The bound on that one: things that really are a run of digits behind
// phone-shaped punctuation must still be masked.
func TestRedactPII_RealPhoneShapesStayMasked(t *testing.T) {
	for _, in := range []string{
		"(555) 123-4567",
		"555-123-4567",
		"+1 555 123 4567",
		"4111111111111111",
	} {
		if got := redactString(in); got != "[phone redacted]" {
			t.Fatalf("phone-shaped value was not masked\n   in: %q\n  out: %q", in, got)
		}
	}
}

// A DSN with inline credentials matches none of the "password=" / "pwd=" /
// "secret=" / "token=" patterns that route a string to MaskConnectionString, so
// the *email* branch of redactPII is the only thing between that password and
// the event store. That is why the email rule is deliberately left broad here
// while the phone rule is narrowed — tightening it would leak the credential.
// Locked so a later tightening has to confront the consequence first.
func TestRedactString_DSNWithInlineCredentialsNeverLeaksThePassword(t *testing.T) {
	in := "postgres://svc:hunter2@db.internal:5432/demo"
	got := redactString(in)
	if strings.Contains(got, "hunter2") {
		t.Fatalf("DSN password survived redaction\n   in: %q\n  out: %q", in, got)
	}
}
