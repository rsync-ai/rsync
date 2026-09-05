package llmscrub

import (
	"strings"
	"testing"
)

func TestScrub_MasksRowValues(t *testing.T) {
	cases := []struct {
		name       string
		in         string
		mustHide   []string // substrings that must NOT survive
		mustRemain []string // substrings that MUST survive (schema metadata / shape)
	}{
		{
			name:       "postgres duplicate key detail",
			in:         `ERROR: duplicate key value violates unique constraint "users_pkey" DETAIL: Key (email)=(jane.doe@acme.com) already exists.`,
			mustHide:   []string{"jane.doe@acme.com"},
			mustRemain: []string{`"users_pkey"`, "Key (email)=", "already exists"},
		},
		{
			name:       "mysql duplicate entry",
			in:         `Error 1062: Duplicate entry 'jane.doe@acme.com' for key 'users.uq_email'`,
			mustHide:   []string{"jane.doe@acme.com"},
			mustRemain: []string{"Error 1062", "Duplicate entry"},
		},
		{
			name:       "insert values list multi-tuple",
			in:         `dest write failed: INSERT INTO public.customers (id, ssn, name) VALUES (9384756, '078-05-1120', 'Jane Doe'), (9384757, '078-05-1121', 'John Doe') ON CONFLICT DO NOTHING`,
			mustHide:   []string{"078-05-1120", "Jane Doe", "9384756"},
			mustRemain: []string{"INSERT INTO public.customers (id, ssn, name)", "VALUES ("},
		},
		{
			name:       "connection url credentials",
			in:         `dial failed: postgres://appuser:S3cr3tPass@db.acme.internal:5432/prod`,
			mustHide:   []string{"S3cr3tPass", "appuser:"},
			mustRemain: []string{"postgres://", "db.acme.internal:5432/prod", "dial failed"},
		},
		{
			name:       "kv credential pair",
			in:         `auth failed for password=hunter22 token: eyJabc`,
			mustHide:   []string{"hunter22", "eyJabc"},
			mustRemain: []string{"auth failed", "password="},
		},
		{
			name:       "bare email and long id",
			in:         `row rejected: contact bob@customer.io id 88812345678 not found`,
			mustHide:   []string{"bob@customer.io", "88812345678"},
			mustRemain: []string{"row rejected", "not found"},
		},
		// Review-confirmed gaps (P0 adversarial review):
		{
			name:       "postgres failing row detail dumps whole row",
			in:         `ERROR: null value in column "email" violates not-null constraint DETAIL: Failing row contains (42, Jane Doe, 555-0182, 123 Main St, jane).`,
			mustHide:   []string{"Jane Doe", "555-0182", "123 Main St"},
			mustRemain: []string{`"email"`, "not-null constraint", "Failing row contains ("},
		},
		{
			name:       "postgres double-quoted offending value after colon",
			in:         `invalid input syntax for type integer: "jane-ssn-078-05-1120"`,
			mustHide:   []string{"jane-ssn-078-05-1120"},
			mustRemain: []string{"invalid input syntax for type integer"},
		},
		{
			name:       "authorization bearer jwt",
			in:         `request failed: 401 for header Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiI0MiJ9.c2lnbmF0dXJl`,
			mustHide:   []string{"eyJhbGciOiJIUzI1NiJ9", "c2lnbmF0dXJl"},
			mustRemain: []string{"request failed: 401"},
		},
		{
			name:       "bare jwt without bearer prefix",
			in:         `token rejected eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiI0MiJ9.c2ln by upstream`,
			mustHide:   []string{"eyJzdWIiOiI0MiJ9"},
			mustRemain: []string{"by upstream"},
		},
		{
			name:       "json-encoded failing record",
			in:         `sink reject: {"name": "Jane Doe", "ssn": "078-05-1120", "age": 41}`,
			mustHide:   []string{"Jane Doe", "078-05-1120", ": 41"},
			mustRemain: []string{`"name"`, `"ssn"`, "sink reject"},
		},
		{
			name:       "dashed ssn and phone outside quotes",
			in:         `rejected row with ssn 078-05-1120 phone 415-555-0182`,
			mustHide:   []string{"078-05-1120", "415-555-0182"},
			mustRemain: []string{"rejected row"},
		},
		{
			name:       "truncation-reopened quoted literal",
			in:         `Duplicate entry 'jane.doe@acme.com for key uq_em`, // closing quote cut off upstream
			mustHide:   []string{"jane.doe@acme.com"},
			mustRemain: []string{"Duplicate entry"},
		},
		{
			// The contraction case, and the one that reached users: the
			// apostrophe in "Couldn't" was read as an opening quote, so the run
			// from there to the quote before the pipeline name was consumed as
			// the literal. Result: the prose was redacted and the NAME survived
			// in the clear — the scrubber's contract exactly inverted. Both
			// assertions below failed before the fix, in opposite directions.
			name:       "apostrophe in prose is not a quote opener",
			in:         `Couldn't apply schema change to pipeline 'orders-sync'.`,
			mustHide:   []string{"orders-sync"},
			mustRemain: []string{"Couldn't apply schema change to pipeline"},
		},
		{
			// Two literals plus two possessives in one sentence — the shape a
			// healer message actually has.
			name:       "possessives around a real literal",
			in:         `We didn't apply the change for pipeline 'drift-test-batch' because the user's approval is pending.`,
			mustHide:   []string{"drift-test-batch"},
			mustRemain: []string{"didn't", "the user's approval is pending"},
		},
		{
			// A fully-paired MySQL error must not grow a phantom tail: the old
			// second pass re-read the `'[redacted]'` the first pass had written
			// and appended another `'[redacted]` to it, swallowing whatever
			// diagnostic text followed the last literal.
			name:       "paired literals leave the surrounding text intact",
			in:         `value was 'topsecret' in the row`,
			mustHide:   []string{"topsecret"},
			mustRemain: []string{"value was", "in the row"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Scrub(tc.in)
			for _, h := range tc.mustHide {
				if strings.Contains(got, h) {
					t.Errorf("Scrub leaked %q in output: %s", h, got)
				}
			}
			for _, r := range tc.mustRemain {
				if !strings.Contains(got, r) {
					t.Errorf("Scrub destroyed diagnostic shape %q in output: %s", r, got)
				}
			}
		})
	}
}

func TestScrub_PreservesHarmlessDiagnostics(t *testing.T) {
	// Messages with no customer data must pass through unchanged — diagnosis
	// quality depends on keeping error shape intact.
	unchanged := []string{
		"context deadline exceeded",
		"connection refused",
		`relation "public.orders" does not exist`,
		"waiting_for_table_selection status stuck for 3600 seconds",
		"read 15000 rows, landed 0 rows (silent_drop)",
		"executor stage batch_transfer stalled at 80%",
		"port 5432 unreachable",
		"last event at 2026-07-02 14:10:33 (3600 seconds ago)",
	}
	for _, s := range unchanged {
		if got := Scrub(s); got != s {
			t.Errorf("Scrub altered harmless text:\n in: %s\nout: %s", s, got)
		}
	}
}

func TestScrub_Idempotent(t *testing.T) {
	in := `DETAIL: Key (email)=(jane@acme.com) already exists. INSERT INTO t (a) VALUES ('x')`
	once := Scrub(in)
	twice := Scrub(once)
	if once != twice {
		t.Errorf("Scrub not idempotent:\n once: %s\ntwice: %s", once, twice)
	}
}

func TestScrubMax_TruncatesAfterScrub(t *testing.T) {
	in := "prefix " + strings.Repeat("a ", 200) + "email bob@x.io"
	got := ScrubMax(in, 50)
	if strings.Contains(got, "bob@x.io") {
		t.Errorf("ScrubMax leaked email after truncation: %s", got)
	}
	if len([]rune(got)) > 51 { // 50 + ellipsis
		t.Errorf("ScrubMax did not truncate: len=%d", len([]rune(got)))
	}
}

func TestScrub_EmptyString(t *testing.T) {
	if got := Scrub(""); got != "" {
		t.Errorf("Scrub(\"\") = %q", got)
	}
}
