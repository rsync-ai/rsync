package main

import (
	"net/http"
	"testing"
)

// TestReadinessVerdict pins what /ready is allowed to call ready.
//
// The "pool healthy, schema empty" row is the regression this exists for: it
// was reproduced by booting the gateway against a Postgres that started
// afterwards. The pool self-healed, /ready answered 200, and the public schema
// held zero tables — every request 500s while Kubernetes keeps sending them.
func TestReadinessVerdict(t *testing.T) {
	cases := []struct {
		name       string
		dbPresent  bool
		pingOK     bool
		schemaOK   bool
		wantCode   int
		wantReason string
	}{
		{"no database url", false, false, false, http.StatusServiceUnavailable, "db_not_connected"},
		{"pool present but unreachable", true, false, false, http.StatusServiceUnavailable, "db_ping_failed"},
		{"pool healthy, schema never migrated", true, true, false, http.StatusServiceUnavailable, "schema_not_migrated"},
		{"fully ready", true, true, true, http.StatusOK, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, reason := readinessVerdict(tc.dbPresent, tc.pingOK, tc.schemaOK)
			if code != tc.wantCode || reason != tc.wantReason {
				t.Errorf("readinessVerdict(%v, %v, %v) = (%d, %q), want (%d, %q)",
					tc.dbPresent, tc.pingOK, tc.schemaOK, code, reason, tc.wantCode, tc.wantReason)
			}
		})
	}
}
