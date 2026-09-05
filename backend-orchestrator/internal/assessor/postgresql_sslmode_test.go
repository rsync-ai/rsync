package assessor

import "testing"

// The readiness probe dials a PostgreSQL server whose address a user supplies,
// and stored PG connections carry no sslmode key — so the probe must supply a
// host-aware default that encrypts against remote managed servers. This mirrors
// the prod-proven cdc.ResolvePostgresSSLMode used by the executor/CDC path.
//
// The "prefer"/"allow" cases below guard a security property, and they became
// MORE important when this repo moved off lib/pq: lib/pq rejected those values
// outright, so a leak would have shown up as a failed connection. pgx accepts
// them and silently falls back to cleartext, so nothing but this fold would
// surface the downgrade.
func TestResolveAssessorPostgresSSLMode(t *testing.T) {
	const remote = "pg-managed.example.com"

	cases := []struct {
		name string
		cfg  map[string]string
		host string
		want string
	}{
		// No sslmode key + remote host must NOT yield "prefer"; it defaults to
		// verify-full (encrypt AND verify) to close the MITM window.
		{"no-key remote defaults to verify-full", map[string]string{}, remote, "verify-full"},
		// The plaintext-fallback modes must be folded, not passed through verbatim.
		{"explicit prefer folds to require", map[string]string{"sslmode": "prefer"}, remote, "require"},
		{"explicit allow folds to require", map[string]string{"sslmode": "allow"}, remote, "require"},
		// Local/dev PG rarely runs TLS.
		{"no-key local defaults to disable", map[string]string{}, "localhost", "disable"},
		{"dotless docker host defaults to disable", map[string]string{}, "pg-e2e", "disable"},
		// Valid explicit modes are honored.
		{"explicit disable honored", map[string]string{"sslmode": "disable"}, remote, "disable"},
		{"explicit require honored", map[string]string{"sslmode": "require"}, remote, "require"},
		{"explicit verify-full honored", map[string]string{"sslmode": "verify-full"}, remote, "verify-full"},
		// The ssl_mode spelling is accepted too.
		{"ssl_mode key honored", map[string]string{"ssl_mode": "disable"}, remote, "disable"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveAssessorPostgresSSLMode(tc.cfg, tc.host)
			if got != tc.want {
				t.Errorf("resolveAssessorPostgresSSLMode(%v, %q) = %q; want %q", tc.cfg, tc.host, got, tc.want)
			}
			// Hard guard: the probe must only ever emit one of libpq's four
			// canonical modes. "prefer"/"allow" are the ones that matter — pgx
			// accepts both and silently downgrades to cleartext, so a regression
			// that let one through would leak the password with no error.
			switch got {
			case "disable", "require", "verify-ca", "verify-full":
			default:
				t.Errorf("resolveAssessorPostgresSSLMode returned %q; only libpq's four "+
					"canonical modes are allowed — anything else risks a silent "+
					"plaintext fallback under pgx", got)
			}
		})
	}
}
