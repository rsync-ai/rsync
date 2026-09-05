package handlers

import "testing"

func TestNormalizeMySQLTLS(t *testing.T) {
	cases := map[string]string{
		// off family
		"disable": "false", "disabled": "false", "false": "false",
		"off": "false", "0": "false", "none": "false",
		"DISABLE": "false", "Off": "false",
		// encrypt-no-verify family (libpq "require" equivalent)
		"require": "skip-verify", "required": "skip-verify",
		"skip-verify": "skip-verify", "skip_verify": "skip-verify",
		"REQUIRE": "skip-verify",
		// prefer family
		"prefer": "preferred", "preferred": "preferred",
		// strict verify family
		"verify-ca": "true", "verify_ca": "true",
		"verify-full": "true", "verify_full": "true",
		"verify-identity": "true", "verify_identity": "true",
		"true": "true", "on": "true", "1": "true",
		// passthrough for a custom RegisterTLSConfig name
		"myCustomTLS": "myCustomTLS",
	}
	for in, want := range cases {
		if got := normalizeMySQLTLS(in); got != want {
			t.Errorf("normalizeMySQLTLS(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestResolveMySQLTLSMode(t *testing.T) {
	tests := []struct {
		name   string
		config map[string]interface{}
		host   string
		want   string
	}{
		{"local empty host", map[string]interface{}{}, "", "false"},
		{"localhost", map[string]interface{}{}, "localhost", "false"},
		{"127.0.0.1", map[string]interface{}{}, "127.0.0.1", "false"},
		{"docker internal", map[string]interface{}{}, "host.docker.internal", "false"},
		{"dotless service name", map[string]interface{}{}, "mysql", "false"},
		// Remote hosts now default to VERIFIED TLS ("true"), closing the MITM
		// window the old "skip-verify" default left open on customer-DB traffic.
		{"remote azure verified by default", map[string]interface{}{}, "mydb.mysql.database.azure.com", "true"},
		{"remote rds verified by default", map[string]interface{}{}, "x.rds.amazonaws.com", "true"},
		// Explicit opt-out for a MySQL whose CA isn't in the image trust store.
		{"remote opt-out via require", map[string]interface{}{"ssl_mode": "require"}, "x.rds.amazonaws.com", "skip-verify"},
		{"explicit ssl_mode wins over remote default", map[string]interface{}{"ssl_mode": "disable"}, "x.rds.amazonaws.com", "false"},
		{"explicit sslmode require on local", map[string]interface{}{"sslmode": "require"}, "localhost", "skip-verify"},
		{"explicit tls verify-full", map[string]interface{}{"tls": "verify-full"}, "x.azure.com", "true"},
		{"explicit tls_mode preferred", map[string]interface{}{"tls_mode": "prefer"}, "localhost", "preferred"},
		{"nil explicit value falls through to host default", map[string]interface{}{"ssl_mode": nil}, "x.azure.com", "true"},
		{"empty explicit value falls through", map[string]interface{}{"ssl_mode": "  "}, "x.azure.com", "true"},
		{"first non-empty key wins", map[string]interface{}{"ssl_mode": "", "tls": "require"}, "localhost", "skip-verify"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveMySQLTLSMode(tc.config, tc.host); got != tc.want {
				t.Errorf("resolveMySQLTLSMode(%v, %q) = %q, want %q", tc.config, tc.host, got, tc.want)
			}
		})
	}
}

// Guards the parity invariant: MySQL host-based default must encrypt on remote
// hosts exactly where Postgres does, and stay off where Postgres disables.
func TestMySQLPostgresParity(t *testing.T) {
	hosts := []string{"", "localhost", "127.0.0.1", "host.docker.internal", "mysql", "x.azure.com", "y.rds.amazonaws.com"}
	for _, h := range hosts {
		pg := resolvePostgresSSLMode(map[string]interface{}{}, h)
		my := resolveMySQLTLSMode(map[string]interface{}{}, h)
		pgEncrypts := pg != "disable"
		myEncrypts := my != "false"
		if pgEncrypts != myEncrypts {
			t.Errorf("host %q: pg=%q (encrypt=%v) vs mysql=%q (encrypt=%v) — parity broken", h, pg, pgEncrypts, my, myEncrypts)
		}
	}
}
