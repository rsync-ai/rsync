package cdc

import "testing"

// TestResolveMySQLTLSMode_VerifyByDefault locks the H7 posture on the CDC data
// path: remote MySQL verifies TLS by default, with ssl_mode=require as the
// explicit opt-out for an untrusted-CA server.
func TestResolveMySQLTLSMode_VerifyByDefault(t *testing.T) {
	cases := []struct {
		name       string
		cfg        map[string]interface{}
		host, want string
	}{
		{"local", map[string]interface{}{}, "localhost", "false"},
		{"remote verified default", map[string]interface{}{}, "db.mysql.database.azure.com", "true"},
		{"remote opt-out require", map[string]interface{}{"ssl_mode": "require"}, "x.rds.amazonaws.com", "skip-verify"},
		{"explicit disable", map[string]interface{}{"ssl_mode": "disable"}, "x.azure.com", "false"},
		{"explicit verify-full", map[string]interface{}{"tls": "verify-full"}, "x.azure.com", "true"},
	}
	for _, c := range cases {
		if got := resolveMySQLTLSMode(c.cfg, c.host); got != c.want {
			t.Errorf("%s: resolveMySQLTLSMode(%v, %q) = %q, want %q", c.name, c.cfg, c.host, got, c.want)
		}
	}
}
