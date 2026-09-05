package assessor

import "testing"

// TestAssessorMySQLTLSMode_VerifyByDefault locks the H7 posture on the assessor
// path (must match cdc + api-gateway): remote MySQL verifies TLS by default,
// ssl_mode=require is the explicit opt-out.
func TestAssessorMySQLTLSMode_VerifyByDefault(t *testing.T) {
	cases := []struct {
		name       string
		cfg        map[string]string
		host, want string
	}{
		{"local", map[string]string{}, "localhost", "false"},
		{"remote verified default", map[string]string{}, "db.mysql.database.azure.com", "true"},
		{"remote opt-out require", map[string]string{"ssl_mode": "require"}, "x.rds.amazonaws.com", "skip-verify"},
		{"explicit disable", map[string]string{"sslmode": "disable"}, "x.azure.com", "false"},
	}
	for _, c := range cases {
		if got := assessorMySQLTLSMode(c.cfg, c.host); got != c.want {
			t.Errorf("%s: assessorMySQLTLSMode(%v, %q) = %q, want %q", c.name, c.cfg, c.host, got, c.want)
		}
	}
}
