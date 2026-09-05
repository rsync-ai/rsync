package handlers

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

// TestMySQLLiveTLSConnect exercises the real outbound DSN the fix builds,
// against a live remote MySQL. Skips unless MYSQL_HOST/USER/PASSWORD are set,
// so it never runs in normal CI. Run with:
//   MYSQL_HOST=... MYSQL_USER=... MYSQL_PASSWORD=... MYSQL_DATABASE=... \
//     go test ./internal/handlers/ -run TestMySQLLiveTLSConnect -v
func TestMySQLLiveTLSConnect(t *testing.T) {
	host := os.Getenv("MYSQL_HOST")
	user := os.Getenv("MYSQL_USER")
	pass := os.Getenv("MYSQL_PASSWORD")
	db := os.Getenv("MYSQL_DATABASE")
	if host == "" || user == "" || pass == "" {
		t.Skip("set MYSQL_HOST/MYSQL_USER/MYSQL_PASSWORD to run live test")
	}
	if db == "" {
		db = "information_schema"
	}

	// What the resolver actually picks for this host (no explicit config).
	resolved := resolveMySQLTLSMode(map[string]interface{}{}, host)
	t.Logf("resolveMySQLTLSMode(host=%q) -> tls=%q", host, resolved)

	try := func(tlsMode string) (string, error) {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?parseTime=true&timeout=10s&readTimeout=10s&writeTimeout=10s&tls=%s",
			user, pass, host, db, tlsMode)
		conn, err := sql.Open("mysql", dsn)
		if err != nil {
			return "", err
		}
		defer conn.Close()
		var cipher string
		// Ssl_cipher is non-empty only when the session is actually encrypted.
		row := conn.QueryRow("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
		var name string
		if err := row.Scan(&name, &cipher); err != nil {
			return "", err
		}
		return cipher, nil
	}

	// The mode the fix selects must connect AND be encrypted.
	cipher, err := try(resolved)
	if err != nil {
		t.Fatalf("connect with resolved tls=%q failed: %v", resolved, err)
	}
	if resolved != "false" && cipher == "" {
		t.Fatalf("resolved tls=%q connected but session is NOT encrypted (Ssl_cipher empty)", resolved)
	}
	t.Logf("PASS resolved tls=%q connected, Ssl_cipher=%q", resolved, cipher)

	// Diagnostic comparison across modes (failures here are informational).
	for _, m := range []string{"false", "skip-verify", "true", "preferred"} {
		if c, e := try(m); e != nil {
			t.Logf("  tls=%-12s -> ERROR: %v", m, e)
		} else {
			t.Logf("  tls=%-12s -> ok, Ssl_cipher=%q", m, c)
		}
	}
}
