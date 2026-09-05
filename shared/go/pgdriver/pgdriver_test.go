package pgdriver

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os/exec"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"strings"
	"testing"
)

// TestOwnsThePostgresName is the one that fails first if lib/pq ever comes back:
// database/sql panics inside init() on the duplicate Register, so this test
// binary would not reach a test function at all. That makes every package
// importing pgdriver a tripwire, not just this one.
//
// database/sql hands back the *inner* driver here rather than pgxDriver,
// because pgxDriver implements DriverContext and sql.Open therefore goes through
// OpenConnector. Seeing *stdlib.Driver is thus two facts at once: pgx owns the
// name, and the connector path — not the legacy Open path — is what runs.
func TestOwnsThePostgresName(t *testing.T) {
	db, err := sql.Open("postgres", "")
	if err != nil {
		t.Fatalf(`sql.Open("postgres") failed: %v`, err)
	}
	defer db.Close()
	if _, ok := db.Driver().(*stdlib.Driver); !ok {
		t.Fatalf(`the "postgres" driver is %T, want *stdlib.Driver (pgx)`, db.Driver())
	}
}

// TestSSLDefaultIsSemanticallyRequire checks the property rather than the
// string. pgx encodes sslmode in the connection plan: "require" yields a TLS
// primary and *no* fallback, while "prefer" yields a TLS primary plus a
// plaintext fallback it will silently take if the server declines TLS. So the
// question that matters — can this DSN end up unencrypted? — is answerable from
// the parsed config alone, with no server involved.
//
// Delete withSSLDefault's injection and this fails: an sslmode-less DSN parses
// back to pgx's own default of prefer, and plaintext becomes reachable again.
func TestSSLDefaultIsSemanticallyRequire(t *testing.T) {
	t.Setenv("PGSSLMODE", "")

	plaintextReachable := func(t *testing.T, dsn string) bool {
		t.Helper()
		cfg, err := pgx.ParseConfig(withSSLDefault(dsn))
		if err != nil {
			t.Fatalf("pgx could not parse the shim's output for %q: %v", dsn, err)
		}
		if cfg.TLSConfig == nil {
			return true
		}
		for _, fb := range cfg.Fallbacks {
			if fb.TLSConfig == nil {
				return true
			}
		}
		return false
	}

	for _, dsn := range []string{"postgres://u:p@h:5432/db", "host=h user=u dbname=db"} {
		if plaintextReachable(t, dsn) {
			t.Errorf("%q: a DSN naming no sslmode must not be able to fall back to "+
				"plaintext — lib/pq refused outright, and this shim exists to keep that", dsn)
		}
	}

	// The shim restores a default; it must not override a deliberate choice,
	// including the local/compose stacks that legitimately run sslmode=disable.
	if !plaintextReachable(t, "postgres://u:p@h/db?sslmode=disable") {
		t.Error("an explicit sslmode=disable must survive the shim untouched")
	}
}

// TestLibPQIsGone states the invariant in the default suite rather than leaving
// it to a boot-time panic in production. A partially migrated module compiles
// and vets clean; only linking it and running it reveals the collision, and by
// then it is a crash loop.
func TestLibPQIsGone(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "./...").Output()
	if err != nil {
		t.Skipf("go list unavailable: %v", err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(line) == "github.com/lib/pq" {
			t.Fatal("github.com/lib/pq is back in this module's dependency graph; " +
				"it registers the \"postgres\" driver name in init() and will panic " +
				"against pgdriver at process start")
		}
	}
}

func TestWithSSLDefault(t *testing.T) {
	t.Setenv("PGSSLMODE", "")

	cases := []struct {
		name string
		dsn  string
		want string
	}{
		// The security-relevant pair: absent means require, matching lib/pq.
		{"url without sslmode", "postgres://u:p@h:5432/db", "postgres://u:p@h:5432/db?sslmode=require"},
		{"postgresql scheme", "postgresql://u:p@h/db", "postgresql://u:p@h/db?sslmode=require"},
		{"kv without sslmode", "host=h user=u dbname=db", "host=h user=u dbname=db sslmode=require"},

		// An explicit choice always wins, including the weak ones — this shim
		// restores a default, it does not enforce a policy.
		{"url with sslmode", "postgres://u:p@h/db?sslmode=disable", "postgres://u:p@h/db?sslmode=disable"},
		{"url with verify-full", "postgres://u:p@h/db?sslmode=verify-full", "postgres://u:p@h/db?sslmode=verify-full"},
		{"kv with sslmode", "host=h sslmode=disable", "host=h sslmode=disable"},
		{"kv sslmode first", "sslmode=disable host=h", "sslmode=disable host=h"},

		// A password that merely contains the text "sslmode=" is not a setting.
		// Splitting on "sslmode=" instead of on whole keys would read this as
		// configured and leave the connection un-defaulted.
		{"password containing sslmode", "host=h password=xsslmode=disable dbname=db",
			"host=h password=xsslmode=disable dbname=db sslmode=require"},

		// Malformed input is pgx's to reject, with pgx's error message.
		{"empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := withSSLDefault(tc.dsn); got != tc.want {
				t.Errorf("withSSLDefault(%q)\n got %q\nwant %q", tc.dsn, got, tc.want)
			}
		})
	}
}

// PGSSLMODE is how libpq lets an operator answer this outside the DSN. Honouring
// it keeps that escape hatch working; overriding it would make the environment
// variable silently inert.
func TestWithSSLDefaultRespectsPGSSLMODE(t *testing.T) {
	t.Setenv("PGSSLMODE", "disable")
	const dsn = "postgres://u:p@h/db"
	if got := withSSLDefault(dsn); got != dsn {
		t.Errorf("PGSSLMODE=disable should leave the DSN alone; got %q", got)
	}
}

func TestQuoteIdentifier(t *testing.T) {
	// Values cross-checked against pq.QuoteIdentifier before lib/pq was removed.
	cases := map[string]string{
		"users":       `"users"`,
		`a"b`:         `"a""b"`,
		"":            `""`,
		"Mixed Case":  `"Mixed Case"`,
		`" OR 1=1 --`: `""" OR 1=1 --"`,
	}
	for in, want := range cases {
		if got := QuoteIdentifier(in); got != want {
			t.Errorf("QuoteIdentifier(%q) = %s, want %s", in, got, want)
		}
	}
}

func ExampleQuoteIdentifier() {
	fmt.Println(QuoteIdentifier("public") + "." + QuoteIdentifier(`odd"name`))
	// Output: "public"."odd""name"
}

func TestSQLState(t *testing.T) {
	pgErr := &pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint"}

	if got := SQLState(pgErr); got != "23505" {
		t.Errorf("bare PgError: got %q, want 23505", got)
	}
	// The wrapped case is the one a type assertion gets wrong, and handlers wrap.
	if got := SQLState(fmt.Errorf("insert schedule: %w", pgErr)); got != "23505" {
		t.Errorf("wrapped PgError: got %q, want 23505", got)
	}
	if got := SQLState(errors.New("connection refused")); got != "" {
		t.Errorf("non-server error should have no SQLSTATE, got %q", got)
	}
	if got := SQLState(nil); got != "" {
		t.Errorf("nil should have no SQLSTATE, got %q", got)
	}
}

func TestStringArrayValue(t *testing.T) {
	cases := []struct {
		name string
		in   StringArray
		want any
	}{
		{"nil is SQL NULL, not an empty array", nil, nil},
		{"empty", StringArray{}, `{}`},
		{"plain", StringArray{"a", "b"}, `{"a","b"}`},
		{"comma and brace are inert once quoted", StringArray{"a,b", "{c}"}, `{"a,b","{c}"}`},
		{"embedded quote", StringArray{`say "hi"`}, `{"say \"hi\""}`},
		{"embedded backslash", StringArray{`c:\tmp`}, `{"c:\\tmp"}`},
		// Quoting is what keeps this the 4-character string rather than SQL NULL.
		{"literal NULL text", StringArray{"NULL"}, `{"NULL"}`},
		{"empty element", StringArray{""}, `{""}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.in.Value()
			if err != nil {
				t.Fatalf("Value() error: %v", err)
			}
			if got != tc.want {
				t.Errorf("Value() = %#v, want %#v", got, tc.want)
			}
		})
	}
}

// The point of StringArray is that it survives database/sql's own converter —
// the one every test double uses and the one a bare []string fails. Asserting
// that directly stops a future "simplification" back to the bare slice from
// looking harmless.
func TestStringArraySurvivesTheDefaultConverter(t *testing.T) {
	if _, err := driver.DefaultParameterConverter.ConvertValue([]string{"a"}); err == nil {
		t.Fatal("expected database/sql to reject a bare []string; if this now " +
			"passes, StringArray may no longer be necessary")
	}
	v, err := driver.DefaultParameterConverter.ConvertValue(StringArray{"a", "b"})
	if err != nil {
		t.Fatalf("StringArray rejected by the default converter: %v", err)
	}
	if v != `{"a","b"}` {
		t.Errorf("converted to %#v", v)
	}
}
