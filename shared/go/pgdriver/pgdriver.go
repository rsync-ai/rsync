// Package pgdriver registers pgx as the database/sql driver named "postgres".
//
// It exists so the rest of the tree can keep calling sql.Open("postgres", dsn)
// while the implementation underneath is pgx rather than lib/pq. Importing it
// for effect is the whole interface:
//
//	import _ "github.com/rsync-ai/shared/pgdriver"
//
// Why the move: lib/pq is in maintenance mode, and the seven advisories open
// against it (GO-2026-6166, 6168..6173) are all "Fixed in: N/A" — a hostile or
// MITM'd PostgreSQL *server* can panic the client or drive it to exhaust memory
// with malformed protocol frames, and no release will ever repair that. rsync
// dials PostgreSQL servers whose address a user supplies, so those frames are
// reachable from untrusted input, not just from our own control-plane database.
// The only remedy an unfixable advisory leaves is to stop using the module.
//
// # The name is load-bearing
//
// lib/pq claims "postgres" from its own init(). database/sql panics on a second
// Register of the same name, so this package and lib/pq cannot coexist in one
// binary — a *partially* migrated binary compiles clean and then dies on boot
// with "sql: Register called twice for driver postgres". Neither the compiler
// nor govulncheck can see that. TestLibPQIsGone is what does.
package pgdriver

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/url"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

func init() { sql.Register("postgres", pgxDriver{}) }

// pgxDriver is pgx's stdlib driver with one behaviour restored: lib/pq's
// default of sslmode=require.
//
// This is not cosmetic. The two drivers disagree about a DSN that names no
// sslmode at all — lib/pq refuses to continue without TLS, pgx follows libpq's
// own default of "prefer" and will fall back to a plaintext session if the
// server declines TLS. Swapping drivers without this shim would therefore
// quietly downgrade every such connection, turning a fix for a denial-of-service
// bug into a confidentiality regression. Verified against a TLS-less server:
// lib/pq answers "SSL is not enabled on the server", pgx connects.
//
// Every DSN rsync builds today already pins sslmode explicitly, so this changes
// nothing that currently runs. It is here so that the next DSN to be written —
// or an operator's hand-set DATABASE_URL — cannot silently opt out of TLS by
// omission.
type pgxDriver struct{}

func (pgxDriver) Open(dsn string) (driver.Conn, error) {
	return stdlib.GetDefaultDriver().Open(withSSLDefault(dsn))
}

// OpenConnector keeps the pooling path off the per-connection DSN parse that
// Open would otherwise repeat; database/sql prefers it when the driver offers it.
func (pgxDriver) OpenConnector(dsn string) (driver.Connector, error) {
	dc, ok := stdlib.GetDefaultDriver().(driver.DriverContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return dc.OpenConnector(withSSLDefault(dsn))
}

// withSSLDefault appends sslmode=require to a DSN that specifies no sslmode,
// matching lib/pq. A DSN that already names one is returned untouched, in either
// of the two forms pgx accepts (URL and libpq keyword/value).
//
// PGSSLMODE is honoured the way libpq honours it — if the operator set it in the
// environment, that is an explicit choice and the DSN is left alone.
func withSSLDefault(dsn string) string {
	if os.Getenv("PGSSLMODE") != "" {
		return dsn
	}
	trimmed := strings.TrimSpace(dsn)
	if trimmed == "" {
		return dsn
	}

	if isURLDSN(trimmed) {
		u, err := url.Parse(trimmed)
		if err != nil {
			// Not our job to reject it; hand the original to pgx and let it
			// produce the real parse error.
			return dsn
		}
		q := u.Query()
		if q.Get("sslmode") != "" {
			return dsn
		}
		q.Set("sslmode", "require")
		u.RawQuery = q.Encode()
		return u.String()
	}

	// Keyword/value form: "host=… user=… sslmode=…". Match on a whole key so a
	// password containing the text "sslmode=" cannot masquerade as one.
	for _, field := range strings.Fields(trimmed) {
		if k, _, ok := strings.Cut(field, "="); ok && strings.EqualFold(strings.TrimSpace(k), "sslmode") {
			return dsn
		}
	}
	return trimmed + " sslmode=require"
}

func isURLDSN(dsn string) bool {
	lower := strings.ToLower(dsn)
	return strings.HasPrefix(lower, "postgres://") || strings.HasPrefix(lower, "postgresql://")
}

// QuoteIdentifier renders s as a double-quoted PostgreSQL identifier, doubling
// any embedded quote. It replaces pq.QuoteIdentifier, which has no pgx
// equivalent because pgx's own API never needs the caller to interpolate an
// identifier by hand.
//
// It is not a substitute for validating the identifier: it makes a *known* name
// safe to splice into DDL, it does not make an attacker-chosen one safe.
func QuoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// SQLState returns the five-character SQLSTATE code PostgreSQL reported for err,
// or "" if err did not come from the server. Callers compare against the code
// they care about, e.g. "23505" for a unique violation.
//
// This exists so that recognising a constraint violation does not require every
// caller to import the driver's error type. That coupling is the reason moving
// off lib/pq touched two dozen files instead of one; there is no reason to
// rebuild it around pgx.
//
// errors.As, not a type assertion: database/sql and the callers above it wrap
// freely, and a bare assertion silently stops matching the first time someone
// adds a %w.
func SQLState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// StringArray adapts a []string for use as a single bind parameter against a
// PostgreSQL array column or ANY($1), replacing pq.StringArray/pq.Array.
//
// pgx can in fact encode a bare []string directly, because its stdlib driver
// implements CheckNamedValue and reaches for pgtype. Relying on that would be a
// mistake: it makes the *call site* depend on which driver is underneath, and
// database/sql's default converter — the one sqlmock and every other test double
// use — rejects a []string outright. Code written against the bare slice
// therefore passes in production and fails in unit tests, which is precisely
// backwards. A driver.Valuer works everywhere.
type StringArray []string

// Value renders the PostgreSQL array literal. Every element is quoted, which is
// always legal and avoids having to reason about which unquoted forms would be
// misread — a bare `NULL` element meaning the null value rather than the
// four-character string being the trap that matters here.
func (a StringArray) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, s := range a {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('"')
		// Backslash first: escaping the quotes first would then double the
		// backslashes this step introduces.
		b.WriteString(strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `"`, `\"`))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String(), nil
}
