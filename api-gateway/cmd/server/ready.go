package main

import "net/http"

// readinessVerdict decides whether this gateway should be routed traffic.
//
// The schema check is the half that is easy to leave out and the half that
// matters most. A pool that pings is not a database you can query: when
// Init loses the cold-boot race against Postgres, main() skips migrations
// entirely, and database/sql then redials lazily as soon as Postgres is up.
// From that point the pool is genuinely healthy and every query still fails
// with `relation "..." does not exist`. A ping-only readiness probe answers
// 200 for that state -- verified by running the gateway against a Postgres
// started after boot: /ready returned 200 with zero tables in the schema.
func readinessVerdict(dbPresent, pingOK, schemaOK bool) (int, string) {
	switch {
	case !dbPresent:
		return http.StatusServiceUnavailable, "db_not_connected"
	case !pingOK:
		return http.StatusServiceUnavailable, "db_ping_failed"
	case !schemaOK:
		return http.StatusServiceUnavailable, "schema_not_migrated"
	default:
		return http.StatusOK, ""
	}
}
