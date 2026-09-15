package main

import "net/http"

// explorerFindStatus maps a connector `find` error_code to the HTTP status the
// /agent/explorer-find route returns. A refused request (operator_not_allowed,
// invalid_filter, invalid_cursor, ...) is the caller's to fix: 400. A server-side
// timeout is 504. Anything that means the connector or the database could not do the
// work (unreachable, bad config, a server error) is 502.
func explorerFindStatus(code string) int {
	switch code {
	case "query_timeout":
		return http.StatusGatewayTimeout
	case "connection_failed", "query_failed", "invalid_config":
		return http.StatusBadGateway
	default:
		return http.StatusBadRequest
	}
}
