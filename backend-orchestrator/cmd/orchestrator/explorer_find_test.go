package main

import (
	"net/http"
	"testing"
)

func TestExplorerFindStatus(t *testing.T) {
	cases := map[string]int{
		"query_timeout":        http.StatusGatewayTimeout,
		"connection_failed":    http.StatusBadGateway,
		"query_failed":         http.StatusBadGateway,
		"invalid_config":       http.StatusBadGateway,
		"operator_not_allowed": http.StatusBadRequest,
		"invalid_filter":       http.StatusBadRequest,
		"filter_too_deep":      http.StatusBadRequest,
		"invalid_cursor":       http.StatusBadRequest,
		"invalid_skip":         http.StatusBadRequest,
		"":                     http.StatusBadRequest,
	}
	for code, want := range cases {
		if got := explorerFindStatus(code); got != want {
			t.Errorf("explorerFindStatus(%q) = %d, want %d", code, got, want)
		}
	}
}
