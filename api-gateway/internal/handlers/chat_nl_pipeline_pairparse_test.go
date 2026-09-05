package handlers

import (
	"testing"

	"api-gateway/internal/db"
)

// TestQuickParseDataSyncIntent_ConnectionNamesBetweenTypes covers OBS-2: a
// connection NAME sitting between the connector type and the "to" preposition
// (e.g. "postgresql test-pg-src-batch to postgresql test-pg-dst") used to defeat
// the adjacency-based reToPair regex, dropping the message onto the single-
// connector path — which for a SAME-type pair collapsed both endpoints to one
// connector and re-asked for the destination the user already gave. The token-
// stream fallback must now recover the pair, while single-connector phrasings
// must still return nil so slot-filling can ask for the missing side.
func TestQuickParseDataSyncIntent_ConnectionNamesBetweenTypes(t *testing.T) {
	// Pin the global DB to nil so isKnownConnector uses its deterministic
	// common-connector fallback rather than a leaked (closed) mock DB from a
	// sibling test, which would flip it into its fail-open branch and accept any
	// word as a connector. Production always has a healthy catalog DB.
	prev := db.DB
	db.DB = nil
	t.Cleanup(func() { db.DB = prev })

	h := &ChatHandler{}

	pair := []struct {
		msg     string
		wantSrc string
		wantDst string
	}{
		// Same-type pair with interposed connection names — the OBS-2 regression.
		{"sync postgresql test-pg-src-batch to postgresql test-pg-dst, mask all pii", "postgresql", "postgresql"},
		// Different-type pair with interposed connection names.
		{"sync postgresql test-pg-src-batch to aws-s3 test-s3-dst", "postgresql", "aws-s3"},
		// Pure types (must keep working via the fast regex path).
		{"sync postgresql to postgresql", "postgresql", "postgresql"},
		{"sync postgresql to aws-s3", "postgresql", "aws-s3"},
		// A noun ("tables") between the source type and "to" must not break it.
		{"sync mysql tables to postgresql", "mysql", "postgresql"},
	}
	for _, tc := range pair {
		got := h.quickParseDataSyncIntent(tc.msg)
		if got == nil {
			t.Errorf("expected pair %s→%s, got nil for %q", tc.wantSrc, tc.wantDst, tc.msg)
			continue
		}
		if got.SourceType != tc.wantSrc || got.DestinationType != tc.wantDst {
			t.Errorf("for %q: got %s→%s, want %s→%s", tc.msg, got.SourceType, got.DestinationType, tc.wantSrc, tc.wantDst)
		}
	}

	// Negative: single-connector phrasings and turn-2 slot replies must NOT be
	// misread as a pair (no known connector on both sides of the pivot).
	noPair := []string{
		"load data into postgresql",
		"sync postgresql",
		"send it to postgresql test-pg-dst",
		"export all tables to a csv file",
	}
	for _, msg := range noPair {
		if got := h.quickParseDataSyncIntent(msg); got != nil {
			t.Errorf("expected nil (no pair) for %q, got %s→%s", msg, got.SourceType, got.DestinationType)
		}
	}
}
