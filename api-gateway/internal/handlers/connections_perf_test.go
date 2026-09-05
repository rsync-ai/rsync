package handlers

import (
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// Regression guard for the O(N)-round-trip ListConnections bug: resolving OAuth
// token expiry per row issued one `SELECT expires_at FROM oauth_tokens` per
// connection, which took 38–41s for 14 connections at ~3s/query and tipped the
// planner's 3s connections-read budget into timeout. applyTokenExpiryBatch must
// resolve every OAuth connection in a SINGLE query regardless of N.
//
// sqlmock enforces the bound: exactly one batched query is expected, so a revert
// to the per-row form (whose SQL doesn't match the `id = ANY` matcher) leaves the
// expectation unfulfilled and fails the test.
func TestApplyTokenExpiryBatch_SingleQueryForManyConnections(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	const userID = "00000000-0000-0000-0000-000000000001"
	const N = 25

	conns := make([]*Connection, 0, N)
	rows := sqlmock.NewRows([]string{"id", "expires_at"})
	future := time.Now().Add(time.Hour).UTC()
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("tok-%d", i)
		conns = append(conns, &Connection{Config: map[string]interface{}{"oauth_token_id": id}})
		rows.AddRow(id, future)
	}

	// Exactly ONE batched lookup. If the code issued N per-row queries instead,
	// this single expectation would be both unmet (no `id = ANY` query) and the
	// extra calls would be rejected as unexpected.
	mock.ExpectQuery(`SELECT id, expires_at FROM oauth_tokens WHERE id = ANY`).
		WillReturnRows(rows)

	applyTokenExpiryBatch(mockDB, userID, conns)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected exactly one batched oauth_tokens query: %v", err)
	}
	for i, c := range conns {
		if c.TokenExpiresAt == nil {
			t.Fatalf("conn %d: TokenExpiresAt not backfilled from batch result", i)
		}
		if c.IsExpired {
			t.Fatalf("conn %d: future token wrongly marked expired", i)
		}
	}
}

// Correctness of the backfill: an expired token flips IsExpired, a non-OAuth
// connection is never touched (and never contributes to the query), and a
// shared token id fans out to every connection that references it.
func TestApplyTokenExpiryBatch_MarksExpiredAndSkipsNonOAuth(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	past := time.Now().Add(-time.Hour).UTC()
	expiredA := &Connection{Config: map[string]interface{}{"oauth_token_id": "tok-shared"}}
	expiredB := &Connection{Config: map[string]interface{}{"oauth_token_id": "tok-shared"}} // shares the id
	plain := &Connection{Config: map[string]interface{}{"host": "db.example.com"}}          // no token
	nilCfg := &Connection{}

	mock.ExpectQuery(`SELECT id, expires_at FROM oauth_tokens WHERE id = ANY`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "expires_at"}).AddRow("tok-shared", past))

	applyTokenExpiryBatch(mockDB, "u1", []*Connection{expiredA, plain, expiredB, nilCfg})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}
	for name, c := range map[string]*Connection{"expiredA": expiredA, "expiredB": expiredB} {
		if c.TokenExpiresAt == nil || !c.IsExpired {
			t.Fatalf("%s: expected expired with TokenExpiresAt set, got expired=%v ts=%v", name, c.IsExpired, c.TokenExpiresAt)
		}
	}
	if plain.TokenExpiresAt != nil || plain.IsExpired {
		t.Fatalf("non-OAuth connection must be left untouched, got ts=%v expired=%v", plain.TokenExpiresAt, plain.IsExpired)
	}
}

// No OAuth-backed connections in the page → zero queries (the helper returns
// before touching the DB). Asserting via a fresh mock with NO expectations:
// any query the helper issued here would have to match an expectation, so a
// clean ExpectationsWereMet plus untouched fields proves the short-circuit.
func TestApplyTokenExpiryBatch_NoOAuthConnectionsIssuesNoQuery(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	plain := &Connection{Config: map[string]interface{}{"host": "db"}}
	applyTokenExpiryBatch(mockDB, "u1", []*Connection{plain, {}})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}
	if plain.TokenExpiresAt != nil {
		t.Fatalf("non-OAuth connection unexpectedly got expiry set")
	}
}
