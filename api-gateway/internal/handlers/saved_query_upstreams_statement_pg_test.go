//go:build integration_pg

// Real-Postgres coverage for statement models as upstream producers. A statement model
// stores no destination; the tables it writes are read out of its sql_text. That makes
// sql_text a new column the model query hands to the resolver, and the half of this
// that can go wrong without a Go test noticing is which ROWS that query returns: a
// private model's SQL names its tables just as plainly as its target_table does.
//
// Uses the fixture and helpers in saved_query_upstreams_pg_test.go; run it the same way:
//
//	UPSTREAM_PG_DSN='postgres://postgres:verify@localhost:55443/cplane?sslmode=disable' \
//	    go test -tags integration_pg ./internal/handlers/ -run PG_Upstream -v

package handlers

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
)

const (
	upStmtShared   = "eeeeeeee-2222-0000-0000-000000000001" // workspace-visible, theirs; MERGEs analytics.shared_load
	upStmtPrivate  = "eeeeeeee-2222-0000-0000-000000000002" // private to them; INSERTs analytics.their_load
	upStmtMine     = "eeeeeeee-2222-0000-0000-000000000003" // private to me; UPDATEs analytics.my_load
	upStmtOtherCn  = "eeeeeeee-2222-0000-0000-000000000004" // writes analytics.shared_load on another connection
	upStmtOtherWS  = "eeeeeeee-2222-0000-0000-000000000005" // writes analytics.shared_load in another workspace
	upStmtLeftover = "eeeeeeee-2222-0000-0000-000000000006" // target_table analytics.leftover; SQL writes analytics.real_out
)

// statementFixture adds statement models to upstreamFixture's rows. Each one the
// resolver must NOT offer writes a table some assertion below reads, beside one it must
// offer, so a missing predicate arrives as an extra candidate rather than a quiet pass.
func statementFixture(t *testing.T) (*sql.DB, upstreamLookup) {
	t.Helper()
	db := upstreamPGDB(t)
	upstreamFixture(t, db)

	mustExec(t, db, `INSERT INTO saved_queries
		(id, workspace_id, connection_id, name, visibility, created_by, materialization, target_table, sql_text)
		VALUES
		($1, $7, $9,  'Shared load',   'workspace', $12, 'statement', NULL,
		    'MERGE INTO analytics.shared_load t USING analytics.orders o ON t.id = o.id WHEN MATCHED THEN DELETE'),
		($2, $7, $9,  'Their load',    'private',   $12, 'statement', NULL,
		    'WITH recent AS (SELECT * FROM analytics.orders) INSERT INTO analytics.their_load SELECT * FROM recent'),
		($3, $7, $9,  'My load',       'private',   $11, 'statement', NULL,
		    'UPDATE analytics.my_load SET total = 0'),
		($4, $7, $10, 'Other conn',    'workspace', $12, 'statement', NULL,
		    'INSERT INTO analytics.shared_load SELECT 1'),
		($5, $8, $9,  'Other ws',      'workspace', $12, 'statement', NULL,
		    'INSERT INTO analytics.shared_load SELECT 1'),
		($6, $7, $9,  'Leftover',      'workspace', $12, 'statement', 'analytics.leftover',
		    'INSERT INTO analytics.real_out SELECT 1')`,
		upStmtShared, upStmtPrivate, upStmtMine, upStmtOtherCn, upStmtOtherWS, upStmtLeftover,
		upWS, upOtherWS, upWarehse, upOtherCn, upMe, upThem)

	return db, lookup("")
}

func TestPG_UpstreamOffersTheStatementModelThatWritesTheTable(t *testing.T) {
	db, in := statementFixture(t)

	// Three statement models write analytics.shared_load: one here, one on another
	// connection, one in another workspace. Only the first writes the table this model
	// reads.
	in.SQLText = "SELECT * FROM analytics.shared_load"
	resp := resolveAs(t, db, in)
	if got := names(resp); len(got) != 1 || got[0] != "Shared load<-analytics.shared_load" {
		t.Fatalf("want only this connection's statement model offered, got %v", got)
	}
	c := resp.Candidates[0]
	if c.Kind != assetKindModel || c.ID != upStmtShared || c.Table != "analytics.shared_load" || !c.Qualified {
		t.Fatalf("candidate is not the model as the picker needs it: %+v", c)
	}

	// A table model and a statement model feeding one query are both offered.
	in.SQLText = "SELECT * FROM analytics.shared_load s JOIN analytics.customer_dim d ON d.id = s.id"
	both := resolveAs(t, db, in)
	want := "Customer dim<-analytics.customer_dim,Shared load<-analytics.shared_load"
	if got := strings.Join(names(both), ","); got != want {
		t.Fatalf("want both kinds of model offered, got %s", got)
	}
}

func TestPG_UpstreamNeverNamesAnotherUsersPrivateStatementModel(t *testing.T) {
	db, in := statementFixture(t)

	in.SQLText = `SELECT * FROM analytics.my_load m JOIN analytics.their_load t ON t.id = m.id`

	// My private statement model is mine to follow. Theirs is theirs: offering it would
	// tell me it exists, what it is called, and which table its SQL writes.
	mine := resolveAs(t, db, in)
	if got := names(mine); len(got) != 1 || got[0] != "My load<-analytics.my_load" {
		t.Fatalf("want only my own private statement model offered, got %v", got)
	}
	if len(mine.Unresolved) != 1 || mine.Unresolved[0] != "analytics.their_load" {
		t.Fatalf("another user's private statement model must leave its table unresolved, got %v", mine.Unresolved)
	}
	body, err := json.Marshal(mine)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, leak := range []string{upStmtPrivate, "Their load"} {
		if strings.Contains(string(body), leak) {
			t.Fatalf("the answer to another member names their private statement model (%s): %s", leak, body)
		}
	}

	// The control: the same query asked by its author finds it. Without this, a model
	// query that returned no statement models at all would pass everything above.
	asThem := in
	asThem.UserID = upThem
	theirs := resolveAs(t, db, asThem)
	if got := names(theirs); len(got) != 1 || got[0] != "Their load<-analytics.their_load" {
		t.Fatalf("the author should see their own private statement model, got %v", got)
	}
}

func TestPG_UpstreamStatementModelIsJudgedByItsSQLNotALeftoverTarget(t *testing.T) {
	db, in := statementFixture(t)

	// The target_table on this row is left from an earlier 'table' life. What the model
	// writes now is what its SQL writes.
	in.SQLText = "SELECT * FROM analytics.leftover"
	if resp := resolveAs(t, db, in); len(resp.Candidates) != 0 {
		t.Fatalf("a statement model was offered for its leftover target_table: %v", names(resp))
	}
	in.SQLText = "SELECT * FROM analytics.real_out"
	if got := names(resolveAs(t, db, in)); len(got) != 1 || got[0] != "Leftover<-analytics.real_out" {
		t.Fatalf("want the statement model offered for the table its SQL writes, got %v", got)
	}
}
