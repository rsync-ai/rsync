package handlers

// checkConnections turns a chat request into the connection ids the pipeline row
// records. These tests run it against rows shaped like a workspace with more than one
// connection of a type, which is where it either picks, defers to the user, or loses a
// side.

import (
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/rsync-ai/shared/crypto"
)

const (
	ccWorkspace = "77777777-7777-7777-7777-777777777777"
	ccSourceCRM = "a1111111-1111-1111-1111-111111111111"
	ccSourceShp = "a2222222-2222-2222-2222-222222222222"
	ccDest      = "b1111111-1111-1111-1111-111111111111"
)

type ccConn struct {
	id, name, connectorType, database string
}

func ccEncrypt(t *testing.T, database string) string {
	t.Helper()
	enc, err := crypto.EncryptString(`{"database":"` + database + `"}`)
	if err != nil {
		t.Fatalf("encrypt config: %v", err)
	}
	return enc
}

// expectResolution arms the four reads checkConnections makes when the request names
// no connection: the name scan per direction, then the type pick per direction.
func expectResolution(t *testing.T, mock sqlmock.Sqlmock, sources, dests []ccConn) {
	t.Helper()
	nameRows := func(conns []ccConn) *sqlmock.Rows {
		r := sqlmock.NewRows([]string{"id", "name", "alias"})
		for _, c := range conns {
			r.AddRow(c.id, c.name, "")
		}
		return r
	}
	pickRows := func(conns []ccConn) *sqlmock.Rows {
		r := sqlmock.NewRows([]string{"id", "connector_type", "config", "ts"})
		for i, c := range conns {
			r.AddRow(c.id, c.connectorType, ccEncrypt(t, c.database), time.Unix(int64(2000-i), 0))
		}
		return r
	}
	nameScan := `SELECT id, name, COALESCE\(alias, ''\)`
	typePick := `SELECT id, connector_type, config, COALESCE\(updated_at, created_at\) AS ts`
	mock.ExpectQuery(nameScan).WithArgs(ccWorkspace, "source").WillReturnRows(nameRows(sources))
	mock.ExpectQuery(nameScan).WithArgs(ccWorkspace, "destination").WillReturnRows(nameRows(dests))
	mock.ExpectQuery(typePick).WithArgs(ccWorkspace, "source").WillReturnRows(pickRows(sources))
	mock.ExpectQuery(typePick).WithArgs(ccWorkspace, "destination").WillReturnRows(pickRows(dests))
}

func ccSetup(t *testing.T) sqlmock.Sqlmock {
	t.Helper()
	t.Setenv("ENCRYPTION_KEY", "unit-test-encryption-key-0123456789ab")
	mock, cleanup := wsScopeMockDB(t)
	t.Cleanup(cleanup)
	return mock
}

var ccGCS = []ccConn{{id: ccDest, name: "Lake bucket", connectorType: "gcs"}}

// Two sources of one type used to go to the user even when the request named the
// database, because the hint pattern never matched anything.
func TestCheckConnectionsPicksTheSourceWhoseDatabaseTheRequestNames(t *testing.T) {
	mock := ccSetup(t)
	expectResolution(t, mock, []ccConn{
		{id: ccSourceCRM, name: "Mongo one", connectorType: "mongodb", database: "crm"},
		{id: ccSourceShp, name: "Mongo two", connectorType: "mongodb", database: "shop"},
	}, ccGCS)

	src, dst, err := (&ChatHandler{}).checkConnections(ccWorkspace,
		"sync shop.orders from mongodb to google cloud storage", "mongodb", "google-cloud-storage")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if src != ccSourceShp {
		t.Errorf("source = %q, want the connection on database shop (%s)", src, ccSourceShp)
	}
	if dst != ccDest {
		t.Errorf("destination = %q, want %s: a gcs row must satisfy a google-cloud-storage intent", dst, ccDest)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// The hint only decides when it singles one connection out.
func TestCheckConnectionsLeavesTwoSourcesOnTheSameDatabaseToTheUser(t *testing.T) {
	mock := ccSetup(t)
	expectResolution(t, mock, []ccConn{
		{id: ccSourceCRM, name: "Mongo one", connectorType: "mongodb", database: "shop"},
		{id: ccSourceShp, name: "Mongo two", connectorType: "mongodb", database: "shop"},
	}, ccGCS)

	src, dst, err := (&ChatHandler{}).checkConnections(ccWorkspace,
		"sync shop.orders from mongodb to gcs", "mongodb", "gcs")

	if err != nil {
		t.Fatalf("an ambiguous source is a question for the user, not an error: %v", err)
	}
	if src != "" {
		t.Errorf("source = %q, want empty so the workflow asks which connection", src)
	}
	if dst != ccDest {
		t.Errorf("destination = %q, want %s: the ambiguous source must not cost the resolved side", dst, ccDest)
	}
}

// A source that does not exist is reported, and the destination that did resolve is
// still returned for the caller to record.
func TestCheckConnectionsKeepsTheSideThatResolved(t *testing.T) {
	mock := ccSetup(t)
	expectResolution(t, mock, nil, ccGCS)

	src, dst, err := (&ChatHandler{}).checkConnections(ccWorkspace,
		"sync orders from mongodb to gcs", "mongodb", "gcs")

	if err == nil || !strings.Contains(err.Error(), "source connection not found") {
		t.Fatalf("err = %v, want the missing source reported", err)
	}
	if src != "" {
		t.Errorf("source = %q, want empty", src)
	}
	if dst != ccDest {
		t.Errorf("destination = %q, want %s returned alongside the error", dst, ccDest)
	}
}

func TestDatabaseHintFromRequest(t *testing.T) {
	for req, want := range map[string]string{
		"sync shop.orders from mongodb to gcs": "shop",
		"copy public.users to s3":              "public",
		"sync orders from mongodb to gcs":      "",
		"":                                     "",
	} {
		if got := databaseHintFromRequest(req); got != want {
			t.Errorf("databaseHintFromRequest(%q) = %q, want %q", req, got, want)
		}
	}
}
