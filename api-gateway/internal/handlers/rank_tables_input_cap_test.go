package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/rsync-ai/shared/crypto"
)

// Source discovery now returns up to 5000 tables. Sent whole, they made a
// ~200k-token rank-tables prompt that failed every call; the ranker gets a
// shortlist instead.

func wideTables(n, colsPer int) []TableMetadata {
	tables := make([]TableMetadata, n)
	for i := range tables {
		cols := make([]ColumnMetadata, colsPer)
		for j := range cols {
			cols[j] = ColumnMetadata{Name: fmt.Sprintf("c%d", j), Type: "text"}
		}
		tables[i] = TableMetadata{Name: fmt.Sprintf("t%04d", i), Schema: "public", RowCount: int64(i), Columns: cols}
	}
	return tables
}

func TestShortlistForRanking(t *testing.T) {
	tables := wideTables(1000, 1)
	// A small table the intent names must survive the cut; the rest go by size.
	tables[3] = TableMetadata{Name: "customer_orders", Schema: "sales", RowCount: 2}
	got := shortlistForRanking(tables, "Sync Orders to the lake", 5)
	names := []string{}
	for _, tb := range got {
		names = append(names, tb.Name)
	}
	want := []string{"customer_orders", "t0999", "t0998", "t0997", "t0996"}
	if fmt.Sprint(names) != fmt.Sprint(want) {
		t.Fatalf("shortlist = %v, want %v", names, want)
	}

	// Control: a source under the cap is passed through untouched.
	small := wideTables(4, 1)
	if got := shortlistForRanking(small, "orders", 5); len(got) != 4 || got[0].Name != "t0000" {
		t.Fatalf("under the cap: got %d tables starting %q, want the 4 in order", len(got), got[0].Name)
	}
}

func TestRankTablesByLLM_CapsTablesAndColumns(t *testing.T) {
	var gotTables []struct {
		Name    string            `json:"name"`
		Columns []json.RawMessage `json:"columns"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Tables []struct {
				Name    string            `json:"name"`
				Columns []json.RawMessage `json:"columns"`
			} `json:"tables"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode rank request: %v", err)
		}
		gotTables = body.Tables
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()
	t.Setenv("LLM_SERVICE_URL", srv.URL)

	send := func(tables []TableMetadata) {
		t.Helper()
		gotTables = nil
		if _, err := rankTablesByLLM(context.Background(), tables, "orders", "replication", 10); err != nil {
			t.Fatalf("rankTablesByLLM: %v", err)
		}
	}

	send(wideTables(5000, 40))
	if len(gotTables) != rankInputCap {
		t.Fatalf("5000-table source: ranker got %d tables, want %d", len(gotTables), rankInputCap)
	}
	for _, tb := range gotTables {
		if len(tb.Columns) != rankColumnsPerTable {
			t.Fatalf("table %s sent with %d columns, want %d", tb.Name, len(tb.Columns), rankColumnsPerTable)
		}
	}

	// Control: a small source goes whole, with every column it has.
	send(wideTables(20, 3))
	if len(gotTables) != 20 || len(gotTables[0].Columns) != 3 {
		t.Fatalf("20-table source: ranker got %d tables / %d columns, want 20 / 3", len(gotTables), len(gotTables[0].Columns))
	}
}

// The picker loads a large source in one /metadata page; the old 200 cap cut
// a 300-table source to 200 with no way to reach the rest from the picker.
func TestGetConnectionMetadata_LimitAllowsAFullDiscoveryPage(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	t.Setenv("ENCRYPTION_KEY", "unit-test-encryption-key-0123456789ab")
	enc, err := crypto.EncryptString(`{"host":"db.internal","database":"appdb"}`)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	tables := wideTables(300, 1)
	envelope, _ := json.Marshal(map[string]interface{}{
		"tables": tables, "total_tables_available": 300, "total_tables_discovered": 300,
	})
	docFindOrchestrator(t, "/api/v1/agent/discover-schema", http.StatusOK, string(envelope))

	get := func(query string) map[string]interface{} {
		t.Helper()
		mock.ExpectQuery(`SELECT 1 FROM connections WHERE id = \$1 AND workspace_id = \$2`).
			WithArgs(docFindConnID, wsScopeWS).
			WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
		mock.ExpectQuery(`SELECT connector_type, type, config\s+FROM connections\s+WHERE id = \$1 AND workspace_id = \$2`).
			WithArgs(docFindConnID, wsScopeWS).
			WillReturnRows(sqlmock.NewRows([]string{"connector_type", "type", "config"}).AddRow("postgresql", "source", enc))
		r := wsScopeRouter(http.MethodGet, "/api/v1/connections/:id/metadata", GetConnectionMetadata)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/connections/"+docFindConnID+"/metadata"+query, nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
		}
		var body map[string]interface{}
		if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return body
	}

	full := get("?limit=5000&include_columns=false")
	if got := len(full["tables"].([]interface{})); got != 300 || full["limit"] != float64(maxMetadataPageSize) {
		t.Fatalf("limit=5000: %d tables, limit %v; want 300, %d", got, full["limit"], maxMetadataPageSize)
	}
	if full["total"] != float64(300) || full["total_tables_available"] != float64(300) {
		t.Fatalf("totals = %v / %v, want 300 / 300", full["total"], full["total_tables_available"])
	}

	// Control: the default page is still 25.
	if got := len(get("")["tables"].([]interface{})); got != 25 {
		t.Fatalf("default page: %d tables, want 25", got)
	}
}
