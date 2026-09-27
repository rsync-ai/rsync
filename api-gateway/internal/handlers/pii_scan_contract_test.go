package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"api-gateway/internal/db"
	"api-gateway/internal/kafka"

	"github.com/DATA-DOG/go-sqlmock"
)

// KI-PII-ASYNC-SCAN-ALWAYS-FAILS. The gateway sent bare table names and the
// scanner, given no columns, reported every table as scanned and clean, so
// the prune deleted stored findings. Both messages are now pinned in
// shared/pii_scan_contract_golden.json, which llm-service's
// test_pii_scan_names_only.py asserts from the other end.

type piiScanGolden struct {
	Request  json.RawMessage `json:"request"`
	Response json.RawMessage `json:"response"`
}

func loadPIIScanGolden(t *testing.T) piiScanGolden {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "shared", "pii_scan_contract_golden.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var g piiScanGolden
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	return g
}

// The discovered schema the golden request was built from.
func piiGoldenSchema() []TableMetadata {
	return []TableMetadata{
		{Schema: "public", Name: "customers", Columns: []ColumnMetadata{
			{Name: "id", Type: "integer"},
			{Name: "email", Type: "text"},
			{Name: "email_verified", Type: "boolean"},
			{Name: "customerPAN", Type: "varchar"},
			{Name: "company_name", Type: "text"},
			{Name: "cell_phone", Type: "text"},
		}},
		{Schema: "public", Name: "orders", Columns: []ColumnMetadata{
			{Name: "id", Type: "integer"},
			{Name: "total", Type: "numeric"},
			{Name: "distinct_ids", Type: "text"},
		}},
	}
}

func asJSONValue(t *testing.T, v interface{}) interface{} {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out interface{}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

func TestPIIScanRequestMatchesTheContract(t *testing.T) {
	g := loadPIIScanGolden(t)

	tables := piiScanTablesFor(piiGoldenSchema(), []string{"public.customers", "orders", "public.archived"})
	got := asJSONValue(t, piiScanRequestPayload(
		"5c0e1d2a-0000-4000-8000-000000000001", "5c0e1d2a-0000-4000-8000-0000000000c1", tables, true))

	var want interface{}
	if err := json.Unmarshal(g.Request, &want); err != nil {
		t.Fatalf("decode golden request: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		gb, _ := json.MarshalIndent(got, "", "  ")
		t.Fatalf("pii.scan.request drifted from shared/pii_scan_contract_golden.json:\n%s", gb)
	}
}

// Decoded the way cmd/server/main.go decodes pii.scan.response, then projected.
func TestPIIScanResponseProjectsFromTheContract(t *testing.T) {
	g := loadPIIScanGolden(t)

	var resp kafka.AgentResponse
	if err := json.Unmarshal(g.Response, &resp); err != nil {
		t.Fatalf("decode golden response as AgentResponse: %v", err)
	}
	if resp.Agent != "pii_scanner" || resp.Status != "completed" {
		t.Fatalf("agent/status = %q/%q", resp.Agent, resp.Status)
	}
	if id, _ := resp.Result["scan_id"].(string); id != "5c0e1d2a-0000-4000-8000-000000000001" {
		t.Fatalf("result.scan_id = %v", resp.Result["scan_id"])
	}

	findings, scanned := piiFindingsFrom(resp.Result)

	type f struct {
		table, column, piiType, method, masking string
		confidence                              float64
	}
	var got []f
	for _, x := range findings {
		got = append(got, f{x.table, x.column, x.piiType, x.method, x.masking, x.confidence})
	}
	want := []f{
		{"public.customers", "email", "email", "column_name", "hash", 0.6},
		{"public.customers", "customerPAN", "credit_card", "column_name", "hash", 0.6},
		{"public.customers", "cell_phone", "phone", "column_name", "partial_mask", 0.6},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("findings =\n  %+v\nwant\n  %+v", got, want)
	}

	// public.orders was scanned and is clean, so it prunes. public.archived
	// was not scanned, so it must not.
	if !reflect.DeepEqual(scanned, []string{"public.customers", "public.orders"}) {
		t.Errorf("scanned = %v, want [public.customers public.orders]", scanned)
	}
}

// The gateway's own guard, for a scanner that lists a table it never looked
// at: a table with no columns is not a clean scan.
func TestPIIFindingsFromTableWithNoColumnsIsNotScanned(t *testing.T) {
	for name, raw := range map[string]string{
		"columns empty":   `{"tables":[{"table_name":"public.users","columns":[]}]}`,
		"columns missing": `{"tables":[{"table_name":"public.users"}]}`,
		"only unnamed":    `{"tables":[{"table_name":"public.users","columns":[{"column_name":"","is_pii":false}]}]}`,
	} {
		findings, scanned := piiFindingsFrom(decodeScanPayload(t, raw))
		if len(findings) != 0 || len(scanned) != 0 {
			t.Errorf("%s: want nothing reported, got %d findings / %v scanned", name, len(findings), scanned)
		}
	}
}

func TestPIIScanTablesFor(t *testing.T) {
	schema := []TableMetadata{
		{Schema: "public", Name: "users", Columns: []ColumnMetadata{{Name: "email", Type: "text"}, {Name: " ", Type: "text"}}},
		{Schema: "audit", Name: "users", Columns: []ColumnMetadata{{Name: "id", Type: "bigint"}}},
		{Name: "events", Columns: []ColumnMetadata{{Name: "payload"}}},
		{Schema: "public", Name: "  ", Columns: []ColumnMetadata{{Name: "x"}}},
	}
	names := func(ts []piiScanTable) []string {
		out := []string{}
		for _, x := range ts {
			out = append(out, x.TableName)
		}
		return out
	}

	all := piiScanTablesFor(schema, nil)
	if got := names(all); !reflect.DeepEqual(got, []string{"public.users", "audit.users", "events"}) {
		t.Errorf("no tables named: got %v, want every named table", got)
	}
	if !reflect.DeepEqual(all[0].Columns, []piiScanColumn{{ColumnName: "email", DataType: "text"}}) {
		t.Errorf("a blank column name must be dropped, got %+v", all[0].Columns)
	}
	if all[2].Columns[0].DataType != "" {
		t.Errorf("an undeclared type stays empty, got %q", all[2].Columns[0].DataType)
	}

	// A bare name found in two schemas scans both; naming one of them again,
	// in another case, does not send it twice.
	if got := names(piiScanTablesFor(schema, []string{"USERS", "public.users"})); !reflect.DeepEqual(got, []string{"public.users", "audit.users"}) {
		t.Errorf("bare name: got %v", got)
	}

	// A name the schema does not have is still sent, with no columns, so the
	// scanner reports it rather than the request dropping it.
	missing := piiScanTablesFor(schema, []string{"public.gone", "", "public.gone"})
	if len(missing) != 1 || missing[0].TableName != "public.gone" || missing[0].Columns == nil || len(missing[0].Columns) != 0 {
		t.Errorf("missing table: got %+v", missing)
	}
	if b, _ := json.Marshal(missing[0]); string(b) != `{"table_name":"public.gone","columns":[]}` {
		t.Errorf("missing table must marshal columns as [], got %s", b)
	}

	if got := piiScanTablesFor(nil, nil); len(got) != 0 {
		t.Errorf("empty schema: got %v", got)
	}
}

func stubPIIScanSchema(t *testing.T, tables []TableMetadata, err error) {
	t.Helper()
	prev := loadPIIScanSchema
	loadPIIScanSchema = func(context.Context, *sql.DB, string, string, string) ([]TableMetadata, error) {
		return tables, err
	}
	t.Cleanup(func() { loadPIIScanSchema = prev })
}

// pgx binds the pii_scan_jobs.tables []string as a text[]; sqlmock's default
// converter refuses it, so this mock passes it through.
type piiArrayConverter struct{}

func (piiArrayConverter) ConvertValue(v interface{}) (driver.Value, error) {
	if s, ok := v.([]string); ok {
		return s, nil
	}
	return driver.DefaultParameterConverter.ConvertValue(v)
}

func piiScanMockDB(t *testing.T) sqlmock.Sqlmock {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.ValueConverterOption(piiArrayConverter{}))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	prev := db.DB
	db.DB = mockDB
	t.Cleanup(func() {
		db.DB = prev
		_ = mockDB.Close()
	})
	return mock
}

func postPIIScan(t *testing.T, mock sqlmock.Sqlmock, thenExpect func()) *httptest.ResponseRecorder {
	t.Helper()
	mock.ExpectQuery(`SELECT wm.role\s+FROM connections r`).
		WithArgs("conn-1", wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("member"))
	if thenExpect != nil {
		thenExpect()
	}

	h := newPIIHandlerWithMock()
	r := piiRoleRouter(http.MethodPost, "/api/v1/pii/scan", "member", h.TriggerScan)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pii/scan", bytes.NewBufferString(`{"connection_id":"conn-1"}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

// A scan that cannot read the schema would only report every table as not
// scanned, so it is refused before a job row exists.
func TestTriggerScan_SchemaUnreadableStartsNoJob(t *testing.T) {
	mock := piiScanMockDB(t)
	stubPIIScanSchema(t, nil, errors.New("connection refused"))

	w := postPIIScan(t, mock, nil)

	if w.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502: %s", w.Code, w.Body.String())
	}
	var body map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if body["error"] != "schema_unavailable" {
		t.Errorf("error = %v, want schema_unavailable", body["error"])
	}
	// No INSERT INTO pii_scan_jobs was expected, so sqlmock would have
	// failed the Exec had the handler reached it.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestTriggerScan_NoTablesStartsNoJob(t *testing.T) {
	mock := piiScanMockDB(t)
	stubPIIScanSchema(t, []TableMetadata{}, nil)

	w := postPIIScan(t, mock, nil)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestTriggerScan_ReadableSchemaStartsAJob(t *testing.T) {
	mock := piiScanMockDB(t)
	stubPIIScanSchema(t, piiGoldenSchema(), nil)
	w := postPIIScan(t, mock, func() {
		mock.ExpectExec(`INSERT INTO pii_scan_jobs`).WillReturnResult(sqlmock.NewResult(0, 1))
	})

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}
