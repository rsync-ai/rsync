package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

const maskedColumnsQuery = `FROM transform_definitions td\s+JOIN pipelines p ON p.id = td.pipeline_id\s+WHERE p.workspace_id = \$1`

func getMaskedColumns(t *testing.T, role string) (*httptest.ResponseRecorder, map[string]json.RawMessage) {
	t.Helper()
	h := newPIIHandlerWithMock()
	r := piiRoleRouter(http.MethodGet, "/api/v1/pii/masked-columns", role, h.ListMaskedColumns)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/pii/masked-columns", nil))
	var body map[string]json.RawMessage
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	return w, body
}

// transform_definitions has no workspace_id, so the read MUST reach it through
// the pipeline's workspace, bound to the ACTIVE workspace.
func TestListMaskedColumns_ScopedToActiveWorkspace(t *testing.T) {
	mock := piiScanMockDB(t)
	mock.ExpectQuery(maskedColumnsQuery).
		WithArgs(wsScopeWS, maxPIIMaskedTransformRows+1).
		WillReturnRows(sqlmock.NewRows([]string{"pipeline_id", "name", "transform_config", "enabled"}).
			AddRow("p-1", "orders to lake", []byte(`{"operation":"mask","column":"email"}`), true).
			AddRow("p-1", "orders to lake", []byte(`{"operation":"filter","condition":"id > 0"}`), true))

	w, body := getMaskedColumns(t, "viewer")

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var cols []PIIMaskedColumn
	if err := json.Unmarshal(body["columns"], &cols); err != nil {
		t.Fatalf("decode columns: %v (%s)", err, w.Body.String())
	}
	want := []PIIMaskedColumn{{PipelineID: "p-1", PipelineName: "orders to lake", Column: "email", Action: "hash"}}
	if !reflect.DeepEqual(cols, want) {
		t.Errorf("columns = %+v, want %+v", cols, want)
	}
	if string(body["truncated"]) != "false" {
		t.Errorf("truncated = %s, want false", body["truncated"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

// No role in the active workspace reads nothing, and never reaches the query.
func TestListMaskedColumns_NoWorkspaceRoleIs403(t *testing.T) {
	mock := piiScanMockDB(t)

	w, _ := getMaskedColumns(t, "")

	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

// A failed read is an error, never an empty list: the page counts these rows
// in its headline, and an empty list would read as "nothing is masked".
func TestListMaskedColumns_QueryErrorIs500(t *testing.T) {
	mock := piiScanMockDB(t)
	mock.ExpectQuery(maskedColumnsQuery).WillReturnError(http.ErrHandlerTimeout)

	w, _ := getMaskedColumns(t, "viewer")

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500: %s", w.Code, w.Body.String())
	}
}

func TestListMaskedColumns_ReportsTruncation(t *testing.T) {
	mock := piiScanMockDB(t)
	rows := sqlmock.NewRows([]string{"pipeline_id", "name", "transform_config", "enabled"})
	for i := 0; i <= maxPIIMaskedTransformRows; i++ {
		rows.AddRow("p-1", "p", []byte(`{"operation":"mask","column":"email"}`), true)
	}
	mock.ExpectQuery(maskedColumnsQuery).WillReturnRows(rows)

	w, body := getMaskedColumns(t, "viewer")

	if w.Code != http.StatusOK || string(body["truncated"]) != "true" {
		t.Fatalf("status = %d, truncated = %s, want 200/true", w.Code, body["truncated"])
	}
}

func TestPIIMaskedColumnsFrom(t *testing.T) {
	row := func(pipeline, cfg string, enabled bool) piiStoredTransform {
		return piiStoredTransform{PipelineID: pipeline, PipelineName: "name-" + pipeline, Config: []byte(cfg), Enabled: enabled}
	}
	col := func(pipeline, table, column, action string) PIIMaskedColumn {
		return PIIMaskedColumn{PipelineID: pipeline, PipelineName: "name-" + pipeline, Table: table, Column: column, Action: action}
	}

	cases := []struct {
		name string
		in   []piiStoredTransform
		want []PIIMaskedColumn
	}{
		{"mask with no mask_type hashes", []piiStoredTransform{row("a", `{"operation":"mask","column":"email"}`, true)},
			[]PIIMaskedColumn{col("a", "", "email", "hash")}},
		{"builder hash card: field alias, implied mask_type", []piiStoredTransform{row("a", `{"operation":"hash","field":"ssn"}`, true)},
			[]PIIMaskedColumn{col("a", "", "ssn", "hash")}},
		{"columns list, explicit mask_type, table scope", []piiStoredTransform{
			row("a", `{"operation":"mask_pii","columns":["users.email","phone"],"mask_type":"redact","table":"public.users"}`, true)},
			[]PIIMaskedColumn{col("a", "public.users", "email", "redact"), col("a", "public.users", "phone", "redact")}},
		{"nested path target", []piiStoredTransform{row("a", `{"operation":"mask_pii","path":"contact.email","mask_type":"partial"}`, true)},
			[]PIIMaskedColumn{col("a", "", "contact.email", "partial")}},
		{"disabled rule masks nothing", []piiStoredTransform{row("a", `{"operation":"mask","column":"email"}`, false)},
			[]PIIMaskedColumn{}},
		{"not a mask", []piiStoredTransform{row("a", `{"operation":"filter","condition":"id > 0"}`, true)},
			[]PIIMaskedColumn{}},
		{"a mask the engine refuses is not listed", []piiStoredTransform{
			row("a", `{"operation":"mask"}`, true),
			row("a", `{"operation":"mask","column":"email","deep":"yes"}`, true),
			row("a", `not json`, true),
			row("a", ``, true),
			row("a", `{}`, true)},
			[]PIIMaskedColumn{}},
		{"same column twice in one pipeline is listed once; another pipeline still counts", []piiStoredTransform{
			row("a", `{"operation":"mask","column":"email"}`, true),
			row("a", `{"operation":"hash","column":"EMAIL"}`, true),
			row("b", `{"operation":"mask","column":"email"}`, true)},
			[]PIIMaskedColumn{col("a", "", "email", "hash"), col("b", "", "email", "hash")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := piiMaskedColumnsFrom(tc.in); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got  %+v\nwant %+v", got, tc.want)
			}
		})
	}
}
