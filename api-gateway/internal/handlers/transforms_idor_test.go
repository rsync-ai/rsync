package handlers

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// Cross-tenant IDOR regression suite for the transform handlers.
//
// transform_definitions has only a pipeline_id FK (no workspace_id column) and
// there is no Postgres RLS, so tenant isolation must be enforced per-query. Every
// transform handler now proves the target pipeline lives in the CALLER'S ACTIVE
// workspace before reading or mutating its rows:
//   - pipeline-keyed handlers → requirePipelineWorkspaceRole(pipelineID, …)
//   - id-keyed handlers       → resolve transform_definitions.pipeline_id, then
//                               requirePipelineWorkspaceRole (join to pipelines)
//
// These tests pin BOTH directions: a cross-workspace request must 404 (never
// reveal existence), and a legitimate member of the pipeline's workspace must
// still be allowed through (the gate isn't over-broad).
//
// Shared helpers come from the same package: wsScopeUser / wsScopeWS /
// wsScopeRouter / wsScopeMockDB (workspace_scoping_test.go) and gateRoleRows
// (workspace_mutation_scoping_test.go). wsScopeMockDB points db.DB at the mock;
// the handler is built from db.GetDB() so h.db and the authz gate share it.

const (
	idorPipelineID  = "33333333-3333-3333-3333-333333333333"
	idorTransformID = "55555555-5555-5555-5555-555555555555"

	// gateQueryRe matches requireResourceRole's pipelines authz join.
	gateQueryRe = `SELECT wm\.role\s+FROM pipelines r`
	// pipelineLookupRe matches requireTransformWorkspaceRole's parent-pipeline resolve.
	pipelineLookupRe = `SELECT pipeline_id::text FROM transform_definitions WHERE id = \$1`
)

func idorMustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// TestTransformIDOR_CrossWorkspaceReturns404 proves every transform handler
// rejects a request whose target pipeline is NOT in the caller's active
// workspace with 404 — before any read or write of transform_definitions.
func TestTransformIDOR_CrossWorkspaceReturns404(t *testing.T) {
	cases := []struct {
		name    string
		method  string
		route   string
		url     string
		body    []byte
		handler func(*TransformHandler) gin.HandlerFunc
		setup   func(m sqlmock.Sqlmock)
	}{
		{
			name:    "GetPipelineTransforms",
			method:  http.MethodGet,
			route:   "/api/v1/transforms/pipeline/:pipeline_id",
			url:     "/api/v1/transforms/pipeline/" + idorPipelineID,
			handler: func(h *TransformHandler) gin.HandlerFunc { return h.GetPipelineTransforms },
			setup: func(m sqlmock.Sqlmock) {
				// Pipeline belongs to another workspace → no membership row.
				m.ExpectQuery(gateQueryRe).
					WithArgs(idorPipelineID, wsScopeUser, wsScopeWS).
					WillReturnError(sql.ErrNoRows)
			},
		},
		{
			name:    "SavePipelineTransforms",
			method:  http.MethodPost,
			route:   "/api/v1/transforms/pipeline/:pipeline_id",
			url:     "/api/v1/transforms/pipeline/" + idorPipelineID,
			body:    idorMustJSON(map[string]any{"transforms": []map[string]any{{"transform_type": "producer"}}}),
			handler: func(h *TransformHandler) gin.HandlerFunc { return h.SavePipelineTransforms },
			setup: func(m sqlmock.Sqlmock) {
				// Gate runs BEFORE any BEGIN/DELETE/INSERT — the write never starts.
				m.ExpectQuery(gateQueryRe).
					WithArgs(idorPipelineID, wsScopeUser, wsScopeWS).
					WillReturnError(sql.ErrNoRows)
			},
		},
		{
			name:    "DeletePipelineTransforms",
			method:  http.MethodDelete,
			route:   "/api/v1/transforms/pipeline/:pipeline_id",
			url:     "/api/v1/transforms/pipeline/" + idorPipelineID,
			handler: func(h *TransformHandler) gin.HandlerFunc { return h.DeletePipelineTransforms },
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gateQueryRe).
					WithArgs(idorPipelineID, wsScopeUser, wsScopeWS).
					WillReturnError(sql.ErrNoRows)
			},
		},
		{
			name:    "UpdateTransform",
			method:  http.MethodPut,
			route:   "/api/v1/transforms/:id",
			url:     "/api/v1/transforms/" + idorTransformID,
			body:    idorMustJSON(map[string]any{"enabled": true}),
			handler: func(h *TransformHandler) gin.HandlerFunc { return h.UpdateTransform },
			setup: func(m sqlmock.Sqlmock) {
				// Transform exists and resolves to a pipeline in ANOTHER workspace…
				m.ExpectQuery(pipelineLookupRe).
					WithArgs(idorTransformID).
					WillReturnRows(sqlmock.NewRows([]string{"pipeline_id"}).AddRow(idorPipelineID))
				// …but the caller is not a member of that workspace → 404, no UPDATE.
				m.ExpectQuery(gateQueryRe).
					WithArgs(idorPipelineID, wsScopeUser, wsScopeWS).
					WillReturnError(sql.ErrNoRows)
			},
		},
		{
			name:    "DeleteTransform",
			method:  http.MethodDelete,
			route:   "/api/v1/transforms/:id",
			url:     "/api/v1/transforms/" + idorTransformID,
			handler: func(h *TransformHandler) gin.HandlerFunc { return h.DeleteTransform },
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(pipelineLookupRe).
					WithArgs(idorTransformID).
					WillReturnRows(sqlmock.NewRows([]string{"pipeline_id"}).AddRow(idorPipelineID))
				m.ExpectQuery(gateQueryRe).
					WithArgs(idorPipelineID, wsScopeUser, wsScopeWS).
					WillReturnError(sql.ErrNoRows)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()
			h := NewTransformHandler(db.GetDB(), "")

			tc.setup(mock)

			r := wsScopeRouter(tc.method, tc.route, tc.handler(h))
			w := httptest.NewRecorder()

			var req *http.Request
			if tc.body != nil {
				req = httptest.NewRequest(tc.method, tc.url, bytes.NewReader(tc.body))
				req.Header.Set("Content-Type", "application/json")
			} else {
				req = httptest.NewRequest(tc.method, tc.url, nil)
			}
			r.ServeHTTP(w, req)

			if w.Code != http.StatusNotFound {
				t.Fatalf("expected 404 (cross-workspace denied), got %d: %s", w.Code, w.Body.String())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("db expectations: %v", err)
			}
		})
	}
}

// --- Happy-path: a legitimate member of the pipeline's workspace is allowed. ---

func TestGetPipelineTransforms_MemberAllowed(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	h := NewTransformHandler(db.GetDB(), "")

	mock.ExpectQuery(gateQueryRe).
		WithArgs(idorPipelineID, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("viewer")) // read requires only >= viewer
	mock.ExpectQuery(`FROM transform_definitions\s+WHERE pipeline_id = \$1`).
		WithArgs(idorPipelineID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "pipeline_id", "transform_type", "transform_order",
			"transform_config", "enabled", "created_at", "updated_at",
		}))

	r := wsScopeRouter(http.MethodGet, "/api/v1/transforms/pipeline/:pipeline_id", h.GetPipelineTransforms)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/transforms/pipeline/"+idorPipelineID, nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 (member reads shared transforms), got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestSavePipelineTransforms_MemberAllowed(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	h := NewTransformHandler(db.GetDB(), "")

	mock.ExpectQuery(gateQueryRe).
		WithArgs(idorPipelineID, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("member"))
	mock.ExpectBegin()
	mock.ExpectExec(`DELETE FROM transform_definitions WHERE pipeline_id = \$1`).
		WithArgs(idorPipelineID).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`INSERT INTO transform_definitions`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	body := idorMustJSON(map[string]any{
		"transforms": []map[string]any{
			{"transform_type": "producer", "transform_config": map[string]any{"operation": "mask", "column": "email"}},
		},
	})
	r := wsScopeRouter(http.MethodPost, "/api/v1/transforms/pipeline/:pipeline_id", h.SavePipelineTransforms)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/transforms/pipeline/"+idorPipelineID, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 (member saves shared transforms), got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// A workspace VIEWER may not overwrite transforms — the mutator gate is >= member.
func TestSavePipelineTransforms_ViewerForbidden(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	h := NewTransformHandler(db.GetDB(), "")

	mock.ExpectQuery(gateQueryRe).
		WithArgs(idorPipelineID, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("viewer"))

	body := idorMustJSON(map[string]any{"transforms": []map[string]any{{"transform_type": "producer"}}})
	r := wsScopeRouter(http.MethodPost, "/api/v1/transforms/pipeline/:pipeline_id", h.SavePipelineTransforms)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/transforms/pipeline/"+idorPipelineID, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 (viewer cannot write transforms), got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestDeletePipelineTransforms_MemberAllowed(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	h := NewTransformHandler(db.GetDB(), "")

	mock.ExpectQuery(gateQueryRe).
		WithArgs(idorPipelineID, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("member"))
	mock.ExpectExec(`DELETE FROM transform_definitions WHERE pipeline_id = \$1`).
		WithArgs(idorPipelineID).
		WillReturnResult(sqlmock.NewResult(0, 3))

	r := wsScopeRouter(http.MethodDelete, "/api/v1/transforms/pipeline/:pipeline_id", h.DeletePipelineTransforms)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/transforms/pipeline/"+idorPipelineID, nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 (member deletes shared transforms), got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestUpdateTransform_MemberAllowed(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	h := NewTransformHandler(db.GetDB(), "")

	mock.ExpectQuery(pipelineLookupRe).
		WithArgs(idorTransformID).
		WillReturnRows(sqlmock.NewRows([]string{"pipeline_id"}).AddRow(idorPipelineID))
	mock.ExpectQuery(gateQueryRe).
		WithArgs(idorPipelineID, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("member"))
	mock.ExpectExec(`UPDATE transform_definitions SET updated_at`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	body := idorMustJSON(map[string]any{"enabled": true})
	r := wsScopeRouter(http.MethodPut, "/api/v1/transforms/:id", h.UpdateTransform)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/api/v1/transforms/"+idorTransformID, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 (member updates transform), got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestDeleteTransform_MemberAllowed(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	h := NewTransformHandler(db.GetDB(), "")

	mock.ExpectQuery(pipelineLookupRe).
		WithArgs(idorTransformID).
		WillReturnRows(sqlmock.NewRows([]string{"pipeline_id"}).AddRow(idorPipelineID))
	mock.ExpectQuery(gateQueryRe).
		WithArgs(idorPipelineID, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("member"))
	mock.ExpectExec(`DELETE FROM transform_definitions WHERE id = \$1`).
		WithArgs(idorTransformID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	r := wsScopeRouter(http.MethodDelete, "/api/v1/transforms/:id", h.DeleteTransform)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/transforms/"+idorTransformID, nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 (member deletes transform), got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// A missing (or orphaned) transform id resolves to no pipeline → 404, and the
// authz gate is never reached (no wm.role query is expected).
func TestUpdateTransform_MissingTransform404(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	h := NewTransformHandler(db.GetDB(), "")

	mock.ExpectQuery(pipelineLookupRe).
		WithArgs(idorTransformID).
		WillReturnError(sql.ErrNoRows)

	body := idorMustJSON(map[string]any{"enabled": true})
	r := wsScopeRouter(http.MethodPut, "/api/v1/transforms/:id", h.UpdateTransform)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/api/v1/transforms/"+idorTransformID, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 (transform not found), got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}
