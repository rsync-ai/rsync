package heal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

// TestBackoffRetry_PostsToInternalEndpointWithSecret is the regression guard for
// KI-HEAL-RERUN-UNAUTH: the healer's automatic re-run must target the
// InternalServiceMiddleware-guarded route and authenticate with X-Internal-Secret,
// not the user-facing /api/v1/pipelines/:id/run (which fail-closes 401 in prod).
func TestBackoffRetry_PostsToInternalEndpointWithSecret(t *testing.T) {
	// Shrink the 5s production backoff so the test runs instantly; restore after.
	orig := backoffRetryDelay
	backoffRetryDelay = 0
	defer func() { backoffRetryDelay = orig }()

	const secret = "test-internal-secret"
	t.Setenv("INTERNAL_SERVICE_SECRET", secret)

	var gotMethod, gotPath, gotSecret, gotSource string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotSecret = r.Header.Get("X-Internal-Secret")
		gotSource = r.Header.Get("X-Internal-Source")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	const pipelineID = "11111111-1111-1111-1111-111111111111"

	// Retry-cap guard: zero prior healer_backoff_retry events → proceed with the retry.
	mock.ExpectQuery("SELECT COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	// writeHealEvent INSERT after a successful re-enqueue.
	mock.ExpectExec("INSERT INTO pipeline_run_events").
		WillReturnResult(sqlmock.NewResult(1, 1))

	exec := &BackoffRetryExecutor{
		DB:            db,
		HTTPClient:    srv.Client(),
		APIGatewayURL: srv.URL,
	}

	if err := exec.Run(context.Background(), diagnose.Signal{
		PipelineID:  pipelineID,
		ExecutionID: "22222222-2222-2222-2222-222222222222",
	}); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method: want POST, got %s", gotMethod)
	}
	wantPath := "/api/v1/internal/pipelines/" + pipelineID + "/run"
	if gotPath != wantPath {
		t.Errorf("path: want %s, got %s", wantPath, gotPath)
	}
	if gotSecret != secret {
		t.Errorf("X-Internal-Secret: want %q, got %q", secret, gotSecret)
	}
	if gotSource != "healer" {
		t.Errorf("X-Internal-Source: want %q, got %q", "healer", gotSource)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestBackoffRetry_OmitsSecretHeaderWhenUnset mirrors refresh_client.go: with no
// INTERNAL_SERVICE_SECRET the header is left off entirely (rather than sent empty),
// so api-gateway's InternalServiceMiddleware returns a visible 401 instead of the
// request silently masquerading as authenticated.
func TestBackoffRetry_OmitsSecretHeaderWhenUnset(t *testing.T) {
	orig := backoffRetryDelay
	backoffRetryDelay = 0
	defer func() { backoffRetryDelay = orig }()

	t.Setenv("INTERNAL_SERVICE_SECRET", "")

	var secretPresent bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, secretPresent = r.Header["X-Internal-Secret"]
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec("INSERT INTO pipeline_run_events").
		WillReturnResult(sqlmock.NewResult(1, 1))

	exec := &BackoffRetryExecutor{
		DB:            db,
		HTTPClient:    srv.Client(),
		APIGatewayURL: srv.URL,
	}

	if err := exec.Run(context.Background(), diagnose.Signal{
		PipelineID: "33333333-3333-3333-3333-333333333333",
	}); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if secretPresent {
		t.Error("X-Internal-Secret header should be absent when INTERNAL_SERVICE_SECRET is unset")
	}
}
