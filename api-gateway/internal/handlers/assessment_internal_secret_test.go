package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestFetchSourceReadinessSendsInternalSecret pins the other half of the
// orchestrator assessment gate.
//
// POST /api/v1/pipelines/:id/assess on the orchestrator used to be anonymous, and
// this proxy was the one api-gateway → orchestrator call site that sent no
// X-Internal-Secret — it worked *because* the route was open. Gating the route
// without this header would have taken the Assessment tab from working to 401 on
// every load, so the two changes are one change, and this test is what keeps them
// from being separated again.
//
// The empty-secret case is the control: it proves the assertion can fail for the
// right reason rather than matching whatever the request happens to carry.
func TestFetchSourceReadinessSendsInternalSecret(t *testing.T) {
	const pipelineID = "abd8a64d-0000-0000-0000-000000000000"

	t.Run("secret set is forwarded", func(t *testing.T) {
		const secret = "unit-test-internal-service-secret"
		var got string
		var gotPath string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = r.Header.Get("X-Internal-Secret")
			gotPath = r.URL.Path
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"passed","checks":[]}`))
		}))
		defer srv.Close()

		t.Setenv("ORCHESTRATOR_URL", srv.URL)
		t.Setenv("INTERNAL_SERVICE_SECRET", secret)

		if _, err := fetchSourceReadiness(context.Background(), pipelineID); err != nil {
			t.Fatalf("fetchSourceReadiness: %v", err)
		}
		if gotPath != "/api/v1/pipelines/"+pipelineID+"/assess" {
			t.Fatalf("proxied to %q; want the assess route", gotPath)
		}
		if got != secret {
			t.Fatalf("X-Internal-Secret = %q; want %q — the orchestrator's requirePrincipal "+
				"gate will answer 401 and the Assessment tab will be empty", got, secret)
		}
	})

	// Control: with no secret configured (dev/e2e) the header must be absent, not
	// some empty-string artefact that would make the assertion above vacuous.
	t.Run("no secret configured sends no header", func(t *testing.T) {
		present := true
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, present = r.Header["X-Internal-Secret"]
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"passed","checks":[]}`))
		}))
		defer srv.Close()

		t.Setenv("ORCHESTRATOR_URL", srv.URL)
		t.Setenv("INTERNAL_SERVICE_SECRET", "")

		if _, err := fetchSourceReadiness(context.Background(), pipelineID); err != nil {
			t.Fatalf("fetchSourceReadiness: %v", err)
		}
		if present {
			t.Fatal("X-Internal-Secret was sent with no secret configured")
		}
	})
}
